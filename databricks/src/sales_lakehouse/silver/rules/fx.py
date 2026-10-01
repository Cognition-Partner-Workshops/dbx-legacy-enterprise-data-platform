"""FX rules: local transaction currency -> reporting currency (USD).

Spec (docs/domain-model/business-domains.md, "Currency and FX"):

    Transactions are in local currency; the warehouse reports in a single
    currency. `[Transaction Currency Code]`, `[Currency Key]`, `[FX Rate To
    Reporting]`, `[FX Rate Effective Date]` and `[FX Rate Source Code]` on
    the facts carry the conversion. Rates come from `WWI_REF.FX_RATE_DAILY`
    via `PKG_FX`.

    The three regions do not use the same rate source or the same
    effective-date convention - some rows convert at transaction date, some
    at period-end - and `[FX Rate Source Code]` is the only record of which.
    Where no rate is found the loads default the rate to 1 rather than
    failing, so a missing rate produces a wrong number rather than a null.

Effective-date conventions, from ``stg.usp_ConvertCurrencyAmounts`` header
(sqlserver/staging/procedures) and the SSIS fact packages:

    NA   SPOT on the transaction date, falling back up to @MaxFallbackDays
         (default 7) calendar days to the most recent prior rate.
    EU   rate on the invoice date (``FACT_EU_Load_Sale`` "Lookup Effective FX
         Rate"), else the most recent prior day with no window ("Retry Held
         Lines With Prior Day Rate": ``RateDate < FxRateDate ORDER BY RateDate
         DESC``). ``usp_ConvertCurrencyAmounts`` additionally uses PERIOD_END
         for closed GL periods; the lakehouse has no period-status feed, so
         that branch is not reproduced (see README "Open questions").
    APAC CORPORATE monthly rate: the rate effective on the first of the month
         is used for the whole month, "which is why AppliedRateDate is often
         weeks before RequestedRateDate". ``PKG_FX.backoff_days`` allows 7
         days of carry-forward for CORP rates when the 1st has no row
         (the APAC feed has no weekend rows).

Same-currency rows are copied at 1.0 (``usp_ConvertCurrencyAmounts`` /
``PKG_FX.get_rate``). Money is multiplied and rounded half-up to the cent as
``ROUND(w.[Net Amount] * w.[Fx Rate], 2)`` in ``Integration.usp_LoadFactSale``.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver.rules.dq import tagDq

MONEY = DecimalType(19, 4)
RATE = DecimalType(19, 8)

REF_FX_RATE = "ref_fx_rate"

SOURCE_SAME_CCY = "SAME_CCY"
SOURCE_DEFAULT_1 = "DEFAULT_1"

NA_FALLBACK_DAYS = 7  # @MaxFallbackDays default, stg.usp_ConvertCurrencyAmounts
APAC_FALLBACK_DAYS = 7  # PKG_FX.backoff_days('CORP')

OUTPUT_COLUMNS: tuple[str, ...] = ("fx_rate_to_reporting", "fx_rate_source_code", "fx_rate_effective_date")

_KEY_CCY = "_fx_key_ccy"
_KEY_REGION = "_fx_key_region"
_KEY_ANCHOR = "_fx_key_anchor"


def _rateTypePreference(regionCol: Column, rateTypeCol: Column) -> Column:
    """Lower is better. Which rate *type* each regional load reads."""
    rateType = F.upper(F.coalesce(rateTypeCol, F.lit("")))
    na = F.when(rateType == "SPOT", 0).when(rateType == "CLOSE", 1).when(rateType == "CORP", 2).otherwise(9)
    eu = F.when(rateType == "ECB", 0).when(rateType == "CLOSE", 1).when(rateType == "SPOT", 2).when(rateType == "MEND", 3).otherwise(9)
    apac = F.when(rateType == "CORP", 0).when(rateType == "BANK", 1).when(rateType == "SPOT", 2).otherwise(9)
    return F.when(regionCol == "NA", na).when(regionCol == "EU", eu).when(regionCol == "APAC", apac).otherwise(F.lit(9))


def _anchorDate(regionCol: Column, txnDate: Column) -> Column:
    # LEGACY QUIRK: APAC converts the whole month at the rate effective on the
    # first of the month (usp_ConvertCurrencyAmounts "CORPORATE monthly rate").
    return F.when(regionCol == "APAC", F.trunc(txnDate, "MM")).otherwise(txnDate)


def _lookbackDays(regionCol: Column) -> Column:
    """NULL means unbounded (EU prior-day retry has no window)."""
    return (
        F.when(regionCol == "NA", F.lit(NA_FALLBACK_DAYS))
        .when(regionCol == "APAC", F.lit(APAC_FALLBACK_DAYS))
        .otherwise(F.lit(None).cast("int"))
    )


def applyFx(
    df: DataFrame,
    spark: SparkSession,
    cfg: PipelineConfig,
    amountCols: list[str],
    currencyCol: str = "currency_code",
    dateCol: str = "transaction_date",
    regionCol: str = "region_code",
) -> DataFrame:
    """Add ``fx_rate_to_reporting`` (decimal(19,8)), ``fx_rate_source_code``,
    ``fx_rate_effective_date`` and ``<col>_usd`` (decimal(19,4)) per amount.

    Reads ``silver.ref_fx_rate`` through ``cfg.fqn``; no other I/O. Every
    input row is returned exactly once.
    """
    reporting = cfg.reportingCurrency.upper()
    suffix = f"_{reporting.lower()}"
    derived = [f"{c}{suffix}" for c in amountCols]
    base = df.drop(*[c for c in (*OUTPUT_COLUMNS, *derived) if c in df.columns])

    region = F.upper(F.trim(F.col(regionCol).cast(StringType())))
    currency = F.upper(F.trim(F.col(currencyCol).cast(StringType())))
    txnDate = F.col(dateCol).cast("date")

    # The rate depends only on (currency, region, anchor date): resolve it once per distinct
    # key and join back, so the result does not rely on a synthetic row id that a re-evaluated
    # plan (no caching on serverless) could assign differently.
    base = (
        base.withColumn(_KEY_CCY, currency)
        .withColumn(_KEY_REGION, region)
        .withColumn(_KEY_ANCHOR, _anchorDate(F.col(_KEY_REGION), txnDate))
    )
    keys = (
        base.select(_KEY_CCY, _KEY_REGION, _KEY_ANCHOR)
        .filter(F.col(_KEY_CCY).isNotNull() & (F.col(_KEY_CCY) != reporting) & F.col(_KEY_ANCHOR).isNotNull())
        .distinct()
        .withColumn("_lookback", _lookbackDays(F.col(_KEY_REGION)))
    )

    rates = spark.table(cfg.fqn("silver", REF_FX_RATE)).select(
        F.upper(F.col("currency_code")).alias("_r_ccy"),
        F.col("rate_date").cast("date").alias("_r_rate_date"),
        F.col("effective_date").cast("date").alias("_r_effective_date"),
        F.col("rate_to_usd").cast(RATE).alias("_r_rate"),
        F.col("rate_source_code").alias("_r_source"),
        F.col("region_code").alias("_r_region"),
        F.col("rate_type_code").alias("_r_type"),
    )
    candidates = keys.join(
        rates,
        (F.col(_KEY_CCY) == F.col("_r_ccy"))
        & (F.col("_r_rate_date") <= F.col(_KEY_ANCHOR))
        & (F.col("_lookback").isNull() | (F.col("_r_rate_date") >= F.date_sub(F.col(_KEY_ANCHOR), F.col("_lookback")))),
        "inner",
    )
    ranking = Window.partitionBy(_KEY_CCY, _KEY_REGION, _KEY_ANCHOR).orderBy(
        F.col("_r_rate_date").desc(),
        F.when(F.col("_r_region") == F.col(_KEY_REGION), 0).otherwise(1),
        _rateTypePreference(F.col(_KEY_REGION), F.col("_r_type")),
        F.col("_r_source"),
    )
    chosen = (
        candidates.withColumn("_rn", F.row_number().over(ranking))
        .filter(F.col("_rn") == 1)
        .select(
            F.col(_KEY_CCY).alias("_c_ccy"),
            F.col(_KEY_REGION).alias("_c_region"),
            F.col(_KEY_ANCHOR).alias("_c_anchor"),
            "_r_rate",
            "_r_source",
            "_r_effective_date",
            "_r_rate_date",
        )
    )

    joined = base.join(
        chosen,
        (F.col(_KEY_CCY) == F.col("_c_ccy"))
        & F.col(_KEY_REGION).eqNullSafe(F.col("_c_region"))
        & (F.col(_KEY_ANCHOR) == F.col("_c_anchor")),
        "left",
    ).drop("_c_ccy", "_c_region", "_c_anchor")
    sameCurrency = F.coalesce(currency == reporting, F.lit(False))
    # LEGACY QUIRK: a missing rate defaults to 1.0 and is tagged, never NULL
    # and never rejected (usp_LoadFactSale "SET [Fx Rate] = 1.0 ... WHERE [Fx
    # Rate] IS NULL"; business-domains.md "default the rate to 1 rather than
    # failing"). The amount is therefore wrong, not absent.
    missing = ~sameCurrency & F.col("_r_rate").isNull()
    rateOut = (
        F.when(sameCurrency, F.lit(1))
        .when(missing, F.lit(1))
        .otherwise(F.col("_r_rate"))
        .cast(RATE)
    )
    sourceOut = (
        F.when(sameCurrency, F.lit(SOURCE_SAME_CCY))
        .when(missing, F.lit(SOURCE_DEFAULT_1))
        .otherwise(F.col("_r_source"))
    )
    effectiveOut = F.when(sameCurrency | missing, txnDate).otherwise(
        F.coalesce(F.col("_r_effective_date"), F.col("_r_rate_date"))
    )
    out = (
        joined.withColumn("fx_rate_to_reporting", rateOut)
        .withColumn("fx_rate_source_code", sourceOut)
        .withColumn("fx_rate_effective_date", effectiveOut)
    )
    for col, target in zip(amountCols, derived, strict=True):
        out = out.withColumn(target, F.round(F.col(col).cast(MONEY) * F.col("fx_rate_to_reporting"), 2).cast(MONEY))
    out = tagDq(out, F.col("fx_rate_source_code") == SOURCE_DEFAULT_1, "WARN", "FX_RATE_DEFAULTED")
    return out.drop(_KEY_CCY, _KEY_REGION, _KEY_ANCHOR, "_r_rate", "_r_source", "_r_effective_date", "_r_rate_date")
