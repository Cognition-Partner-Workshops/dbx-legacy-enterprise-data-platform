"""Thin adapter over the workstream-5 rule library ``sales_lakehouse.silver.rules``.

Gold codes against the agreed signatures::

    applyTax(df, regionCol=...)
    applyFx(df, spark, cfg, amountCols, currencyCol, dateCol, regionCol)
    resolveFiscalPeriod(df, spark, cfg, dateCol, regionCol)

The silver rule modules are the only implementation of the regional tax / FX /
fiscal behaviour. This module only translates between the gold fact column
contract (``tax_amount`` / ``total_excluding_tax`` / ``total_including_tax`` /
``<amount>_reporting``) and the rule library's (``*_amount_local`` /
``<amount>_usd``), and derives the legacy Fact.Sale tax descriptors
(``tax_rate``, ``vat_rate``, ``gst_rate``, ``vat_reverse_charge_flag``,
``gst_free_flag``, ``tax_regime_code``) from the rule outputs.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.silver.rules import fiscal, fx, tax

FX_DEFAULT_SOURCE = fx.SOURCE_DEFAULT_1
FX_SAME_CURRENCY_SOURCE = fx.SOURCE_SAME_CCY
FISCAL_CALENDAR_BY_REGION: dict[str, str] = dict(fiscal.REGION_CALENDARS)

MONEY = "decimal(19,4)"
RATE = "decimal(19,8)"
_REVERSE_CHARGE_COLUMNS = ("is_reverse_charge", "vat_reverse_charge_flag")


def _firstPresent(df: DataFrame, candidates: Sequence[str], dataType: str):
    for name in candidates:
        if name in df.columns:
            return F.col(name).cast(dataType)
    return F.lit(None).cast(dataType)


def applyTax(
    df: DataFrame,
    regionCol: str = "region_code",
    netCol: str = "net_amount",
    rateCol: str = "tax_rate_percent",
    vatRegCol: str = "vat_registration_number",
) -> DataFrame:
    """Regional tax on the staged line value ``netCol`` (``rateCol`` is a percent).

    NA / EU: tax is added on top of the net. APAC: the staged value is the
    GST-inclusive price and the tax is backed out of it (``silver.rules.tax``).
    Adds ``tax_amount``, ``total_excluding_tax``, ``total_including_tax``,
    ``tax_treatment_code``, ``tax_residual_local`` and the legacy descriptors.
    """
    region = F.upper(F.trim(F.col(regionCol)))
    isApac = region == "APAC"
    isEu = region == "EU"
    net = F.col(netCol).cast(MONEY)
    ratePercent = F.col(rateCol).cast("decimal(18,3)") if rateCol in df.columns else F.lit(None).cast("decimal(18,3)")
    reverse = F.coalesce(_firstPresent(df, _REVERSE_CHARGE_COLUMNS, "boolean"), F.lit(False))
    prepared = (
        df.withColumn("_tax_net", F.when(isApac, F.lit(None).cast(MONEY)).otherwise(net))
        .withColumn("_tax_gross", F.when(isApac, net).otherwise(F.lit(None).cast(MONEY)))
        .withColumn("_tax_rate", (ratePercent / F.lit(100)).cast(RATE))
        .withColumn("_tax_reverse", reverse)
    )
    out = tax.applyTax(
        prepared,
        regionCol=regionCol,
        grossCol="_tax_gross",
        netCol="_tax_net",
        taxRateCol="_tax_rate",
        isReverseChargeCol="_tax_reverse",
        vatRegCol=vatRegCol,
    )
    rateOut = F.coalesce(ratePercent, F.lit(0)).cast("decimal(18,3)")
    existingRegime = F.col("tax_regime_code") if "tax_regime_code" in df.columns else F.lit(None).cast("string")
    return (
        out.withColumn("tax_amount", F.col("tax_amount_local"))
        .withColumn("total_excluding_tax", F.col("net_amount_local"))
        .withColumn("total_including_tax", F.col("gross_amount_local"))
        .withColumn("tax_rate", rateOut)
        .withColumn("vat_rate", F.when(isEu, rateOut).cast("decimal(18,3)"))
        .withColumn("gst_rate", F.when(isApac, rateOut).cast("decimal(18,3)"))
        .withColumn("vat_reverse_charge_flag", isEu & F.col("_tax_reverse"))
        .withColumn("gst_free_flag", isApac & (rateOut == 0))
        .withColumn(
            "tax_regime_code",
            F.coalesce(
                existingRegime,
                F.when(isEu, F.lit("VAT")).when(isApac, F.lit("GST")).otherwise(F.lit("SALESTAX")),
            ),
        )
        .drop("_tax_net", "_tax_gross", "_tax_rate", "_tax_reverse")
    )


def applyFx(
    df: DataFrame,
    spark: SparkSession,
    cfg: PipelineConfig,
    amountCols: Sequence[str],
    currencyCol: str,
    dateCol: str,
    regionCol: str,
) -> DataFrame:
    """``silver.rules.fx.applyFx`` with each ``<amount>_<reporting ccy>`` exposed as ``<amount>_reporting``."""
    out = fx.applyFx(
        df,
        spark,
        cfg,
        amountCols=list(amountCols),
        currencyCol=currencyCol,
        dateCol=dateCol,
        regionCol=regionCol,
    )
    suffix = f"_{cfg.reportingCurrency.lower()}"
    for c in amountCols:
        out = out.withColumnRenamed(f"{c}{suffix}", f"{c}_reporting")
    return out


def resolveFiscalPeriod(
    df: DataFrame, spark: SparkSession, cfg: PipelineConfig, dateCol: str, regionCol: str
) -> DataFrame:
    """``silver.rules.fiscal.resolveFiscalPeriod``; an unresolved period is the ``-1`` unknown member."""
    out = fiscal.resolveFiscalPeriod(df, spark, cfg, dateCol=dateCol, regionCol=regionCol)
    return (
        out.withColumn("fiscal_year", F.col("fiscal_year").cast("smallint"))
        .withColumn("fiscal_period", F.col("fiscal_period").cast("smallint"))
        .withColumn("fiscal_period_key", F.coalesce(F.col("fiscal_period_key"), F.lit(-1)).cast("int"))
    )
