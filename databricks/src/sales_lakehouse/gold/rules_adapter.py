"""Thin adapter over the workstream-5 rule library ``sales_lakehouse.silver.rules``.

Gold codes against the agreed signatures::

    applyTax(df, regionCol=...)
    applyFx(df, spark, cfg, amountCols, currencyCol, dateCol, regionCol)
    resolveFiscalPeriod(df, spark, cfg, dateCol, regionCol)

If the silver rule modules are not on the branch yet, a minimal in-package
fallback is used (FX lookup on ``ref_fx_rate`` with the legacy 1.0 default,
calendar lookup on ``dim_fiscal_calendar``, regional tax formulas lifted from
the SSIS FACT_<region>_Load_Sale derived columns). The integration session
removes the fallback once workstream 5 lands.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import tableExists

FX_DEFAULT_SOURCE = "DEFAULT_1"
FX_SAME_CURRENCY_SOURCE = "SAME_CCY"
FISCAL_CALENDAR_BY_REGION: dict[str, str] = {"NA": "NA445", "EU": "EUCAL", "APAC": "APACJUN"}

TaxFn = Callable[..., DataFrame]
FxFn = Callable[..., DataFrame]
FiscalFn = Callable[..., DataFrame]


def _importRule(module: str, name: str) -> Callable[..., DataFrame] | None:
    try:
        mod = importlib.import_module(f"sales_lakehouse.silver.rules.{module}")
    except ModuleNotFoundError:
        return None
    fn = mod.__dict__.get(name)
    return fn if callable(fn) else None


def _fallbackApplyTax(
    df: DataFrame,
    regionCol: str = "region_code",
    netCol: str = "net_amount",
    rateCol: str = "tax_rate_percent",
    reverseChargeCol: str = "vat_reverse_charge_flag",
) -> DataFrame:
    """Regional tax as specified by the SSIS FACT_{NA,EU,APAC}_Load_Sale expressions."""
    rate = F.coalesce(F.col(rateCol).cast("decimal(19,8)"), F.lit(0))
    net = F.col(netCol).cast("decimal(19,4)")
    reverse = F.col(reverseChargeCol) if reverseChargeCol in df.columns else F.lit(False)
    region = F.upper(F.col(regionCol))
    # LEGACY QUIRK: NA sales tax is computed on the discounted line value and added on top.
    naTax = F.round(net * rate / 100, 4)
    # LEGACY QUIRK: EU VAT on net; a reverse-charge customer gets 0 VAT applied but keeps the rate.
    euTax = F.when(reverse == F.lit(True), F.lit(0)).otherwise(F.round(net * rate / 100, 4))
    # LEGACY QUIRK: APAC prices are GST-inclusive - tax is extracted from the gross, not added.
    apacTax = F.round(net - net / (1 + rate / 100), 4)
    out = df.withColumn(
        "tax_amount",
        F.when(region == "NA", naTax).when(region == "EU", euTax).when(region == "APAC", apacTax).otherwise(naTax),
    )
    out = out.withColumn(
        "total_excluding_tax",
        F.when(region == "APAC", net - F.col("tax_amount")).otherwise(net),
    ).withColumn(
        "total_including_tax",
        F.when(region == "APAC", net).otherwise(net + F.col("tax_amount")),
    )
    out = out.withColumn(
        "tax_treatment_code",
        F.when(region == "NA", F.lit("SALESTAX_ADD"))
        .when((region == "EU") & (reverse == F.lit(True)), F.lit("VAT_REVERSE_CHARGE"))
        .when(region == "EU", F.lit("VAT_STANDARD"))
        .when((region == "APAC") & (rate == 0), F.lit("GST_FREE"))
        .when(region == "APAC", F.lit("GST_INCLUSIVE"))
        .otherwise(F.lit("SALESTAX_ADD")),
    )
    out = out.withColumn(
        "tax_regime_code",
        F.coalesce(
            F.col("tax_regime_code") if "tax_regime_code" in df.columns else F.lit(None).cast("string"),
            F.when(region == "EU", F.lit("VAT")).when(region == "APAC", F.lit("GST")).otherwise(F.lit("SALESTAX")),
        ),
    )
    out = out.withColumn("tax_rate", F.col(rateCol).cast("decimal(18,3)"))
    out = out.withColumn("vat_rate", F.when(region == "EU", F.col("tax_rate")).cast("decimal(18,3)"))
    out = out.withColumn("gst_rate", F.when(region == "APAC", F.col("tax_rate")).cast("decimal(18,3)"))
    out = out.withColumn("vat_reverse_charge_flag", (region == "EU") & (reverse == F.lit(True)))
    out = out.withColumn("gst_free_flag", (region == "APAC") & (rate == 0))
    return out


def _fallbackApplyFx(
    df: DataFrame,
    spark: SparkSession,
    cfg: PipelineConfig,
    amountCols: Sequence[str],
    currencyCol: str,
    dateCol: str,
    regionCol: str,
) -> DataFrame:
    """Latest ``ref_fx_rate`` on or before ``dateCol`` for (currency, region);
    adds ``<amount>_reporting`` for each amount plus ``fx_rate_to_reporting``,
    ``fx_rate_source_code`` and ``fx_rate_effective_date``."""
    out = df.withColumn("_rowid", F.monotonically_increasing_id())
    fxFqn = cfg.fqn("silver", "ref_fx_rate")
    rateCol = F.lit(None).cast("decimal(19,8)")
    srcCol = F.lit(None).cast("string")
    effCol = F.lit(None).cast("date")
    if tableExists(spark, fxFqn):
        fx = spark.table(fxFqn)
        regionMatch = (
            (F.col("_fx_region").isNull() | (F.col("_fx_region") == F.col(regionCol)))
            if "region_code" in fx.columns
            else F.lit(True)
        )
        fx = fx.select(
            F.col("from_currency_code").alias("_fx_from"),
            F.col("to_currency_code").alias("_fx_to"),
            (F.col("region_code") if "region_code" in fx.columns else F.lit(None).cast("string")).alias("_fx_region"),
            F.col("effective_date").cast("date").alias("_fx_date"),
            F.col("rate").cast("decimal(19,8)").alias("_fx_rate"),
            F.col("rate_source_code").alias("_fx_source"),
        ).filter(F.col("_fx_to") == F.lit(cfg.reportingCurrency))
        cond = (
            (F.col(currencyCol) == F.col("_fx_from")) & (F.col("_fx_date") <= F.col(dateCol).cast("date")) & regionMatch
        )
        w = Window.partitionBy("_rowid").orderBy(F.col("_fx_date").desc(), F.col("_fx_region").desc_nulls_last())
        out = (
            out.join(fx, cond, "left")
            .withColumn("_fx_rn", F.row_number().over(w))
            .filter(F.col("_fx_rn") == 1)
            .drop("_fx_rn", "_fx_from", "_fx_to", "_fx_region")
        )
        rateCol, srcCol, effCol = F.col("_fx_rate"), F.col("_fx_source"), F.col("_fx_date")
    sameCcy = F.col(currencyCol) == F.lit(cfg.reportingCurrency)
    # LEGACY QUIRK: a missing rate is not a rejection - the legacy load defaulted the
    # rate to 1.0 and carried on; the row is tagged fx_rate_source_code = 'DEFAULT_1'.
    out = out.withColumn(
        "fx_rate_to_reporting",
        F.when(sameCcy, F.lit(1)).otherwise(F.coalesce(rateCol, F.lit(1))).cast("decimal(19,8)"),
    )
    out = out.withColumn(
        "fx_rate_source_code",
        F.when(sameCcy, F.lit(FX_SAME_CURRENCY_SOURCE)).otherwise(F.coalesce(srcCol, F.lit(FX_DEFAULT_SOURCE))),
    )
    out = out.withColumn(
        "fx_rate_effective_date", F.when(sameCcy, F.col(dateCol).cast("date")).otherwise(effCol).cast("date")
    )
    for c in amountCols:
        out = out.withColumn(
            f"{c}_reporting",
            F.round(F.col(c).cast("decimal(19,4)") * F.col("fx_rate_to_reporting"), 4).cast("decimal(19,4)"),
        )
    return out.drop("_rowid", "_fx_rate", "_fx_source", "_fx_date")


def _fallbackResolveFiscalPeriod(
    df: DataFrame, spark: SparkSession, cfg: PipelineConfig, dateCol: str, regionCol: str
) -> DataFrame:
    """Regional calendar lookup on ``dim_fiscal_calendar``; adds ``fiscal_calendar_code``,
    ``fiscal_year``, ``fiscal_period``, ``fiscal_period_key`` (-1 when unresolved)."""
    mapping = F.create_map(*[F.lit(x) for kv in FISCAL_CALENDAR_BY_REGION.items() for x in kv])
    out = df.withColumn("fiscal_calendar_code", mapping[F.upper(F.col(regionCol))])
    calFqn = cfg.fqn("silver", "dim_fiscal_calendar")
    if tableExists(spark, calFqn):
        cal = spark.table(calFqn).select(
            F.col("calendar_code").alias("_cal"),
            F.col("fiscal_year").cast("int").alias("_fy"),
            F.col("fiscal_period").cast("int").alias("_fp"),
            F.col("period_start").cast("date").alias("_ps"),
            F.col("period_end").cast("date").alias("_pe"),
        )
        d = F.col(dateCol).cast("date")
        cond = (F.col("fiscal_calendar_code") == F.col("_cal")) & (F.col("_ps") <= d) & (d <= F.col("_pe"))
        out = out.join(cal, cond, "left").drop("_cal", "_ps", "_pe")
    else:
        out = out.withColumn("_fy", F.lit(None).cast("int")).withColumn("_fp", F.lit(None).cast("int"))
    out = out.withColumn("fiscal_year", F.col("_fy").cast("smallint")).withColumn(
        "fiscal_period", F.col("_fp").cast("smallint")
    )
    out = out.withColumn(
        "fiscal_period_key",
        F.coalesce(F.col("_fy") * 100 + F.col("_fp"), F.lit(-1)).cast("int"),
    )
    return out.drop("_fy", "_fp")


_taxRule = _importRule("tax", "applyTax")
_fxRule = _importRule("fx", "applyFx")
_fiscalRule = _importRule("fiscal", "resolveFiscalPeriod")
USING_FALLBACK: dict[str, bool] = {"tax": _taxRule is None, "fx": _fxRule is None, "fiscal": _fiscalRule is None}


def applyTax(df: DataFrame, regionCol: str = "region_code") -> DataFrame:
    if _taxRule is not None:
        return _taxRule(df, regionCol=regionCol)
    return _fallbackApplyTax(df, regionCol=regionCol)


def applyFx(
    df: DataFrame,
    spark: SparkSession,
    cfg: PipelineConfig,
    amountCols: Sequence[str],
    currencyCol: str,
    dateCol: str,
    regionCol: str,
) -> DataFrame:
    if _fxRule is not None:
        return _fxRule(df, spark, cfg, amountCols, currencyCol, dateCol, regionCol)
    return _fallbackApplyFx(df, spark, cfg, amountCols, currencyCol, dateCol, regionCol)


def resolveFiscalPeriod(
    df: DataFrame, spark: SparkSession, cfg: PipelineConfig, dateCol: str, regionCol: str
) -> DataFrame:
    if _fiscalRule is not None:
        return _fiscalRule(df, spark, cfg, dateCol, regionCol)
    return _fallbackResolveFiscalPeriod(df, spark, cfg, dateCol, regionCol)
