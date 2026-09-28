"""Input contract shared by the gold reporting modules.

The gold reporting layer reads workstream-6 facts (``sales_gold.fact_*``),
workstream-3/4 silver dimensions (``sales_silver.dim_*`` / ``ref_fx_rate``) and
a handful of bronze operational tables.  Column names follow CONVENTIONS.md:
the legacy warehouse column name in snake_case (``[Net Amount Reporting]`` ->
``net_amount_reporting``).  Where the legacy warehouse has no equivalent the
name used here is documented in the ``*_SCHEMA`` DDL strings below.

Tables that are optional for the sales domain (stock item dimension, returns
fact, warehouse site ...) are read through :func:`readOrEmpty`, which returns
an empty DataFrame with the contract schema when the table does not exist so
that every gold table can still be produced.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import tableExists
from sales_lakehouse.silver.rules.fiscal import na445Parts

# LEGACY QUIRK: cross-region period bucketing uses the NA 4-4-5 calendar
# (docs/domain-model/business-domains.md "Fiscal calendar": "The aggregates
# choose the NA convention, silently").
NA_FISCAL_CALENDAR_CODE = "NA445"

# Legacy Aggregate.Monthly Sales Summary / Regional Sales Performance labels.
LEGACY_FISCAL_CALENDAR_LABEL: dict[str, str] = {"NA": "CY12", "EU": "APR12", "APAC": "JUL13"}
# LEGACY QUIRK: usp_RefreshAggregateRegionalSales hard-codes the local currency per region.
LEGACY_REGION_CURRENCY: dict[str, str] = {"NA": "USD", "EU": "EUR", "APAC": "AUD"}

FX_RATE_TYPE_AVERAGE = "AVERAGE"
FX_RATE_TYPE_DAILY = "DAILY"

FACT_RETURN_SCHEMA = (
    "return_key BIGINT, return_date_key DATE, customer_key INT, stock_item_key INT, "
    "sales_territory_key INT, salesperson_key INT, region_code STRING, "
    "original_invoice_number STRING, quantity_returned DECIMAL(18,3), "
    "net_credit_amount DECIMAL(18,2), fx_rate_to_reporting DECIMAL(18,6), "
    "net_credit_amount_reporting DECIMAL(18,2)"
)
DIM_STOCK_ITEM_SCHEMA = (
    "stock_item_key INT, wwi_stock_item_id INT, stock_item STRING, brand STRING, size STRING, "
    "product_category_key INT, primary_supplier_key INT, is_discontinued BOOLEAN, "
    "valid_from TIMESTAMP, is_current_row BOOLEAN"
)
DIM_PRODUCT_CATEGORY_SCHEMA = "product_category_key INT, product_category STRING"
DIM_CUSTOMER_SEGMENT_SCHEMA = "customer_segment_key INT, customer_segment STRING"
DIM_LOYALTY_TIER_SCHEMA = "loyalty_tier_key INT, loyalty_tier STRING"
DIM_WAREHOUSE_SITE_SCHEMA = "warehouse_site_key INT, warehouse_site STRING"
SALES_BUDGET_SCHEMA = (
    "sales_territory_key INT, budget_month DATE, budget_amount_reporting DECIMAL(18,2)"
)
FACT_LOYALTY_POINTS_SCHEMA = (
    "customer_key INT, transaction_date_key DATE, points_earned DECIMAL(18,2), "
    "points_redeemed DECIMAL(18,2), loyalty_tier_key INT"
)
FACT_WEB_SESSION_SCHEMA = "customer_key INT, session_date_key DATE, session_count INT"
FACT_CUSTOMER_BALANCE_SCHEMA = (
    "customer_key INT, balance_date_key DATE, current_balance_reporting DECIMAL(18,2), "
    "overdue_balance_reporting DECIMAL(18,2)"
)


def readOrEmpty(
    spark: SparkSession, cfg: PipelineConfig, layer: str, table: str, schemaDdl: str
) -> DataFrame:
    """Read ``layer.table`` or return an empty DataFrame with the contract schema."""
    fqn = cfg.fqn(layer, table)
    if tableExists(spark, fqn):
        return spark.table(fqn)
    return spark.createDataFrame([], schemaDdl)


def naFiscalCalendar(fiscalCalendar: DataFrame) -> DataFrame:
    """NA 4-4-5 periods as (na_fiscal_year, na_fiscal_period, period_start, period_end)."""
    return fiscalCalendar.filter(F.col("calendar_code") == NA_FISCAL_CALENDAR_CODE).select(
        F.col("fiscal_year").alias("na_fiscal_year"),
        F.col("fiscal_period").alias("na_fiscal_period"),
        F.col("period_start").alias("na_period_start"),
        F.col("period_end").alias("na_period_end"),
    )


def withNaFiscalPeriod(df: DataFrame, dateCol: str, fiscalCalendar: DataFrame) -> DataFrame:
    """Attach ``na_fiscal_year`` / ``na_fiscal_period`` for ``dateCol``.

    # LEGACY QUIRK: every region is bucketed on the NA calendar regardless of
    # the per-row regional [Fiscal Year]/[Fiscal Period] carried by the facts.
    """
    cal = naFiscalCalendar(fiscalCalendar)
    joined = df.join(
        cal,
        (F.col(dateCol) >= F.col("na_period_start")) & (F.col(dateCol) <= F.col("na_period_end")),
        "left",
    )
    # dim_fiscal_calendar wins (it is what the legacy aggregates join); dates it
    # does not cover fall back to the silver NA445 arithmetic (usp_PopulateDateDimension).
    parts = na445Parts(F.col(dateCol))
    return (
        joined.withColumn("na_fiscal_year", F.coalesce(F.col("na_fiscal_year"), parts["fiscal_year"]))
        .withColumn("na_fiscal_period", F.coalesce(F.col("na_fiscal_period"), parts["fiscal_period"]))
        .drop("na_period_start", "na_period_end")
    )


def naFiscalPeriodLabel(fiscalYear: Column, fiscalPeriod: Column) -> Column:
    """``FY2019-P07`` style label used by Sales.SalesQuotas.FiscalPeriodLabel."""
    return F.concat(
        F.lit("FY"), fiscalYear.cast("string"), F.lit("-P"), F.lpad(fiscalPeriod.cast("string"), 2, "0")
    )


def monthlyAverageFxRate(fxRate: DataFrame, toCurrency: str) -> DataFrame:
    """Month-average rate per source currency into ``toCurrency``.

    Returns (fx_from_currency_code, fx_rate_month, monthly_average_rate) with the
    latest AVERAGE-type rate whose effective_date falls inside the month
    (legacy stg.FxRateMonthly keyed on the month).
    """
    avg = fxRate.filter(
        (F.col("rate_type_code") == FX_RATE_TYPE_AVERAGE) & (F.col("to_currency_code") == toCurrency)
    ).withColumn("fx_rate_month", F.trunc(F.col("effective_date"), "month"))
    latest = avg.groupBy("from_currency_code", "fx_rate_month").agg(
        F.max_by("conversion_rate", "effective_date").alias("monthly_average_rate")
    )
    return latest.withColumnRenamed("from_currency_code", "fx_from_currency_code")


def safePercent(numerator: Column, denominator: Column, scale: int = 2) -> Column:
    """``ROUND(100.0 * num / den, scale)`` with NULL when the denominator is zero/NULL."""
    return F.when(
        denominator.isNull() | (denominator == 0), F.lit(None).cast("decimal(18,4)")
    ).otherwise(F.round(F.lit(100.0) * numerator / denominator, scale))


def safeDivide(numerator: Column, denominator: Column, scale: int = 2) -> Column:
    return F.when(
        denominator.isNull() | (denominator == 0), F.lit(None).cast("decimal(18,4)")
    ).otherwise(F.round(numerator / denominator, scale))


def monthIndex(dateCol: Column) -> Column:
    """Monotonic month number (year * 12 + month) for month-offset windows."""
    return F.year(dateCol) * 12 + F.month(dateCol)


def notReversal(df: DataFrame) -> DataFrame:
    """Legacy ``ISNULL([Correction Type Code], 'ORIG') <> 'REV'`` filter."""
    return df.filter(F.coalesce(F.col("correction_type_code"), F.lit("ORIG")) != "REV")
