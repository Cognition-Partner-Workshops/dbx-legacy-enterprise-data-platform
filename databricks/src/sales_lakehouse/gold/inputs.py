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
from sales_lakehouse.silver.business_keys import sourceSystemKey
from sales_lakehouse.silver.rules.fiscal import na445Parts

# LEGACY QUIRK: cross-region period bucketing uses the NA 4-4-5 calendar
# (docs/domain-model/business-domains.md "Fiscal calendar": "The aggregates
# choose the NA convention, silently").
NA_FISCAL_CALENDAR_CODE = "NA445"

# Legacy Aggregate.Monthly Sales Summary / Regional Sales Performance labels.
LEGACY_FISCAL_CALENDAR_LABEL: dict[str, str] = {"NA": "CY12", "EU": "APR12", "APAC": "JUL13"}
# LEGACY QUIRK: usp_RefreshAggregateRegionalSales hard-codes the local currency per region.
LEGACY_REGION_CURRENCY: dict[str, str] = {"NA": "USD", "EU": "EUR", "APAC": "AUD"}

# silver.ref_fx_rate quotes rate_to_usd; cross rates are derived through this pivot.
FX_PIVOT_CURRENCY = "USD"
FX_RATE_TYPE = "decimal(19,8)"
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


def _firstOf(df: DataFrame, candidates: tuple[str, ...], castTo: str) -> Column:
    for c in candidates:
        if c in df.columns:
            return F.col(c).cast(castTo)
    return F.lit(None).cast(castTo)


def conformDimCustomer(customer: DataFrame, territory: DataFrame | None = None) -> DataFrame:
    """Expose ``silver.dim_customer`` (workstream 3) under the legacy ``Dimension.Customer`` column
    names the aggregates / sales-ops / partner-feed readers were written against.

    ``is_current_row`` <- ``is_current``; ``customer`` <- ``customer_name``; ``category`` <-
    ``customer_category_name``; ``source_customer_reference`` <- ``customer_business_key``;
    ``sales_territory_key`` is resolved through ``dim_sales_territory.wwi_sales_territory_id`` when
    only ``sales_territory_id`` is present. Columns silver does not carry (``erasure_requested_on``,
    ``is_house_account``, ``customer_segment_key``, ``account_manager_employee_key``) are NULL.
    """
    out = customer
    if "sales_territory_key" not in out.columns and territory is not None and "sales_territory_id" in out.columns:
        terr = territory.select(
            F.col("wwi_sales_territory_id").cast("bigint").alias("_terr_id"),
            F.col("sales_territory_key").alias("_terr_key"),
            F.col("region_code").alias("_terr_region"),
        ).dropDuplicates(["_terr_id"])
        out = out.join(terr, F.col("sales_territory_id").cast("bigint") == F.col("_terr_id"), "left")
        out = out.withColumn("sales_territory_key", F.col("_terr_key"))
        if "region_code" not in out.columns:
            out = out.withColumn("region_code", F.col("_terr_region"))
        out = out.drop("_terr_id", "_terr_key", "_terr_region")
    mapping: dict[str, tuple[tuple[str, ...], str]] = {
        "is_current_row": (("is_current_row", "is_current"), "boolean"),
        "customer": (("customer", "customer_name"), "string"),
        "category": (("category", "customer_category_name"), "string"),
        "source_customer_reference": (("source_customer_reference", "customer_business_key", "wwi_customer_id"), "string"),
        "region_code": (("region_code",), "string"),
        "sales_territory_key": (("sales_territory_key",), "bigint"),
        "customer_segment_key": (("customer_segment_key",), "bigint"),
        "account_manager_employee_key": (("account_manager_employee_key",), "bigint"),
        "primary_contact_email": (("primary_contact_email",), "string"),
        "credit_limit_amount": (("credit_limit_amount",), "decimal(19,4)"),
        "marketing_consent_flag": (("marketing_consent_flag",), "boolean"),
        "erasure_requested_on": (("erasure_requested_on",), "date"),
        "is_house_account": (("is_house_account",), "boolean"),
    }
    for target, (candidates, castTo) in mapping.items():
        if target not in out.columns:
            out = out.withColumn(target, _firstOf(out, candidates, castTo))
    return out


def readDimCustomer(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """``silver.dim_customer`` conformed with :func:`conformDimCustomer`."""
    customer = spark.table(cfg.fqn("silver", "dim_customer"))
    terrFqn = cfg.fqn("silver", "dim_sales_territory")
    territory = spark.table(terrFqn) if tableExists(spark, terrFqn) else None
    return conformDimCustomer(customer, territory)


def conformDimSalesperson(salesperson: DataFrame) -> DataFrame:
    """Expose ``silver.dim_salesperson`` (workstream 3) under the names the gold readers use.

    ``salesperson_business_key`` <- ``sourceSystemKey(source_system_code, wwi_employee_id)`` (the same
    ``WWI_OLTP|<PersonID>`` key silver transactions stamp on order / sale rows);
    ``wwi_person_id`` <- ``wwi_employee_id``.
    """
    out = salesperson
    if "salesperson_business_key" not in out.columns and "wwi_employee_id" in out.columns:
        system = F.col("source_system_code") if "source_system_code" in out.columns else F.lit("WWI_OLTP")
        out = out.withColumn("salesperson_business_key", sourceSystemKey(system, F.col("wwi_employee_id")))
    if "wwi_person_id" not in out.columns:
        out = out.withColumn("wwi_person_id", _firstOf(out, ("wwi_employee_id",), "int"))
    return out


def readDimSalesperson(spark: SparkSession, cfg: PipelineConfig) -> DataFrame | None:
    """``silver.dim_salesperson`` conformed with :func:`conformDimSalesperson`; ``None`` when absent."""
    fqn = cfg.fqn("silver", "dim_salesperson")
    return conformDimSalesperson(spark.table(fqn)) if tableExists(spark, fqn) else None


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

    ``silver.ref_fx_rate`` quotes every currency against the pivot (``rate_to_usd``
    per ``rate_date``); the legacy stg.FxRateMonthly month rate is the mean of the
    month's daily quotes, cross-rated through the pivot when ``toCurrency`` is not
    the pivot. Returns (fx_from_currency_code, fx_rate_month, monthly_average_rate).
    """
    monthly = (
        fxRate.withColumn("fx_rate_month", F.trunc(F.col("rate_date"), "month"))
        .groupBy(F.upper(F.col("currency_code")).alias("fx_from_currency_code"), "fx_rate_month")
        .agg(F.avg(F.col("rate_to_usd").cast(FX_RATE_TYPE)).alias("_to_pivot"))
    )
    if toCurrency.upper() == FX_PIVOT_CURRENCY:
        return monthly.withColumn("monthly_average_rate", F.col("_to_pivot").cast(FX_RATE_TYPE)).drop("_to_pivot")
    target = monthly.filter(F.col("fx_from_currency_code") == toCurrency.upper()).select(
        "fx_rate_month", F.col("_to_pivot").alias("_target_to_pivot")
    )
    return (
        monthly.join(target, "fx_rate_month", "inner")
        .withColumn("monthly_average_rate", (F.col("_to_pivot") / F.col("_target_to_pivot")).cast(FX_RATE_TYPE))
        .drop("_to_pivot", "_target_to_pivot")
    )


def latestMonthlyFxRate(fxRate: DataFrame, toCurrency: str) -> DataFrame:
    """Most recent month-average rate per source currency (fx_from_currency_code, latest_average_rate)."""
    return (
        monthlyAverageFxRate(fxRate, toCurrency)
        .groupBy("fx_from_currency_code")
        .agg(F.max_by("monthly_average_rate", "fx_rate_month").alias("latest_average_rate"))
    )


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
