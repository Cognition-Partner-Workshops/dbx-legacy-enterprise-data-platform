"""Shared helpers for the WWI_Aggregates (session 09) notebooks.

Everything here is pure Python / PySpark and has no dependency on
``dbx_etl_common``; the notebooks compose the two.  Table resolution goes
through ``naming.table`` in the notebook so the catalog is never hard-coded.
"""
from __future__ import annotations

import calendar
import datetime as dt
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_NAME = "WWI_Aggregates"
SOURCE_SYSTEM_CODE = "WWIDW"

# logical name -> (schema, table).  Legacy object names map to Delta names
# with the estate naming contract (snake_case, spaces -> "_").
TABLES: Dict[str, Tuple[str, str]] = {
    # gold facts
    "fact_sale": ("gold", "fact_sale"),
    "fact_sales_margin": ("gold", "fact_sales_margin"),
    "fact_return": ("gold", "fact_return"),
    "fact_credit_note": ("gold", "fact_credit_note"),
    "fact_daily_inventory_snapshot": ("gold", "fact_daily_inventory_snapshot"),
    "fact_stock_movement": ("gold", "fact_stock_movement"),
    "fact_shipment": ("gold", "fact_shipment"),
    "fact_purchase": ("gold", "fact_purchase"),
    "fact_purchase_receipt": ("gold", "fact_purchase_receipt"),
    "fact_supplier_payment": ("gold", "fact_supplier_payment"),
    "fact_gl_posting": ("gold", "fact_gl_posting"),
    "fact_monthly_ar_aging": ("gold", "fact_monthly_ar_aging"),
    "fact_monthly_ap_aging": ("gold", "fact_monthly_ap_aging"),
    "fact_monthly_customer_balance": ("gold", "fact_monthly_customer_balance"),
    "fact_customer_payment": ("gold", "fact_customer_payment"),
    "fact_loyalty_points": ("gold", "fact_loyalty_points"),
    "fact_web_session": ("gold", "fact_web_session"),
    "fact_promotion_eligibility": ("gold", "fact_promotion_eligibility"),
    "fact_order_fulfilment": ("gold", "fact_order_fulfilment"),
    # gold dimensions
    "dim_date": ("gold", "dim_date"),
    "dim_customer": ("gold", "dim_customer"),
    "dim_stock_item": ("gold", "dim_stock_item"),
    "dim_product_category": ("gold", "dim_product_category"),
    "dim_sales_territory": ("gold", "dim_sales_territory"),
    "dim_sales_channel": ("gold", "dim_sales_channel"),
    "dim_customer_segment": ("gold", "dim_customer_segment"),
    "dim_loyalty_tier": ("gold", "dim_loyalty_tier"),
    "dim_warehouse_site": ("gold", "dim_warehouse_site"),
    "dim_supplier": ("gold", "dim_supplier"),
    "dim_promotion": ("gold", "dim_promotion"),
    "dim_gl_account": ("gold", "dim_gl_account"),
    "dim_legal_entity": ("gold", "dim_legal_entity"),
    "dim_carrier": ("gold", "dim_carrier"),
    # silver reference / integration work objects
    "ref_fx_rate_monthly": ("silver", "ref_fx_rate_monthly"),
    "ref_sales_budget": ("silver", "ref_sales_budget"),
    "ref_accounting_period": ("silver", "ref_accounting_period"),
    "int_reporting_publication": ("silver", "int_reporting_publication"),
    # gold aggregates (targets)
    "agg_daily_sales_summary": ("gold", "agg_daily_sales_summary"),
    "agg_daily_inventory_health": ("gold", "agg_daily_inventory_health"),
    "agg_monthly_sales_summary": ("gold", "agg_monthly_sales_summary"),
    "agg_monthly_margin_analysis": ("gold", "agg_monthly_margin_analysis"),
    "agg_customer_360": ("gold", "agg_customer_360"),
    "agg_customer_rolling_12_month": ("gold", "agg_customer_rolling_12_month"),
    "agg_product_performance": ("gold", "agg_product_performance"),
    "agg_supplier_performance": ("gold", "agg_supplier_performance"),
    "agg_regional_sales_performance": ("gold", "agg_regional_sales_performance"),
    "agg_finance_close_summary": ("gold", "agg_finance_close_summary"),
    "agg_promotion_effectiveness": ("gold", "agg_promotion_effectiveness"),
    "agg_delivery_performance_summary": ("gold", "agg_delivery_performance_summary"),
    "rpt_publish_state": ("gold", "rpt_publish_state"),
}

LEGACY_OBJECT_NAMES: Dict[str, str] = {
    "agg_daily_sales_summary": "Aggregate.Daily Sales Summary",
    "agg_daily_inventory_health": "Aggregate.Daily Inventory Health",
    "agg_monthly_sales_summary": "Aggregate.Monthly Sales Summary",
    "agg_monthly_margin_analysis": "Aggregate.Monthly Margin Analysis",
    "agg_customer_360": "Aggregate.Customer 360",
    "agg_customer_rolling_12_month": "Aggregate.Customer Rolling 12 Month",
    "agg_product_performance": "Aggregate.Product Performance",
    "agg_supplier_performance": "Aggregate.Supplier Performance",
    "agg_regional_sales_performance": "Aggregate.Regional Sales Performance",
    "agg_finance_close_summary": "Aggregate.Finance Close Summary",
    "agg_promotion_effectiveness": "Aggregate.Promotion Effectiveness",
    "agg_delivery_performance_summary": "Aggregate.Delivery Performance Summary",
}

REJECT_REASON_CODE = "AGG_QUALITY_GATE"
REJECT_REASON = "Aggregate row failed the summary quality gate"


def resolveTables(catalog: str, tableFn: Callable[[str, str, str], str]) -> Dict[str, str]:
    """Return {logical name: fully qualified table} using ``naming.table``."""
    return {key: tableFn(catalog, schema, table) for key, (schema, table) in TABLES.items()}


def parseBool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text == "":
        return default
    return text in ("1", "true", "t", "yes", "y")


def parseDate(value: Optional[str], default: Optional[dt.date] = None) -> Optional[dt.date]:
    if value is None or str(value).strip() == "":
        return default
    return dt.date.fromisoformat(str(value).strip()[:10])


def parseDecimal(value: Optional[str], default: float) -> float:
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def parseInt(value: Optional[str], default: int) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(float(value))
    except ValueError:
        return default


def getOptionalWidget(dbutils, name: str, default: str = "") -> str:
    """Job parameters surface as widgets; package-specific ones may be absent."""
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return default
    return default if value is None else value


def monthStart(day: dt.date) -> dt.date:
    return day.replace(day=1)


def monthEnd(day: dt.date) -> dt.date:
    return day.replace(day=calendar.monthrange(day.year, day.month)[1])


def addMonths(day: dt.date, months: int) -> dt.date:
    yearIndex = day.year * 12 + (day.month - 1) + months
    year, monthZero = divmod(yearIndex, 12)
    lastDay = calendar.monthrange(year, monthZero + 1)[1]
    return dt.date(year, monthZero + 1, min(day.day, lastDay))


def isoWeekStart(day: dt.date) -> dt.date:
    return day - dt.timedelta(days=day.weekday())


def accountingPeriodStart(accountingPeriodCode: Optional[str], businessDate: dt.date) -> dt.date:
    """``yyyy-MM`` -> first day of that month; blank -> month of BusinessDate."""
    text = "" if accountingPeriodCode is None else str(accountingPeriodCode).strip()
    if text == "" or text.startswith("1900"):
        return monthStart(businessDate)
    return dt.date(int(text[0:4]), int(text[5:7]), 1)


@dataclass(frozen=True)
class RefreshWindow:
    fromDate: dt.date
    toDate: dt.date

    def sqlLiteral(self, column: str) -> str:
        return f"{column} BETWEEN DATE'{self.fromDate.isoformat()}' AND DATE'{self.toDate.isoformat()}'"


def dailyWindow(refreshFrom: Optional[str], refreshTo: Optional[str], businessDate: dt.date,
                trailingDays: int, reloadFullHistory: bool,
                fullHistoryFrom: dt.date = dt.date(2013, 1, 1)) -> RefreshWindow:
    """Legacy Init Refresh Window: ToDate defaults to BusinessDate, FromDate to
    ToDate - TrailingDays; a full reload rewinds to the start of history."""
    toDate = parseDate(refreshTo, businessDate)
    if reloadFullHistory:
        return RefreshWindow(fullHistoryFrom, toDate)
    fromDate = parseDate(refreshFrom, toDate - dt.timedelta(days=trailingDays))
    if fromDate > toDate:
        raise ValueError(f"RefreshFromDate {fromDate} is after RefreshToDate {toDate}")
    return RefreshWindow(fromDate, toDate)


def periodWindow(accountingPeriodCode: Optional[str], rebuildPriorPeriods: int,
                 businessDate: dt.date, reloadFullHistory: bool,
                 fullHistoryFrom: dt.date = dt.date(2013, 1, 1)) -> RefreshWindow:
    """Legacy Init Refresh Period: the window covers the target accounting
    period and ``RebuildPriorPeriods`` preceding calendar months."""
    periodStart = accountingPeriodStart(accountingPeriodCode, businessDate)
    if reloadFullHistory:
        return RefreshWindow(fullHistoryFrom, monthEnd(periodStart))
    return RefreshWindow(addMonths(periodStart, -max(rebuildPriorPeriods, 0)), monthEnd(periodStart))


def monthsInWindow(window: RefreshWindow) -> list:
    months = []
    cursor = monthStart(window.fromDate)
    while cursor <= window.toDate:
        months.append(cursor)
        cursor = addMonths(cursor, 1)
    return months


def tableExists(spark: SparkSession, tableName: str) -> bool:
    return spark.catalog.tableExists(tableName)


def overwriteWindow(spark: SparkSession, df: DataFrame, targetTable: str, predicate: str) -> int:
    """Idempotent window rebuild: Delta ``replaceWhere`` replaces exactly the
    rows matching ``predicate`` (the legacy DELETE window + INSERT) in one
    transaction.  Returns the number of rows written."""
    if tableExists(spark, targetTable):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", predicate)
           .option("mergeSchema", "false")
           .saveAsTable(targetTable))
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(targetTable)
    return spark.sql(f"SELECT COUNT(*) AS c FROM {targetTable} WHERE {predicate}").collect()[0]["c"]


def overwriteFull(spark: SparkSession, df: DataFrame, targetTable: str) -> int:
    """Atomic TRUNCATE + INSERT equivalent (single Delta commit)."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(targetTable))
    return spark.table(targetTable).count()


def splitRejects(df: DataFrame, rejectCondition) -> Tuple[DataFrame, DataFrame]:
    """Conditional Split: rows matching ``rejectCondition`` go to the reject
    output, the rest to the insert outputs."""
    rejected = df.filter(rejectCondition)
    accepted = df.filter(~F.coalesce(rejectCondition, F.lit(False)))
    return accepted, rejected


def stampRefresh(df: DataFrame, batchId: int) -> DataFrame:
    return (df.withColumn("refresh_batch_id", F.lit(int(batchId)).cast("bigint"))
              .withColumn("refreshed_datetime", F.current_timestamp()))


def reconciliationVariance(sourceRowCount: int, targetRowCount: int, rejectRowCount: int) -> int:
    """etl.RowCountAudit.VarianceRowCount for an aggregate: source rows are the
    distinct grain rows produced by the aggregation, so a healthy run is 0."""
    return int(sourceRowCount) - int(targetRowCount) - int(rejectRowCount)


def assertAggregateReconciliation(expectedRowCount: int, actualRowCount: int, rejectRowCount: int,
                                  objectName: str) -> None:
    variance = reconciliationVariance(expectedRowCount, actualRowCount, rejectRowCount)
    if variance != 0:
        raise RuntimeError(
            f"Row count reconciliation failed for {objectName}: expected {expectedRowCount}, "
            f"actual {actualRowCount}, rejected {rejectRowCount}, variance {variance}")


def measureTotals(df: DataFrame, measureColumns) -> Dict[str, float]:
    row = df.agg(*[F.sum(F.col(c)).alias(c) for c in measureColumns]).collect()[0]
    return {c: (float(row[c]) if row[c] is not None else 0.0) for c in measureColumns}
