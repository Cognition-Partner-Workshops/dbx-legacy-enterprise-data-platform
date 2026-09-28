"""Row-count / measure / hash reconciliation of gold.agg_* and gold.rpt_* against the SQL Server baseline."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession

from agg_common import LEGACY_OBJECT_NAMES, tableExists
from rpt_views import REPORT_VIEWS

STAMP_COLUMNS = ("refresh_batch_id", "refreshed_datetime")
TOTAL_PERIOD = "*"


@dataclass(frozen=True)
class ReconSpec:
    targetKey: str
    objectName: str
    periodColumn: Optional[str]
    measures: Sequence[str]


RECONCILIATION_SPECS: List[ReconSpec] = [
    ReconSpec("agg_daily_sales_summary", LEGACY_OBJECT_NAMES["agg_daily_sales_summary"], "sales_date",
              ("net_sales_amount", "gross_margin_amount", "quantity_sold_base_uom", "invoice_count")),
    ReconSpec("agg_daily_inventory_health", LEGACY_OBJECT_NAMES["agg_daily_inventory_health"], "snapshot_date",
              ("total_quantity_on_hand", "total_stock_value_reporting", "stockout_sku_count", "sku_count")),
    ReconSpec("agg_monthly_sales_summary", LEGACY_OBJECT_NAMES["agg_monthly_sales_summary"], "calendar_month",
              ("net_revenue_reporting", "gross_margin_reporting", "order_count", "invoice_count")),
    ReconSpec("agg_monthly_margin_analysis", LEGACY_OBJECT_NAMES["agg_monthly_margin_analysis"], "calendar_month",
              ("net_revenue_reporting", "cost_of_sales_reporting", "gross_margin_reporting", "quantity_sold_base_uom")),
    ReconSpec("agg_customer_360", LEGACY_OBJECT_NAMES["agg_customer_360"], None,
              ("lifetime_net_revenue", "lifetime_gross_margin", "lifetime_order_count")),
    ReconSpec("agg_customer_rolling_12_month", LEGACY_OBJECT_NAMES["agg_customer_rolling_12_month"], "calendar_month",
              ("net_revenue_reporting", "gross_margin_reporting", "order_count", "rolling_12_month_revenue")),
    ReconSpec("agg_product_performance", LEGACY_OBJECT_NAMES["agg_product_performance"], "calendar_month",
              ("units_sold_base_uom", "net_revenue_reporting", "gross_margin_reporting", "units_returned")),
    ReconSpec("agg_supplier_performance", LEGACY_OBJECT_NAMES["agg_supplier_performance"], "calendar_month",
              ("recognised_spend_reporting", "committed_spend_reporting", "receipt_count", "on_time_receipt_count")),
    ReconSpec("agg_regional_sales_performance", LEGACY_OBJECT_NAMES["agg_regional_sales_performance"], "calendar_month",
              ("net_sales_daily_rate", "gross_margin_reporting", "order_count", "budget_net_sales_reporting")),
    ReconSpec("agg_finance_close_summary", LEGACY_OBJECT_NAMES["agg_finance_close_summary"], "fiscal_period",
              ("closing_balance_reporting", "period_debits_local", "period_credits_local", "manual_journal_count")),
    ReconSpec("agg_promotion_effectiveness", LEGACY_OBJECT_NAMES["agg_promotion_effectiveness"], None,
              ("promotion_revenue_reporting", "incremental_revenue_reporting", "discount_cost_reporting",
               "promoted_units_sold")),
    ReconSpec("agg_delivery_performance_summary", LEGACY_OBJECT_NAMES["agg_delivery_performance_summary"],
              "iso_week_start_date",
              ("consignment_count", "on_time_count", "freight_cost_reporting", "service_credit_reporting")),
]

REPORT_SPECS: List[ReconSpec] = [
    ReconSpec(rpt, legacy, None, ()) for legacy, rpt in REPORT_VIEWS.items()
]


@dataclass
class Summary:
    objectName: str
    period: str
    rowCount: int
    rowHash: Optional[int]
    measures: Dict[str, float] = field(default_factory=dict)


@dataclass
class Comparison:
    objectName: str
    period: str
    metric: str
    expected: Optional[float]
    actual: Optional[float]
    variance: Optional[float]
    withinTolerance: bool


def hashExpression(columns: Sequence[str]) -> str:
    """Deterministic row hash: xxhash64 over the sorted, null-normalised columns; summed per group
    so it is order independent (the baseline side uses the same recipe in T-SQL via HASHBYTES/CHECKSUM)."""
    cols = ", ".join(f"COALESCE(CAST(`{c}` AS STRING), '<null>')" for c in sorted(columns))
    return f"SUM(xxhash64(concat_ws('|', {cols})))"


def summarySql(table: str, columns: Sequence[str], periodColumn: Optional[str], measures: Sequence[str]) -> str:
    hashCols = [c for c in columns if c not in STAMP_COLUMNS]
    measureSql = "".join(f", SUM(CAST(`{m}` AS DOUBLE)) AS `m__{m}`" for m in measures if m in columns)
    periodSql = f"CAST(`{periodColumn}` AS STRING)" if periodColumn else f"'{TOTAL_PERIOD}'"
    groupSql = f"GROUP BY `{periodColumn}`" if periodColumn else ""
    return (f"SELECT {periodSql} AS period, COUNT(*) AS row_count, {hashExpression(hashCols)} AS row_hash{measureSql} "
            f"FROM {table} {groupSql}")


def summarise(spark: SparkSession, table: str, spec: ReconSpec) -> List[Summary]:
    if not tableExists(spark, table):
        return []
    columns = spark.table(table).columns
    rows = spark.sql(summarySql(table, columns, spec.periodColumn, spec.measures)).collect()
    out = []
    for r in rows:
        measures = {m: float(r[f"m__{m}"]) if r[f"m__{m}"] is not None else 0.0
                    for m in spec.measures if f"m__{m}" in r.asDict()}
        out.append(Summary(spec.objectName, str(r["period"]), int(r["row_count"]),
                           int(r["row_hash"]) if r["row_hash"] is not None else None, measures))
    if spec.periodColumn:
        total = spark.sql(summarySql(table, columns, None, spec.measures)).collect()[0]
        measures = {m: float(total[f"m__{m}"]) if total[f"m__{m}"] is not None else 0.0
                    for m in spec.measures if f"m__{m}" in total.asDict()}
        out.append(Summary(spec.objectName, TOTAL_PERIOD, int(total["row_count"]),
                           int(total["row_hash"]) if total["row_hash"] is not None else None, measures))
    return out


def baselineFromJson(text: str) -> Dict[Tuple[str, str], Summary]:
    """JSON parameter form: {"objects":[{"objectName":..,"period":..,"rowCount":..,"rowHash":..,"measures":{..}}]}"""
    if not text or not text.strip():
        return {}
    doc = json.loads(text)
    objects = doc["objects"] if isinstance(doc, dict) else doc
    out: Dict[Tuple[str, str], Summary] = {}
    for o in objects:
        period = str(o.get("period", TOTAL_PERIOD))
        rowHash = o.get("rowHash")
        out[(o["objectName"], period)] = Summary(
            o["objectName"], period, int(o["rowCount"]), int(rowHash) if rowHash is not None else None,
            {k: float(v) for k, v in (o.get("measures") or {}).items()})
    return out


def baselineFromTable(spark: SparkSession, table: str) -> Dict[Tuple[str, str], Summary]:
    """Delta form (long): ObjectName, PeriodValue, RowCount, RowHash, MeasureName, MeasureValue."""
    if not tableExists(spark, table):
        return {}
    out: Dict[Tuple[str, str], Summary] = {}
    for r in spark.table(table).collect():
        period = str(r["PeriodValue"]) if r["PeriodValue"] is not None else TOTAL_PERIOD
        key = (r["ObjectName"], period)
        if key not in out:
            out[key] = Summary(r["ObjectName"], period, int(r["RowCount"]),
                               int(r["RowHash"]) if r["RowHash"] is not None else None, {})
        if r["MeasureName"]:
            out[key].measures[r["MeasureName"]] = float(r["MeasureValue"] or 0.0)
    return out


def withinTolerance(expected: float, actual: float, absoluteTolerance: float, percentTolerance: float) -> bool:
    variance = abs(actual - expected)
    if variance <= absoluteTolerance:
        return True
    if expected != 0 and (variance / abs(expected)) * 100.0 <= percentTolerance:
        return True
    return False


def compare(actuals: Sequence[Summary], baseline: Dict[Tuple[str, str], Summary],
            absoluteTolerance: float = 0.0, percentTolerance: float = 0.0) -> List[Comparison]:
    results: List[Comparison] = []
    for a in actuals:
        b = baseline.get((a.objectName, a.period))
        if b is None:
            continue
        results.append(Comparison(a.objectName, a.period, "row_count", float(b.rowCount), float(a.rowCount),
                                  float(a.rowCount - b.rowCount),
                                  withinTolerance(b.rowCount, a.rowCount, absoluteTolerance, percentTolerance)))
        if b.rowHash is not None and a.rowHash is not None:
            results.append(Comparison(a.objectName, a.period, "row_hash", float(b.rowHash), float(a.rowHash),
                                      None, b.rowHash == a.rowHash))
        for m, expected in b.measures.items():
            actual = a.measures.get(m)
            ok = actual is not None and withinTolerance(expected, actual, absoluteTolerance, percentTolerance)
            results.append(Comparison(a.objectName, a.period, m, expected, actual,
                                      None if actual is None else actual - expected, ok))
    return results


def missingBaselines(actuals: Sequence[Summary], baseline: Dict[Tuple[str, str], Summary]) -> List[str]:
    have = {a.objectName for a in actuals}
    return sorted(o for o in {k[0] for k in baseline} if o not in have)
