"""AGG_Publish_ReportingLayer: gate the aggregates, publish report_* views, keep prior state on failure."""

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_performance import aggregates, config
from sales_performance.commissions import writeMetrics
from sales_performance.common import readTable, saveTable, withAudit
from sales_performance.sale_line import FACT_SALE_TABLE

PACKAGE = "AGG_Publish_ReportingLayer"
STATE_TABLE = "gold_publish_state"
GATE_TABLE = "gold_publish_gate_result"

REPORT_VIEWS = {
    "report_vw_monthly_sales_summary": (aggregates.MONTHLY_SALES_TABLE, ["customer_key", "region_code", "fiscal_year", "fiscal_period"]),
    "report_vw_regional_sales_performance": (aggregates.REGIONAL_TABLE, ["region_code", "territory_code", "fiscal_year", "fiscal_period"]),
    "report_vw_product_performance": (aggregates.PRODUCT_TABLE, ["stock_item_key", "region_code", "calendar_month"]),
    "report_vw_promotion_effectiveness": (aggregates.PROMO_EFFECT_TABLE, ["promotion_id"]),
    "report_vw_monthly_margin_analysis": (
        aggregates.MARGIN_TABLE,
        ["product_category", "territory_code", "region_code", "fiscal_year", "fiscal_period"],
    ),
}
# Aggregates the legacy publish also refreshed but that belong to sibling groups (see README).
OUT_OF_SCOPE_AGGREGATES = (
    "Aggregate.Customer Lifetime Value",
    "Aggregate.Inventory Turnover",
    "Aggregate.Supplier Scorecard",
    "Aggregate.Loyalty Tier Movement",
)


def evaluateGates(spark: SparkSession, staleAfterHours: int = 36, allowEmpty=("gold_agg_promotion_effectiveness",)):
    """Per aggregate: exists, non-empty (unless its source is legitimately empty), no null grain
    keys, refreshed after the fact it depends on and within the staleness threshold."""
    fact = readTable(spark, FACT_SALE_TABLE)
    factLoaded = fact.agg(F.max("loaded_at_utc")).collect()[0][0] if "loaded_at_utc" in fact.columns else None
    results = []
    for view, (table, grain) in REPORT_VIEWS.items():
        fullName = config.tableName(table)
        gate = {"view": view, "table": table, "exists": spark.catalog.tableExists(fullName)}
        if gate["exists"]:
            df = spark.table(fullName)
            gate["row_count"] = df.count()
            nullKeys = df.filter(" OR ".join(f"`{g}` IS NULL" for g in grain)).count()
            gate["null_grain_rows"] = nullKeys
            loadedAt = df.agg(F.max("loaded_at_utc")).collect()[0][0]
            gate["loaded_at_utc"] = loadedAt.isoformat() if loadedAt else None
            fresh = loadedAt is not None and (factLoaded is None or loadedAt >= factLoaded)
            if loadedAt is not None:
                ageHours = spark.sql(
                    f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(TIMESTAMP '{loadedAt.isoformat()}')) / 3600 AS h"
                ).collect()[0]["h"]
                fresh = fresh and ageHours <= staleAfterHours
                gate["age_hours"] = round(float(ageHours), 2)
            gate["fresh"] = fresh
            gate["non_empty"] = gate["row_count"] > 0 or table in allowEmpty
            gate["passed"] = gate["non_empty"] and nullKeys == 0 and fresh
        else:
            gate["passed"] = False
        results.append(gate)
    return results


def publishViews(spark: SparkSession):
    for view, (table, _) in REPORT_VIEWS.items():
        spark.sql(f"CREATE OR REPLACE VIEW {config.tableName(view)} AS SELECT * FROM {config.tableName(table)}")


def recordState(spark: SparkSession, gates, status: str, batchId: int):
    """Every attempt lands in gold_publish_gate_result; gold_publish_state only changes when the
    layer is actually (re)published, so a quarantined run preserves the prior publish state."""
    rows = [(g["view"], g["table"], status, json.dumps(g), bool(g["passed"])) for g in gates]
    df = spark.createDataFrame(
        rows, "report_object_name string, source_table string, publish_status string, gate_result_json string, gate_passed boolean"
    )
    df = withAudit(
        df.withColumn("published_at_utc", F.when(F.col("publish_status").isin("PUBLISHED", "FORCED"), F.current_timestamp())),
        PACKAGE,
        batchId,
    )
    saveTable(df, GATE_TABLE, mode="append")
    if status in ("PUBLISHED", "FORCED") or not spark.catalog.tableExists(config.tableName(STATE_TABLE)):
        saveTable(df, STATE_TABLE)


def runPublish(
    spark: SparkSession,
    batchId: int,
    refreshAggregates: bool = True,
    forcePublish: bool = False,
    staleAfterHours: int = 36,
    monthsBack: int = 0,
):
    if refreshAggregates:
        aggregates.refreshAll(spark, batchId, monthsBack)
    gates = evaluateGates(spark, staleAfterHours)
    allPassed = all(g["passed"] for g in gates)
    if allPassed or forcePublish:
        publishViews(spark)
        status = "PUBLISHED" if allPassed else "FORCED"
    else:
        status = "QUARANTINED"
    recordState(spark, gates, status, batchId)
    metrics = {
        "gate_count": len(gates),
        "gates_passed": sum(1 for g in gates if g["passed"]),
        "published": 1 if status != "QUARANTINED" else 0,
        "views_published": len(REPORT_VIEWS) if status != "QUARANTINED" else 0,
    }
    writeMetrics(spark, PACKAGE, metrics, batchId)
    return {"status": status, "gates": gates, **metrics}
