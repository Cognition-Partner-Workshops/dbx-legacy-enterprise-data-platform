# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_DeliveryPerformanceSummary
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_DeliveryPerformanceSummary.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_delivery_performance_summary` (legacy `Aggregate.Delivery Performance Summary`).
# MAGIC
# MAGIC **Strategy: incremental ISO-week window rebuild (Delta `replaceWhere` on `iso_week_start_date`)**
# MAGIC
# MAGIC The legacy package deletes the delivered-date window and rebuilds the weekly carrier/lane grain for it (rolling weeks, `OnTimeGraceHours` grace). Whole ISO weeks are replaced so partially loaded weeks are always recomputed. Conditional split: `ShipmentCount < 5` -> reject (thin sample); cross-border and domestic lanes both insert.
# MAGIC
# MAGIC Legacy control flow: Init Refresh Window -> Log Package Start -> Delete Refresh Window -> Rebuild Aggregate (data flow with derived columns + conditional split) -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts -> Log Package Success; OnError -> Log Error -> Mark Execution Failed.
# MAGIC
# MAGIC `dbx_etl_common` is attached to the job task as a wheel library (see
# MAGIC `resources/wwi_09_aggregates.yml`, `../../common/dbx_etl_common/dist/*.whl`).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402

import agg_common  # noqa: E402
import agg_sql  # noqa: E402
from agg_package import RefreshSpec, runRefresh  # noqa: E402

# COMMAND ----------

# Package-specific parameters ($Package::* in the .dtsx); job parameters override the widget defaults.
dbutils.widgets.text("RefreshFromDate", "")
dbutils.widgets.text("RefreshToDate", "")
dbutils.widgets.text("OnTimeGraceHours", "12")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
reloadFullHistory = p["reloadFullHistory"]
t = agg_common.resolveTables(catalog, naming.table)

# The legacy defaults ("1900-01-01") mean "not supplied"; the window is then derived from BusinessDate.
refreshFromDate = agg_common.getOptionalWidget(dbutils, "RefreshFromDate")
refreshToDate = agg_common.getOptionalWidget(dbutils, "RefreshToDate")
refreshFromDate = "" if refreshFromDate.startswith("1900-") else refreshFromDate
refreshToDate = "" if refreshToDate.startswith("1900-") else refreshToDate
onTimeGraceHours = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "OnTimeGraceHours", "12"), 12.0)

# COMMAND ----------

# Init Refresh Window: default window is the ISO week of BusinessDate plus the preceding week, snapped to Mondays.
window = agg_common.dailyWindow(refreshFromDate, refreshToDate, businessDate,
                                trailingDays=7, reloadFullHistory=reloadFullHistory)
window = agg_common.RefreshWindow(agg_common.isoWeekStart(window.fromDate), window.toDate)
predicate = window.sqlLiteral("iso_week_start_date")
print(f"Refresh window {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.deliveryPerformanceSummarySql(t, window, onTimeGraceHours)

spec = RefreshSpec(
    packageName="AGG_Refresh_DeliveryPerformanceSummary",
    targetKey="agg_delivery_performance_summary",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("consignment_count") < 5,
    rejectReasonCode="AGG_THIN_SAMPLE",
    rejectReason="Thin sample: fewer than 5 consignments",
    keyMeasures=("consignment_count", "on_time_count", "freight_cost_reporting", "service_credit_reporting"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_delivery_performance_summary"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_DeliveryPerformanceSummary",
    "target": t["agg_delivery_performance_summary"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "refreshFromDate": window.fromDate.isoformat(), "refreshToDate": window.toDate.isoformat(),
}))
