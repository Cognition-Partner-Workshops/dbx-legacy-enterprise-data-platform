# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_DailyInventoryHealth
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_DailyInventoryHealth.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_daily_inventory_health` (legacy `Aggregate.Daily Inventory Health`).
# MAGIC
# MAGIC **Strategy: incremental snapshot-date rebuild (Delta `replaceWhere` on `snapshot_date`)**
# MAGIC
# MAGIC The legacy package deletes one or more `[Snapshot Date Key]` values and rebuilds them from the daily inventory snapshot; it also runs intraday, so only the affected snapshot dates are replaced. `StockOutThreshold` drives the stock-out SKU count; the derived health rating (RED/AMBER/OVERSTOCK/GREEN) and percentage columns come from the SSIS derived-column component. Conditional split: `SkuCount == 0` -> reject, `HealthRatingCode == "RED"` and default -> insert.
# MAGIC
# MAGIC Legacy control flow: Init Refresh Window -> Log Package Start -> Delete Refresh Window -> Rebuild Aggregate (data flow with derived columns + conditional split) -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts -> Log Package Success; OnError -> Log Error -> Mark Execution Failed.
# MAGIC
# MAGIC `dbx_etl_common` is attached to the job task as a wheel library (see
# MAGIC `resources/wwi_09_aggregates.yml`, variable `dbx_etl_common_wheel`).

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
dbutils.widgets.text("StockOutThreshold", "0")

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
stockOutThreshold = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "StockOutThreshold", "0"), 0.0)

# COMMAND ----------

# Init Refresh Window + Delete Refresh Window: the window is replaced atomically with Delta replaceWhere.
window = agg_common.dailyWindow(refreshFromDate, refreshToDate, businessDate,
                                trailingDays=0, reloadFullHistory=reloadFullHistory)
predicate = window.sqlLiteral("snapshot_date")
print(f"Refresh window {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.dailyInventoryHealthSql(t, window, stockOutThreshold)

spec = RefreshSpec(
    packageName="AGG_Refresh_DailyInventoryHealth",
    targetKey="agg_daily_inventory_health",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("sku_count") == 0,
    rejectReasonCode="AGG_EMPTY_WAREHOUSE",
    rejectReason="Empty warehouse: no SKUs in snapshot",
    keyMeasures=("total_quantity_on_hand", "total_stock_value_reporting", "stockout_sku_count", "sku_count"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_daily_inventory_health"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_DailyInventoryHealth",
    "target": t["agg_daily_inventory_health"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "refreshFromDate": window.fromDate.isoformat(), "refreshToDate": window.toDate.isoformat(),
}))
