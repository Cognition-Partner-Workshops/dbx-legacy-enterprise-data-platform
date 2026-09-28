# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_DailySalesSummary
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_DailySalesSummary.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_daily_sales_summary` (legacy `Aggregate.Daily Sales Summary`).
# MAGIC
# MAGIC **Strategy: incremental window rebuild (Delta `replaceWhere` on `sales_date`)**
# MAGIC
# MAGIC The legacy package deletes `[Invoice Date Key]` between RefreshFromDate and RefreshToDate and re-inserts only that window (late-arriving invoices/returns), so a full-rebuild materialized view would re-scan all of `fact_sale` daily; a windowed overwrite preserves the legacy cost profile and idempotency. Conditional split: `CustomerCount < 3` -> reject (privacy suppression), `MarginAmount < 0` and default -> insert.
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

# COMMAND ----------

# Init Refresh Window + Delete Refresh Window: the window is replaced atomically with Delta replaceWhere.
window = agg_common.dailyWindow(refreshFromDate, refreshToDate, businessDate,
                                trailingDays=3, reloadFullHistory=reloadFullHistory)
predicate = window.sqlLiteral("sales_date")
print(f"Refresh window {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.dailySalesSummarySql(t, window)

spec = RefreshSpec(
    packageName="AGG_Refresh_DailySalesSummary",
    targetKey="agg_daily_sales_summary",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("distinct_customer_count") < 3,
    rejectReasonCode="AGG_SUPPRESSED_SMALL_CELL",
    rejectReason="Suppressed small cell: fewer than 3 distinct customers",
    keyMeasures=("net_sales_amount", "gross_margin_amount", "quantity_sold_base_uom", "invoice_count"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_daily_sales_summary"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_DailySalesSummary",
    "target": t["agg_daily_sales_summary"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "refreshFromDate": window.fromDate.isoformat(), "refreshToDate": window.toDate.isoformat(),
}))
