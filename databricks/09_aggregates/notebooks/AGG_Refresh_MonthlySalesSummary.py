# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_MonthlySalesSummary
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_MonthlySalesSummary.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_monthly_sales_summary` (legacy `Aggregate.Monthly Sales Summary`).
# MAGIC
# MAGIC **Strategy: incremental accounting-period rebuild (Delta `replaceWhere` on `calendar_month`)**
# MAGIC
# MAGIC The legacy package rebuilds `AccountingPeriodCode` and `RebuildPriorPeriods` preceding open periods; closed periods are never touched, so a full-rebuild materialized view would violate the period lock. Regional fiscal calendars (NA JAN445 / EU APR12 / APAC JUL13) and EU reverse-charge revenue are applied in the SQL. Conditional split: `NetAmount == 0` -> reject (empty period); loss-making and default -> insert.
# MAGIC
# MAGIC Legacy control flow: Init Refresh Period -> Log Package Start -> Delete Refresh Period -> Rebuild Aggregate (data flow with derived columns + conditional split) -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts -> Log Package Success; OnError -> Log Error -> Mark Execution Failed.
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
dbutils.widgets.text("AccountingPeriodCode", "")
dbutils.widgets.text("RebuildPriorPeriods", "0")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
reloadFullHistory = p["reloadFullHistory"]
t = agg_common.resolveTables(catalog, naming.table)

# The legacy default ("1900-01") means "not supplied"; the period is then the month of BusinessDate.
accountingPeriodCode = agg_common.getOptionalWidget(dbutils, "AccountingPeriodCode")
accountingPeriodCode = "" if accountingPeriodCode.startswith("1900-") else accountingPeriodCode
rebuildPriorPeriods = agg_common.parseInt(agg_common.getOptionalWidget(dbutils, "RebuildPriorPeriods", "0"), 0)

# COMMAND ----------

# Init Refresh Period + Delete Refresh Period: whole calendar months are replaced atomically (replaceWhere).
window = agg_common.periodWindow(accountingPeriodCode, rebuildPriorPeriods, businessDate, reloadFullHistory)
predicate = window.sqlLiteral("calendar_month")
print(f"Refresh period {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.monthlySalesSummarySql(t, window)

spec = RefreshSpec(
    packageName="AGG_Refresh_MonthlySalesSummary",
    targetKey="agg_monthly_sales_summary",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("net_revenue_reporting") == 0,
    rejectReasonCode="AGG_EMPTY_PERIOD",
    rejectReason="Empty period: zero net revenue",
    keyMeasures=("net_revenue_reporting", "gross_margin_reporting", "order_count", "invoice_count"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_monthly_sales_summary"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_MonthlySalesSummary",
    "target": t["agg_monthly_sales_summary"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
