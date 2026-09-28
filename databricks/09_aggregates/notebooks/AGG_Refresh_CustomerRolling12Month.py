# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_CustomerRolling12Month
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_CustomerRolling12Month.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_customer_rolling_12_month` (legacy `Aggregate.Customer Rolling 12 Month`).
# MAGIC
# MAGIC **Strategy: incremental rolling-window rebuild (Delta `replaceWhere` on `calendar_month`)**
# MAGIC
# MAGIC The legacy package deletes accounting periods from `AccountingPeriodCode - 11` months and rebuilds the rolling window (`RollingMonths`); older months are immutable. Rolling 12/3-month sums, trend and consecutive-inactive-month counters are window functions over an 11-month look-back read. The split routes established / new / churn-risk rows to the same table; only source errors reject.
# MAGIC
# MAGIC Legacy control flow: Init Refresh Period -> Log Package Start -> Delete Refresh Period -> Rebuild Aggregate (data flow with derived columns + conditional split) -> Assert Reconciliation -> Log Rejected Records -> Log Row Counts -> Log Package Success; OnError -> Log Error -> Mark Execution Failed.
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
dbutils.widgets.text("AccountingPeriodCode", "")
dbutils.widgets.text("RollingMonths", "12")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
reloadFullHistory = p["reloadFullHistory"]
t = agg_common.resolveTables(catalog, naming.table)

accountingPeriodCode = agg_common.getOptionalWidget(dbutils, "AccountingPeriodCode")
accountingPeriodCode = "" if accountingPeriodCode.startswith("1900-") else accountingPeriodCode
rollingMonths = agg_common.parseInt(agg_common.getOptionalWidget(dbutils, "RollingMonths", "12"), 12)

# COMMAND ----------

asAtMonth = agg_common.accountingPeriodStart(accountingPeriodCode, businessDate)
rollingFrom = agg_common.addMonths(asAtMonth, -(rollingMonths - 1))
window = agg_common.RefreshWindow(rollingFrom, agg_common.monthEnd(asAtMonth))
predicate = window.sqlLiteral("calendar_month")
print(f"Rolling window {window.fromDate} .. {window.toDate} ({rollingMonths} months)")

# COMMAND ----------

sql = agg_sql.customerRolling12MonthSql(t, rollingFrom, asAtMonth, rollingMonths)

spec = RefreshSpec(
    packageName="AGG_Refresh_CustomerRolling12Month",
    targetKey="agg_customer_rolling_12_month",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=None,
    rejectReasonCode="AGG_QUALITY_GATE",
    rejectReason="Aggregate row failed the summary quality gate",
    keyMeasures=("net_revenue_reporting", "gross_margin_reporting", "order_count", "rolling_12_month_revenue"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_customer_rolling_12_month"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_CustomerRolling12Month",
    "target": t["agg_customer_rolling_12_month"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
