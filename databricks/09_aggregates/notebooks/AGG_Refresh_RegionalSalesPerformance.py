# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_RegionalSalesPerformance
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_RegionalSalesPerformance.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_regional_sales_performance` (legacy `Aggregate.Regional Sales Performance`).
# MAGIC
# MAGIC **Strategy: incremental accounting-period rebuild (Delta `replaceWhere` on `calendar_month`)**
# MAGIC
# MAGIC The legacy package rebuilds the period, then applies NA sales-tax, EU reverse-charge/VAT and APAC GST adjustments sequentially and refreshes regional rankings; all three are region-conditional expressions in one SQL pass. FX translation difference compares daily-rate reporting amounts with the monthly average rate for `ReportingCurrency`. Conditional split: NA/EU/APAC insert; the default output (unassigned territories / unknown region) rejects.
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
dbutils.widgets.text("RebuildPriorPeriods", "0")
dbutils.widgets.text("ReportingCurrency", "USD")

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
reportingCurrency = agg_common.getOptionalWidget(dbutils, "ReportingCurrency", "USD") or "USD"

# COMMAND ----------

# Init Refresh Period + Delete Refresh Period: whole calendar months are replaced atomically (replaceWhere).
window = agg_common.periodWindow(accountingPeriodCode, rebuildPriorPeriods, businessDate, reloadFullHistory)
predicate = window.sqlLiteral("calendar_month")
print(f"Refresh period {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.regionalSalesPerformanceSql(t, window, reportingCurrency)

spec = RefreshSpec(
    packageName="AGG_Refresh_RegionalSalesPerformance",
    targetKey="agg_regional_sales_performance",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=~F.col("region_code").isin("NA", "EU", "APAC"),
    rejectReasonCode="AGG_UNASSIGNED_TERRITORY",
    rejectReason="Unassigned territory: region is not NA/EU/APAC",
    keyMeasures=("net_sales_daily_rate", "gross_margin_reporting", "order_count", "budget_net_sales_reporting"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_regional_sales_performance"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_RegionalSalesPerformance",
    "target": t["agg_regional_sales_performance"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
