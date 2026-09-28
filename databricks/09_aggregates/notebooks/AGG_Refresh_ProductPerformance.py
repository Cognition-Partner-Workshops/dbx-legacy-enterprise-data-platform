# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_ProductPerformance
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_ProductPerformance.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_product_performance` (legacy `Aggregate.Product Performance`).
# MAGIC
# MAGIC **Strategy: incremental accounting-period rebuild (Delta `replaceWhere` on `calendar_month`)**
# MAGIC
# MAGIC The legacy package rebuilds the requested period(s) only and then recalculates ABC classification (`AbcThresholdA`/`AbcThresholdB` cumulative revenue cut-offs) and XYZ demand variability; prior-month ABC class and ranks are window functions. Conditional split: `QuantitySold == 0` -> reject (non-moving); high-return-rate and default -> insert.
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
dbutils.widgets.text("AbcThresholdA", "80")
dbutils.widgets.text("AbcThresholdB", "95")

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
abcThresholdA = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "AbcThresholdA", "80"), 80.0) / 100.0
abcThresholdB = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "AbcThresholdB", "95"), 95.0) / 100.0

# COMMAND ----------

# Init Refresh Period + Delete Refresh Period: whole calendar months are replaced atomically (replaceWhere).
window = agg_common.periodWindow(accountingPeriodCode, rebuildPriorPeriods, businessDate, reloadFullHistory)
predicate = window.sqlLiteral("calendar_month")
print(f"Refresh period {window.fromDate} .. {window.toDate}")

# COMMAND ----------

sql = agg_sql.productPerformanceSql(t, window, abcThresholdA, abcThresholdB)

spec = RefreshSpec(
    packageName="AGG_Refresh_ProductPerformance",
    targetKey="agg_product_performance",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("units_sold_base_uom") == 0,
    rejectReasonCode="AGG_NON_MOVING",
    rejectReason="Non-moving product: zero units sold",
    keyMeasures=("units_sold_base_uom", "net_revenue_reporting", "gross_margin_reporting", "units_returned"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_product_performance"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_ProductPerformance",
    "target": t["agg_product_performance"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
