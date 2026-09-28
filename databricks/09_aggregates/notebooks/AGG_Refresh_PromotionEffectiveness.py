# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_PromotionEffectiveness
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_PromotionEffectiveness.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_promotion_effectiveness` (legacy `Aggregate.Promotion Effectiveness`).
# MAGIC
# MAGIC **Strategy: incremental rebuild of promotions active in the period (Delta `replaceWhere` on `promotion_key`)**
# MAGIC
# MAGIC The legacy package rebuilds every promotion overlapping the accounting-period window, with a same-length pre-promotion baseline (`BaselineWeeks` bounds the look-back) and EU consent restrictions. Promotions are the natural partition, so the affected `promotion_key`s are replaced. Conditional split: `BaselineWeeklyQuantity == 0` -> reject (no baseline); value-destroying and default -> insert.
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
dbutils.widgets.text("BaselineWeeks", "8")

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
baselineWeeks = agg_common.parseInt(agg_common.getOptionalWidget(dbutils, "BaselineWeeks", "8"), 8)

# COMMAND ----------

window = agg_common.periodWindow(accountingPeriodCode, rebuildPriorPeriods, businessDate, reloadFullHistory)
promotionKeys = [r["promotion_key"] for r in spark.sql(
    f"SELECT promotion_key FROM {t['dim_promotion']} WHERE promotion_key > 0 "
    f"AND end_date >= DATE'{window.fromDate}' AND start_date <= DATE'{window.toDate}'").collect()]
predicate = ("promotion_key IN (" + ",".join(str(k) for k in promotionKeys) + ")") if promotionKeys else "1 = 0"
print(f"Refresh window {window.fromDate} .. {window.toDate}; {len(promotionKeys)} promotions")

# COMMAND ----------

sql = agg_sql.promotionEffectivenessSql(t, window, baselineWeeks)

spec = RefreshSpec(
    packageName="AGG_Refresh_PromotionEffectiveness",
    targetKey="agg_promotion_effectiveness",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=F.col("baseline_revenue_reporting") == 0,
    rejectReasonCode="AGG_NO_BASELINE",
    rejectReason="No baseline trading before promotion",
    keyMeasures=("promotion_revenue_reporting", "incremental_revenue_reporting", "discount_cost_reporting", "promoted_units_sold"),
    watermarkTo=None,
    stepName="Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_promotion_effectiveness"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_PromotionEffectiveness",
    "target": t["agg_promotion_effectiveness"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
