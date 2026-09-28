# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Refresh_FinanceCloseSummary
# MAGIC Migrated from `ssis/09_aggregates/AGG_Refresh_FinanceCloseSummary.dtsx` (generator `build_aggregate_packages.py`).
# MAGIC Target: `${catalog}.gold.agg_finance_close_summary` (legacy `Aggregate.Finance Close Summary`).
# MAGIC
# MAGIC **Strategy: incremental accounting-period rebuild (Delta `replaceWhere` on `fiscal_year`/`fiscal_period`)**
# MAGIC
# MAGIC The legacy package rebuilds the fiscal periods whose postings fall inside the requested window, compares GL closing balances with the AR/AP sub-ledgers and applies regional materiality (`MaterialityAmount`, capped for EU/APAC). Runs in the Close Aggregates phase. Conditional split: `!IsBalanced` -> reject (control variance); high manual-journal share and default -> insert.
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
dbutils.widgets.text("MaterialityAmount", "100")

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
materialityAmount = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "MaterialityAmount", "100"), 100.0)

# COMMAND ----------

window = agg_common.periodWindow(accountingPeriodCode, rebuildPriorPeriods, businessDate, reloadFullHistory)
# Finance close is keyed by fiscal period, so the replaced partition is the set of periods posted in the window.
periodKeys = [r["k"] for r in spark.sql(
    f"SELECT DISTINCT fiscal_year * 100 + fiscal_period AS k FROM {t['fact_gl_posting']} "
    f"WHERE {window.sqlLiteral('posting_date_key')}").collect()]
predicate = ("fiscal_year * 100 + fiscal_period IN (" + ",".join(str(k) for k in periodKeys) + ")") if periodKeys else "1 = 0"
print(f"Refresh window {window.fromDate} .. {window.toDate}; fiscal periods {sorted(periodKeys)}")

# COMMAND ----------

sql = agg_sql.financeCloseSummarySql(t, window, materialityAmount)

spec = RefreshSpec(
    packageName="AGG_Refresh_FinanceCloseSummary",
    targetKey="agg_finance_close_summary",
    sql=sql,
    replacePredicate=predicate,
    rejectCondition=~F.col("within_tolerance_flag"),
    rejectReasonCode="AGG_CONTROL_VARIANCE",
    rejectReason="Control variance: sub-ledger to GL difference exceeds tolerance",
    keyMeasures=("closing_balance_reporting", "period_debits_local", "period_credits_local", "manual_journal_count"),
    watermarkTo=None,
    stepName="Close Aggregates",
)

# COMMAND ----------

result = runRefresh(spark, control, catalog, batchId, spec, t["agg_finance_close_summary"])

dbutils.notebook.exit(json.dumps({
    "package": "AGG_Refresh_FinanceCloseSummary",
    "target": t["agg_finance_close_summary"],
    "sourceRowCount": result.sourceRowCount,
    "targetRowCount": result.targetRowCount,
    "rejectRowCount": result.rejectRowCount,
    "measureTotals": result.measureTotals,
    "accountingPeriodFrom": window.fromDate.isoformat()[:7], "accountingPeriodTo": window.toDate.isoformat()[:7],
}))
