# Databricks notebook source
# MAGIC %md
# MAGIC # AGG_Reconcile_Aggregates — reconciliation of gold.agg_* / gold.rpt_* against the SQL Server baseline
# MAGIC
# MAGIC Ports `validation/runtime/02_row_count_reconciliation.sql` (hop counts) to the aggregate layer and adds the
# MAGIC measure-total and deterministic-hash comparison requested for session 09:
# MAGIC
# MAGIC * per target: row count per period (`sales_date`, `snapshot_date`, `calendar_month`, `fiscal_period`,
# MAGIC   `iso_week_start_date`; `*` = whole table), `SUM(xxhash64(concat_ws('|', sorted columns)))` and the
# MAGIC   sum of the key measures;
# MAGIC * baseline supplied either as the `BaselineJson` parameter or as the Delta table `BaselineTable`
# MAGIC   (long form `ObjectName, PeriodValue, RowCount, RowHash, MeasureName, MeasureValue`), captured from
# MAGIC   SQL Server with the equivalent `Aggregate.*` / `Report.*` queries;
# MAGIC * every comparison is written to `etl.row_count_log` through `control.logRowCount`
# MAGIC   (`SourceRowCount` = baseline, `TargetRowCount` = Databricks, `RejectRowCount` = failed metrics).

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params, naming  # noqa: E402

import agg_common  # noqa: E402
import agg_reconcile  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("BaselineJson", "")
dbutils.widgets.text("BaselineTable", "")
dbutils.widgets.text("AbsoluteTolerance", "0")
dbutils.widgets.text("PercentTolerance", "0")
dbutils.widgets.text("FailOnVariance", "False")
dbutils.widgets.text("IncludeReportViews", "True")

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
t = agg_common.resolveTables(catalog, naming.table)

baselineJson = agg_common.getOptionalWidget(dbutils, "BaselineJson", "")
baselineTable = agg_common.getOptionalWidget(dbutils, "BaselineTable", "") \
    or naming.table(catalog, "etl", "agg_reconciliation_baseline")
absoluteTolerance = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "AbsoluteTolerance", "0"), 0.0)
percentTolerance = agg_common.parseDecimal(agg_common.getOptionalWidget(dbutils, "PercentTolerance", "0"), 0.0)
failOnVariance = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "FailOnVariance", "False"), False)
includeReportViews = agg_common.parseBool(agg_common.getOptionalWidget(dbutils, "IncludeReportViews", "True"), True)

# COMMAND ----------

baseline = agg_reconcile.baselineFromJson(baselineJson) or agg_reconcile.baselineFromTable(spark, baselineTable)
print(f"Baseline entries: {len(baseline)} (json={bool(baselineJson)}, table={baselineTable})")

# COMMAND ----------

specs = list(agg_reconcile.RECONCILIATION_SPECS)
if includeReportViews:
    specs += agg_reconcile.REPORT_SPECS

with control.packageRun(spark, catalog, batchId, "AGG_Reconcile_Aggregates",
                        projectName=agg_common.PROJECT_NAME, stepName="Validation") as run:
    packageExecutionId = run.packageExecutionId
    actuals = []
    for spec in specs:
        table = t[spec.targetKey] if spec.targetKey in t else naming.table(catalog, "gold", spec.targetKey)
        actuals += agg_reconcile.summarise(spark, table, spec)

    comparisons = agg_reconcile.compare(actuals, baseline, absoluteTolerance, percentTolerance)
    failed = [c for c in comparisons if not c.withinTolerance]

    for a in actuals:
        b = baseline.get((a.objectName, a.period))
        failedHere = sum(1 for c in failed if c.objectName == a.objectName and c.period == a.period)
        objectName = a.objectName if a.period == agg_reconcile.TOTAL_PERIOD else f"{a.objectName}#{a.period}"
        control.logRowCount(spark, catalog, packageExecutionId, objectName,
                            sourceRowCount=b.rowCount if b else None, targetRowCount=a.rowCount,
                            rejectRowCount=failedHere)

    run.rowsRead = len(actuals)
    run.rowsInserted = len(comparisons)
    run.rowsRejected = len(failed)
    if failed and failOnVariance:
        raise RuntimeError(f"{len(failed)} reconciliation metric(s) outside tolerance: "
                           + "; ".join(f"{c.objectName}[{c.period}].{c.metric} expected {c.expected} got {c.actual}"
                                       for c in failed[:20]))

# COMMAND ----------

display(spark.createDataFrame(
    [(a.objectName, a.period, a.rowCount, a.rowHash, json.dumps(a.measures)) for a in actuals],
    "object_name string, period string, row_count long, row_hash long, measures string"))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "objectsSummarised": len(actuals),
    "comparisons": len(comparisons),
    "failed": [{"objectName": c.objectName, "period": c.period, "metric": c.metric,
                "expected": c.expected, "actual": c.actual} for c in failed],
    "missingBaselines": agg_reconcile.missingBaselines(actuals, baseline),
}))
