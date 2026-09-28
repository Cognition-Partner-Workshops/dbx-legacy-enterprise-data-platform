# Databricks notebook source
# MAGIC %md
# MAGIC # RECON_SqlServerExtract — raw-layer reconciliation for `02_sqlserver_extract`
# MAGIC
# MAGIC Spark port of `validation/runtime/02_row_count_reconciliation.sql` for the 22 bronze targets
# MAGIC written by the `EXT_SQL_*` notebooks. For every target it computes the Delta row count
# MAGIC (total and for the given `BatchId`) and an order-independent `bit_xor(xxhash64(...))` row hash over
# MAGIC the business columns, compares them with the SQL Server baseline and writes one
# MAGIC `etl.row_count_log` row per object through `control.logRowCount`.
# MAGIC
# MAGIC Baseline input (either):
# MAGIC * `BaselineTable` — a Delta table with `ObjectName`, `SourceRowCount`, `SourceRowHash` (nullable) captured
# MAGIC   from SQL Server (`SELECT 'raw.SqlOrder', COUNT(*) ... FROM raw.SqlOrder WHERE BatchId = @BatchId`), or
# MAGIC * `BaselineJson` — `{"raw.SqlOrder": 1234, "bronze.raw_sql_invoice": {"rowCount": 99, "rowHash": -42}}`
# MAGIC   or a list of `{ObjectName, SourceRowCount, SourceRowHash}` objects.
# MAGIC
# MAGIC Tolerance comes from `etl.configuration` key `RowCountVariancePercentTolerance` (fallback 0.5 %, the value
# MAGIC used by the legacy runtime query). The notebook also surfaces the three legacy result sets: logged
# MAGIC hops per package, packages that ran without a row-count row ("no audit"), and rejects outstanding past SLA.

# COMMAND ----------

import json
import os
import sys
from decimal import Decimal

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

from wwi_sqlserver_extract import extract, reconcile, specs  # noqa: E402

# COMMAND ----------

for widgetName, widgetDefault in extract.JOB_PARAMETER_DEFAULTS.items():
    dbutils.widgets.text(widgetName, widgetDefault)
dbutils.widgets.text("BaselineTable", "")
dbutils.widgets.text("BaselineJson", "")
dbutils.widgets.text("RejectSlaDays", "3")
dbutils.widgets.text("FailOnVariance", "False")

jobParams = params.getJobParams(dbutils)
catalog = jobParams["catalog"]
batchId = int(jobParams["batchId"])
baselineTable = dbutils.widgets.get("BaselineTable").strip() or None
baselineJson = dbutils.widgets.get("BaselineJson").strip() or None
rejectSlaDays = int(dbutils.widgets.get("RejectSlaDays") or "3")
failOnVariance = dbutils.widgets.get("FailOnVariance").strip().lower() in ("true", "1", "yes")

# COMMAND ----------

packageExecutionId = control.logPackageStart(
    spark, catalog, batchId, "RECON_SqlServerExtract", projectName=specs.PROJECT_NAME, stepName="Reconcile Raw"
)
try:
    baseline = reconcile.loadBaseline(spark, baselineTable, baselineJson)
    tolerancePercent = reconcile.resolveTolerancePercent(spark, catalog, control, jobParams["environmentCode"])
    results = reconcile.reconcileTargets(
        spark, catalog, naming, control, int(packageExecutionId), baseline, batchId or None, tolerancePercent
    )
    results = results.cache()
    display(results.orderBy("Status", "ObjectName"))
    failing = results.filter(results.Status.isin("OutsideTolerance", "HashMismatch", "MissingTarget")).count()
    control.logPackageEnd(
        spark,
        catalog,
        packageExecutionId,
        status="Succeeded" if failing == 0 else "SucceededWithWarnings",
        rowsRead=results.count(),
        rowsRejected=failing,
    )
except Exception as exc:
    control.logError(
        spark,
        catalog,
        packageExecutionId=packageExecutionId,
        batchId=batchId,
        errorSeverity="Error",
        sourceName="RECON_SqlServerExtract",
        errorDescription=("%s: %s" % (type(exc).__name__, exc))[:4000],
    )
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Legacy result sets (ported from `02_row_count_reconciliation.sql`)

# COMMAND ----------

if batchId:
    display(spark.sql(reconcile.loggedHopsSql(catalog, batchId)))
    display(spark.sql(reconcile.missingRowCountSql(catalog, batchId)))
display(spark.sql(reconcile.outstandingRejectsSql(catalog, rejectSlaDays)))

# COMMAND ----------

if batchId:
    # usp_AssertRowCountReconciliation semantics: fail the batch only when explicitly requested.
    control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=failOnVariance)

summary = {
    "packageExecutionId": int(packageExecutionId),
    "objects": results.count(),
    "failing": failing,
    "tolerancePercent": str(tolerancePercent),
}
dbutils.notebook.exit(json.dumps(summary))
