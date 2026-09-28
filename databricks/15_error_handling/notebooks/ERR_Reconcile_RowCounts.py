# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Reconcile_RowCounts
# MAGIC Row-count reconciliation (legacy `ssis/15_error_handling/ERR_Reconcile_RowCounts.dtsx`).
# MAGIC
# MAGIC Builds the per-object reconciliation set for the batch from `etl.row_count_log` (+ `etl.rejected_record`,
# MAGIC `etl.row_count_tolerance`), evaluates it exactly like the legacy derived columns
# MAGIC (`MATCHED` / `EXPLAINED` / `TOLERATED` / `FAILED`), writes every object to `etl.reconciliation_result`
# MAGIC (`ReconciliationName = 'ROW_COUNT'`) and the failures to `silver.work_row_count_failure`, then hands the
# MAGIC gate to `control.assertRowCountReconciliation` when anything failed and `RaiseOnFailure` is true.
# MAGIC
# MAGIC Package parameters: `DefaultTolerancePercent` (default `0`), `RaiseOnFailure` (default `True`).

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import reconcile, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Reconcile_RowCounts"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])
defaultTolerancePercent = widgets.getInt(dbutils, "DefaultTolerancePercent", 0)
raiseOnFailure = widgets.getBool(dbutils, "RaiseOnFailure", True)

rowCountLogTable = naming.table(catalog, "etl", "row_count_log")
packageExecutionTable = naming.table(catalog, "etl", "package_execution")
rejectedRecordTable = naming.table(catalog, "etl", "rejected_record")
toleranceTable = naming.table(catalog, "etl", "row_count_tolerance")
reconciliationResultTable = naming.table(catalog, "etl", "reconciliation_result")
reconciliationSetTable = tables.ensureWorkTable(spark, naming, catalog, "work_row_count_reconciliation")
failureTable = tables.ensureWorkTable(spark, naming, catalog, "work_row_count_failure")

# COMMAND ----------

# MAGIC %md ## Build the reconciliation set
# MAGIC `etl.row_count_log` records `SourceRowCount` (rows read into the hop) and `TargetRowCount` (rows landed) per package
# MAGIC execution; the legacy `CountStage` pivot has no column in the control schema, so `StagingRowCount` is the summed
# MAGIC `SourceRowCount` of the hop and `SourceRowCount` mirrors it (see the mapping doc, "needs decision").


def buildReconciliationSet():
    tables.clearForBatch(spark, reconciliationSetTable, batchId, column="BatchId")
    toleranceJoin = ""
    toleranceSelect = "CAST(%d AS DECIMAL(9,4)) AS TolerancePercent, CAST(NULL AS STRING) AS ExplanationCode" % defaultTolerancePercent
    if spark.catalog.tableExists(toleranceTable):
        toleranceJoin = "LEFT OUTER JOIN %s t ON t.ObjectName = a.ObjectName" % toleranceTable
        toleranceSelect = "COALESCE(t.TolerancePercent, CAST(%d AS DECIMAL(9,4))) AS TolerancePercent, t.ExplanationCode" % defaultTolerancePercent
    spark.sql(
        """
        INSERT INTO {reconSet} (BatchId, ObjectName, SourceRowCount, StagingRowCount, TargetRowCount, RejectedRowCount, TolerancePercent, ExplanationCode)
        SELECT pe.BatchId, a.ObjectName,
               SUM(COALESCE(a.SourceRowCount, 0)) AS SourceRowCount,
               SUM(COALESCE(a.SourceRowCount, 0)) AS StagingRowCount,
               SUM(COALESCE(a.TargetRowCount, 0)) AS TargetRowCount,
               COALESCE((SELECT COUNT(*) FROM {rejected} r WHERE r.BatchId = pe.BatchId AND r.ObjectName = a.ObjectName), 0) AS RejectedRowCount,
               {toleranceSelect}
        FROM {rowCountLog} a
        JOIN {packageExecution} pe ON pe.PackageExecutionId = a.PackageExecutionId
        {toleranceJoin}
        WHERE pe.BatchId = {batchId}
        GROUP BY pe.BatchId, a.ObjectName{groupTolerance}
        """.format(reconSet=reconciliationSetTable, rejected=rejectedRecordTable, rowCountLog=rowCountLogTable, packageExecution=packageExecutionTable,
                   toleranceJoin=toleranceJoin, toleranceSelect=toleranceSelect, batchId=batchId,
                   groupTolerance=", t.TolerancePercent, t.ExplanationCode" if toleranceJoin else "")
    )
    return spark.table(reconciliationSetTable).where(F.col("BatchId") == batchId)


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Failure Handling")
result = {"package": PACKAGE_NAME, "batchId": batchId}
try:
    evaluated = reconcile.evaluateDataFrame(buildReconciliationSet()).cache()
    reconciledObjects = evaluated.count()

    spark.sql("DELETE FROM %s WHERE BatchId = %d AND ReconciliationName = 'ROW_COUNT'" % (reconciliationResultTable, batchId))
    (evaluated.select(
        F.col("BatchId"), F.lit("ROW_COUNT").alias("ReconciliationName"), F.col("ObjectName"), F.lit(None).cast("string").alias("SourceKey"),
        F.lit(None).cast("string").alias("LedgerCode"), F.lit(None).cast("string").alias("AccountingPeriod"), F.lit(None).cast("string").alias("AccountCode"),
        F.lit(None).cast("string").alias("RegionCode"),
        F.col("ExpectedTargetRowCount").cast(T.DecimalType(19, 4)).alias("SourceAmount"),
        F.col("TargetRowCount").cast(T.DecimalType(19, 4)).alias("TargetAmount"),
        F.col("DifferenceRowCount").cast(T.DecimalType(19, 4)).alias("VarianceAmount"),
        F.col("ReconciliationStatus").alias("VarianceStatus"), F.col("ExplanationCode"), F.col("EvaluatedAtUtc"))
     .write.format("delta").mode("append").saveAsTable(reconciliationResultTable))

    tables.clearForBatch(spark, failureTable, batchId, column="BatchId")
    failed = evaluated.where(F.col("ReconciliationStatus") == reconcile.FAILED)
    failed.select(*[f.name for f in spark.table(failureTable).schema.fields]).write.format("delta").mode("append").saveAsTable(failureTable)

    failedObjectCount = failed.count()
    toleratedCount = evaluated.where(F.col("ReconciliationStatus") == reconcile.TOLERATED).count()
    result.update({"reconciledObjects": reconciledObjects, "failedObjectCount": failedObjectCount, "toleratedCount": toleratedCount})
    print(json.dumps(result))

    controlFailedObjectCount = None
    if failedObjectCount > 0 and raiseOnFailure:
        # Legacy Assert Row Count Reconciliation (etl.usp_AssertRowCountReconciliation @RaiseOnFailure = 1).
        controlFailedObjectCount = control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=True)
    result["controlFailedObjectCount"] = controlFailedObjectCount
    control.logRowCount(spark, catalog, packageExecutionId, "etl.RowCountAudit", sourceRowCount=reconciledObjects, targetRowCount=reconciledObjects,
                        insertRowCount=reconciledObjects, rejectRowCount=failedObjectCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=reconciledObjects, rowsInserted=reconciledObjects, rowsRejected=failedObjectCount)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="failedObjectCount", value=result["failedObjectCount"])
dbutils.jobs.taskValues.set(key="toleratedCount", value=result["toleratedCount"])
dbutils.notebook.exit(json.dumps(result))
