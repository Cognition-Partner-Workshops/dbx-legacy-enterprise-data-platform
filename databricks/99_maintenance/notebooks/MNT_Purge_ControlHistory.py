# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Purge_ControlHistory
# MAGIC Legacy: `ssis/99_maintenance/MNT_Purge_ControlHistory.dtsx`.
# MAGIC Archives closed batches into `etl.batch_archive`, then delegates the child-before-parent purge of
# MAGIC `etl.batch_step`, `etl.batch`, `etl.package_execution`, `etl.error_log`, `etl.rejected_record` and the
# MAGIC quality history to `control.purgeControlHistory` (the port of `etl.usp_PurgeControlHistory`).
# MAGIC Watermarks are never purged; unresolved rejects are retained.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Purge_ControlHistory"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
batchRetentionDays = mnt.toInt(mnt.widgetOr(dbutils, "BatchRetentionDays", "400"), 400)
errorRetentionDays = mnt.toInt(mnt.widgetOr(dbutils, "ErrorRetentionDays", "180"), 180)
rejectRetentionDays = mnt.toInt(mnt.widgetOr(dbutils, "RejectRetentionDays", "90"), 90)
dryRun = mnt.toBool(mnt.widgetOr(dbutils, "DryRun", "False"))

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Purge")
try:
    archiveTable = mnt.ensureOpsTable(spark, catalog, "etl.batch_archive")
    batchTable = naming.table(catalog, "etl", "batch")
    stepTable = naming.table(catalog, "etl", "batch_step")

    archiveSql = f"""
        INSERT INTO {mnt.quoted(archiveTable)}
            (BatchId, BatchType, BatchStatus, StartedAtUtc, EndedAtUtc, StepCount, FailedStepCount, ArchivedAtUtc)
        SELECT b.BatchId, b.BatchType, b.Status, b.StartedAtUtc, b.CompletedAtUtc,
               (SELECT COUNT(*) FROM {mnt.quoted(stepTable)} s WHERE s.BatchId = b.BatchId),
               (SELECT COUNT(*) FROM {mnt.quoted(stepTable)} s WHERE s.BatchId = b.BatchId AND s.Status = 'Failed'),
               current_timestamp()
        FROM {mnt.quoted(batchTable)} b
        WHERE b.CompletedAtUtc < current_timestamp() - INTERVAL {batchRetentionDays} DAYS
          AND b.Status <> 'Running'
          AND NOT EXISTS (SELECT 1 FROM {mnt.quoted(archiveTable)} a WHERE a.BatchId = b.BatchId)
    """
    if dryRun:
        toArchive = spark.sql(
            f"SELECT COUNT(*) FROM {mnt.quoted(batchTable)} b "
            f"WHERE b.CompletedAtUtc < current_timestamp() - INTERVAL {batchRetentionDays} DAYS "
            f"AND b.Status <> 'Running' "
            f"AND NOT EXISTS (SELECT 1 FROM {mnt.quoted(archiveTable)} a WHERE a.BatchId = b.BatchId)"
        ).collect()[0][0]
        mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, f"DRYRUN: {toArchive} batches would be archived")
    else:
        spark.sql(archiveSql)

    control.purgeControlHistory(
        spark, catalog, retentionDays=None,
        executionHistoryMonths=mnt.monthsFromDays(batchRetentionDays),
        errorHistoryMonths=mnt.monthsFromDays(errorRetentionDays),
        rejectHistoryMonths=mnt.monthsFromDays(rejectRetentionDays),
        qualityHistoryMonths=mnt.monthsFromDays(batchRetentionDays),
        whatIf=dryRun)

    measure = spark.sql(f"""
        SELECT (SELECT COUNT(*) FROM {mnt.quoted(archiveTable)}) AS ArchivedBatches,
               (SELECT COUNT(*) FROM {mnt.quoted(naming.table(catalog, 'etl', 'error_log'))}) AS RemainingErrors,
               (SELECT COUNT(*) FROM {mnt.quoted(naming.table(catalog, 'etl', 'rejected_record'))}) AS RemainingRejects,
               (SELECT COUNT(*) FROM {mnt.quoted(batchTable)}) AS RemainingBatches
    """).collect()[0]
    mnt.insertMaintenanceLog(
        spark, catalog, PACKAGE_NAME,
        f"Archived batches: {measure['ArchivedBatches']}; remaining errors: {measure['RemainingErrors']}; "
        f"remaining rejects: {measure['RemainingRejects']}; dryRun={dryRun}")
    control.logRowCount(spark, catalog, packageExecutionId, "etl.batch",
                        targetRowCount=measure["RemainingBatches"])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=measure["RemainingBatches"])
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Purge Control History Proc", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise
