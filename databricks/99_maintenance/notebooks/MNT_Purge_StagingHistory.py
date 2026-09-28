# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Purge_StagingHistory
# MAGIC Legacy: `ssis/99_maintenance/MNT_Purge_StagingHistory.dtsx` (WWI_Maintenance).
# MAGIC Retention deletes on `bronze.raw_*`, `silver.stg_*`, `silver.err_*`, `silver.work_*` by load-date /
# MAGIC BatchId age, one `etl.purge_audit` row per table, then `VACUUM` with the configured retention.
# MAGIC Delta deletes are single transactions, so `DeleteChunkRows` is accepted for compatibility only.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Purge_StagingHistory"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
defaultRetentionDays = mnt.toInt(mnt.widgetOr(dbutils, "DefaultRetentionDays", "90"), 90)
deleteChunkRows = mnt.toInt(mnt.widgetOr(dbutils, "DeleteChunkRows", "50000"), 50000)
dryRun = mnt.toBool(mnt.widgetOr(dbutils, "DryRun", "False"))
runVacuum = mnt.toBool(mnt.widgetOr(dbutils, "RunVacuum", "True"))
vacuumRetentionHours = mnt.toInt(mnt.widgetOr(dbutils, "VacuumRetentionHours", "0"), 0) or None

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Purge")
totalDeleted = 0
try:
    planTable = mnt.ensureWorkTable(spark, catalog, "silver.work_staging_purge_plan")
    auditTable = mnt.ensureOpsTable(spark, catalog, "etl.purge_audit")

    register = None
    if mnt.tableExists(spark, catalog, "etl", "staging_table_register"):
        register = [r.asDict() for r in spark.table(naming.table(catalog, "etl", "staging_table_register")).collect()]

    asOfDate = mnt.utcNow().date()
    plan = mnt.buildStagingPurgePlan(spark, catalog, defaultRetentionDays, asOfDate, register)
    plannedAt = mnt.utcNow()
    mnt.insertRows(spark, planTable,
                   [dict(r, PlannedAtUtc=plannedAt) for r in plan],
                   mnt.WORK_TABLE_DDL["silver.work_staging_purge_plan"])

    for row in plan:
        fqn = naming.table(catalog, row["SchemaName"], row["TableName"])
        if not row["Predicate"]:
            mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                                     f"SKIPPED {fqn}: no load-date or BatchId column to age by")
            continue
        if dryRun:
            wouldDelete = spark.sql(f"SELECT COUNT(*) FROM {mnt.quoted(fqn)} WHERE {row['Predicate']}").collect()[0][0]
            mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                                     f"DRYRUN {fqn}: {wouldDelete} rows older than {row['CutoffDate']}")
            continue
        result = spark.sql(f"DELETE FROM {mnt.quoted(fqn)} WHERE {row['Predicate']}").collect()
        deleted = int(result[0][0]) if result and result[0][0] is not None else 0
        totalDeleted += deleted
        spark.sql(
            f"INSERT INTO {mnt.quoted(auditTable)} (BatchId, SchemaName, TableName, CutoffDate, RowsDeleted, PurgedAtUtc) "
            f"VALUES ({batchId if batchId is not None else 'NULL'}, {mnt.sqlString(row['SchemaName'])}, "
            f"{mnt.sqlString(row['TableName'])}, DATE'{row['CutoffDate'].isoformat()}', {deleted}, current_timestamp())")
        if runVacuum:
            detail = mnt.deltaTableDetail(spark, fqn)
            tableRetention = mnt.deletedFileRetentionHours(detail["properties"])
            retain = vacuumRetentionHours
            if retain is not None and retain < tableRetention:
                mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                                         f"VACUUM {fqn}: requested RETAIN {retain} HOURS is below "
                                         f"delta.deletedFileRetentionDuration ({tableRetention}h); using table setting")
                retain = None
            spark.sql(mnt.vacuumStatement(fqn, retain))

    mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, f"Rows deleted in this run: {totalDeleted}")
    control.logRowCount(spark, catalog, packageExecutionId, "silver.work_staging_purge_plan",
                        sourceRowCount=len(plan), targetRowCount=len(plan), deleteRowCount=totalDeleted)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(plan), rowsDeleted=totalDeleted)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Purge Each Staging Table", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed", rowsDeleted=totalDeleted)
    raise
