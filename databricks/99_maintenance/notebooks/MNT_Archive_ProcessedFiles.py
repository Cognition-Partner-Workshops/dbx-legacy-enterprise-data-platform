# Databricks notebook source
# MAGIC %md
# MAGIC # MNT_Archive_ProcessedFiles
# MAGIC Legacy: `ssis/99_maintenance/MNT_Archive_ProcessedFiles.dtsx`.
# MAGIC Moves settled files from `<landing>/inbound/processed` in the UC Volume into
# MAGIC `<landing>/archive/<yyyy>/<MM>`, stamps `etl.inbound_file_register`, and lists archived files past
# MAGIC `ArchiveRetentionDays` in `etl.archive_expiry_list`. The legacy service account could not delete from
# MAGIC the archive share, so deletion stays opt-in (`DeleteExpiredArchives`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
import maintenance_lib as mnt  # noqa: E402

PACKAGE_NAME = "MNT_Archive_ProcessedFiles"
PROJECT_NAME = "WWI_Maintenance"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"] or None
archiveRetentionDays = mnt.toInt(mnt.widgetOr(dbutils, "ArchiveRetentionDays", "730"), 730)
minimumFileAgeHours = mnt.toInt(mnt.widgetOr(dbutils, "MinimumFileAgeHours", "6"), 6)
deleteExpiredArchives = mnt.toBool(mnt.widgetOr(dbutils, "DeleteExpiredArchives", "False"))
landingVolumePath = mnt.widgetOr(dbutils, "landingVolumePath", f"/Volumes/{catalog}/bronze/landing")

# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME,
                                             projectName=PROJECT_NAME, stepName="Archive")
archivedFileCount = 0
try:
    now = mnt.utcNow()
    archiveRoot = mnt.archiveFolder(landingVolumePath, now)
    processedRoot = mnt.processedFolder(landingVolumePath)
    dbutils.fs.mkdirs(archiveRoot)

    queueTable = mnt.ensureWorkTable(spark, catalog, "silver.work_file_archive_queue")
    registerTable = mnt.ensureOpsTable(spark, catalog, "etl.inbound_file_register")
    expiryTable = mnt.ensureOpsTable(spark, catalog, "etl.archive_expiry_list")

    # legacy: ProcessingStatus = 'Processed' (a value the CHECK constraint never allowed) -> 'Loaded'
    spark.sql(f"""
        INSERT INTO {mnt.quoted(queueTable)} (FileName, FilePath, FeedCode, ProcessedAtUtc, QueuedAtUtc)
        SELECT f.FileName, f.FilePath, f.FeedCode, f.ProcessedAtUtc, current_timestamp()
        FROM {mnt.quoted(registerTable)} f
        WHERE f.ProcessingStatus = 'Loaded' AND f.IsArchived = false
          AND f.ProcessedAtUtc < current_timestamp() - INTERVAL {minimumFileAgeHours} HOURS
    """)
    queued = {r["FilePath"] for r in spark.table(queueTable).select("FilePath").collect()}

    try:
        entries = dbutils.fs.ls(processedRoot)
    except Exception:
        entries = []
    for entry in entries:
        if entry.isDir():
            continue
        if not mnt.fileIsSettled(entry.modificationTime, now, minimumFileAgeHours) and entry.path not in queued:
            continue
        destination = f"{archiveRoot}/{entry.name}"
        try:
            dbutils.fs.mv(entry.path, destination)
            spark.sql(f"UPDATE {mnt.quoted(registerTable)} SET IsArchived = true, ArchivedAtUtc = current_timestamp(), "
                      f"ArchivePath = {mnt.sqlString(destination)} "
                      f"WHERE FilePath IN ({mnt.sqlString(entry.path)}, {mnt.sqlString(entry.path.replace('dbfs:', ''))}) "
                      f"AND IsArchived = false")
            archivedFileCount += 1
        except Exception as exc:  # legacy: the loop continues past a failed move (Completion constraint)
            mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, f"FAILED move {entry.path} -> {destination}: {str(exc)[:1000]}")

    spark.sql(f"""
        INSERT INTO {mnt.quoted(expiryTable)} (FileName, ArchivePath, ArchivedAtUtc, ListedAtUtc, IsDeleted)
        SELECT r.FileName, r.ArchivePath, r.ArchivedAtUtc, current_timestamp(), false
        FROM {mnt.quoted(registerTable)} r
        WHERE r.IsArchived = true AND r.ArchivePath IS NOT NULL
          AND r.ArchivedAtUtc < current_timestamp() - INTERVAL {archiveRetentionDays} DAYS
          AND NOT EXISTS (SELECT 1 FROM {mnt.quoted(expiryTable)} l WHERE l.ArchivePath = r.ArchivePath)
    """)

    deletedCount = 0
    if deleteExpiredArchives:
        for row in spark.table(expiryTable).filter("IsDeleted = false").select("ArchiveExpiryListId", "ArchivePath").collect():
            try:
                dbutils.fs.rm(row["ArchivePath"])
                spark.sql(f"UPDATE {mnt.quoted(expiryTable)} SET IsDeleted = true, DeletedAtUtc = current_timestamp() "
                          f"WHERE ArchiveExpiryListId = {row['ArchiveExpiryListId']}")
                deletedCount += 1
            except Exception as exc:
                mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME, f"FAILED delete {row['ArchivePath']}: {str(exc)[:1000]}")

    expiredFileCount = spark.sql(f"SELECT COUNT(*) FROM {mnt.quoted(expiryTable)} "
                                 f"WHERE ListedAtUtc >= current_timestamp() - INTERVAL 6 HOURS").collect()[0][0]
    registerCount = spark.table(registerTable).count()
    mnt.insertMaintenanceLog(spark, catalog, PACKAGE_NAME,
                             f"Queued: {len(queued)}; archived: {archivedFileCount}; newly expired: {expiredFileCount}; "
                             f"deleted: {deletedCount}; archive folder: {archiveRoot}")
    control.logRowCount(spark, catalog, packageExecutionId, "etl.inbound_file_register",
                        sourceRowCount=len(queued), targetRowCount=registerCount, updateRowCount=archivedFileCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded",
                          rowsRead=len(queued), rowsUpdated=archivedFileCount, rowsDeleted=deletedCount)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId,
                     errorSeverity="Error", sourceName=PACKAGE_NAME,
                     sourceComponent="Archive Each File", errorDescription=str(exc)[:4000])
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed", rowsUpdated=archivedFileCount)
    raise
