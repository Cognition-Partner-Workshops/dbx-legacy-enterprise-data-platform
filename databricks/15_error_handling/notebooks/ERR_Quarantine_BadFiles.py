# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Quarantine_BadFiles
# MAGIC Bad-file quarantine (legacy `ssis/15_error_handling/ERR_Quarantine_BadFiles.dtsx`).
# MAGIC
# MAGIC Gathers inbound files registered as `ProcessingStatus = 'Failed'` and not yet quarantined from
# MAGIC `etl.inbound_file_register`, classifies the reason (`ZERO_LENGTH` / `STRUCTURE` / `UNKNOWN_FEED` / `OTHER`),
# MAGIC moves each file into `<quarantineVolumePath>/<QuarantineFolder>/<yyyyMM>/` with `dbutils.fs.mv`
# MAGIC (legacy File System Task MoveFile), marks the register row quarantined and raises one `FILE_QUARANTINE`
# MAGIC operator notification. `DeleteZeroLengthFiles = True` deletes zero-length files instead of moving them.
# MAGIC
# MAGIC Package parameters: `QuarantineFolder` (default `quarantine`), `DeleteZeroLengthFiles` (default `False`),
# MAGIC `quarantineVolumePath`, `inboundVolumePath` (files in `<inboundVolumePath>/failed/` that are not registered are also moved).

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import quarantine, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Quarantine_BadFiles"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])
quarantineFolderName = widgets.getText(dbutils, "QuarantineFolder", "quarantine")
deleteZeroLengthFiles = widgets.getBool(dbutils, "DeleteZeroLengthFiles", False)
quarantineVolumePath = widgets.getText(dbutils, "quarantineVolumePath", "")
inboundVolumePath = widgets.getText(dbutils, "inboundVolumePath", "")
if not quarantineVolumePath:
    raise ValueError("quarantineVolumePath is required (bundle variable quarantine_volume_path)")

registerTable = naming.table(catalog, "etl", "inbound_file_register")
notificationTable = naming.table(catalog, "etl", "operator_notification")
queueTable = tables.ensureWorkTable(spark, naming, catalog, "work_bad_file_queue")

asOfDate = datetime.now(timezone.utc).date()
quarantineDir = quarantine.quarantineFolder(quarantineVolumePath, quarantineFolderName, asOfDate)

# COMMAND ----------


def gatherBadFiles():
    """Legacy Gather Bad Files: failed, not-yet-quarantined register rows into the queue (idempotent per batch)."""
    spark.sql(
        """
        INSERT INTO {queue} (BatchId, InboundFileId, FileName, FilePath, QuarantineReasonCode, DetectedAtUtc, IsMoved)
        SELECT {batchId}, f.InboundFileId, f.FileName, f.FilePath,
               CASE WHEN f.FileSizeBytes = 0 THEN 'ZERO_LENGTH'
                    WHEN f.StructuralCheckStatus = 'Failed' THEN 'STRUCTURE'
                    WHEN f.FeedCode IS NULL THEN 'UNKNOWN_FEED'
                    ELSE 'OTHER' END,
               current_timestamp(), 0
        FROM {register} f
        WHERE f.ProcessingStatus = 'Failed' AND COALESCE(f.IsQuarantined, false) = false
          AND NOT EXISTS (SELECT 1 FROM {queue} q WHERE q.FilePath = f.FilePath AND q.IsMoved = 0)
        """.format(queue=queueTable, register=registerTable, batchId=batchId)
    )
    if inboundVolumePath:
        failedFolder = os.path.join(inboundVolumePath.rstrip("/"), "failed")
        try:
            entries = dbutils.fs.ls(failedFolder)
        except Exception:
            entries = []
        for entry in entries:
            if entry.isDir():
                continue
            known = tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE FilePath = %s AND IsMoved = 0" % (queueTable, tables.sqlLiteral(entry.path)))
            if int(known or 0) == 0:
                reason = quarantine.quarantineReasonCode(entry.size, None, None)
                spark.sql(
                    "INSERT INTO %s (BatchId, InboundFileId, FileName, FilePath, QuarantineReasonCode, DetectedAtUtc, IsMoved) "
                    "VALUES (%d, NULL, %s, %s, %s, current_timestamp(), 0)"
                    % (queueTable, batchId, tables.sqlLiteral(entry.name), tables.sqlLiteral(entry.path), tables.sqlLiteral(reason))
                )


def moveFile(row):
    """Legacy Move File To Quarantine + Register Quarantined File for one queue row."""
    destination = quarantine.quarantineDestination(quarantineVolumePath, quarantineFolderName, asOfDate, row["FileName"])
    try:
        if row["QuarantineReasonCode"] == "ZERO_LENGTH" and deleteZeroLengthFiles:
            dbutils.fs.rm(row["FilePath"])
            destination = None
        else:
            dbutils.fs.mkdirs(quarantineDir)
            dbutils.fs.mv(row["FilePath"], destination)
        spark.sql(
            "UPDATE %s SET IsMoved = 1, MovedAtUtc = current_timestamp(), QuarantinePath = %s, MoveError = NULL WHERE BadFileQueueId = %d"
            % (queueTable, tables.sqlLiteral(destination), int(row["BadFileQueueId"]))
        )
        return True
    except Exception as exc:
        spark.sql("UPDATE %s SET MoveError = %s WHERE BadFileQueueId = %d" % (queueTable, tables.sqlLiteral(str(exc)[:1000]), int(row["BadFileQueueId"])))
        control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, errorSeverity="Warning",
                         sourceName=row["FilePath"], sourceComponent="Move File To Quarantine", errorDescription=str(exc))
        return False


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="File Quarantine")
result = {"package": PACKAGE_NAME, "batchId": batchId, "quarantineDir": quarantineDir}
try:
    gatherBadFiles()
    queued = spark.sql("SELECT * FROM %s WHERE IsMoved = 0 ORDER BY BadFileQueueId" % queueTable).collect()
    badFileCount = len(queued)
    movedCount = 0
    if badFileCount > 0:
        for row in queued:
            if moveFile(row):
                movedCount += 1
        # Legacy Mark Register Quarantined (ProcessingStatus 'Quarantined' is in the legacy CHECK list).
        spark.sql(
            """
            MERGE INTO {register} r
            USING (SELECT FilePath, MAX(QuarantineReasonCode) AS QuarantineReasonCode FROM {queue} WHERE IsMoved = 1 AND BatchId = {batchId} GROUP BY FilePath) q
            ON q.FilePath = r.FilePath
            WHEN MATCHED THEN UPDATE SET r.IsQuarantined = true, r.QuarantinedAtUtc = current_timestamp(),
                                         r.QuarantineReason = q.QuarantineReasonCode, r.ProcessingStatus = 'Quarantined'
            """.format(register=registerTable, queue=queueTable, batchId=batchId)
        )
        if movedCount > 0:
            spark.sql(
                """
                INSERT INTO {notification} (BatchId, NotificationTypeCode, Severity, Subject, Body, ObjectName, RaisedAtUtc, IsAcknowledged)
                SELECT {batchId}, 'FILE_QUARANTINE', 'WARNING', 'Inbound files quarantined',
                       CONCAT('Files moved to quarantine: ', CAST(COUNT(*) AS STRING)), 'etl.InboundFileRegister', current_timestamp(), false
                FROM {queue} WHERE IsMoved = 1 AND BatchId = {batchId} HAVING COUNT(*) > 0
                """.format(notification=notificationTable, queue=queueTable, batchId=batchId)
            )
    result.update({"badFileCount": badFileCount, "movedCount": movedCount, "failedMoves": badFileCount - movedCount})
    control.logRowCount(spark, catalog, packageExecutionId, "etl.InboundFileRegister", sourceRowCount=badFileCount, targetRowCount=movedCount,
                        updateRowCount=movedCount, rejectRowCount=badFileCount - movedCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=badFileCount, rowsUpdated=movedCount, rowsRejected=badFileCount - movedCount)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="badFileCount", value=result["badFileCount"])
dbutils.jobs.taskValues.set(key="movedCount", value=result["movedCount"])
dbutils.notebook.exit(json.dumps(result))
