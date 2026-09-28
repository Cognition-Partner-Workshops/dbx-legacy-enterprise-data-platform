# Databricks notebook source
# MAGIC %md
# MAGIC # ERR_Route_RejectedRows
# MAGIC Reject router (legacy `ssis/15_error_handling/ERR_Route_RejectedRows.dtsx`).
# MAGIC
# MAGIC 1. Registers rows sitting in the per-object `silver.err_*` reject tables that are not yet in
# MAGIC    `etl.rejected_record` (`control.logRejectedRecordSet`), so the stewards work from one queue.
# MAGIC 2. Gathers unresolved `etl.rejected_record` rows (`IsReprocessed = 0`, not yet routed), derives their age,
# MAGIC    splits escalated (`AgeDays > RejectEscalationDays`, destination QUARANTINE) from standard rejects
# MAGIC    (destination REPROCESS), writes `silver.work_reject_routing_history` / `silver.work_reject_escalation`,
# MAGIC    writes the `rejects_<BatchId>_<yyyyMMdd>.csv` file to the reject volume and raises one
# MAGIC    `REJECT_ESCALATION` operator notification per escalated object.
# MAGIC
# MAGIC Package parameters: `RejectEscalationDays` (default `5`), `ObjectScope` (default `ALL`), `rejectVolumePath`.

# COMMAND ----------

import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import rejects, tables, widgets  # noqa: E402

PACKAGE_NAME = "ERR_Route_RejectedRows"
PROJECT_NAME = "WWI_ErrorHandling"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])
rejectEscalationDays = widgets.getInt(dbutils, "RejectEscalationDays", 5)
objectScope = widgets.getText(dbutils, "ObjectScope", "ALL")
rejectVolumePath = widgets.getText(dbutils, "rejectVolumePath", "")

rejectedRecordTable = naming.table(catalog, "etl", "rejected_record")
notificationTable = naming.table(catalog, "etl", "operator_notification")
routingSetTable = tables.ensureWorkTable(spark, naming, catalog, "work_reject_routing_set")
historyTable = tables.ensureWorkTable(spark, naming, catalog, "work_reject_routing_history")
escalationTable = tables.ensureWorkTable(spark, naming, catalog, "work_reject_escalation")
registrationTable = tables.ensureWorkTable(spark, naming, catalog, "work_err_reject_registration")

asOfDate = datetime.now(timezone.utc).date()
rejectFileName = rejects.rejectFileName(batchId, asOfDate)

# COMMAND ----------

# MAGIC %md ## Register silver.err_* rows in etl.rejected_record


def registerErrTables():
    registered = 0
    for errTable, businessKeyColumn in rejects.ERR_TABLES.items():
        schemaName, tableName = errTable.split(".")
        fullName = naming.table(catalog, schemaName, tableName)
        if not spark.catalog.tableExists(fullName):
            continue
        if objectScope != "ALL" and objectScope not in (tableName, errTable):
            continue
        errDf = spark.table(fullName).alias("e")
        columns = set(errDf.columns)
        if "ReprocessStatusCode" in columns:
            errDf = errDf.where(F.col("ReprocessStatusCode") == "NEW")
        already = spark.table(registrationTable).where(F.col("ErrTableName") == errTable).select("RejectId").alias("r")
        pending = errDf.join(already, errDf["RejectId"] == already["RejectId"], "left_anti")
        if pending.isEmpty():
            continue
        keyColumn = businessKeyColumn if businessKeyColumn in columns else None
        rejectedDf = pending.withColumn("RecordPayload", F.to_json(F.struct(*[F.col(c) for c in pending.columns if c != "RecordPayload"])))
        if "RecordPayload" in columns:
            rejectedDf = pending.withColumn("RecordPayload", F.coalesce(F.col("RecordPayload"), F.to_json(F.struct(*[F.col(c) for c in pending.columns if c != "RecordPayload"]))))
        for batchRow in pending.select("BatchId").distinct().collect():
            subset = rejectedDf.where(F.col("BatchId") == batchRow["BatchId"])
            sourceSystemCode = None
            if "SourceSystemCode" in columns:
                codes = [r[0] for r in subset.select("SourceSystemCode").distinct().collect()]
                sourceSystemCode = codes[0] if len(codes) == 1 else None
            stage = "Stage"
            if "RejectStage" in columns:
                stages = [r[0] for r in subset.select("RejectStage").distinct().collect()]
                stage = stages[0] if len(stages) == 1 and stages[0] else "Stage"
            registered += int(control.logRejectedRecordSet(
                spark, catalog, errTable, subset, batchId=int(batchRow["BatchId"]), packageExecutionId=packageExecutionId,
                sourceSystemCode=sourceSystemCode, rejectStage=stage, rejectReasonCode=None, businessKeyColumn=keyColumn,
            ) or 0)
        (pending.select(F.lit(errTable).alias("ErrTableName"), F.col("RejectId").cast("bigint"), F.col("BatchId").cast("bigint"),
                        F.lit(batchId).cast("bigint").alias("RoutedInBatchId"), F.current_timestamp().alias("RegisteredAtUtc"))
         .write.format("delta").mode("append").saveAsTable(registrationTable))
    return registered


# COMMAND ----------

# MAGIC %md ## Gather, classify, split, route

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Reject Routing")
result = {"package": PACKAGE_NAME, "batchId": batchId, "rejectFileName": rejectFileName}
try:
    registeredCount = registerErrTables()

    tables.clearForBatch(spark, routingSetTable, batchId)
    scopeFilter = "" if objectScope == "ALL" else " AND r.ObjectName = %s" % tables.sqlLiteral(objectScope)
    spark.sql(
        """
        INSERT INTO {routingSet}
            (RejectedRecordId, BatchId, ObjectName, RejectStage, RejectReasonCode, RejectReasonDescription, SourceKey, RejectedAtUtc, AgeDays, RoutedInBatchId)
        SELECT r.RejectedRecordId, r.BatchId, r.ObjectName, r.RejectStage, r.RejectReasonCode, r.RejectReason, r.BusinessKey, r.LoggedAtUtc,
               DATEDIFF(current_timestamp(), r.LoggedAtUtc), {batchId}
        FROM {rejected} r
        WHERE r.IsReprocessed = false{scopeFilter}
          AND NOT EXISTS (SELECT 1 FROM {history} h WHERE h.RejectedRecordId = r.RejectedRecordId)
        """.format(routingSet=routingSetTable, rejected=rejectedRecordTable, history=historyTable, batchId=batchId, scopeFilter=scopeFilter)
    )

    routingSet = spark.table(routingSetTable).where(F.col("RoutedInBatchId") == batchId).orderBy("ObjectName", "RejectedAtUtc")
    classified = rejects.classifyRejects(routingSet, rejectEscalationDays, rejectFileName)
    classified.cache()
    routedRowCount = classified.count()
    escalated = classified.where(F.col("IsEscalated"))

    historyColumns = ["RejectedRecordId", "BatchId", "ObjectName", "RejectStage", "RejectReasonCode", "RejectReasonDescription", "SourceKey",
                      "RejectedAtUtc", "AgeDays", "IsEscalated", "RejectFileLine", "RejectFileName", "RoutingDestination", "RoutedInBatchId", "RoutedAtUtc"]
    classified.select(*historyColumns).write.format("delta").mode("append").saveAsTable(historyTable)

    tables.clearForBatch(spark, escalationTable, batchId)
    (escalated.select("RejectedRecordId", "BatchId", "ObjectName", "RejectStage", "RejectReasonCode", "RejectReasonDescription", "SourceKey",
                      "RejectedAtUtc", "AgeDays", "RejectFileLine", "RoutedInBatchId", F.col("RoutedAtUtc").alias("EscalatedAtUtc"))
     .write.format("delta").mode("append").saveAsTable(escalationTable))

    if rejectVolumePath and routedRowCount > 0:
        lines = [r["RejectFileLine"] for r in classified.select("RejectFileLine").collect()]
        dbutils.fs.put(os.path.join(rejectVolumePath.rstrip("/"), rejectFileName), "ObjectName|RejectReasonCode|SourceKey\n" + "\n".join(lines) + "\n", True)

    # Legacy Escalate Aged Rejects: one REJECT_ESCALATION per object.
    spark.sql(
        """
        INSERT INTO {notification} (BatchId, NotificationTypeCode, Severity, Subject, Body, ObjectName, RaisedAtUtc, IsAcknowledged)
        SELECT {batchId}, 'REJECT_ESCALATION', 'WARNING',
               CONCAT('Aged rejects for ', ObjectName),
               CONCAT('Rejects unresolved beyond the escalation window: ', CAST(COUNT(*) AS STRING)),
               ObjectName, current_timestamp(), false
        FROM {escalation} WHERE RoutedInBatchId = {batchId}
        GROUP BY ObjectName
        """.format(notification=notificationTable, escalation=escalationTable, batchId=batchId)
    )
    escalatedCount = int(tables.scalar(spark, "SELECT COUNT(*) FROM %s WHERE RoutedInBatchId = %d" % (escalationTable, batchId)) or 0)

    result.update({"registeredErrRows": registeredCount, "routedRowCount": routedRowCount, "escalatedCount": escalatedCount,
                   "reprocessCount": routedRowCount - escalatedCount})
    control.logRowCount(spark, catalog, packageExecutionId, "etl.RejectedRecord", sourceRowCount=routedRowCount, targetRowCount=routedRowCount,
                        insertRowCount=routedRowCount, rejectRowCount=escalatedCount)
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded", rowsRead=routedRowCount, rowsInserted=routedRowCount, rowsRejected=escalatedCount)
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, sourceComponent="OnError", errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

# COMMAND ----------

dbutils.jobs.taskValues.set(key="routedRowCount", value=result["routedRowCount"])
dbutils.jobs.taskValues.set(key="escalatedCount", value=result["escalatedCount"])
dbutils.notebook.exit(json.dumps(result))
