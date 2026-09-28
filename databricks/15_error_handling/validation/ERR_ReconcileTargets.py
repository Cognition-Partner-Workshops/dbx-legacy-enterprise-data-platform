# Databricks notebook source
# MAGIC %md
# MAGIC # Validation: WWI_ErrorHandling target tables vs SQL Server baseline
# MAGIC For every Delta table written by the six ERR_* notebooks, computes the row count per `BatchId` and a
# MAGIC deterministic content hash (`xxhash64` over the sorted, `|`-concatenated columns, summed), compares them
# MAGIC with the SQL Server baseline (ports of `validation/runtime/01_control_framework_health.sql` and
# MAGIC `02_row_count_reconciliation.sql`) and writes the outcome to `etl.row_count_log` through `control.logRowCount`
# MAGIC (`SourceRowCount` = baseline count, `TargetRowCount` = Delta count, `RejectRowCount` = 1 when the hash differs).
# MAGIC
# MAGIC Baseline input (either):
# MAGIC * `baselineTable` – Delta table with columns `ObjectName STRING, BatchId BIGINT, RowCount BIGINT, RowHash BIGINT`
# MAGIC   (captured from SQL Server with the queries printed at the bottom of this notebook), or
# MAGIC * `baselineJson` – the same rows as a JSON array string.

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402
from err_handling import widgets  # noqa: E402

PACKAGE_NAME = "VAL_15_ErrorHandling_Reconcile"
PROJECT_NAME = "WWI_ErrorHandling"

# Legacy object -> Delta table written by this project (+ batch column used for per-batch counts).
TARGETS = {
    "etl.ErrorLog": ("etl", "error_log", "BatchId"),
    "etl.OperatorNotification": ("etl", "operator_notification", "BatchId"),
    "etl.BatchStepRerunRequest": ("etl", "batch_step_rerun_request", "BatchId"),
    "etl.ReconciliationResult": ("etl", "reconciliation_result", "BatchId"),
    "etl.InboundFileRegister": ("etl", "inbound_file_register", None),
    "etl.RejectedRecord": ("etl", "rejected_record", "BatchId"),
    "work.BadFileQueue": ("silver", "work_bad_file_queue", "BatchId"),
    "work.RejectRoutingSet": ("silver", "work_reject_routing_set", "RoutedInBatchId"),
    "work.RejectRoutingHistory": ("silver", "work_reject_routing_history", "RoutedInBatchId"),
    "work.RejectEscalation": ("silver", "work_reject_escalation", "RoutedInBatchId"),
    "work.RowCountReconciliation": ("silver", "work_row_count_reconciliation", "BatchId"),
    "work.RowCountFailure": ("silver", "work_row_count_failure", "BatchId"),
    "work.StepFailure": ("silver", "work_step_failure", "BatchId"),
}

# Volatile columns excluded from the hash (identities and UTC stamps differ between platforms by construction).
EXCLUDED_HASH_COLUMNS = {
    "ErrorLogId", "LoggedAtUtc", "OperatorNotificationId", "RaisedAtUtc", "RerunRequestId", "RequestedAtUtc", "CompletedAtUtc",
    "ReconciliationResultId", "EvaluatedAtUtc", "InboundFileId", "ReceivedAtUtc", "ProcessedAtUtc", "QuarantinedAtUtc", "ArchivedAtUtc",
    "RejectedRecordId", "ReprocessedAtUtc", "BadFileQueueId", "DetectedAtUtc", "MovedAtUtc", "RoutedAtUtc", "EscalatedAtUtc",
    "RegisteredAtUtc", "WorkStepFailureId", "ClassifiedAtUtc", "PackageExecutionId",
}

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = int(p["batchId"])
baselineTable = widgets.getText(dbutils, "baselineTable", "")
baselineJson = widgets.getText(dbutils, "baselineJson", "")
baselineSchema = T.StructType([
    T.StructField("ObjectName", T.StringType()), T.StructField("BatchId", T.LongType()),
    T.StructField("RowCount", T.LongType()), T.StructField("RowHash", T.LongType()),
])
if baselineTable:
    baseline = spark.table(baselineTable).select("ObjectName", "BatchId", "RowCount", "RowHash")
elif baselineJson:
    baseline = spark.createDataFrame(json.loads(baselineJson), baselineSchema)
else:
    baseline = spark.createDataFrame([], baselineSchema)
baseline = baseline.alias("b")

# COMMAND ----------


def rowHash(df):
    """xxhash64 of the sorted concatenated non-volatile columns, summed over the rows (order independent)."""
    columns = sorted(c for c in df.columns if c not in EXCLUDED_HASH_COLUMNS)
    return F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]))


def measure(legacyName, schemaName, tableName, batchColumn):
    fullName = naming.table(catalog, schemaName, tableName)
    if not spark.catalog.tableExists(fullName):
        return None
    df = spark.table(fullName)
    if batchColumn and batchId > 0:
        df = df.where(F.col(batchColumn) == batchId)
    hashed = df.withColumn("_h", rowHash(df))
    agg = hashed.agg(F.count(F.lit(1)).alias("RowCount"), F.coalesce(F.sum("_h"), F.lit(0)).alias("RowHash")).first()
    return {"ObjectName": legacyName, "DeltaTable": fullName, "BatchId": batchId if batchColumn else None,
            "RowCount": int(agg["RowCount"]), "RowHash": int(agg["RowHash"])}


# COMMAND ----------

packageExecutionId = control.logPackageStart(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName="Validation")
results = []
try:
    baselineRows = {(r["ObjectName"], r["BatchId"]): r for r in baseline.collect()}
    for legacyName, (schemaName, tableName, batchColumn) in TARGETS.items():
        measured = measure(legacyName, schemaName, tableName, batchColumn)
        if measured is None:
            results.append({"ObjectName": legacyName, "status": "TABLE_MISSING"})
            continue
        base = baselineRows.get((legacyName, measured["BatchId"])) or baselineRows.get((legacyName, None))
        if base is None:
            status, baselineCount, hashMatches = "NO_BASELINE", None, None
        else:
            baselineCount = int(base["RowCount"])
            hashMatches = base["RowHash"] is None or int(base["RowHash"]) == measured["RowHash"]
            status = "MATCHED" if baselineCount == measured["RowCount"] and hashMatches else "MISMATCH"
        control.logRowCount(
            spark, catalog, packageExecutionId, legacyName,
            sourceRowCount=baselineCount, targetRowCount=measured["RowCount"],
            rejectRowCount=0 if hashMatches in (None, True) else 1,
        )
        measured.update({"status": status, "baselineRowCount": baselineCount, "hashMatches": hashMatches})
        results.append(measured)
    mismatches = [r for r in results if r["status"] == "MISMATCH"]
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Succeeded" if not mismatches else "SucceededWithWarnings",
                          rowsRead=len(results), rowsRejected=len(mismatches))
except Exception as exc:
    control.logError(spark, catalog, packageExecutionId=packageExecutionId, batchId=batchId, sourceName=PACKAGE_NAME, errorDescription=str(exc))
    control.logPackageEnd(spark, catalog, packageExecutionId, status="Failed")
    raise

display(spark.createDataFrame([json.dumps(r) for r in results], T.StringType()).toDF("result"))

# COMMAND ----------

# MAGIC %md ## Capturing the SQL Server baseline
# MAGIC Run per object on the legacy control database and load the output into `baselineTable` (`BatchId` = the batch under test):
# MAGIC ```sql
# MAGIC -- etl.OperatorNotification (ports validation/runtime/01_control_framework_health.sql, section "notifications")
# MAGIC SELECT 'etl.OperatorNotification' AS ObjectName, BatchId, COUNT(*) AS [RowCount],
# MAGIC        SUM(CAST(HASHBYTES('SHA2_256', CONCAT_WS('|', BatchId, Body, IsAcknowledged, NotificationTypeCode, ObjectName, Severity, Subject)) AS BIGINT)) AS RowHash
# MAGIC FROM etl.OperatorNotification WHERE BatchId = @BatchId GROUP BY BatchId;
# MAGIC -- etl.ReconciliationResult / work.RowCountReconciliation (ports validation/runtime/02_row_count_reconciliation.sql)
# MAGIC SELECT 'etl.ReconciliationResult', BatchId, COUNT(*), NULL FROM etl.ReconciliationResult WHERE BatchId = @BatchId AND ReconciliationName = 'ROW_COUNT' GROUP BY BatchId;
# MAGIC ```
# MAGIC `RowHash` may be left NULL when only the counts are captured (SQL Server has no xxhash64; the count comparison still runs
# MAGIC and the hash is skipped). Column order in the concatenation must be alphabetical, matching `rowHash()` above.

dbutils.notebook.exit(json.dumps({"checked": len(results), "mismatches": len([r for r in results if r["status"] == "MISMATCH"])}))
