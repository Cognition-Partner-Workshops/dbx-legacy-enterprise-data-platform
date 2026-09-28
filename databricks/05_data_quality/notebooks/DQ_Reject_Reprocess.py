# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Reject_Reprocess
# MAGIC Migrated from `ssis/05_data_quality/DQ_Reject_Reprocess.dtsx` (WWI_DataQuality).
# MAGIC Replays `silver.err_rejected_lookup_failure` rows once late-arriving stock items land: each reject
# MAGIC gets at most five retries over thirty days; rows that resolve are replayed into `silver.stg_order_line`
# MAGIC (Delta MERGE on the line business key) and closed out, rows that age out or stay unresolved remain
# MAGIC quarantined for stewardship. Rule group `REPROCESS` is evaluated and the legacy warning gate raised.
# MAGIC Finally the "Referential Rescreen" phase of the orchestration plan is run through
# MAGIC `DQ_Referential_Screen` with `ScreenScope = Referential Rescreen`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io, screens
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_Reject_Reprocess"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "err.RejectedLookupFailure"
REPLAY_TARGET = "stg.OrderLine"
RESCREEN_TIMEOUT_SECONDS = 3600

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

errTable = deltaTable(catalog, OBJECT_NAME)
delta_io.ensureColumns(spark, errTable, {"ReprocessAttemptCount": "INT", "ReprocessedAtUtc": "TIMESTAMP",
                                         "ReprocessedByExecutionId": "BIGINT", "ReprocessStatusCode": "STRING"})

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    err = spark.table(errTable)
    stockItemDf = (spark.table(deltaTable(catalog, "stg.StockItem"))
                   .select(F.col("StockItemBusinessKey").alias("StockItemId"), "StockItemName"))

    # ERR Rejected Lookup Failures: WHERE Reprocessed = 0 AND RetryCount < 5 (any batch)
    openRejects = (err.filter(F.coalesce(F.col("ReprocessStatusCode"), F.lit("NEW")).isin("NEW", "RETRY")
                              & (F.coalesce(F.col("ReprocessAttemptCount"), F.lit(0)) < screens.MAX_RETRY_COUNT)
                              & (F.col("SourceObjectName") == REPLAY_TARGET))
                   .select(F.col("RejectId").alias("RejectedRowId"),
                           F.col("SourceObjectName").alias("ObjectName"),
                           F.when(F.col("SourceBusinessKey").contains("|"), F.col("SourceBusinessKey"))
                           .otherwise(F.concat_ws("|", F.col("SourceBusinessKey"), F.col("LookupValue"))).alias("BusinessKey"),
                           "RejectReasonCode",
                           F.col("ReprocessAttemptCount").alias("RetryCount"),
                           F.col("RejectedAtUtc").alias("FirstRejectedAtUtc"),
                           F.col("RecordPayload").alias("PayloadJson"),
                           "SourceSystemCode", "SourceBusinessKey", "LookupName", "LookupValue"))
    rowsRead = openRejects.count()

    result = screens.prepareReprocess(openRejects, stockItemDf)
    resolved = result.passed.cache()
    agedOut = result.branches["Aged Out"]
    unresolved = result.branches["Still Unresolved"]

    # STG OrderLine Replay: the quarantined payload is the original staged row
    replayTarget = deltaTable(catalog, REPLAY_TARGET)
    targetSchema = spark.table(replayTarget).schema
    replayRows = (resolved.filter(F.col("PayloadJson").isNotNull())
                  .select(F.from_json(F.col("PayloadJson"), targetSchema).alias("r"), "RejectedRowId")
                  .select("r.*")
                  .filter(F.col("OrderLineBusinessKey").isNotNull())
                  .withColumn("BatchId", F.lit(batchId).cast("long"))
                  .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
                  .withColumn("DqStatusCode", F.lit("REPLAYED"))
                  .dropDuplicates(["OrderLineBusinessKey"]))
    replayRows = delta_io.alignToTable(spark, replayRows, replayTarget)
    rowsReplayed = replayRows.count()
    if rowsReplayed:
        replayRows.createOrReplaceTempView("dq_reprocess_replay")
        spark.sql("""
            MERGE INTO %s AS t
            USING dq_reprocess_replay AS s
              ON t.OrderLineBusinessKey = s.OrderLineBusinessKey
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """ % replayTarget)

    # Close Reprocessed Rejects / route Aged Out + Still Unresolved (in-place status update)
    outcome = screens.unionAll([
        resolved.select("RejectedRowId", "RetryCount").withColumn("Outcome", F.lit("RESOLVED")),
        agedOut.select("RejectedRowId", "RetryCount").withColumn("Outcome", F.lit("ABANDONED")),
        unresolved.select("RejectedRowId", "RetryCount").withColumn("Outcome", F.lit("RETRY")),
    ])
    outcome.createOrReplaceTempView("dq_reprocess_outcome")
    spark.sql("""
        MERGE INTO %s AS t
        USING dq_reprocess_outcome AS s ON t.RejectId = s.RejectedRowId
        WHEN MATCHED THEN UPDATE SET
            t.ReprocessStatusCode = s.Outcome,
            t.ReprocessAttemptCount = s.RetryCount,
            t.ReprocessedAtUtc = CASE WHEN s.Outcome = 'RESOLVED' THEN current_timestamp() ELSE t.ReprocessedAtUtc END,
            t.ReprocessedByExecutionId = CASE WHEN s.Outcome = 'RESOLVED' THEN CAST(%d AS BIGINT) ELSE t.ReprocessedByExecutionId END
    """ % (errTable, int(packageExecutionId or 0)))

    rowsResolved = resolved.count()
    rowsRejected = agedOut.count() + unresolved.count()

    stillRejected = (result.rejected
                     .withColumn("RejectReasonCode", F.when(F.col("RejectBranch") == "Aged Out", F.lit("DQ_REPROCESS_AGED_OUT"))
                                 .otherwise(F.lit("DQ_REPROCESS_UNRESOLVED"))))
    delta_io.registerRejects(spark, catalog, stillRejected, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "BusinessKey", control.logRejectedRecordSet, rejectStage="Reprocess")

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="REPROCESS", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_REPROCESS_RESOLVED", rowsResolved,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Resolved Now output; replayed %d rows into %s" % (rowsReplayed, REPLAY_TARGET)),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_REPROCESS_OPEN", rowsRejected,
                            thresholdValue=0, rowsEvaluated=rowsRead, detailText="Aged Out + Still Unresolved"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, REPLAY_TARGET, sourceRowCount=rowsRead,
                        targetRowCount=rowsReplayed, insertRowCount=rowsReplayed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsUpdated, run.rowsRejected = rowsRead, rowsReplayed, rowsResolved, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "rowsResolved", rowsResolved)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))

    applyGates(spark, catalog, [
        Gate("Warn On Persistent Rejects", "Rejected rows remain unresolved after the retry window.",
             "Warning", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"FailedRuleCount": int(failedRuleCount or 0)}, packageExecutionId, batchId, PACKAGE_NAME, control.logError)

# COMMAND ----------

# MAGIC %md ## Referential Rescreen (orchestration-plan phase after Reject Routing)

# COMMAND ----------

rescreenArgs = {name: dbutils.widgets.get(name) for name in
                ["BatchId", "BusinessDate", "ReloadFullHistory", "EnvironmentCode", "RestartFromStep", "catalog"]}
rescreenArgs["ScreenScope"] = "Referential Rescreen"
dbutils.notebook.run("./DQ_Referential_Screen", RESCREEN_TIMEOUT_SECONDS, rescreenArgs)
