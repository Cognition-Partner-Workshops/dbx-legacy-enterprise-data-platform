# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Threshold_Gate
# MAGIC Migrated from `ssis/05_data_quality/DQ_Threshold_Gate.dtsx` (WWI_DataQuality).
# MAGIC Reconciles the batch control totals (`etl.row_count_log` per object), publishes the batch reject rate
# MAGIC and quality scorecard into `etl.data_quality_result`, writes per-object totals into
# MAGIC `etl.reconciliation_result`, quarantines reconciliation breaches into
# MAGIC `silver.err_rejected_constraint_violation`, and fails when the reject rate exceeds the configured
# MAGIC tolerance, reconciliation fails, or the scorecard drops below the acceptable score.
# MAGIC
# MAGIC Task values published for the job's `condition_task` (`RejectedRowCount` semantics of the
# MAGIC orchestration plan): `gatePassed`, `rejectedRowCount`, `rejectPercent`, `failedObjectCount`,
# MAGIC `qualityScore`, `failedRuleCount`. Thresholds: `etl.configuration` `MaxRejectPercent` (blocking, legacy
# MAGIC constant 5), `WarnRejectPercent` (legacy 2) and `MinQualityScore` (legacy 90) per EnvironmentCode.

# COMMAND ----------

import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401

from dq_quality import delta_io, gate_math
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import controlTable, deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_Threshold_Gate"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "etl.RowCountAudit"
ERR_TABLE = "err.RejectedConstraintViolation"
RECONCILIATION_NAME = "DQ_CONTROL_TOTALS"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId, environmentCode = p["catalog"], p["batchId"], p["environmentCode"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

warnRejectPercent = delta_io.configurationDecimal(control.getConfiguration, spark, catalog, "WarnRejectPercent",
                                                  environmentCode, gate_math.DEFAULT_WARN_REJECT_PERCENT)
failRejectPercent = delta_io.configurationDecimal(control.getConfiguration, spark, catalog, "MaxRejectPercent",
                                                  environmentCode, gate_math.DEFAULT_FAIL_REJECT_PERCENT)
minQualityScore = delta_io.configurationDecimal(control.getConfiguration, spark, catalog, "MinQualityScore",
                                                environmentCode, gate_math.QUALITY_SCORE_PASS)

rowCountTable = controlTable(catalog, "row_count_log")
if not delta_io.tableExists(spark, rowCountTable):
    rowCountTable = controlTable(catalog, "row_count_audit")
resultTable = controlTable(catalog, "data_quality_result")
ruleTable = controlTable(catalog, "data_quality_rule")

gatePassed = False
with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId

    # ETL RowCountAudit Totals: row counts of every package execution of this batch
    batchRowCounts = (spark.table(rowCountTable).alias("a")
                      .join(spark.table(controlTable(catalog, "package_execution")).alias("e"),
                            F.col("a.PackageExecutionId") == F.col("e.PackageExecutionId"))
                      .filter(F.col("e.BatchId") == batchId)
                      .select("a.*"))
    totalsDf = gate_math.computeControlTotals(batchRowCounts).cache()
    totals = [r.asDict() for r in totalsDf.collect()]
    rowsRead = len(totals)

    # Measure Batch Reject Rate (DQ_BATCH_REJECT_RATE)
    rejectedRowCount, sourceRowCount, rejectPercent = gate_math.batchRejectRate(totals)

    # Assert Row Count Reconciliation (shared etl.usp_AssertRowCountReconciliation; the gate below raises)
    failedObjectCount = int(control.assertRowCountReconciliation(spark, catalog, batchId, raiseOnFailure=False) or 0)
    breaches = [t for t in totals if t["ReconciliationStatusCode"] == "BREACH"]
    failedObjectCount = max(failedObjectCount, len(breaches))

    # Publish Quality Scorecard
    scored = (spark.table(resultTable).alias("r")
              .join(spark.table(ruleTable).alias("d"), F.col("r.RuleCode") == F.col("d.RuleCode"))
              .filter(F.col("r.BatchId") == batchId)
              .select(F.col("r.MeasuredValue"), F.col("d.ThresholdValue")).collect())
    score = gate_math.qualityScore([(r["MeasuredValue"], r["ThresholdValue"]) for r in scored])
    failedRuleCount = sum(1 for r in scored if r["MeasuredValue"] is not None and r["ThresholdValue"] is not None
                          and float(r["MeasuredValue"]) > float(r["ThresholdValue"]))

    # Route Reconciliation Outcome -> ETL ReconciliationResult / ERR Reconciliation Breach
    reconRows = (totalsDf
                 .select(F.lit(batchId).cast("long").alias("BatchId"),
                         F.lit(RECONCILIATION_NAME).alias("ReconciliationName"),
                         "ObjectName",
                         F.col("ObjectName").alias("SourceKey"),
                         F.lit(None).cast("string").alias("LedgerCode"),
                         F.lit(None).cast("string").alias("AccountingPeriod"),
                         F.lit(None).cast("string").alias("AccountCode"),
                         F.lit(None).cast("string").alias("RegionCode"),
                         F.col("SourceRowCount").cast("decimal(19,4)").alias("SourceAmount"),
                         (F.col("TargetRowCount") + F.col("RejectRowCount")).cast("decimal(19,4)").alias("TargetAmount"),
                         F.col("VarianceRowCount").cast("decimal(19,4)").alias("VarianceAmount"),
                         F.col("ReconciliationStatusCode").alias("VarianceStatus"),
                         F.lit(None).cast("string").alias("ExplanationCode"),
                         F.current_timestamp().alias("EvaluatedAtUtc")))
    delta_io.writeReplaceWhere(spark, reconRows, controlTable(catalog, "reconciliation_result"),
                               "BatchId = %d AND ReconciliationName = '%s'" % (batchId, RECONCILIATION_NAME))

    breachDf = (totalsDf.filter(F.col("ReconciliationStatusCode") == "BREACH")
                .select(F.col("ObjectName").alias("TargetObjectName"),
                        F.lit("ROW_COUNT_RECONCILIATION").alias("ConstraintName"),
                        F.lit("RECON").alias("ConstraintTypeCode"),
                        F.col("ObjectName").alias("ViolatingBusinessKey"),
                        F.lit("VarianceRowCount").alias("ViolatingColumnName"),
                        F.col("VarianceRowCount").cast("string").alias("ViolatingValue"),
                        F.col("RejectReasonCode"),
                        F.lit("Reconciliation Breach").alias("RejectReason"),
                        F.lit("Reconciliation").alias("RejectStage"),
                        F.to_json(F.struct("SourceRowCount", "TargetRowCount", "RejectRowCount", "VarianceRowCount",
                                           "RejectPercent", "VariancePercent")).alias("RecordPayload"))
                .withColumn("BatchId", F.lit(batchId).cast("long"))
                .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
                .withColumn("ReprocessStatusCode", F.lit("NEW"))
                .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, breachDf, deltaTable(catalog, ERR_TABLE), batchId,
                                         [gate_math.RECON_BREACH_REASON], rejectStage="Reconciliation")
    delta_io.registerRejects(spark, catalog, breachDf, OBJECT_NAME, batchId, packageExecutionId, SOURCE_SYSTEM_CODE,
                             "ViolatingBusinessKey", control.logRejectedRecordSet, rejectStage="Reconciliation")

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_BATCH_REJECT_RATE", float(rejectPercent),
                            thresholdValue=float(failRejectPercent), rowsEvaluated=sourceRowCount,
                            detailText="rejected=%d source=%d warn>%s fail>%s" % (rejectedRowCount, sourceRowCount,
                                                                                  warnRejectPercent, failRejectPercent)),
        delta_io.measureRow(batchId, packageExecutionId, "BATCH", "DQ_SCORECARD", float(score),
                            thresholdValue=float(minQualityScore), rowsEvaluated=len(scored),
                            resultStatus="Passed" if score >= minQualityScore else "Failed",
                            detailText="100 - avg(100 if MeasuredValue > ThresholdValue else 0) over batch results"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=sourceRowCount,
                        targetRowCount=sourceRowCount - rejectedRowCount, rejectRowCount=rejectedRowCount)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, len(totals), rowsRejected

    decision = gate_math.gateDecision(rejectPercent, failedObjectCount, score, warnRejectPercent, failRejectPercent,
                                      minQualityScore)
    gatePassed = not (decision["failRejectRate"] or decision["failReconciliation"] or decision["failQualityScore"])
    setTaskValue(dbutils, "gatePassed", "true" if gatePassed else "false")
    setTaskValue(dbutils, "rejectedRowCount", int(rejectedRowCount))
    setTaskValue(dbutils, "rejectPercent", str(rejectPercent))
    setTaskValue(dbutils, "failedObjectCount", failedObjectCount)
    setTaskValue(dbutils, "qualityScore", str(score))
    setTaskValue(dbutils, "failedRuleCount", failedRuleCount)

    measures = {"MeasuredValue": rejectPercent, "FailedObjectCount": failedObjectCount, "QualityScore": score}
    applyGates(spark, catalog, [
        Gate("Warn On Reject Rate", "Batch reject rate exceeded the configured warning tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > warnRejectPercent, "@[User::MeasuredValue] > 2"),
        Gate("Fail On Reject Rate", "Batch reject rate exceeded the blocking tolerance.",
             "Failure", lambda m: m["MeasuredValue"] > failRejectPercent, "@[User::MeasuredValue] > 5"),
        Gate("Fail On Reconciliation Breach", "Control total reconciliation failed for one or more objects.",
             "Failure", lambda m: m["FailedObjectCount"] > 0, "@[User::FailedObjectCount] > 0"),
        Gate("Fail On Low Quality Score", "The batch quality scorecard fell below the acceptable score.",
             "Failure", lambda m: m["QualityScore"] < minQualityScore, "@[User::QualityScore] < 90"),
    ], measures, packageExecutionId, batchId, PACKAGE_NAME, control.logError)

dbutils.notebook.exit("passed" if gatePassed else "failed")
