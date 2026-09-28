# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Payment_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_Payment_Screen.dtsx` (WWI_DataQuality).
# MAGIC Joins staged supplier payments to `silver.work_payment_matched`, flags orphan (unmatched),
# MAGIC future-dated and value-date-before-payment payments, aggregates per payment method / currency and
# MAGIC quarantines method totals dominated by an implausibly large payment into
# MAGIC `silver.err_rejected_payment`, evaluates rule group `PAYMENT` and raises the legacy gates.

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

PACKAGE_NAME = "DQ_Payment_Screen"
SOURCE_SYSTEM_CODE = "ORA_ERP"
OBJECT_NAME = "stg.Payment"
ERR_TABLE = "err.RejectedPayment"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    stgPayment = spark.table(deltaTable(catalog, OBJECT_NAME))
    matched = (spark.table(deltaTable(catalog, "work.PaymentMatched"))
               .select("PaymentBusinessKey", F.col("MatchRuleCode").alias("MatchTypeCode")))

    # Measure Orphan Payment Rate (whole table, LEFT OUTER JOIN work.PaymentMatched)
    orphanRate = (stgPayment.alias("p")
                  .join(matched.alias("m"), F.col("p.PaymentBusinessKey") == F.col("m.PaymentBusinessKey"), "left")
                  .select((F.lit(100.0) * F.sum(F.when(F.col("m.PaymentBusinessKey").isNull(), 1).otherwise(0))
                           / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(9,4)").alias("m"))
                  .collect()[0]["m"])
    measuredValue = float(orphanRate or 0)

    paymentDf = (stgPayment.filter(F.col("BatchId") == batchId)
                 .select("PaymentNumber",
                         F.col("SupplierBusinessKey").alias("SupplierCode"),
                         "PaymentAmount",
                         F.col("TransactionCurrencyCode").alias("PaymentCurrencyCode"),
                         "PaymentDate",
                         F.col("ClearedDate").alias("ValueDate"),
                         "PaymentMethodCode",
                         "SourceSystemCode", "PaymentBusinessKey", "UnappliedAmount", "BatchId"))
    matchedDf = (paymentDf.select("PaymentNumber", "PaymentBusinessKey")
                 .join(matched, "PaymentBusinessKey", "inner")
                 .select("PaymentNumber", "MatchTypeCode"))

    result = screens.screenPayment(paymentDf, matchedDf, asOfDate=F.lit(p["businessDate"]).cast("date"))
    rowsRead = paymentDf.count()
    rowsPassed = result.passed.count()  # plausible method totals (legacy pass-log rows)

    rejects = (delta_io.withPayload(result.rejected, excludeColumns=["RejectBranch"])
               .withColumn("BatchId", F.lit(batchId).cast("long"))
               .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
               .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
               .withColumn("SupplierReference", F.col("SupplierCode"))
               .withColumn("PaymentAmountText", F.col("PaymentAmount").cast("string"))
               .withColumn("CurrencyCode", F.col("PaymentCurrencyCode"))
               .withColumn("RejectReason", F.col("RejectBranch"))
               .withColumn("RejectStage", F.lit("Quality"))
               .withColumn("ReprocessStatusCode", F.lit("NEW"))
               .withColumn("ReprocessAttemptCount", F.lit(0))
               .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.PAYMENT_REASON_CODES)
    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "PaymentNumber", control.logRejectedRecordSet)

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="PAYMENT", objectName=OBJECT_NAME)

    flagged = result.branches["Flagged Payments"]
    flagCounts = flagged.agg(F.sum(F.when(F.col("OrphanFlag") == "Y", 1).otherwise(0)).alias("orphan"),
                             F.sum(F.when(F.col("FutureDatedFlag") == "Y", 1).otherwise(0)).alias("future"),
                             F.sum(F.when(F.col("ValueDateBeforePaymentFlag") == "Y", 1).otherwise(0)).alias("valueDate")
                             ).collect()[0]
    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_PAY_ORPHAN_RATE", measuredValue,
                            thresholdValue=5, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_PAY_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Plausible Method Total output of DFT Screen Payment; flags orphan=%d future=%d valueDate=%d"
                            % (flagCounts["orphan"] or 0, flagCounts["future"] or 0, flagCounts["valueDate"] or 0)),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Orphan Payments", "Orphan payment rate exceeded the configured tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 5, "@[User::MeasuredValue] > 5"),
        Gate("Fail On Payment Rule Breach", "One or more blocking payment quality rules failed.",
             "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
