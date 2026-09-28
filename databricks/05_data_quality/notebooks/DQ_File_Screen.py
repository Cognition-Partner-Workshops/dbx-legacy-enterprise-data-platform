# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_File_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_File_Screen.dtsx` (WWI_DataQuality).
# MAGIC Structural screen of the ingested partner sales rows in `bronze.raw_file_partner_sales`: the row must
# MAGIC carry exactly eight delimiters (nine interface fields), a parseable sale date and amount, and no
# MAGIC U+FFFD replacement characters. Offenders are quarantined into `silver.err_rejected_file_row`
# MAGIC (RejectStage `Extract`), rule group `FILEROW` is evaluated and the legacy gates are raised.

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

PACKAGE_NAME = "DQ_File_Screen"
SOURCE_SYSTEM_CODE = "PARTNER_FILE"
OBJECT_NAME = "raw.FilePartnerSales"
ERR_TABLE = "err.RejectedFileRow"

# The nine agreed partner interface fields; bronze already holds the parsed row, so the raw line
# (and its delimiter count) is reassembled from these columns (see mapping doc).
INTERFACE_COLUMNS = ["PartnerCode", "PartnerOutletCode", "TransactionReference", "TransactionDate",
                     "PartnerProductCode", "QuantitySold", "NetAmount", "CurrencyCode", "CountryCode"]

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    rawRows = screens.reconstructFileRow(spark.table(deltaTable(catalog, OBJECT_NAME)), INTERFACE_COLUMNS)

    # Measure File Malformed Rate (whole table)
    malformed = rawRows.select(
        (F.lit(100.0) * F.sum(F.when(F.col("DelimiterCount") != screens.EXPECTED_FILE_DELIMITERS, 1).otherwise(0))
         / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(9,4)").alias("m")).collect()[0]["m"]
    measuredValue = float(malformed or 0)

    fileRowDf = (rawRows.filter(F.col("BatchId") == batchId)
                 .select(F.concat_ws(":", F.col("SourceFileName"), F.col("SourceRowNumber").cast("string")).alias("FileRowId"),
                         "SourceFileName",
                         F.col("SourceRowNumber").alias("FileLineNumber"),
                         "RawLine", "DelimiterCount",
                         F.col("TransactionDate").alias("SaleDateText"),
                         F.col("NetAmount").alias("AmountText"),
                         "PartnerCode", "CurrencyCode", "FileFormatVersion", "SourceSystemCode", "BatchId"))

    result = screens.screenFileRows(fileRowDf)
    rowsRead = fileRowDf.count()
    rowsPassed = result.passed.count()

    rejects = (result.rejected
               .withColumn("BatchId", F.lit(batchId).cast("long"))
               .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
               .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
               .withColumn("SourceRowNumber", F.col("FileLineNumber"))
               .withColumn("RawRowText", F.col("RawLine"))
               .withColumn("ExpectedColumnCount", F.lit(screens.EXPECTED_FILE_DELIMITERS + 1))
               .withColumn("ActualColumnCount", F.col("DelimiterCount") + 1)
               .withColumn("DelimiterUsed", F.lit("|"))
               .withColumn("RejectReason", F.col("RejectBranch"))
               .withColumn("RejectStage", F.lit("Extract"))
               .withColumn("ReprocessStatusCode", F.lit("NEW"))
               .withColumn("ReprocessAttemptCount", F.lit(0))
               .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.FILE_REASON_CODES, rejectStage="Extract")
    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "FileRowId", control.logRejectedRecordSet, rejectStage="Extract")

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="FILEROW", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_FILE_MALFORMED_RATE", measuredValue,
                            thresholdValue=1, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_FILE_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Well Formed Row output of DFT Screen Partner File"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Malformed File Rows", "Malformed partner file row rate exceeded tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 1, "@[User::MeasuredValue] > 1"),
        Gate("Fail On File Structure Breach", "The partner file failed a blocking structural rule.",
             "Failure", lambda m: m["MeasuredValue"] > 25 or m["FailedRuleCount"] > 0,
             "@[User::MeasuredValue] > 25 || @[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
