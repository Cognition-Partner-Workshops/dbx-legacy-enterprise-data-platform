# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Supplier_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_Supplier_Screen.dtsx` (WWI_DataQuality).
# MAGIC Normalises supplier tax identifiers (trim, upper-case, strip `-` and spaces), detects duplicate and
# MAGIC missing identifiers and missing payment terms, quarantines the duplicate / missing groups into
# MAGIC `silver.err_rejected_supplier`, evaluates rule group `SUPPLIER` and raises the legacy gates.

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

PACKAGE_NAME = "DQ_Supplier_Screen"
SOURCE_SYSTEM_CODE = "ORA_ERP"
OBJECT_NAME = "stg.Supplier"
ERR_TABLE = "err.RejectedSupplier"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    stgSupplier = spark.table(deltaTable(catalog, OBJECT_NAME))

    # Measure Supplier Duplicate Rate (whole table, as in the legacy scalar SELECT)
    normalizedTaxId = F.regexp_replace(F.regexp_replace(F.coalesce(F.col("TaxIdentifier"), F.lit("NONE")), "-", ""), " ", "")
    dupRate = stgSupplier.select(
        (F.lit(100.0) * (F.count(F.lit(1)) - F.countDistinct(normalizedTaxId))
         / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(9,4)").alias("m")).collect()[0]["m"]
    measuredValue = float(dupRate or 0)

    # stg.Supplier has no CountryCode; the EU VAT prefix check compares against this expression
    # (NULL -> the prefix comparison cannot fire; see mapping doc "needs decision").
    supplierDf = (stgSupplier.filter(F.col("BatchId") == batchId)
                  .select(F.col("SupplierBusinessKey").alias("SupplierCode"),
                          "SupplierName", "TaxIdentifier", "PaymentTermsCode",
                          F.lit(None).cast("string").alias("CountryCode"),
                          "RegionCode",
                          (F.col("SupplierStatusCode") == "ACTIVE").alias("IsActive"),
                          "SourceSystemCode", "SourceSupplierId", "SupplierBusinessKey", "BatchId"))

    result = screens.screenSupplier(supplierDf)
    rowsRead = supplierDf.count()
    rowsPassed = result.passed.count()

    rejects = (delta_io.withPayload(result.rejected, excludeColumns=["RejectBranch"])
               .withColumn("BatchId", F.lit(batchId).cast("long"))
               .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
               .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
               .withColumn("RejectReason", F.concat(F.col("RejectBranch"), F.lit(" (group size "),
                                                    F.col("SupplierCount").cast("string"), F.lit(")")))
               .withColumn("RejectStage", F.lit("Quality"))
               .withColumn("FailedColumnName", F.when(F.col("RejectReasonCode") == "DQ_SUPP_TERMS_NULL", "PaymentTermsCode")
                           .otherwise("TaxIdentifier"))
               .withColumn("FailedValue", F.when(F.col("RejectReasonCode") == "DQ_SUPP_TERMS_NULL", F.col("PaymentTermsCode"))
                           .otherwise(F.col("NormalizedTaxId")))
               .withColumn("ReprocessStatusCode", F.lit("NEW"))
               .withColumn("ReprocessAttemptCount", F.lit(0))
               .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.SUPPLIER_REASON_CODES)
    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "SupplierCode", control.logRejectedRecordSet)

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="SUPPLIER", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_SUPP_DUP_RATE", measuredValue,
                            thresholdValue=1, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_SUPP_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Unique Supplier output of DFT Screen Supplier"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Supplier Duplicates", "Supplier tax identifier duplicate rate exceeded tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 1, "@[User::MeasuredValue] > 1"),
        Gate("Fail On Supplier Rule Breach", "One or more blocking supplier quality rules failed.",
             "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
