# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_Customer_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_Customer_Screen.dtsx` (WWI_DataQuality).
# MAGIC Screens the staged customer set for completeness, EU consent / retention breaches, implausible
# MAGIC credit limits and unknown countries, quarantines offenders into `silver.err_rejected_customer`,
# MAGIC registers them through `control.logRejectedRecordSet`, evaluates rule group `CUSTOMER` through
# MAGIC `control.evaluateDataQualityRules` and raises the legacy warning / failure gates.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, params, naming  # noqa: F401 - naming used via dq_quality.naming_map

from dq_quality import delta_io, screens
from dq_quality.gates import Gate, applyGates
from dq_quality.naming_map import controlTable, deltaTable
from dq_quality.notebook_support import PACKAGE_STEP, PROJECT_NAME, ensureWidgets, setTaskValue, shouldSkipForRestart

PACKAGE_NAME = "DQ_Customer_Screen"
SOURCE_SYSTEM_CODE = "ORA_ERP"
OBJECT_NAME = "stg.Customer"
ERR_TABLE = "err.RejectedCustomer"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

# MAGIC %md ## Source (OLE DB Source "STG Customer" + Derived Column + Lookup + Conditional Split)
# MAGIC The legacy query selected `CustomerCode, CountryCode, CustomerClassCode, RetentionMonths`; those are
# MAGIC mapped onto the stg.Customer columns that actually exist (see mapping doc, "schema drift").

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    stgCustomer = spark.table(deltaTable(catalog, OBJECT_NAME))

    # Measure Customer Null Rate (record_measure; legacy measures the whole table, not the batch)
    nullRate = stgCustomer.select(
        (F.lit(100.0) * F.sum(F.when(F.col("CustomerName").isNull() | (F.trim("CustomerName") == ""), 1).otherwise(0))
         / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(9,4)").alias("m")).collect()[0]["m"]
    measuredValue = float(nullRate or 0)

    customerDf = (stgCustomer.filter(F.col("BatchId") == batchId)
                  .select(F.col("CustomerBusinessKey").alias("CustomerCode"),
                          "CustomerName",
                          F.col("PrimaryCountryCode").alias("CountryCode"),
                          "RegionCode",
                          F.col("CustomerCategoryCode").alias("CustomerClassCode"),
                          "TaxRegistrationNumber",
                          "MarketingConsentFlag",
                          F.months_between(F.col("RetentionExpiryDate"),
                                           F.coalesce(F.col("MarketingConsentDate"), F.col("AccountOpenedDate")))
                          .alias("RetentionMonths"),
                          "CreditLimitAmount",
                          "SourceSystemCode", "SourceCustomerId", "CustomerBusinessKey", "BatchId"))
    countryDf = (spark.table(deltaTable(catalog, "ref.Country"))
                 .filter(F.col("IsActive").cast("int") == 1)
                 .select("CountryCode", F.col("RegionCode").alias("ReferenceRegionCode")))

    result = screens.screenCustomer(customerDf, countryDf)
    rowsRead = customerDf.count()
    rowsPassed = result.passed.count()

    failedColumn = (F.when(F.col("RejectReasonCode") == "DQ_CUST_NAME_NULL", "CustomerName")
                    .when(F.col("RejectReasonCode") == "DQ_CUST_CONSENT", "MarketingConsentFlag")
                    .when(F.col("RejectReasonCode") == "DQ_CUST_CREDIT_NEG", "CreditLimitAmount")
                    .otherwise("CountryCode"))
    failedValue = (F.when(F.col("RejectReasonCode") == "DQ_CUST_NAME_NULL", F.col("CustomerName"))
                   .when(F.col("RejectReasonCode") == "DQ_CUST_CONSENT", F.col("MarketingConsentFlag"))
                   .when(F.col("RejectReasonCode") == "DQ_CUST_CREDIT_NEG", F.col("CreditLimitAmount").cast("string"))
                   .otherwise(F.col("CountryCode")))
    rejects = (delta_io.withPayload(result.rejected, excludeColumns=["RejectBranch", "LookupName", "LookupColumnName", "LookupValue"])
               .withColumn("BatchId", F.lit(batchId).cast("long"))
               .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
               .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
               .withColumn("RejectReason", F.col("RejectBranch"))
               .withColumn("RejectStage", F.lit("Quality"))
               .withColumn("FailedColumnName", failedColumn)
               .withColumn("FailedValue", failedValue)
               .withColumn("ReprocessStatusCode", F.lit("NEW"))
               .withColumn("ReprocessAttemptCount", F.lit(0))
               .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.CUSTOMER_REASON_CODES)

    # Register Quality Rejects - stg.Customer (cursor over err rows -> usp_LogRejectedRecord)
    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "CustomerCode", control.logRejectedRecordSet)

    # Evaluate Rules - CUSTOMER (etl.usp_EvaluateDataQualityRules)
    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="CUSTOMER", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_CUST_NULL_RATE", measuredValue,
                            thresholdValue=2, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_CUST_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Passes All Rules output of DFT Screen Customer"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Customer Null Rate", "Customer name null rate exceeded the configured tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 2, "@[User::MeasuredValue] > 2"),
        Gate("Fail On Customer Rule Breach", "One or more blocking customer quality rules failed.",
             "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
