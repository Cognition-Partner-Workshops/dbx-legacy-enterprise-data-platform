# Databricks notebook source
# MAGIC %md
# MAGIC # DQ_InvoiceLine_Screen
# MAGIC Migrated from `ssis/05_data_quality/DQ_InvoiceLine_Screen.dtsx` (WWI_DataQuality).
# MAGIC Joins staged sale (invoice) lines to their invoice header, validates the invoice currency against
# MAGIC `silver.ref_currency`, recomputes the line tax, flags tax variance / tax-regime / minor-unit breaches,
# MAGIC quarantines offenders into `silver.err_rejected_invoice_line`, evaluates rule group `INVOICELINE`
# MAGIC and raises the legacy gates.

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

PACKAGE_NAME = "DQ_InvoiceLine_Screen"
SOURCE_SYSTEM_CODE = "WWI_OLTP"
OBJECT_NAME = "stg.SaleLine"
ERR_TABLE = "err.RejectedInvoiceLine"

ensureWidgets(dbutils)
p = params.getJobParams(dbutils)
catalog, batchId = p["catalog"], p["batchId"]

if shouldSkipForRestart(p["restartFromStep"], PACKAGE_STEP[PACKAGE_NAME]):
    dbutils.notebook.exit("skipped: RestartFromStep=%s" % p["restartFromStep"])

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME,
                        stepName=PACKAGE_STEP[PACKAGE_NAME]) as run:
    packageExecutionId = run.packageExecutionId
    stgSaleLine = spark.table(deltaTable(catalog, OBJECT_NAME))
    stgSale = spark.table(deltaTable(catalog, "stg.Sale"))

    # TaxVarianceAmount / RecomputedTaxAmount are derived from the staged amounts (the legacy
    # source query assumed them as stg.SaleLine columns).
    recomputedTax = F.round(F.col("l.NetLineAmount") * F.coalesce(F.col("l.TaxRatePercent"), F.lit(0)) / 100.0, 2)
    lineBase = (stgSaleLine.alias("l")
                .join(stgSale.alias("s"), F.col("l.SaleBusinessKey") == F.col("s.SaleBusinessKey"), "inner")
                .withColumn("RecomputedTaxAmount", recomputedTax.cast("decimal(18,2)"))
                .withColumn("TaxVarianceAmount", F.abs(F.col("l.TaxAmount") - F.col("RecomputedTaxAmount")).cast("decimal(18,2)")))

    # Measure Invoice Tax Variance (whole table)
    variance = lineBase.select(F.coalesce(F.sum("TaxVarianceAmount"), F.lit(0)).cast("decimal(18,4)").alias("m")).collect()[0]["m"]
    measuredValue = float(variance or 0)

    saleLineDf = (lineBase.filter(F.col("l.BatchId") == batchId)
                  .select(F.col("l.SaleLineBusinessKey").alias("InvoiceLineId"),
                          F.col("l.SaleBusinessKey").alias("InvoiceId"),
                          F.col("l.StockItemBusinessKey").alias("StockItemId"),
                          F.col("l.Quantity"),
                          F.col("l.NetLineAmount").alias("NetAmount"),
                          F.col("l.TaxAmount"),
                          F.col("RecomputedTaxAmount"), F.col("TaxVarianceAmount"),
                          F.col("s.TransactionCurrencyCode").alias("SaleCurrencyCode"),
                          F.col("s.RegionCode"),
                          F.col("l.TaxRegimeCode"),
                          F.col("l.SourceSystemCode"), F.col("l.SaleBusinessKey"), F.col("l.SaleLineBusinessKey"),
                          F.col("s.SourceInvoiceId"), F.col("l.LineNumber"), F.col("l.BatchId")))
    currencyDf = (spark.table(deltaTable(catalog, "ref.Currency"))
                  .filter(F.col("IsActive").cast("int") == 1)
                  .select(F.col("CurrencyCode").alias("SaleCurrencyCode"), "CurrencyName", "MinorUnitDigits"))

    result = screens.screenInvoiceLine(saleLineDf, currencyDf)
    rowsRead = saleLineDf.count()
    rowsPassed = result.passed.count()

    rejects = (delta_io.withPayload(result.rejected, excludeColumns=["RejectBranch", "LookupName", "LookupColumnName", "LookupValue"])
               .withColumn("BatchId", F.lit(batchId).cast("long"))
               .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long"))
               .withColumn("SourceSystemCode", F.coalesce(F.col("SourceSystemCode"), F.lit(SOURCE_SYSTEM_CODE)))
               .withColumn("InvoiceBusinessKey", F.col("SaleBusinessKey"))
               .withColumn("InvoiceLineBusinessKey", F.col("SaleLineBusinessKey"))
               .withColumn("InvoiceNumber", F.col("SourceInvoiceId").cast("string"))
               .withColumn("TaxCode", F.col("TaxRegimeCode"))
               .withColumn("LineAmountText", F.col("NetAmount").cast("string"))
               .withColumn("RejectReason", F.col("RejectBranch"))
               .withColumn("RejectStage", F.lit("Quality"))
               .withColumn("ExpectedTaxAmount", F.col("RecomputedTaxAmount"))
               .withColumn("ActualTaxAmount", F.col("TaxAmount"))
               .withColumn("VarianceAmount", F.col("TaxVarianceAmount"))
               .withColumn("ReprocessStatusCode", F.lit("NEW"))
               .withColumn("ReprocessAttemptCount", F.lit(0))
               .withColumn("RejectedAtUtc", F.current_timestamp()))
    rowsRejected = delta_io.writeRejects(spark, rejects, deltaTable(catalog, ERR_TABLE), batchId,
                                         screens.INVOICE_LINE_REASON_CODES)
    delta_io.registerRejects(spark, catalog, rejects, OBJECT_NAME, batchId, packageExecutionId,
                             SOURCE_SYSTEM_CODE, "SaleLineBusinessKey", control.logRejectedRecordSet)

    failedRuleCount = control.evaluateDataQualityRules(spark, catalog, batchId=batchId,
                                                       packageExecutionId=packageExecutionId,
                                                       ruleGroupCode="INVOICELINE", objectName=OBJECT_NAME)

    delta_io.writeResults(spark, catalog, [
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_IL_TAX_VARIANCE", measuredValue,
                            thresholdValue=100, rowsEvaluated=rowsRead),
        delta_io.measureRow(batchId, packageExecutionId, OBJECT_NAME, "DQ_IL_SCREEN_PASS", rowsPassed,
                            rowsEvaluated=rowsRead, resultStatus="Passed",
                            detailText="Passes All Rules output of DFT Screen Invoice Line"),
    ], batchId)

    control.logRowCount(spark, catalog, packageExecutionId, OBJECT_NAME, sourceRowCount=rowsRead,
                        targetRowCount=rowsPassed, rejectRowCount=rowsRejected)
    run.rowsRead, run.rowsInserted, run.rowsRejected = rowsRead, rowsPassed, rowsRejected
    setTaskValue(dbutils, "rowsRejected", rowsRejected)
    setTaskValue(dbutils, "failedRuleCount", int(failedRuleCount or 0))
    setTaskValue(dbutils, "measuredValue", measuredValue)

    applyGates(spark, catalog, [
        Gate("Warn On Aggregate Tax Variance", "Aggregate invoice line tax variance exceeded tolerance.",
             "Warning", lambda m: m["MeasuredValue"] > 100, "@[User::MeasuredValue] > 100"),
        Gate("Fail On Invoice Line Rule Breach", "One or more blocking invoice line quality rules failed.",
             "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ], {"MeasuredValue": measuredValue, "FailedRuleCount": int(failedRuleCount or 0)},
        packageExecutionId, batchId, PACKAGE_NAME, control.logError)
