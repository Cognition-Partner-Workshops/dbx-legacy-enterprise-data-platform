# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Load_WithholdingTax
# MAGIC Splits AP invoice lines into net payable and withholding tax by jurisdiction (NA 1099 service categories, EU treaty rate when the supplier has a registration number, APAC threshold withholding), quarantines lines whose jurisdiction has no rate, posts withheld lines into `gold.fact_payment` and queues EU withholding certificates.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Load_WithholdingTax.dtsx` (WWI_Finance). Control framework calls go through
# MAGIC `dbx_etl_common` (session 00); finance logic lives in `../src/finance_rules.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import delta_io
import finance_common as fc
import finance_rules as rules
import finance_sources as sources
import finance_tables as tables
import period_lock

PACKAGE_NAME = "FIN_Load_WithholdingTax"

# COMMAND ----------

p = params.getJobParams(dbutils)
catalog = p["catalog"]
fin = fc.getFinanceParams(dbutils, p["businessDate"])
if fc.shouldSkipForRestart(PACKAGE_NAME, p.get("restartFromStep")):
    dbutils.notebook.exit(f"{PACKAGE_NAME} skipped: RestartFromStep={p.get('restartFromStep')}")

batchId, ownsBatch = fc.resolveBatchId(spark, catalog, p)
stepName, stepSequence, stepGroup = fc.stepFor(PACKAGE_NAME)
reloadFullHistory = bool(p.get("reloadFullHistory"))

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
tables.ensureFinanceTables(spark, catalog)
tables.ensureGoldTargets(spark, catalog)
print(f"{PACKAGE_NAME} batch={batchId} period={fin.accountingPeriod} businessDate={p['businessDate']} catalog={catalog}")

# COMMAND ----------


workWht = naming.table(catalog, "silver", "work_withholding_tax_line")
errWht = naming.table(catalog, "silver", "err_withholding_tax_reject")
certQueue = naming.table(catalog, "silver", "work_withholding_certificate_queue")
factPayment = naming.table(catalog, "gold", "fact_payment")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    lines = spark.sql(sources.apInvoiceLineSql(catalog))
    whtRates = spark.table(naming.table(catalog, "silver", "stg_withholding_tax_rate"))

    # Data flow "Split Withholding Tax"
    withholding = rules.withholdingLines(lines, whtRates, batchId, fin.jurisdictionScope, reloadFullHistory)
    mapped, unmapped = rules.splitUnmappedJurisdictions(withholding)
    mapped = mapped.withColumn("AccountingPeriod", F.lit(fin.accountingPeriod)).withColumn("BatchId", F.lit(batchId).cast("bigint")).cache()
    unmapped = unmapped.cache()
    run.rowsRead = withholding.count()

    delta_io.replaceWhere(mapped.select(*[c for c in spark.table(workWht).columns if c in mapped.columns]), workWht, f"BatchId = {batchId}")
    rejectRows = unmapped.select("ApInvoiceLineId", "ApInvoiceKey", "SupplierId", "JurisdictionCode", "RegionCode",
                                 "ServiceCategoryCode", "LineAmount", "WithholdingAmount", "RejectReasonCode") \
        .withColumn("BatchId", F.lit(batchId).cast("bigint")).withColumn("RejectedAtUtc", F.current_timestamp())
    delta_io.replaceWhere(rejectRows, errWht, f"BatchId = {batchId}")
    run.rowsRejected = control.logRejectedRecordSet(
        spark, catalog, "stg.ApInvoiceLine", unmapped, batchId=batchId, packageExecutionId=run.packageExecutionId,
        sourceSystemCode="ORAERP", rejectStage="Stage", rejectReasonCode="JURISDICTION_UNMAPPED",
        businessKeyColumn="ApInvoiceLineId",
    ) or 0

    # Integration.usp_PostWithholdingTax -> MERGE withheld lines into gold.fact_payment
    controlAccounts = delta_io.controlAccounts(spark, catalog, "WHT")
    factRows = (
        mapped.where(F.col("IsWithheld"))
        .join(controlAccounts, "LedgerCode", "left")
        .select(
            F.concat(F.lit("WHT|"), F.col("ApInvoiceLineId").cast("string")).alias("PaymentBusinessKey"),
            F.lit("WITHHOLDING").alias("PaymentSourceCode"),
            "SupplierId", "InvoiceDate", "RegionCode", "LedgerCode", "AccountingPeriod", "ControlAccount",
            F.lit("BS").alias("AccountClass"),
            F.col("LineAmount").cast("decimal(19,4)").alias("TransactionAmount"),
            F.col("LineAmount").cast("decimal(19,4)").alias("FunctionalAmount"),
            F.col("NetPayableAmount").alias("PaymentAmount"),
            F.col("WithholdingAmount").alias("WithholdingTaxAmount"),
            "BatchId", F.current_timestamp().alias("LoadDatetime"),
        )
    )
    run.rowsInserted = delta_io.mergeInto(spark, factPayment, factRows, ["PaymentBusinessKey"])

    # "Queue EU Withholding Certificates"
    delta_io.replaceWhere(rules.certificateQueue(mapped, batchId), certQueue, f"BatchId = {batchId}")

    run.logRowCount("Fact.Payment", sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                    insertRowCount=run.rowsInserted, rejectRowCount=run.rowsRejected)
    mapped.unpersist(); unmapped.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: read={run.rowsRead} posted={run.rowsInserted} rejected={run.rowsRejected}")
