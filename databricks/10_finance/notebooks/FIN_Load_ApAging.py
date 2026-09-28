# Databricks notebook source
# MAGIC %md
# MAGIC # FIN_Load_ApAging
# MAGIC Month-end AP aging refresh: open-item aging buckets (CURRENT/B030/B060/B090/B090P) per unpaid supplier invoice, regional reportable amount (NA gross, EU net of recoverable VAT, APAC net of GST input credit), posted into `gold.fact_payment` plus the `gold.agg_ap_aging_summary` close-pack summary.
# MAGIC
# MAGIC Legacy: `ssis/10_finance/FIN_Load_ApAging.dtsx` (WWI_Finance). Control framework calls go through
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

PACKAGE_NAME = "FIN_Load_ApAging"

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


factPayment = naming.table(catalog, "gold", "fact_payment")
workAging = naming.table(catalog, "silver", "work_ap_aging_staging")
errAging = naming.table(catalog, "silver", "err_ap_aging_reject")
aggAging = naming.table(catalog, "gold", "agg_ap_aging_summary")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")

with fc.PackageExecution(spark, catalog, batchId, PACKAGE_NAME, stepName) as run:
    # Reload guard: a locked period cannot be re-aged.
    period_lock.assertPeriodOpen(spark, catalog, fin.accountingPeriod, PACKAGE_NAME)

    invoices = spark.sql(sources.apInvoiceSql(catalog))
    paymentTerms = spark.sql(sources.paymentTermsSql(catalog))

    # Data flow "Load AP Aging Buckets": source SQL + Derive Aging Attributes
    aging = rules.apAgingOpenItems(
        invoices, paymentTerms, fin.agingAsOfDate, batchId,
        includeDisputed=fin.includeDisputedInvoices, reloadFullHistory=reloadFullHistory,
    )

    # Lookup Supplier Key (no match -> err.ApAgingReject)
    if spark.catalog.tableExists(dimSupplier):
        dimCols = {c.lower(): c for c in spark.table(dimSupplier).columns}
        keyCol = dimCols.get("supplierkey")
        idCol = dimCols.get("wwisupplierid") or dimCols.get("supplierid") or dimCols.get("supplierbusinesskey")
        validTo = dimCols.get("validto")
        dim = spark.table(dimSupplier)
        if validTo:
            dim = dim.where(F.col(validTo) > F.current_timestamp())
        supplierDim = dim.select(F.col(keyCol).alias("SupplierKey"), F.col(idCol).cast("string").alias("SupplierId"))
        matched, rejected = rules.lookupSupplierKey(aging, supplierDim)
    else:
        control.logError(spark, catalog, packageExecutionId=run.packageExecutionId, batchId=batchId,
                         errorSeverity="Warning", errorCode=None, sourceName=PACKAGE_NAME,
                         sourceComponent="Lookup Supplier Key", procedureName=None,
                         errorDescription=f"{dimSupplier} not found; SupplierKey left NULL (lookup skipped).")
        matched = aging.withColumn("SupplierKey", F.lit(None).cast("bigint"))
        rejected = aging.limit(0).withColumn("RejectReasonCode", F.lit("SUPPLIER_NOT_FOUND"))

    matched = matched.withColumn("BatchId", F.lit(batchId).cast("bigint")).cache()
    run.rowsRead = aging.count()

    # work.ApAgingStaging (legacy TRUNCATE + load -> replaceWhere per batch)
    delta_io.replaceWhere(matched.select(*[c for c in spark.table(workAging).columns if c in matched.columns]),
                          workAging, f"BatchId = {batchId}")

    rejectedRows = rejected.select("ApInvoiceKey", "SupplierId", "InvoiceNumber", "LedgerCode", "RegionCode",
                                   "OpenAmount", "RejectReasonCode") \
        .withColumn("BatchId", F.lit(batchId).cast("bigint")).withColumn("RejectedAtUtc", F.current_timestamp())
    delta_io.replaceWhere(rejectedRows, errAging, f"BatchId = {batchId}")
    run.rowsRejected = control.logRejectedRecordSet(
        spark, catalog, "stg.ApInvoice", rejected, batchId=batchId, packageExecutionId=run.packageExecutionId,
        sourceSystemCode="ORAERP", rejectStage="Fact", rejectReasonCode="SUPPLIER_NOT_FOUND",
        businessKeyColumn="InvoiceNumber",
    ) or 0

    # Integration.usp_PostApAging -> MERGE into gold.fact_payment
    controlAccounts = delta_io.controlAccounts(spark, catalog, "AP")
    factRows = (
        matched.join(controlAccounts, "LedgerCode", "left")
        .select(
            F.concat(F.lit("APAGING|"), F.col("ApInvoiceKey").cast("string")).alias("PaymentBusinessKey"),
            F.lit("AP_AGING").alias("PaymentSourceCode"),
            "SupplierKey", "SupplierId", "InvoiceNumber", "InvoiceDate", "DueDate", "RegionCode", "LedgerCode",
            F.lit(fin.accountingPeriod).alias("AccountingPeriod"),
            "ControlAccount",
            F.lit("BS").alias("AccountClass"),
            F.col("CurrencyCode").alias("TransactionCurrencyCode"),
            F.lit(None).cast("string").alias("EntityCurrencyCode"),
            F.col("OpenAmount").cast("decimal(19,4)").alias("TransactionAmount"),
            F.col("OpenAmount").cast("decimal(19,4)").alias("FunctionalAmount"),
            F.col("OpenAmount").cast("decimal(19,4)").alias("OpenAmount"),
            F.col("PaidAmount").cast("decimal(19,4)").alias("PaymentAmount"),
            "AgingBucketCode", "AgingBucketSort", "DaysPastDue", "IsPastDue",
            F.col("ReportableAmount").cast("decimal(19,4)").alias("ReportableAmount"),
            "DiscountAtRisk", "AsOfDate",
            F.lit(batchId).cast("bigint").alias("BatchId"),
            F.current_timestamp().alias("LoadDatetime"),
        )
    )
    run.rowsInserted = delta_io.mergeInto(spark, factPayment, factRows, ["PaymentBusinessKey"])

    # "Refresh Aging Summary" cursor -> one set-based aggregate per Ledger/Bucket
    summary = rules.apAgingSummary(matched).withColumn("AsOfDate", F.lit(fin.agingAsOfDate)) \
        .withColumn("AccountingPeriod", F.lit(fin.accountingPeriod)).withColumn("BatchId", F.lit(batchId).cast("bigint"))
    delta_io.replaceWhere(summary, aggAging, f"AsOfDate = '{fin.agingAsOfDate.isoformat()}'")

    run.logRowCount("Fact.Payment", sourceRowCount=run.rowsRead, targetRowCount=run.rowsInserted,
                    insertRowCount=run.rowsInserted, rejectRowCount=run.rowsRejected)
    matched.unpersist()

dbutils.notebook.exit(f"{PACKAGE_NAME} ok: read={run.rowsRead} inserted={run.rowsInserted} rejected={run.rowsRejected}")
