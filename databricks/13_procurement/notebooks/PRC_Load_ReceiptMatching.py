# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Load_ReceiptMatching
# MAGIC Migrated from `ssis/13_procurement/PRC_Load_ReceiptMatching.dtsx` (WWI_Procurement).
# MAGIC
# MAGIC Three-way match of goods receipts against the purchase order line and the AP invoice line using
# MAGIC the regional tolerance table (defaults 2% quantity, 3% / 1.00 price). MATCHED rows and GRNI
# MAGIC accruals inside `AccrualCutoffDays` are MERGEd into `gold.fact_purchase_receipt`; every receipt
# MAGIC is written to `silver.work_receipt_match`; QTYEXCEPT / PRICEEXCEPT rows are raised as
# MAGIC `etl.rejected_record` rows through the control framework.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME, SOURCE_SYSTEM_CODE  # noqa: E402
from procurement_lib import delta_io  # noqa: E402
from procurement_lib import purchase_spend as ps  # noqa: E402
from procurement_lib import receipt_matching as rm  # noqa: E402
from procurement_lib.common import parseInt  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Load_ReceiptMatching"
STEP_NAME = "Procurement Mart"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("AccrualCutoffDays", "45")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
accrualCutoffDays = parseInt(dbutils.widgets.get("AccrualCutoffDays"), 45)

stgReceipt = naming.table(catalog, "silver", "stg_receipt")
stgPurchaseOrderLine = naming.table(catalog, "silver", "stg_purchase_order_line")
stgPurchaseOrder = naming.table(catalog, "silver", "stg_purchase_order")
stgApInvoiceLine = naming.table(catalog, "silver", "stg_ap_invoice_line")
stgMatchTolerance = naming.table(catalog, "silver", "stg_match_tolerance")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")
workReceiptMatch = naming.table(catalog, "silver", "work_receipt_match")
factPurchaseReceipt = naming.table(catalog, "gold", "fact_purchase_receipt")

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName=STEP_NAME) as run:
    # Data flow "Match Receipts": source -> Derive Match Variances -> Evaluate Match Result -> Split Match Outcome
    matchInput = rm.buildReceiptMatchInput(
        spark.table(stgReceipt), spark.table(stgPurchaseOrderLine), spark.table(stgPurchaseOrder),
        spark.table(stgApInvoiceLine), spark.table(stgMatchTolerance), batchId=batchId,
    )
    evaluated = rm.evaluateMatchResult(rm.deriveMatchVariances(matchInput), businessDate).cache()
    matched, grni, exceptions = rm.splitMatchOutcome(evaluated)
    supplierKeys = ps.currentSupplierKeys(spark.table(dimSupplier))

    # "Truncate work_ReceiptMatch" + "work ReceiptMatch" destination (all outcomes, for downstream analysis)
    delta_io.overwriteTable(evaluated, workReceiptMatch)
    rowsRead = delta_io.countWhere(spark, workReceiptMatch)

    # "Fact Purchase Receipt" branch (MATCHED) + "Accrue GRNI" (young GRNI receipts at PO price)
    matchedFact = rm.toFactPurchaseReceipt(matched.join(supplierKeys, "SupplierId", "inner"), batchId)
    accrualFact = rm.toFactPurchaseReceipt(rm.buildAccruals(grni, supplierKeys, accrualCutoffDays), batchId)
    factRows = matchedFact.unionByName(accrualFact).cache()
    rowsInserted = factRows.count()
    delta_io.mergeInto(spark, factRows, factPurchaseReceipt,
                       keyColumns=["receipt_number", "receipt_line_number", "purchase_order_number", "purchase_order_line_number"])

    # "Raise Exceptions": QTYEXCEPT / PRICEEXCEPT -> etl.rejected_record
    rejectDf = exceptions.withColumn("RejectReasonCode", exceptions["MatchResultCode"])
    rowsRejected = control.logRejectedRecordSet(
        spark, catalog, rm.REJECT_OBJECT_NAME, rejectDf, batchId=batchId,
        packageExecutionId=run.packageExecutionId, sourceSystemCode=SOURCE_SYSTEM_CODE,
        rejectStage="Mart", rejectReasonCode="MATCH_EXCEPTION", businessKeyColumn="ReceiptId",
    )

    # "Measure Match Outcomes"
    grniCount, matchExceptionCount = rm.measureMatchOutcomes(grni, exceptions)
    print("GrniCount=%s MatchExceptionCount=%s AccrualCutoffDays=%s" % (grniCount, matchExceptionCount, accrualCutoffDays))

    # "Log Row Counts"
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Purchase Receipt",
                        sourceRowCount=rowsRead, targetRowCount=rowsInserted,
                        insertRowCount=rowsInserted, rejectRowCount=rowsRejected)
    run.rowsRead = rowsRead
    run.rowsInserted = rowsInserted
    run.rowsUpdated = 0
    run.rowsRejected = rowsRejected
    factRows.unpersist()
    evaluated.unpersist()

dbutils.notebook.exit("%s: rowsRead=%s rowsInserted=%s rowsRejected=%s grni=%s"
                      % (PACKAGE_NAME, rowsRead, rowsInserted, rowsRejected, grniCount))
