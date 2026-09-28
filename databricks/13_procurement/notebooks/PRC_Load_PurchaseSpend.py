# Databricks notebook source
# MAGIC %md
# MAGIC # PRC_Load_PurchaseSpend
# MAGIC Migrated from `ssis/13_procurement/PRC_Load_PurchaseSpend.dtsx` (WWI_Procurement).
# MAGIC
# MAGIC Loads purchase order spend for the current batch, resolves the vendor contract (including
# MAGIC pre-2013 legacy contract references), classifies each line ONCONTRACT / OFFCONTRACT / MAVERICK,
# MAGIC derives contracted amount and savings, looks up the supplier key (unknown suppliers ->
# MAGIC `silver.err_purchase_spend_reject`), writes `silver.work_purchase_spend_line` and publishes
# MAGIC to `gold.fact_purchase` (idempotent MERGE on PO number + line number).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402

from procurement_lib import PROJECT_NAME, SOURCE_SYSTEM_CODE  # noqa: E402
from procurement_lib import delta_io  # noqa: E402
from procurement_lib import purchase_spend as ps  # noqa: E402
from procurement_lib.common import parseInt  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "PRC_Load_PurchaseSpend"
STEP_NAME = "Procurement Mart"

for name, default in (("BatchId", "0"), ("BusinessDate", ""), ("ReloadFullHistory", "False"), ("EnvironmentCode", "DEV"),
                      ("RestartFromStep", ""), ("catalog", ""), ("SpendCategoryScope", "ALL"),
                      ("PriceVarianceTolerancePercent", "5")):
    dbutils.widgets.text(name, default)

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = p["batchId"]
businessDate = p["businessDate"]
spendCategoryScope = dbutils.widgets.get("SpendCategoryScope") or "ALL"
priceVarianceTolerancePercent = parseInt(dbutils.widgets.get("PriceVarianceTolerancePercent"), 5)

stgPurchaseOrderLine = naming.table(catalog, "silver", "stg_purchase_order_line")
stgPurchaseOrder = naming.table(catalog, "silver", "stg_purchase_order")
stgVendorContract = naming.table(catalog, "silver", "stg_vendor_contract")
dimSupplier = naming.table(catalog, "gold", "dim_supplier")
workPurchaseSpendLine = naming.table(catalog, "silver", "work_purchase_spend_line")
errPurchaseSpendReject = naming.table(catalog, "silver", "err_purchase_spend_reject")
factPurchase = naming.table(catalog, "gold", "fact_purchase")

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=PROJECT_NAME, stepName=STEP_NAME) as run:
    # Data flow "Load Purchase Spend": source query -> derived columns -> supplier lookup -> destinations
    spendLines = ps.buildPurchaseSpendLines(
        spark.table(stgPurchaseOrderLine), spark.table(stgPurchaseOrder), spark.table(stgVendorContract),
        batchId=batchId, categoryScope=spendCategoryScope,
    )
    spendLines = ps.deriveSavings(ps.classifySpend(spendLines))
    supplierKeys = ps.currentSupplierKeys(spark.table(dimSupplier))
    matched, rejected = ps.lookupSupplierKey(spendLines, supplierKeys)
    matched = matched.cache()

    # "Truncate work_PurchaseSpendLine" + "work PurchaseSpendLine" destination
    delta_io.overwriteTable(matched, workPurchaseSpendLine)
    rowsInserted = delta_io.countWhere(spark, workPurchaseSpendLine)
    rowsRead = rowsInserted + rejected.count()

    # "err PurchaseSpendReject" (lookup no-match redirect) + control-framework reject log
    delta_io.overwriteTable(rejected, errPurchaseSpendReject)
    rowsRejected = control.logRejectedRecordSet(
        spark, catalog, "stg.PurchaseOrderLine", rejected, batchId=batchId,
        packageExecutionId=run.packageExecutionId, sourceSystemCode=SOURCE_SYSTEM_CODE,
        rejectStage="Mart", rejectReasonCode="UNKNOWN_SUPPLIER", businessKeyColumn="PurchaseOrderLineId",
    )

    # "Publish Purchase Spend" (Integration.usp_PostPurchaseSpend): MERGE work rows into the fact
    delta_io.mergeInto(spark, ps.toFactPurchase(matched, batchId), factPurchase,
                       keyColumns=["purchase_order_number", "purchase_order_line_number"])

    # "Measure Maverick Spend"
    offContractCount, maverickSpendAmount = ps.measureMaverickSpend(matched)
    print("OffContractCount=%s MaverickSpendAmount=%s (PriceVarianceTolerancePercent=%s)"
          % (offContractCount, maverickSpendAmount, priceVarianceTolerancePercent))

    # "Log Row Counts"
    control.logRowCount(spark, catalog, run.packageExecutionId, "Fact.Purchase",
                        sourceRowCount=rowsRead, targetRowCount=rowsInserted,
                        insertRowCount=rowsInserted, rejectRowCount=rowsRejected)
    run.rowsRead = rowsRead
    run.rowsInserted = rowsInserted
    run.rowsUpdated = 0
    run.rowsRejected = rowsRejected
    matched.unpersist()

dbutils.notebook.exit("%s: rowsRead=%s rowsInserted=%s rowsRejected=%s offContractCount=%s"
                      % (PACKAGE_NAME, rowsRead, rowsInserted, rowsRejected, offContractCount))
