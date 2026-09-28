# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_PurchaseOrder
# MAGIC Legacy: `ssis/04_staging/STG_Load_PurchaseOrder.dtsx` (WWI_Staging). Watermarked (LAST_UPD_DT) purchase-order header (status conformance, SPOT FX to USD) and line (UoM to eaches, work.ProductCrosswalk item resolution) conformance; `work.usp_BuildProductCrosswalk @OnlyMissing=1` is run by STG_Work_ProductCrosswalk, which this task depends on.
# MAGIC
# MAGIC Control framework: `dbx_etl_common` (session 00) via `stg_common.loader.StagingRun`
# MAGIC (`control.packageRun` / `getWatermark` / `setWatermark` / `logRejectedRecordSet` / `logRowCount`).

# COMMAND ----------

import os
import sys

from pyspark.sql import Window  # noqa: F401
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, naming, params  # noqa: E402,F401
from stg_common import expressions as X  # noqa: E402,F401
from stg_common import refs  # noqa: E402,F401
from stg_common import transforms as T  # noqa: E402,F401
from stg_common import work as W  # noqa: E402,F401
from stg_common.loader import PHASE_STAGE_WORK, StagingRun, lookupIgnore, lookupLeft, splitByCondition  # noqa: E402,F401

p = params.getJobParams(dbutils)  # noqa: F821  (batchId, businessDate, reloadFullHistory, environmentCode, restartFromStep, catalog)

# COMMAND ----------

run = StagingRun(spark, dbutils, "STG_Load_PurchaseOrder", sourceSystemCode="ORA_ERP", objectName="stg.PurchaseOrder", watermark=True, jobParams=p)

HEADER_COLUMNS = ["PurchaseOrderNumber", "SupplierCode", "OrderStatusCode", "BuyingOrgCode", "RegionCode", "OrderCurrencyCode", "ToCurrencyCode", "ConversionRate", "OrderDate", "PromisedDate", "OrderTotalAmount", "OrderTotalAmountUsd", "LastUpdatedDate", "ChangeHash"]
LINE_COLUMNS = ["PurchaseOrderNumber", "LineNumber", "SourceItemCode", "ProductKey", "StockItemId", "SourceUomCode", "ToUomCode", "ConversionFactor", "OrderQuantity", "OrderQuantityBase", "UnitPriceAmount", "ExtendedAmount", "TaxCode", "NeedByDate"]


def fxToUsd():
    return run.silver("stg_fx_rate").where((F.col("ToCurrencyCode") == "USD") & (F.col("RateTypeCode") == "SPOT")).select(
        F.col("FromCurrencyCode").alias("OrderCurrencyCode"), F.col("EffectiveFromDate").alias("FxEffectiveDate"), F.col("ExchangeRate").alias("ConversionRate"), "ToCurrencyCode"
    )


def load(run):
    headers = run.bronzeWatermarked("raw_oracle_purchase_order_hdr", "LAST_UPD_DT")
    run.countRead(headers)
    derived = T.derivePurchaseOrderHeader(headers)
    withFx, unknownFx = lookupLeft(derived, fxToUsd(), ["OrderCurrencyCode", "FxEffectiveDate"], ["ConversionRate", "ToCurrencyCode"])
    run.rejectLookupFailures(unknownFx.withColumn("FxKey", F.concat_ws("|", "OrderCurrencyCode", "FxEffectiveDate")), "PO FX Rate", "FxKey", "PurchaseOrderNumber", "No SPOT rate to USD on the order date")
    valid, negative, unknownStatus = T.splitPurchaseOrderHeader(T.purchaseOrderHeaderFx(withFx))
    run.rejectConstraint(negative, "stg.PurchaseOrder", "CK_stgPurchaseOrder_Total", "PurchaseOrderNumber", "OrderTotalAmount", "NEGATIVE_TOTAL", "Order total is negative")
    run.rejectConstraint(unknownStatus, "stg.PurchaseOrder", "CK_stgPurchaseOrder_Status", "PurchaseOrderNumber", "OrderStatusCode", "UNKNOWN_STATUS", "Order status could not be conformed")
    run.mergeByKey(valid.select(*HEADER_COLUMNS), "stg_purchase_order", ["PurchaseOrderNumber"])

    lines = run.bronze("raw_oracle_purchase_order_line", currentBatchOnly=False).join(headers.select("PO_NBR").distinct(), "PO_NBR")
    lineDerived = T.derivePurchaseOrderLine(lines)
    uom = refs.uomConversion(run).where(F.col("ToUomCode") == "EA").select(F.col("FromUomCode").alias("SourceUomCode"), "ToUomCode", "ConversionFactor")
    withUom, unknownUom = lookupLeft(lineDerived, uom, ["SourceUomCode"], ["ToUomCode", "ConversionFactor"])
    run.rejectLookupFailures(unknownUom, "UoM Conversion", "SourceUomCode", "PurchaseOrderNumber", "Order UoM has no conversion to EA", objectName="stg.PurchaseOrderLine")
    # legacy phase order: Stage Load runs before Stage Work Tables, so the crosswalk is the previous run's
    crosswalkTable = run.silverIfExists("work_product_crosswalk", schemaLike=W.emptyProductCrosswalk(spark))
    crosswalk = crosswalkTable.where(F.col("SourceSystemCode") == "ORA_ERP").select("SourceItemCode", "ProductKey", "StockItemId").dropDuplicates(["SourceItemCode"])
    withItem, unknownItem = lookupLeft(withUom, crosswalk, ["SourceItemCode"], ["ProductKey", "StockItemId"])
    run.rejectLookupFailures(unknownItem, "Product Crosswalk", "SourceItemCode", "PurchaseOrderNumber", "Supplier item not in work.ProductCrosswalk", objectName="stg.PurchaseOrderLine")
    validLine, zeroQty, negPrice = T.splitPurchaseOrderLine(T.purchaseOrderLineUom(withItem))
    keyed = lambda df: df.withColumn("PoLineKey", F.concat_ws("|", "PurchaseOrderNumber", "LineNumber"))  # noqa: E731
    run.rejectConstraint(keyed(zeroQty), "stg.PurchaseOrderLine", "CK_stgPurchaseOrderLine_Qty", "PoLineKey", "OrderQuantityBase", "ZERO_QUANTITY", "Base quantity is not positive")
    run.rejectConstraint(keyed(negPrice), "stg.PurchaseOrderLine", "CK_stgPurchaseOrderLine_Price", "PoLineKey", "UnitPriceAmount", "NEGATIVE_PRICE", "Unit price is negative")
    lineCount = run.mergeByKey(validLine.select(*LINE_COLUMNS), "stg_purchase_order_line", ["PurchaseOrderNumber", "LineNumber"])
    run.counters.rowsUpdated += lineCount
    run.logRowCount(objectName="stg.PurchaseOrderLine", insertRowCount=lineCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
