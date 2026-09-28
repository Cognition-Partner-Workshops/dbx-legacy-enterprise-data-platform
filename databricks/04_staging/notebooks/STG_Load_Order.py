# Databricks notebook source
# MAGIC %md
# MAGIC ### STG_Load_Order
# MAGIC Legacy: `ssis/04_staging/STG_Load_Order.dtsx` (WWI_Staging). Watermarked (LastEditedWhen) order header conformance with free-text scrubbing and stg.Customer lookup, plus line pricing / tax extension / picking state; `stg.usp_AppendIncremental_OrderLine` delete-then-insert per batch is the keyed merge.
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

run = StagingRun(spark, dbutils, "STG_Load_Order", sourceSystemCode="SQL_WWI", objectName="stg.Order", watermark=True, jobParams=p)

ORDER_COLUMNS = ["OrderId", "CustomerId", "CustomerCode", "RegionCode", "SalespersonId", "OrderDate", "ExpectedDeliveryDate", "CustomerPoNumber", "BackorderFlag", "OrderComments", "LastEditedWhen"]
LINE_COLUMNS = ["OrderLineId", "OrderId", "StockItemId", "LineDescription", "Quantity", "UnitPriceAmount", "ExtendedAmount", "LineTaxAmount", "PickedFlag", "PackageTypeCode"]


def rejectLine(df, code, reason):
    return run.reject(
        df, "err_rejected_order_line", code, reason, businessKeyColumn="OrderLineId", objectName="stg.OrderLine",
        keyColumns={
            "OrderBusinessKey": X.sourceSystemKey(F.lit(run.sourceSystemCode), "OrderId"),
            "OrderLineBusinessKey": F.concat_ws("|", X.sourceSystemKey(F.lit(run.sourceSystemCode), "OrderId"), F.col("OrderLineId").cast("string")),
            "LineNumber": F.col("OrderLineId").cast("string"), "StockItemReference": F.col("StockItemId").cast("string"),
            "OrderedQuantityText": F.col("Quantity").cast("string"), "UnitPriceText": F.col("UnitPriceAmount").cast("string"),
        },
    )


def load(run):
    orders = run.bronzeWatermarked("raw_sql_order", "LastEditedWhen")
    run.countRead(orders)
    header = T.deriveOrderHeader(orders)
    # stg.Customer has no CustomerId in the package vocabulary: the OLTP CustomerID is matched on the source customer id kept as CustomerCode
    customers = run.silver("stg_customer").select(F.col("CustomerCode").alias("CustomerId"), "CustomerCode", "RegionCode")
    matched, unknownCustomer = lookupLeft(header, customers, ["CustomerId"], ["CustomerCode", "RegionCode"])
    run.rejectLookupFailures(unknownCustomer, "Customer", "CustomerId", "OrderId", "Order customer not in stg.Customer")
    run.mergeByKey(matched.select(*ORDER_COLUMNS), "stg_order", ["OrderId"])

    lines = run.bronze("raw_sql_order_line", currentBatchOnly=False).join(orders.select("OrderID").distinct(), "OrderID")
    lineDerived = T.deriveOrderLine(lines)
    valid, badQty, badPrice = T.splitOrderLine(lineDerived)
    rejectLine(badQty, "NEG_QTY", "Ordered quantity is not positive")
    rejectLine(badPrice, "NEG_PRICE", "Unit price is negative")
    lineCount = run.mergeByKey(valid.select(*LINE_COLUMNS), "stg_order_line", ["OrderLineId"])
    run.counters.rowsUpdated += lineCount
    run.logRowCount(objectName="stg.OrderLine", insertRowCount=lineCount)


with run.execute():
    if not run.skipped:
        load(run)
        run.logRowCount()
