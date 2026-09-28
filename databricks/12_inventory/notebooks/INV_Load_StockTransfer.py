# Databricks notebook source
# MAGIC %md
# MAGIC # INV_Load_StockTransfer
# MAGIC Port of `ssis/12_inventory/INV_Load_StockTransfer.dtsx` (WWI_Inventory).
# MAGIC Values inter-site transfers (transfer price / uplifted standard cost for cross-region),
# MAGIC stages them in `silver.work_stock_transfer_movement`, posts issue/receipt legs to
# MAGIC `gold.fact_movement` by MERGE (set-based `Integration.usp_PostTransferMovements`) and
# MAGIC escalates aged in-transit transfers (`TRANSFER_AGED_IN_TRANSIT`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "INV_Load_StockTransfer"
ctx = runtime.PackageContext(spark, dbutils, PACKAGE_NAME)  # Log Package Start

inTransitAgeAlertDays = int(runtime.widget(dbutils, "InTransitAgeAlertDays", "10"))

# COMMAND ----------


def body(ctx: runtime.PackageContext) -> None:
    runtime.ensureProjectTables(spark, ctx.catalog)

    stockMovement = ctx.read("stg.StockMovement")
    stockItem = ctx.read("stg.StockItem")
    warehouseSite = ctx.read("stg.WarehouseSite")
    transferPrice = ctx.read("stg.TransferPrice")
    dimStockItem = ctx.read("Dimension.Stock Item")
    dimWarehouseSite = ctx.read("Dimension.Warehouse Site")

    # Truncate work_StockTransferMovement + Load Transfer Movements data flow
    source = transforms.buildTransferSource(stockMovement, stockItem, warehouseSite, transferPrice, ctx.batchId)
    derived = transforms.deriveTransferAttributes(source, runtime.utcNow()).withColumn(
        "BatchId", F.lit(ctx.batchId).cast("bigint")
    )
    despatched, receiptOnly = transforms.splitTransferLegs(derived)
    workTable = ctx.table("work.StockTransferMovement")
    ctx.rowsRead = runtime.overwriteTable(spark, despatched, workTable)
    receiptOnlyCount = runtime.overwriteTable(spark, receiptOnly, ctx.table("work.StockTransferReceiptOnly"))
    transfers = spark.table(workTable)

    inTransitCount = transfers.where(F.col("QuantityInTransit") > 0).count()
    crossRegionCount = transfers.where(F.col("IsCrossRegion") == F.lit(True)).count()
    print(f"InTransitCount={inTransitCount} CrossRegionCount={crossRegionCount} ReceiptOnly={receiptOnlyCount}")

    # Post Transfer Movements (@RowsInserted OUTPUT)
    movements = transforms.buildTransferMovements(transfers, dimStockItem, dimWarehouseSite, ctx.batchId)
    inserted, updated = runtime.mergeByKey(spark, movements, ctx.table("Fact.Movement"), ["NaturalKeyHash"])
    ctx.rowsInserted = inserted
    ctx.rowsUpdated = updated

    # Escalate Aged In Transit
    ctx.logRejectedSet(
        "stg.StockMovement", transforms.agedInTransit(transfers, inTransitAgeAlertDays),
        contracts.REASON_TRANSFER_AGED_IN_TRANSIT,
    )

    # Log Row Counts
    ctx.logPackageRowCounts("Fact.Movement")


runtime.runPackage(ctx, body)
