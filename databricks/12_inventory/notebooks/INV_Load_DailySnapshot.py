# Databricks notebook source
# MAGIC %md
# MAGIC # INV_Load_DailySnapshot
# MAGIC Port of `ssis/12_inventory/INV_Load_DailySnapshot.dtsx` (WWI_Inventory).
# MAGIC Snapshots the closing inventory position for one business date into
# MAGIC `gold.fact_daily_inventory_snapshot` (partition overwrite by snapshot date).
# MAGIC Uses `dbx_etl_common` (session 00) for the package lifecycle and row-count audit.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "INV_Load_DailySnapshot"
ctx = runtime.PackageContext(spark, dbutils, PACKAGE_NAME)  # Log Package Start

# package parameters ($Package::SnapshotBusinessDate / $Package::DeleteExistingSnapshot)
snapshotBusinessDate = runtime.widget(dbutils, "SnapshotBusinessDate", "1900-01-01")
deleteExistingSnapshot = runtime.asBool(runtime.widget(dbutils, "DeleteExistingSnapshot", "True"))

# COMMAND ----------


def body(ctx: runtime.PackageContext) -> None:
    runtime.ensureProjectTables(spark, ctx.catalog)

    # Resolve Snapshot Date: the master passes the batch business date; 1900-01-01 means "use BusinessDate"
    snapshotDate = ctx.businessDate if snapshotBusinessDate in ("", "1900-01-01") else snapshotBusinessDate

    position = ctx.read("work.InventoryPositionDaily")
    stockItem = ctx.read("stg.StockItem")
    dimStockItem = ctx.read("Dimension.Stock Item")
    dimWarehouseSite = ctx.read("Dimension.Warehouse Site")

    # Load Inventory Snapshot data flow: source -> Lookup Stock Item Key -> Derive Snapshot Measures
    matched, rejected = transforms.buildDailySnapshot(position, stockItem, dimStockItem, snapshotDate)
    matched = matched.cache()
    ctx.rowsRead = matched.count() + rejected.count()

    # Lookup no-match output -> err.InventorySnapshotReject
    rejectRows = rejected.select(
        "StockItemId", "WarehouseSiteCode", "BinLocationCode", "SnapshotDate", "QuantityOnHand",
        F.lit(contracts.REASON_UNKNOWN_STOCK_ITEM).alias("RejectReasonCode"),
        F.lit(ctx.batchId).cast("bigint").alias("BatchId"),
        F.lit(ctx.packageExecutionId).cast("bigint").alias("PackageExecutionId"),
        F.current_timestamp().alias("LoggedAtUtc"),
    )
    ctx.rowsRejected = runtime.appendTable(spark, rejectRows, ctx.table("err.InventorySnapshotReject"))

    fact = transforms.toFactDailyInventorySnapshot(matched, ctx.batchId, dimWarehouseSite)
    target = ctx.table("Fact.Daily Inventory Snapshot")
    predicate = f"SnapshotDateKey = DATE'{snapshotDate}'"
    if deleteExistingSnapshot:
        # Delete Existing Snapshot + Fact destination as one atomic partition overwrite
        ctx.rowsDeleted = spark.table(target).where(predicate).count()
        ctx.rowsInserted = runtime.replaceWhere(spark, fact, target, predicate)
    else:
        # legacy behaviour when the flag is off: plain INSERT (double-counts on rerun, as SSIS did)
        ctx.rowsInserted = runtime.appendTable(spark, fact, target)

    # Count Expired Chiller Stock (User::ExpiredChillerCount)
    expiredChillerCount = spark.table(target).where(f"{predicate} AND IsExpiredChillerStock = 1").count()
    print(f"ExpiredChillerCount={expiredChillerCount}")

    # Log Row Counts
    ctx.logPackageRowCounts("Fact.Daily Inventory Snapshot")


runtime.runPackage(ctx, body)  # Log Package Success / OnError -> Log Error + Mark Execution Failed
