# Databricks notebook source
# MAGIC %md
# MAGIC # INV_Load_CycleCountVariance
# MAGIC Port of `ssis/12_inventory/INV_Load_CycleCountVariance.dtsx` (WWI_Inventory).
# MAGIC Compares cycle counts with the operational position, auto-posts adjustment
# MAGIC movements within tolerance to `gold.fact_movement` (set-based MERGE replacing the
# MAGIC `Integration.usp_PostInventoryAdjustment` cursor) and queues the rest for recount
# MAGIC in `etl.rejected_record` (`COUNT_VARIANCE_HELD`).

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "INV_Load_CycleCountVariance"
ctx = runtime.PackageContext(spark, dbutils, PACKAGE_NAME)  # Log Package Start

countToleranceUnits = int(runtime.widget(dbutils, "CountToleranceUnits", "2"))
countToleranceValue = int(runtime.widget(dbutils, "CountToleranceValue", "50"))

# COMMAND ----------


def body(ctx: runtime.PackageContext) -> None:
    runtime.ensureProjectTables(spark, ctx.catalog)

    cycleCount = ctx.read("stg.CycleCount")
    position = ctx.read("work.InventoryPositionDaily")
    stockItem = ctx.read("stg.StockItem")
    dimStockItem = ctx.read("Dimension.Stock Item")
    dimWarehouseSite = ctx.read("Dimension.Warehouse Site")

    # Truncate work_CycleCountVariance + Build Variance Set
    variance = transforms.buildCycleCountVariance(
        cycleCount, position, stockItem, ctx.batchId, countToleranceUnits, countToleranceValue
    )
    workTable = ctx.table("work.CycleCountVariance")
    ctx.rowsRead = runtime.overwriteTable(spark, variance, workTable)
    variance = spark.table(workTable)

    # Measure Variances
    varianceCount, heldForRecountCount = transforms.measureVariances(variance)
    print(f"VarianceCount={varianceCount} HeldForRecountCount={heldForRecountCount}")

    # Post Adjustment Movements (precedence: VarianceCount > 0)
    if varianceCount > 0:
        movements = transforms.buildAdjustmentMovements(variance, dimStockItem, dimWarehouseSite, ctx.batchId)
        inserted, updated = runtime.mergeByKey(spark, movements, ctx.table("Fact.Movement"), ["NaturalKeyHash"])
        ctx.rowsInserted += inserted
        ctx.rowsUpdated += updated

    # Queue Recounts (precedence: HeldForRecountCount > 0)
    if heldForRecountCount > 0:
        ctx.logRejectedSet("stg.CycleCount", transforms.heldRecounts(variance), contracts.REASON_COUNT_VARIANCE_HELD)

    # Log Row Counts (Completion edge from Post Adjustment Movements)
    ctx.logPackageRowCounts("Fact.Movement")


runtime.runPackage(ctx, body)
