# Databricks notebook source
# MAGIC %md
# MAGIC # INV_Load_Replenishment
# MAGIC Port of `ssis/12_inventory/INV_Load_Replenishment.dtsx` (WWI_Inventory).
# MAGIC Builds replenishment suggestions (reorder point / days of cover / outer rounding)
# MAGIC into `silver.work_replenishment_suggestion` and publishes the per-site summary to
# MAGIC `gold.agg_daily_inventory_health` by MERGE.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F  # noqa: E402

from dbx_etl_common import control, params, naming  # noqa: E402,F401
from inv_common import contracts, runtime, transforms  # noqa: E402

# COMMAND ----------

PACKAGE_NAME = "INV_Load_Replenishment"
ctx = runtime.PackageContext(spark, dbutils, PACKAGE_NAME)  # Log Package Start

coverDays = int(runtime.widget(dbutils, "CoverDays", "21"))
suppressChillerSuggestions = runtime.asBool(runtime.widget(dbutils, "SuppressChillerSuggestions", "False"))

# COMMAND ----------


def body(ctx: runtime.PackageContext) -> None:
    runtime.ensureProjectTables(spark, ctx.catalog)

    stockItem = ctx.read("stg.StockItem")
    position = ctx.read("work.InventoryPositionDaily")
    demand = ctx.read("work.StockItemDemand")
    dimWarehouseSite = ctx.read("Dimension.Warehouse Site")

    # Truncate work_ReplenishmentSuggestion + Calculate Replenishment (+ Suppress Chiller Suggestions)
    source = transforms.buildReplenishmentSource(stockItem, position, demand)
    ctx.rowsRead = source.count()
    suggestions = transforms.buildReplenishmentSuggestions(
        stockItem, position, demand, coverDays, ctx.batchId, suppressChiller=suppressChillerSuggestions
    )
    workTable = ctx.table("work.ReplenishmentSuggestion")
    suggestionCount = runtime.overwriteTable(spark, suggestions, workTable)
    suggestions = spark.table(workTable)

    # Publish Inventory Health (MERGE on site; one row per snapshot date + site, as the aggregate is daily)
    health = transforms.aggregateInventoryHealth(suggestions, ctx.businessDate, ctx.batchId, dimWarehouseSite)
    inserted, updated = runtime.mergeByKey(
        spark, health, ctx.table("Aggregate.Daily Inventory Health"),
        ["SnapshotDate", "WarehouseSiteCode", "ProductCategoryKey"],
    )
    ctx.rowsInserted = inserted
    ctx.rowsUpdated = updated

    # Count Stockout Risks
    stockoutRiskCount = suggestions.where(F.col("IsStockoutRisk") == F.lit(True)).count()
    print(f"SuggestionCount={suggestionCount} StockoutRiskCount={stockoutRiskCount}")

    # Log Row Counts
    ctx.logPackageRowCounts("Aggregate.Daily Inventory Health")


runtime.runPackage(ctx, body)
