# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_StockHolding
# MAGIC Port of `ssis/08_facts/FACT_Load_StockHolding.dtsx` (`build_fact_packages.py::build_fact_load_stock_holding`).
# MAGIC
# MAGIC * Point-in-time snapshot: `silver.stg_stock_holding` rows for `PositionDate = BusinessDate` replace the same date in `gold.fact_stock_holding` (legacy `Delete Snapshot For Business Date` + insert == Delta `replaceWhere` on the `as_at_date_key` partition).
# MAGIC * Lookups: stock item (SCD2 as at the position date, miss -> -1), warehouse site (type-1, -1), supplier via the stock item's primary supplier when the dimension exposes it (-1 otherwise).
# MAGIC * Derived: quantity available, stock value at cost, reporting value from the source USD figure, below-reorder flag / reorder status, days of cover, regional costing method (NA WAVG / EU FIFO / APAC STD).
# MAGIC * Partitioned by `as_at_date_key` (one partition per snapshot day) so the overwrite touches exactly one partition.

# COMMAND ----------

import os
import sys
from datetime import timedelta

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from pyspark.sql import functions as F

from dbx_etl_common import control, naming, params

import fact_common as fc
import fact_rules as rules

# COMMAND ----------

PACKAGE_NAME = "FACT_Load_StockHolding"
STEP_NAME = "Load Stock Holding Snapshot"
OBJECT_NAME = "Fact.Stock Holding"
TARGET_TABLE = "fact_stock_holding"
SNAPSHOT_DATE_COL = "as_at_date_key"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
targetName = naming.table(catalog, "gold", TARGET_TABLE)
snapshotDate = p["businessDate"]
print("catalog=%s batchId=%s snapshotDate=%s target=%s" % (catalog, batchId, snapshotDate, targetName))

# COMMAND ----------

def readSnapshotSource(spark, catalog):
    return fc.readTable(spark, catalog, "silver", "stg_stock_holding").where(F.col("PositionDate") == F.lit(snapshotDate))


def buildSnapshotRows(spark, catalog, df):
    stockItem = fc.DIMENSIONS["Stock Item"]
    df = fc.lookupDimension(df, fc.readDimension(spark, catalog, stockItem), stockItem, "StockItemBusinessKey", "stock_item_key", "PositionDate")
    site = fc.DIMENSIONS["Warehouse Site"]
    df = fc.lookupDimension(df, fc.readDimension(spark, catalog, site), site, "WarehouseCode", "warehouse_site_key")
    onHand, allocated, onOrder, cost = F.col("QuantityOnHand"), F.coalesce(F.col("QuantityAllocated"), F.lit(0)), F.coalesce(F.col("QuantityOnOrder"), F.lit(0)), F.col("UnitCostAmount")
    stockValue = rules.money(onHand * cost)
    return df.select(
        F.col("PositionDate").alias("as_at_date_key"),
        F.col("LastStocktakeDate").cast("date").alias("last_stocktake_date_key"),
        "stock_item_key", "warehouse_site_key", F.lit(fc.UNKNOWN_KEY).alias("supplier_key"),
        F.col("RegionCode").alias("region_code"), F.col("WarehouseCode").alias("warehouse_code"),
        onHand.cast("decimal(18,4)").alias("quantity_on_hand"), allocated.cast("decimal(18,4)").alias("quantity_allocated"),
        rules.quantityAvailable(onHand, allocated).cast("decimal(18,4)").alias("quantity_available"),
        onOrder.cast("decimal(18,4)").alias("quantity_on_order"),
        F.lit(0).cast("decimal(18,4)").alias("quantity_in_transit"), F.lit(0).cast("decimal(18,4)").alias("quantity_quarantined"),
        rules.money(cost).alias("unit_cost"), stockValue.alias("stock_value_at_cost"),
        rules.safeDivide(F.col("StockValueAmountUsd"), stockValue).cast(rules.RATE).alias("fx_rate_to_reporting"),
        rules.money(F.coalesce(F.col("StockValueAmountUsd"), stockValue)).alias("stock_value_reporting"),
        F.col("ReorderLevel").cast("int").alias("reorder_level"), F.col("TargetStockLevel").cast("int").alias("target_stock_level"),
        rules.isBelowReorderLevel(onHand, allocated, F.col("ReorderLevel")).alias("below_reorder_level_flag"),
        rules.reorderStatus(rules.quantityAvailable(onHand, allocated), F.col("ReorderLevel"), F.col("TargetStockLevel")).alias("reorder_status_code"),
        rules.costingMethodCode(F.col("RegionCode")).alias("costing_method_code"),
        F.coalesce(F.col("NegativeBalanceFlag"), F.lit(False)).alias("negative_balance_flag"),
        F.col("DqStatusCode").alias("dq_status_code"),
        F.col("stock_item_key_miss").alias("inferred_member_flag"),
        fc.naturalKeyHash(F.col("PositionDate"), F.col("StockItemBusinessKey"), F.col("WarehouseCode")).alias("natural_key_hash"),
    )

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    stepId = control.startBatchStep(spark, catalog, batchId, STEP_NAME, 10, stepGroup="Facts")
    try:
        source = readSnapshotSource(spark, catalog)
        sourceCount = source.count()
        rows = buildSnapshotRows(spark, catalog, source)
        rows = fc.loadAuditColumns(rows, batchId, run.packageExecutionId)
        replaced = fc.replaceDateRange(spark, targetName, rows, SNAPSHOT_DATE_COL, snapshotDate, snapshotDate)
        pass
        run.rowsRead = sourceCount
        run.rowsInserted = replaced["inserted"]
        run.rowsDeleted = replaced["deleted"]
        fc.logFactRowCounts(spark, catalog, run.packageExecutionId, OBJECT_NAME, sourceCount, targetName, replaced)
        control.endBatchStep(spark, catalog, stepId, status="Succeeded")
    except Exception:
        control.endBatchStep(spark, catalog, stepId, status="Failed")
        raise
    print(replaced)
