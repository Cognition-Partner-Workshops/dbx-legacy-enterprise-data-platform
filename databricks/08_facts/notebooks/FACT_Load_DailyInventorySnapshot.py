# Databricks notebook source
# MAGIC %md
# MAGIC # FACT_Load_DailyInventorySnapshot
# MAGIC Port of `ssis/08_facts/FACT_Load_DailyInventorySnapshot.dtsx` (`build_fact_packages.py::build_fact_load_daily_inventory_snapshot`).
# MAGIC
# MAGIC * Periodic snapshot: `silver.stg_daily_inventory_snapshot` rows for `PositionDate = BusinessDate` replace that snapshot date in `gold.fact_daily_inventory_snapshot` (`replaceWhere` on the `snapshot_date_key` partition == legacy DELETE ... WHERE Date = ? + insert).
# MAGIC * Lookups: stock item (SCD2 as at the snapshot date, -1), warehouse site (type-1, -1).
# MAGIC * Derived: cover band from days of cover, slow-moving flag (aged quantity), stock age bucket, excess / stockout flags, obsolescence provision (source value or regional rate).
# MAGIC * `Prune Snapshot History`: rows older than `RetentionDays` (package default 400, configuration `Fact.DailyInventorySnapshot.RetentionDays`) are deleted after the load.

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

PACKAGE_NAME = "FACT_Load_DailyInventorySnapshot"
STEP_NAME = "Load Daily Inventory Snapshot"
OBJECT_NAME = "Fact.Daily Inventory Snapshot"
TARGET_TABLE = "fact_daily_inventory_snapshot"
SNAPSHOT_DATE_COL = "snapshot_date_key"

p = params.getJobParams(dbutils)
catalog = p["catalog"]
batchId = fc.resolveBatchId(spark, catalog, p)
targetName = naming.table(catalog, "gold", TARGET_TABLE)
snapshotDate = p["businessDate"]
print("catalog=%s batchId=%s snapshotDate=%s target=%s" % (catalog, batchId, snapshotDate, targetName))

# COMMAND ----------

retentionDays = int(fc.configurationValue(spark, catalog, "Fact.DailyInventorySnapshot.RetentionDays", p["environmentCode"], "400"))


def readSnapshotSource(spark, catalog):
    return fc.readTable(spark, catalog, "silver", "stg_daily_inventory_snapshot").where(F.col("PositionDate") == F.lit(snapshotDate))


def buildSnapshotRows(spark, catalog, df):
    stockItem = fc.DIMENSIONS["Stock Item"]
    df = fc.lookupDimension(df, fc.readDimension(spark, catalog, stockItem), stockItem, "StockItemBusinessKey", "stock_item_key", "PositionDate")
    site = fc.DIMENSIONS["Warehouse Site"]
    df = fc.lookupDimension(df, fc.readDimension(spark, catalog, site), site, "WarehouseCode", "warehouse_site_key")
    onHand, value, aged, cover = F.col("QuantityOnHand"), F.col("StockValueAmount"), F.coalesce(F.col("QuantityAgedOver"), F.lit(0)), F.col("DaysOfCover")
    return df.select(
        F.col("PositionDate").alias("snapshot_date_key"),
        "stock_item_key", "warehouse_site_key",
        F.col("RegionCode").alias("region_code"), F.col("WarehouseCode").alias("warehouse_code"),
        onHand.cast("decimal(18,4)").alias("quantity_on_hand"),
        aged.cast("decimal(18,4)").alias("quantity_aged_over_threshold"),
        rules.money(value).alias("stock_value_at_cost"),
        rules.money(rules.safeDivide(value, onHand)).alias("unit_cost_at_snapshot"),
        cover.cast("decimal(9,2)").alias("days_of_cover"),
        rules.coverBand(cover).alias("cover_band_code"),
        rules.stockAgeBucket(aged, onHand).alias("stock_age_bucket_code"),
        rules.isSlowMoving(aged, onHand).alias("slow_moving_flag"),
        (onHand <= 0).alias("stockout_flag"),
        (cover > 180).alias("excess_stock_flag"),
        rules.money(F.coalesce(F.col("ObsolescenceProvisionAmount"), rules.obsolescenceProvision(value, aged, onHand, F.col("RegionCode")))).alias("obsolescence_provision_amount"),
        F.col("ProvisionRuleCode").alias("provision_rule_code"),
        F.col("DqStatusCode").alias("dq_status_code"),
        F.col("stock_item_key_miss").alias("inferred_member_flag"),
        fc.naturalKeyHash(F.col("PositionDate"), F.col("StockItemBusinessKey"), F.col("WarehouseCode")).alias("natural_key_hash"),
    )


def pruneSnapshotHistory(spark, catalog):
    cutoff = snapshotDate - timedelta(days=retentionDays)
    pruned = fc.deleteWhere(spark, targetName, "%s < DATE'%s'" % (SNAPSHOT_DATE_COL, cutoff.isoformat()))
    control.logRowCount(spark, catalog, run.packageExecutionId, OBJECT_NAME + " retention prune", deleteRowCount=pruned)
    return pruned

# COMMAND ----------

with control.packageRun(spark, catalog, batchId, PACKAGE_NAME, projectName=fc.PROJECT_NAME, stepName=STEP_NAME) as run:
    stepId = control.startBatchStep(spark, catalog, batchId, STEP_NAME, 20, stepGroup="Facts")
    try:
        source = readSnapshotSource(spark, catalog)
        sourceCount = source.count()
        rows = buildSnapshotRows(spark, catalog, source)
        rows = fc.loadAuditColumns(rows, batchId, run.packageExecutionId)
        replaced = fc.replaceDateRange(spark, targetName, rows, SNAPSHOT_DATE_COL, snapshotDate, snapshotDate)
        pruned = pruneSnapshotHistory(spark, catalog)
        run.rowsRead = sourceCount
        run.rowsInserted = replaced["inserted"]
        run.rowsDeleted = replaced["deleted"]
        fc.logFactRowCounts(spark, catalog, run.packageExecutionId, OBJECT_NAME, sourceCount, targetName, replaced)
        control.endBatchStep(spark, catalog, stepId, status="Succeeded")
    except Exception:
        control.endBatchStep(spark, catalog, stepId, status="Failed")
        raise
    print(replaced)
