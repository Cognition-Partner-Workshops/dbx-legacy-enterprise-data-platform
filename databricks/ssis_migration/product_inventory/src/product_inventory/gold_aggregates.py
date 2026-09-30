"""AGG_Refresh_DailyInventoryHealth: rebuild Aggregate.Daily Inventory Health for a date window."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from product_inventory import rules
from product_inventory.config import PipelineConfig
from product_inventory.control import endPackage, startPackage
from product_inventory.dates import resolveBusinessDate
from product_inventory.gold_facts import GOLD_FACT_DAILY_INVENTORY_SNAPSHOT
from product_inventory.gold_inventory import GOLD_INV_REPLENISHMENT_SUGGESTION
from product_inventory.silver import SILVER_STOCK_MOVEMENT
from product_inventory.tables import readTable, replaceWhere

GOLD_AGG_DAILY_INVENTORY_HEALTH = "gold_agg_daily_inventory_health"


def buildDailyInventoryHealth(snapshots: DataFrame, suggestions: DataFrame, windowStart: date, windowEnd: date) -> DataFrame:
    scoped = snapshots.where((F.col("snapshot_date_key") >= F.lit(windowStart)) & (F.col("snapshot_date_key") <= F.lit(windowEnd)))
    agg = scoped.groupBy(F.col("snapshot_date_key").alias("snapshot_date"), "warehouse_site_code").agg(
        F.count(F.lit(1)).cast("int").alias("stock_item_count"),
        F.sum(F.col("is_stockout").cast("int")).cast("int").alias("stockout_count"),
        F.sum(F.col("is_below_reorder").cast("int")).cast("int").alias("below_reorder_count"),
        F.sum(F.col("is_excess").cast("int")).cast("int").alias("excess_count"),
        F.sum(F.when(F.col("cover_band_code") == "CRITICAL", 1).otherwise(0)).cast("int").alias("critical_cover_count"),
        F.sum(F.when(F.col("cover_band_code") == "TIGHT", 1).otherwise(0)).cast("int").alias("tight_cover_count"),
        F.sum(F.when(F.col("cover_band_code") == "COMFORTABLE", 1).otherwise(0)).cast("int").alias("comfortable_cover_count"),
        F.sum(F.when(F.col("cover_band_code") == "OVERSTOCKED", 1).otherwise(0)).cast("int").alias("overstocked_cover_count"),
        F.sum("closing_quantity").cast("decimal(18,3)").alias("total_quantity_on_hand"),
        F.sum("aged_quantity").cast("decimal(18,3)").alias("aged_quantity"),
        F.sum("stock_value").cast("decimal(18,2)").alias("total_stock_value"),
        F.sum("net_stock_value").cast("decimal(18,2)").alias("total_net_stock_value"),
        F.sum("obsolescence_provision").cast("decimal(18,2)").alias("total_obsolescence_provision"),
        F.avg("days_of_cover").cast("decimal(18,2)").alias("average_days_of_cover"),
        F.sum("received_quantity").cast("decimal(18,3)").alias("received_quantity"),
        F.sum("issued_quantity").cast("decimal(18,3)").alias("issued_quantity"),
    )
    sugg = suggestions.groupBy(F.col("suggestion_date").alias("snapshot_date"), "warehouse_site_code").agg(
        F.sum(F.when(F.col("suggested_quantity") > 0, 1).otherwise(0)).cast("int").alias("replenishment_suggestion_count"),
        F.sum(F.col("is_stockout_risk").cast("int")).cast("int").alias("stockout_risk_count"),
    )
    joined = agg.join(sugg, ["snapshot_date", "warehouse_site_code"], "left")
    return joined.select(
        "snapshot_date",
        "warehouse_site_code",
        "stock_item_count",
        "stockout_count",
        "below_reorder_count",
        "excess_count",
        "critical_cover_count",
        "tight_cover_count",
        "comfortable_cover_count",
        "overstocked_cover_count",
        "total_quantity_on_hand",
        "aged_quantity",
        rules.agedStockPercent(F.col("aged_quantity"), F.col("total_quantity_on_hand")).alias("aged_stock_percent"),
        "total_stock_value",
        "total_net_stock_value",
        "total_obsolescence_provision",
        "average_days_of_cover",
        rules.coverBandCode(F.col("average_days_of_cover")).alias("site_cover_band_code"),
        "received_quantity",
        "issued_quantity",
        F.coalesce(F.col("replenishment_suggestion_count"), F.lit(0)).alias("replenishment_suggestion_count"),
        F.coalesce(F.col("stockout_risk_count"), F.lit(0)).alias("stockout_risk_count"),
        F.when(F.col("stock_item_count") > 0, (F.col("stockout_count") / F.col("stock_item_count")) * 100).otherwise(F.lit(0.0)).cast("decimal(9,2)").alias("stockout_percent"),
    )


def runAggRefreshDailyInventoryHealth(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "AGG_Refresh_DailyInventoryHealth"
    run = startPackage(package)
    businessDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    daysBack = int(cfg.extra.get("agg_days_back", "7"))
    windowStart = businessDate - timedelta(days=daysBack)
    health = buildDailyInventoryHealth(readTable(spark, cfg.fqn(GOLD_FACT_DAILY_INVENTORY_SNAPSHOT)), readTable(spark, cfg.fqn(GOLD_INV_REPLENISHMENT_SUGGESTION)), windowStart, businessDate)
    health = health.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    replaceWhere(spark, health, cfg.fqn(GOLD_AGG_DAILY_INVENTORY_HEALTH), f"snapshot_date >= '{windowStart.isoformat()}' AND snapshot_date <= '{businessDate.isoformat()}'")
    run.rowsInserted = readTable(spark, cfg.fqn(GOLD_AGG_DAILY_INVENTORY_HEALTH)).where(F.col("snapshot_date") >= F.lit(windowStart)).count()
    run.rowsRead = run.rowsInserted
    endPackage(spark, cfg, run, "Succeeded", f"window [{windowStart}, {businessDate}]")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": 0}
