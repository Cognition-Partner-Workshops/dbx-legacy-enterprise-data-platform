"""Gold inventory marts: the five INV_* business-rule packages."""
from __future__ import annotations

from datetime import date
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from product_inventory import rules
from product_inventory.bronze import BRONZE_SQL_STOCK_TRANSFER
from product_inventory.config import PipelineConfig
from product_inventory.control import endPackage, startPackage, writeRejects
from product_inventory.dates import resolveBusinessDate
from product_inventory.gold_facts import GOLD_FACT_STOCK_HOLDING
from product_inventory.silver import SILVER_STOCK_ITEM, SILVER_STOCK_MOVEMENT, WORK_INVENTORY_POSITION_DAILY
from product_inventory.tables import overwriteTable, readTable, replaceWhere

BRONZE_SQL_CYCLE_COUNT = "bronze_sql_cycle_count"
BRONZE_SQL_WAREHOUSE_SITE = "bronze_sql_warehouse_site"
GOLD_INV_CYCLE_COUNT_VARIANCE = "gold_inv_cycle_count_variance"
GOLD_INV_ADJUSTMENT_MOVEMENT = "gold_inv_adjustment_movement"
GOLD_INV_DAILY_SNAPSHOT = "gold_inv_daily_snapshot"
GOLD_INV_REPLENISHMENT_SUGGESTION = "gold_inv_replenishment_suggestion"
GOLD_INV_STOCK_TRANSFER_MOVEMENT = "gold_inv_stock_transfer_movement"
GOLD_INV_ONHAND_RECONCILIATION = "gold_inv_onhand_reconciliation"


def latestPositions(positions: DataFrame) -> DataFrame:
    """Latest work.InventoryPositionDaily row per stock item / site."""
    window = Window.partitionBy("stock_item_id", "warehouse_site_code").orderBy(F.col("position_date").desc())
    return positions.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


# --------------------------------------------------------- INV_Load_CycleCountVariance
def shapeCycleCounts(countLines: DataFrame, counts: DataFrame, sites: DataFrame, bins: DataFrame) -> DataFrame:
    cl = countLines.alias("cl")
    cc = counts.alias("cc")
    ws = sites.alias("ws")
    b = bins.alias("b")
    joined = (
        cl.join(cc, F.col("cc.CycleCountID") == F.col("cl.CycleCountID"), "inner")
        .join(ws, F.col("ws.WarehouseSiteID") == F.col("cc.WarehouseSiteID"), "left")
        .join(b, F.col("b.BinID") == F.col("cl.BinID"), "left")
    )
    return joined.select(
        F.col("cl.CycleCountLineID").cast("long").alias("cycle_count_line_id"),
        F.col("cl.CycleCountID").cast("long").alias("cycle_count_id"),
        F.col("cc.CountReference").alias("count_reference"),
        F.col("cl.StockItemID").cast("int").alias("stock_item_id"),
        F.col("ws.SiteCode").alias("warehouse_site_code"),
        F.col("b.BinCode").alias("bin_location_code"),
        F.col("cl.LotNumber").alias("lot_number"),
        F.col("cl.SystemQuantity").cast("decimal(18,3)").alias("source_system_quantity"),
        F.coalesce(F.col("cl.SecondCountQuantity"), F.col("cl.CountedQuantity")).cast("decimal(18,3)").alias("counted_quantity"),
        F.col("cl.UnitCostAtCount").cast("decimal(18,2)").alias("unit_cost_at_count"),
        F.col("cl.VarianceReasonCode").alias("variance_reason_code"),
        F.col("cl.LineStatus").alias("line_status"),
        F.col("cl.CountedWhen").alias("counted_at_utc"),
        F.col("cc.CountStatus").alias("count_status"),
    )


def buildCycleCountVariance(cycleCounts: DataFrame, positions: DataFrame, currentStockItems: DataFrame, toleranceUnits: int, toleranceValue: float) -> DataFrame:
    pos = latestPositions(positions).select(
        F.col("stock_item_id").alias("_p_item"), F.col("warehouse_site_code").alias("_p_site"), F.col("quantity_on_hand").alias("_p_qty")
    )
    items = currentStockItems.select(F.col("stock_item_id").alias("_i_item"), F.col("standard_unit_cost").alias("_unit_cost"))
    joined = cycleCounts.join(pos, (cycleCounts["stock_item_id"] == pos["_p_item"]) & (cycleCounts["warehouse_site_code"] == pos["_p_site"]), "left").join(
        items, cycleCounts["stock_item_id"] == items["_i_item"], "left"
    )
    systemQty = F.coalesce(F.col("_p_qty"), F.lit(0)).cast("decimal(18,3)")
    varianceQty = (F.col("counted_quantity") - systemQty).cast("decimal(18,3)")
    unitCost = F.coalesce(F.col("unit_cost_at_count"), F.col("_unit_cost"), F.lit(0))
    varianceValue = (varianceQty * unitCost).cast("decimal(18,2)")
    return joined.where(F.col("counted_quantity") != systemQty).select(
        "cycle_count_line_id",
        "cycle_count_id",
        "count_reference",
        "stock_item_id",
        "warehouse_site_code",
        "bin_location_code",
        "lot_number",
        "counted_quantity",
        systemQty.alias("system_quantity"),
        varianceQty.alias("variance_quantity"),
        varianceValue.alias("variance_value"),
        unitCost.cast("decimal(18,2)").alias("unit_cost"),
        "variance_reason_code",
        "counted_at_utc",
        rules.cycleCountStatusCode(varianceQty, varianceValue, toleranceUnits, toleranceValue).alias("count_status_code"),
    )


def runInvLoadCycleCountVariance(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "INV_Load_CycleCountVariance"
    run = startPackage(package)
    landed = shapeCycleCounts(
        readTable(spark, cfg.oltp("Warehouse", "CycleCountLines")),
        readTable(spark, cfg.oltp("Warehouse", "CycleCounts")),
        readTable(spark, cfg.oltp("Warehouse", "WarehouseSites")),
        readTable(spark, cfg.oltp("Warehouse", "Bins")),
    ).withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    overwriteTable(landed, cfg.fqn(BRONZE_SQL_CYCLE_COUNT))
    positions = readTable(spark, cfg.fqn(WORK_INVENTORY_POSITION_DAILY))
    items = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version"))
    variance = buildCycleCountVariance(readTable(spark, cfg.fqn(BRONZE_SQL_CYCLE_COUNT)), positions, items, cfg.countToleranceUnits, cfg.countToleranceValue)
    variance = variance.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    overwriteTable(variance, cfg.fqn(GOLD_INV_CYCLE_COUNT_VARIANCE))
    variance = readTable(spark, cfg.fqn(GOLD_INV_CYCLE_COUNT_VARIANCE))
    # AUTO variances are posted as adjustment movements (the legacy usp_PostStockAdjustment
    # call is represented as rows here; the OLTP host is never written)
    adjustments = variance.where(F.col("count_status_code") == "AUTO").select(
        "cycle_count_line_id",
        "stock_item_id",
        "warehouse_site_code",
        "bin_location_code",
        F.col("variance_quantity").alias("adjustment_quantity"),
        F.col("variance_value").alias("adjustment_value"),
        F.lit("CYCLECOUNT").alias("movement_reason_code"),
        F.lit("ADJUST").alias("movement_type_code"),
        F.col("counted_at_utc").alias("movement_timestamp"),
        "batch_id",
        F.current_timestamp().alias("posted_at_utc"),
    )
    overwriteTable(adjustments, cfg.fqn(GOLD_INV_ADJUSTMENT_MOVEMENT))
    held = variance.where(F.col("count_status_code") == "HOLD")
    run.rowsRejected = writeRejects(spark, cfg, held, package, "work.CycleCountVariance", "BusinessRule", "COUNT_VARIANCE_HELD", "Variance exceeds tolerance; queued for recount", "cycle_count_line_id")
    run.rowsInserted = variance.count()
    run.rowsRead = landed.count()
    endPackage(spark, cfg, run, "Succeeded", f"tolerance units={cfg.countToleranceUnits} value={cfg.countToleranceValue}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# --------------------------------------------------------------- INV_Load_DailySnapshot
def buildInventoryDailySnapshot(snapshotDate: date, currentStockItems: DataFrame, sites: DataFrame, positions: DataFrame) -> DataFrame:
    """Every active item x every active site for the business date."""
    items = currentStockItems.select("stock_item_id", "stock_item_name", "is_chiller_stock", "standard_unit_cost", "reorder_level", "target_stock_level", "quantity_per_outer", "last_movement_when", F.col("primary_warehouse_site_code").alias("_primary_site"), "quantity_on_hand")
    activeSites = sites.where(F.col("is_active")).select("warehouse_site_code", "region_code")
    grid = items.crossJoin(activeSites)
    pos = latestPositions(positions).select(
        F.col("stock_item_id").alias("_p_item"), F.col("warehouse_site_code").alias("_p_site"), F.col("quantity_on_hand").alias("_p_qty"), F.col("last_movement_at").alias("_p_last")
    )
    joined = grid.join(pos, (grid["stock_item_id"] == pos["_p_item"]) & (grid["warehouse_site_code"] == pos["_p_site"]), "left")
    # holdings are kept per item at the primary site; other sites only carry movement-derived positions
    onHand = F.coalesce(F.col("_p_qty"), F.when(F.col("warehouse_site_code") == F.col("_primary_site"), F.col("quantity_on_hand")), F.lit(0)).cast("decimal(18,3)")
    lastMovement = F.coalesce(F.to_date(F.col("_p_last")), F.to_date(F.col("last_movement_when")))
    daysSince = F.datediff(F.lit(snapshotDate), lastMovement)
    return joined.select(
        F.lit(snapshotDate).alias("snapshot_date"),
        "stock_item_id",
        "stock_item_name",
        "warehouse_site_code",
        "region_code",
        onHand.alias("quantity_on_hand"),
        (onHand * F.coalesce(F.col("standard_unit_cost"), F.lit(0))).cast("decimal(18,2)").alias("stock_value"),
        F.col("reorder_level"),
        F.col("target_stock_level"),
        (onHand < F.coalesce(F.col("reorder_level"), F.lit(0))).alias("is_below_reorder"),
        (onHand <= 0).alias("is_stockout"),
        F.col("is_chiller_stock"),
        lastMovement.alias("last_movement_date"),
        daysSince.alias("days_since_last_movement"),
        (F.coalesce(F.col("is_chiller_stock"), F.lit(False)) & (F.coalesce(daysSince, F.lit(0)) > 30) & (onHand > 0)).alias("is_expired_chiller"),
        (F.col("warehouse_site_code") == F.col("_primary_site")).alias("is_primary_site"),
    )


def runInvLoadDailySnapshot(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "INV_Load_DailySnapshot"
    run = startPackage(package)
    snapshotDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    sites = readTable(spark, cfg.oltp("Warehouse", "WarehouseSites")).select(
        F.col("WarehouseSiteID").cast("int").alias("warehouse_site_id"),
        F.col("SiteCode").alias("warehouse_site_code"),
        F.col("SiteName").alias("site_name"),
        F.col("RegionCode").alias("region_code"),
        F.col("SiteType").alias("site_type"),
        F.col("IsActive").cast("boolean").alias("is_active"),
    ).withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    overwriteTable(sites, cfg.fqn(BRONZE_SQL_WAREHOUSE_SITE))
    items = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version") & (F.col("delete_flag") == "N"))
    snapshot = buildInventoryDailySnapshot(snapshotDate, items, readTable(spark, cfg.fqn(BRONZE_SQL_WAREHOUSE_SITE)), readTable(spark, cfg.fqn(WORK_INVENTORY_POSITION_DAILY)))
    snapshot = snapshot.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    expired = snapshot.where(F.col("is_expired_chiller"))
    run.rowsRejected = writeRejects(spark, cfg, expired, package, "Fact.Daily Inventory Snapshot", "BusinessRule", "EXPIRED_CHILLER", "Chiller stock without movement for >30 days", "stock_item_id")
    replaceWhere(spark, snapshot, cfg.fqn(GOLD_INV_DAILY_SNAPSHOT), f"snapshot_date = '{snapshotDate.isoformat()}'")
    run.rowsInserted = readTable(spark, cfg.fqn(GOLD_INV_DAILY_SNAPSHOT)).where(F.col("snapshot_date") == F.lit(snapshotDate)).count()
    run.rowsRead = run.rowsInserted
    endPackage(spark, cfg, run, "Succeeded", f"full daily rewrite for {snapshotDate}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# --------------------------------------------------------------- INV_Load_Replenishment
def buildReplenishmentSuggestions(currentStockItems: DataFrame, movements: DataFrame, businessDate: date, coverDays: int) -> DataFrame:
    trailing = movements.where(F.col("movement_date") > F.date_sub(F.lit(businessDate), 30)).groupBy("stock_item_id").agg(
        (F.sum(F.when(F.col("signed_quantity") < 0, -F.col("signed_quantity")).otherwise(0)) / 30).cast("decimal(18,4)").alias("_trailing_demand")
    )
    joined = currentStockItems.join(trailing, "stock_item_id", "left")
    demand = F.coalesce(F.col("rule_average_daily_demand"), F.col("_trailing_demand"), F.lit(0)).cast("decimal(18,4)")
    leadTime = F.coalesce(F.col("lead_time_days"), F.lit(0))
    safety = rules.regionalSafetyFactor(F.col("region_code"))
    projected = rules.projectedAvailable(F.col("quantity_on_hand"), F.col("quantity_on_order"), F.col("quantity_allocated"))
    raw = rules.rawSuggestedQuantity(demand, coverDays, projected)
    return joined.select(
        F.lit(businessDate).alias("suggestion_date"),
        "stock_item_id",
        "stock_item_name",
        F.col("primary_warehouse_site_code").alias("warehouse_site_code"),
        "region_code",
        F.col("quantity_on_hand").cast("int").alias("quantity_on_hand"),
        F.coalesce(F.col("quantity_on_order"), F.lit(0)).cast("int").alias("quantity_on_order"),
        F.coalesce(F.col("quantity_allocated"), F.lit(0)).cast("int").alias("quantity_allocated"),
        demand.alias("average_daily_demand"),
        leadTime.alias("lead_time_days"),
        safety.cast("decimal(5,2)").alias("safety_factor"),
        rules.reorderPoint(demand, leadTime, safety).alias("reorder_point"),
        projected.alias("projected_available"),
        rules.daysOfCover(F.col("quantity_on_hand"), F.col("quantity_allocated"), demand).alias("days_of_cover"),
        raw.alias("raw_suggested_quantity"),
        rules.suggestedQuantity(raw, F.col("quantity_per_outer")).alias("suggested_quantity"),
        F.col("quantity_per_outer"),
        rules.isStockoutRisk(projected, demand, leadTime).alias("is_stockout_risk"),
        F.coalesce(F.col("is_chiller_stock"), F.lit(False)).alias("is_chiller_stock"),
        F.col("replenishment_rule_code"),
        F.lit(coverDays).alias("cover_days"),
    )


def runInvLoadReplenishment(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "INV_Load_Replenishment"
    run = startPackage(package)
    businessDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    items = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version") & (F.col("delete_flag") == "N"))
    suggestions = buildReplenishmentSuggestions(items, readTable(spark, cfg.fqn(SILVER_STOCK_MOVEMENT)), businessDate, cfg.coverDays)
    if cfg.suppressChillerSuggestions:
        suggestions = suggestions.where(~F.col("is_chiller_stock"))
    suggestions = suggestions.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    replaceWhere(spark, suggestions, cfg.fqn(GOLD_INV_REPLENISHMENT_SUGGESTION), f"suggestion_date = '{businessDate.isoformat()}'")
    run.rowsInserted = readTable(spark, cfg.fqn(GOLD_INV_REPLENISHMENT_SUGGESTION)).where(F.col("suggestion_date") == F.lit(businessDate)).count()
    run.rowsRead = items.count()
    endPackage(spark, cfg, run, "Succeeded", f"cover_days={cfg.coverDays} suppress_chiller={cfg.suppressChillerSuggestions}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": 0}


# --------------------------------------------------------------- INV_Load_StockTransfer
def buildTransferMovements(transfers: DataFrame, currentStockItems: DataFrame, businessDate: date, staleDays: int) -> DataFrame:
    items = currentStockItems.select(F.col("stock_item_id").alias("_i"), F.col("standard_unit_cost").alias("_cost"), F.col("stock_item_name"))
    t = transfers.join(items, transfers["stock_item_id"] == items["_i"], "left")
    unitValue = rules.transferMovementUnitValue(F.col("from_region_code"), F.col("to_region_code"), F.col("unit_cost_at_despatch"), F.coalesce(F.col("_cost"), F.lit(0)))
    common = [
        "stock_transfer_line_id",
        "stock_transfer_id",
        "transfer_reference",
        "stock_item_id",
        "stock_item_name",
        "from_site_code",
        "to_site_code",
        (F.col("from_region_code") != F.col("to_region_code")).alias("is_cross_region"),
        unitValue.alias("unit_value"),
        F.col("transfer_status_code"),
        F.col("in_transit_quantity"),
        F.datediff(F.coalesce(F.to_date(F.col("received_when")), F.lit(businessDate)), F.to_date(F.col("dispatched_when"))).alias("transit_days"),
        (F.col("received_when").isNull() & (F.datediff(F.lit(businessDate), F.to_date(F.col("dispatched_when"))) > staleDays)).alias("is_stale_transfer"),
    ]
    issues = t.where(F.col("dispatched_when").isNotNull()).select(
        *common,
        F.lit("ISSUE").alias("movement_leg_code"),
        F.col("from_site_code").alias("warehouse_site_code"),
        (-F.col("transfer_quantity")).cast("decimal(18,3)").alias("signed_quantity"),
        (-F.col("transfer_quantity") * unitValue).cast("decimal(18,2)").alias("signed_value"),
        F.col("dispatched_when").alias("movement_timestamp"),
    )
    receipts = t.where(F.col("received_when").isNotNull()).select(
        *common,
        F.lit("RECEIPT").alias("movement_leg_code"),
        F.col("to_site_code").alias("warehouse_site_code"),
        F.coalesce(F.col("received_quantity"), F.col("transfer_quantity")).cast("decimal(18,3)").alias("signed_quantity"),
        (F.coalesce(F.col("received_quantity"), F.col("transfer_quantity")) * unitValue).cast("decimal(18,2)").alias("signed_value"),
        F.col("received_when").alias("movement_timestamp"),
    )
    return issues.unionByName(receipts).withColumn("movement_type_code", F.lit("TRANSFER")).withColumn("movement_reason_code", F.lit("XFER"))


def runInvLoadStockTransfer(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "INV_Load_StockTransfer"
    run = startPackage(package)
    businessDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    transfers = readTable(spark, cfg.fqn(BRONZE_SQL_STOCK_TRANSFER))
    items = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version"))
    movements = buildTransferMovements(transfers, items, businessDate, cfg.inTransitAgeAlertDays)
    movements = movements.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    overwriteTable(movements, cfg.fqn(GOLD_INV_STOCK_TRANSFER_MOVEMENT))
    stale = readTable(spark, cfg.fqn(GOLD_INV_STOCK_TRANSFER_MOVEMENT)).where(F.col("is_stale_transfer") & (F.col("movement_leg_code") == "ISSUE"))
    run.rowsRejected = writeRejects(spark, cfg, stale, package, "work.StockTransferMovement", "BusinessRule", "STALE_IN_TRANSIT", f"In transit longer than {cfg.inTransitAgeAlertDays} days", "stock_transfer_line_id")
    run.rowsInserted = readTable(spark, cfg.fqn(GOLD_INV_STOCK_TRANSFER_MOVEMENT)).count()
    run.rowsRead = transfers.count()
    endPackage(spark, cfg, run, "Succeeded", "issue/receipt legs rebuilt")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ---------------------------------------------------------------- INV_Reconcile_OnHand
def buildOnHandReconciliation(holdings: DataFrame, positions: DataFrame, asOf: date, timingWindowMinutes: int, siteScope: str) -> DataFrame:
    dw = holdings.select(
        "wwi_stock_item_id",
        "warehouse_site_code",
        F.col("quantity_on_hand").cast("decimal(18,3)").alias("dw_quantity_on_hand"),
        F.col("as_at_date_key"),
    )
    ops = latestPositions(positions).select(
        F.col("stock_item_id").alias("wwi_stock_item_id"),
        "warehouse_site_code",
        F.col("quantity_on_hand").alias("operational_quantity_on_hand"),
        F.col("last_movement_at"),
    )
    joined = dw.join(ops, ["wwi_stock_item_id", "warehouse_site_code"], "full")
    if siteScope and siteScope.upper() != "ALL":
        joined = joined.where(F.col("warehouse_site_code") == siteScope)
    asOfTs = F.lit(asOf).cast("timestamp") + F.expr("INTERVAL 1 DAY")
    return joined.select(
        F.lit(asOf).alias("reconciliation_date"),
        "wwi_stock_item_id",
        "warehouse_site_code",
        F.coalesce(F.col("dw_quantity_on_hand"), F.lit(0)).cast("decimal(18,3)").alias("dw_quantity_on_hand"),
        F.coalesce(F.col("operational_quantity_on_hand"), F.lit(0)).cast("decimal(18,3)").alias("operational_quantity_on_hand"),
        (F.coalesce(F.col("dw_quantity_on_hand"), F.lit(0)) - F.coalesce(F.col("operational_quantity_on_hand"), F.lit(0))).cast("decimal(18,3)").alias("variance_quantity"),
        rules.onHandVarianceClass(F.col("dw_quantity_on_hand"), F.col("operational_quantity_on_hand"), F.col("last_movement_at"), asOfTs, timingWindowMinutes).alias("variance_class_code"),
        F.col("dw_quantity_on_hand").isNull().alias("is_missing_in_dw"),
        F.col("operational_quantity_on_hand").isNull().alias("is_missing_in_operational"),
        F.col("last_movement_at"),
        F.lit(siteScope).alias("site_scope"),
    )


def runInvReconcileOnHand(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "INV_Reconcile_OnHand"
    run = startPackage(package)
    businessDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    holdings = readTable(spark, cfg.fqn(GOLD_FACT_STOCK_HOLDING)).where(F.col("as_at_date_key") == F.lit(businessDate))
    result = buildOnHandReconciliation(holdings, readTable(spark, cfg.fqn(WORK_INVENTORY_POSITION_DAILY)), businessDate, cfg.timingWindowMinutes, cfg.siteScope)
    result = result.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    replaceWhere(spark, result, cfg.fqn(GOLD_INV_ONHAND_RECONCILIATION), f"reconciliation_date = '{businessDate.isoformat()}'")
    written = readTable(spark, cfg.fqn(GOLD_INV_ONHAND_RECONCILIATION)).where(F.col("reconciliation_date") == F.lit(businessDate))
    run.rowsInserted = written.count()
    run.rowsRead = holdings.count()
    variances = written.where(F.col("variance_class_code") == "VARIANCE").count()
    endPackage(spark, cfg, run, "Succeeded", f"site_scope={cfg.siteScope}; variances={variances}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": 0}


def runInventory(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, Dict[str, int]]:
    return {
        "INV_Load_CycleCountVariance": runInvLoadCycleCountVariance(spark, cfg),
        "INV_Load_DailySnapshot": runInvLoadDailySnapshot(spark, cfg),
        "INV_Load_Replenishment": runInvLoadReplenishment(spark, cfg),
        "INV_Load_StockTransfer": runInvLoadStockTransfer(spark, cfg),
        "INV_Reconcile_OnHand": runInvReconcileOnHand(spark, cfg),
    }
