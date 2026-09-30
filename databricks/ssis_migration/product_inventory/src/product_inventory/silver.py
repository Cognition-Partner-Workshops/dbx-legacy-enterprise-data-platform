"""Silver layer: the five STG_* packages (stg.Product, stg.StockItem, stg.StockMovement,
work.ProductCrosswalk, work.InventoryPositionDaily)."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from product_inventory import rules
from product_inventory.bronze import (
    BRONZE_ORA_PRODUCT_MASTER,
    BRONZE_SQL_STOCK_ITEM,
    BRONZE_SQL_STOCK_MOVEMENT,
)
from product_inventory.config import PipelineConfig
from product_inventory.control import endPackage, startPackage, writeRejects
from product_inventory.dates import resolveBusinessDate
from product_inventory.scd import dedupeLatest
from product_inventory.tables import overwriteTable, readTable, truncateReload, writeBatch

SILVER_PRODUCT = "silver_product"
SILVER_STOCK_ITEM = "silver_stock_item"
SILVER_STOCK_MOVEMENT = "silver_stock_movement"
WORK_PRODUCT_CROSSWALK = "work_product_crosswalk"
WORK_INVENTORY_POSITION_DAILY = "work_inventory_position_daily"

KNOWN_UOM_CODES = ("EA", "BOX", "CTN", "KG", "PLT", "PK", "CS", "DZ", "PR", "L", "ML", "G", "LB")
UOM_TO_EACHES = {"EA": 1, "PR": 2, "DZ": 12, "PK": 6, "BOX": 12, "CS": 24, "CTN": 48, "PLT": 480, "KG": 1, "G": 1, "L": 1, "ML": 1, "LB": 1}


def _uomToEaches(col: F.Column) -> F.Column:
    expr = F.lit(None).cast("decimal(18,4)")
    for code, factor in UOM_TO_EACHES.items():
        expr = F.when(col == code, F.lit(factor)).otherwise(expr)
    return F.coalesce(expr, F.lit(1)).cast("decimal(18,4)")


def _weightToKg(weight: F.Column, uom: F.Column) -> F.Column:
    return (
        F.when(uom == "G", weight / 1000)
        .when(uom == "LB", weight * 0.45359237)
        .otherwise(weight)
        .cast("decimal(18,4)")
    )


# -------------------------------------------------------------------- STG_Load_Product
def shapeStagedProducts(rawProducts: DataFrame) -> DataFrame:
    latest = dedupeLatest(rawProducts, ["product_id"], [F.col("batch_id"), F.col("last_update_dt")])
    baseUom = rules.cleanseCode(F.col("base_uom_cd"), "EA")
    packQty = rules.positiveOrDefault(F.col("pack_qty"), 1.0).cast("decimal(18,4)")
    weightUom = rules.cleanseCode(F.col("weight_uom_cd"), "KG")
    return latest.select(
        F.col("product_id"),
        rules.cleanseCode(F.col("product_cd"), "").alias("product_code"),
        rules.cleanseDescription(F.col("product_desc")).alias("product_description"),
        rules.cleanseCode(F.col("prod_family_cd"), "UNCLASS").alias("product_family_code"),
        F.col("category_cd").alias("category_code"),
        F.col("brand_cd").alias("brand_code"),
        F.col("product_status_cd").alias("product_status_code"),
        baseUom.alias("base_uom_code"),
        packQty.alias("pack_quantity"),
        rules.yesNoFlag(F.col("hazmat_flg")).alias("hazardous_flag"),
        rules.yesNoFlag(F.col("discontinued_flg")).alias("discontinued_flag"),
        weightUom.alias("weight_uom_code"),
        (packQty * _uomToEaches(baseUom)).cast("decimal(18,4)").alias("eaches_per_pack"),
        F.coalesce(_weightToKg(F.col("net_weight"), weightUom), F.lit(0)).cast("decimal(18,4)").alias("net_weight_kg"),
        F.coalesce(F.col("list_price_amt"), F.lit(0)).cast("decimal(18,2)").alias("list_price_amount"),
        rules.cleanseCode(F.col("list_price_ccy"), "USD").alias("list_price_currency_code"),
        F.coalesce(F.col("standard_cost_amt"), F.lit(0)).cast("decimal(18,4)").alias("standard_cost_amount"),
        F.col("cost_currency_cd").alias("cost_currency_code"),
        F.col("hazmat_class_cd").alias("hazard_class_code"),
        F.col("shelf_life_days"),
        F.col("handling_class"),
        F.col("legacy_part_cd").alias("gtin"),
        F.col("wwi_stock_item_id"),
        F.col("source_deleted_flg"),
        F.col("erp_source_sys").alias("erp_source_system_code"),
        F.when(F.col("product_status_cd").isin("Y", "R"), F.lit("Y")).otherwise(F.lit("N")).alias("sellable_flag"),
        F.col("last_update_dt").alias("source_updated_at"),
        F.col("source_system_code"),
        F.col("batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def splitStagedProducts(staged: DataFrame):
    """Conditional split: invalid UOM / negative price -> reject, else load."""
    invalidUom = ~F.col("base_uom_code").isin(*KNOWN_UOM_CODES)
    invalidPrice = F.col("list_price_amount") < 0
    blankCode = F.col("product_code") == ""
    rejectReason = (
        F.when(blankCode, F.lit("MISSING_PRODUCT_CODE"))
        .when(invalidUom, F.lit("INVALID_UOM"))
        .when(invalidPrice, F.lit("INVALID_PRICE"))
    )
    withReason = staged.withColumn("reject_reason_code", rejectReason)
    return withReason.where(F.col("reject_reason_code").isNull()).drop("reject_reason_code"), withReason.where(
        F.col("reject_reason_code").isNotNull()
    )


def runStgLoadProduct(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "STG_Load_Product"
    run = startPackage(package)
    raw = readTable(spark, cfg.fqn(BRONZE_ORA_PRODUCT_MASTER))
    staged = shapeStagedProducts(raw)
    valid, rejected = splitStagedProducts(staged)
    run.rowsRejected = writeRejects(
        spark, cfg, rejected, package, "stg.Product", "Staging", "PRODUCT_VALIDATION",
        "Product failed UOM/price/code validation", "product_id",
    )
    truncateReload(valid, cfg.fqn(SILVER_PRODUCT))
    run.rowsInserted = readTable(spark, cfg.fqn(SILVER_PRODUCT)).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    endPackage(spark, cfg, run, "Succeeded", "truncate-and-reload")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ------------------------------------------------------------------ STG_Load_StockItem
def shapeStagedStockItems(rawStockItems: DataFrame) -> DataFrame:
    versions = dedupeLatest(rawStockItems, ["stock_item_id", "valid_from"], [F.col("batch_id")])
    currentWindow = Window.partitionBy("stock_item_id").orderBy(F.col("valid_from").desc())
    unitPrice = F.coalesce(F.col("unit_price"), F.lit(0)).cast("decimal(18,2)")
    return versions.withColumn("_rn", F.row_number().over(currentWindow)).select(
        F.col("stock_item_id"),
        F.col("stock_item_name"),
        F.coalesce(F.trim(F.col("brand")), F.lit("UNBRANDED")).alias("brand_code"),
        F.coalesce(F.trim(F.col("size")), F.lit("N/A")).alias("size_code"),
        F.col("barcode"),
        F.coalesce(F.col("supplier_id"), F.lit(-1)).cast("int").alias("supplier_id"),
        F.col("color_id"),
        F.col("unit_package_id"),
        F.col("outer_package_id"),
        F.col("lead_time_days"),
        F.col("quantity_per_outer"),
        unitPrice.alias("unit_price"),
        F.col("recommended_retail_price"),
        F.col("tax_rate"),
        (F.coalesce(F.col("typical_weight_per_unit"), F.lit(0)) * 1000).cast("decimal(18,3)").alias("typical_weight_grams"),
        F.col("typical_weight_per_unit"),
        F.substring(F.col("marketing_comments"), 1, 400).alias("marketing_comments"),
        F.col("internal_comments"),
        F.col("search_details"),
        F.col("tags"),
        F.col("is_chiller_stock"),
        F.when(F.col("is_chiller_stock"), F.lit("Y")).otherwise(F.lit("N")).alias("chiller_flag"),
        rules.priceBandCode(unitPrice).alias("price_band_code"),
        F.col("handling_class"),
        F.col("quantity_on_hand"),
        F.col("bin_location"),
        F.col("last_stocktake_quantity"),
        F.coalesce(F.col("last_cost_price"), F.lit(0)).cast("decimal(18,2)").alias("standard_unit_cost"),
        F.col("reorder_level"),
        F.col("target_stock_level"),
        F.col("quantity_allocated"),
        F.col("quantity_in_transit"),
        F.col("quantity_on_order"),
        F.col("abc_class"),
        F.col("last_counted_when"),
        F.col("last_movement_when"),
        F.col("primary_warehouse_site_id"),
        F.col("primary_warehouse_site_code"),
        F.col("region_code"),
        F.col("replenishment_rule_code"),
        F.col("rule_average_daily_demand"),
        F.col("below_reorder_flag"),
        F.col("delete_flag"),
        F.col("valid_from"),
        F.col("valid_to"),
        (F.col("_rn") == 1).alias("is_current_version"),
        F.col("source_system_code"),
        F.col("batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def runStgLoadStockItem(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "STG_Load_StockItem"
    run = startPackage(package)
    raw = readTable(spark, cfg.fqn(BRONZE_SQL_STOCK_ITEM))
    staged = shapeStagedStockItems(raw)
    rejected = staged.where(F.col("stock_item_name").isNull())
    run.rowsRejected = writeRejects(
        spark, cfg, rejected, package, "stg.StockItem", "Staging", "MISSING_NAME", "Stock item without a name", "stock_item_id"
    )
    truncateReload(staged.where(F.col("stock_item_name").isNotNull()), cfg.fqn(SILVER_STOCK_ITEM))
    run.rowsInserted = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    endPackage(spark, cfg, run, "Succeeded", "truncate-and-reload (all temporal versions)")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# -------------------------------------------------------------- STG_Load_StockMovement
def shapeStagedStockMovements(rawMovements: DataFrame, currentStockItems: DataFrame) -> DataFrame:
    m = rawMovements.alias("m")
    si = currentStockItems.select(
        F.col("stock_item_id").alias("_si_id"), F.col("primary_warehouse_site_code").alias("_site")
    ).alias("si")
    joined = m.join(si, F.col("m.stock_item_id") == F.col("si._si_id"), "left")
    conversion = F.lit(1).cast("decimal(18,6)")
    signed = rules.signedMovementQuantity(F.col("m.transaction_type_name"), F.col("m.quantity"), conversion).cast("decimal(18,3)")
    movementType = rules.transactionTypeToMovementTypeCode(F.col("m.transaction_type_name"))
    return joined.select(
        F.col("m.stock_item_transaction_id"),
        F.col("m.stock_item_id"),
        F.col("m.transaction_type_id"),
        F.col("m.transaction_type_name"),
        movementType.alias("movement_type_code"),
        rules.counterpartyTypeCode(F.col("m.customer_id"), F.col("m.supplier_id")).alias("counterparty_type_code"),
        F.col("m.customer_id"),
        F.col("m.supplier_id"),
        F.col("m.invoice_id"),
        F.col("m.purchase_order_id"),
        F.coalesce(F.col("m.warehouse_site_code"), F.col("si._site"), F.lit("UNKNOWN")).alias("warehouse_site_code"),
        F.col("m.bin_code"),
        F.col("m.movement_class"),
        F.col("m.movement_direction_code"),
        F.col("m.quantity").alias("source_quantity"),
        signed.alias("signed_quantity"),
        F.abs(signed).alias("quantity_moved"),
        F.col("m.uom_code"),
        conversion.alias("conversion_factor"),
        F.when(movementType == "ADJUST", F.lit("CYC")).otherwise(F.lit(None).cast("string")).alias("movement_reason_code"),
        F.to_date(F.col("m.transaction_occurred_when")).alias("movement_date"),
        F.col("m.transaction_occurred_when").alias("movement_timestamp"),
        F.col("m.last_edited_when").alias("last_modified_at"),
        F.col("m.source_system_code"),
        F.col("m.batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def runStgLoadStockMovement(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "STG_Load_StockMovement"
    run = startPackage(package)
    raw = readTable(spark, cfg.fqn(BRONZE_SQL_STOCK_MOVEMENT)).where(F.col("batch_id") == cfg.batchId)
    currentItems = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version"))
    staged = shapeStagedStockMovements(raw, currentItems)
    zero = staged.where(F.col("signed_quantity") == 0)
    run.rowsRejected = writeRejects(
        spark, cfg, zero, package, "stg.StockMovement", "Staging", "ZERO_QUANTITY", "Movement nets to zero quantity", "stock_item_transaction_id"
    )
    writeBatch(spark, staged.where(F.col("signed_quantity") != 0), cfg.fqn(SILVER_STOCK_MOVEMENT), cfg.batchId, cfg.reloadFullHistory)
    run.rowsInserted = readTable(spark, cfg.fqn(SILVER_STOCK_MOVEMENT)).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    endPackage(spark, cfg, run, "Succeeded", "incremental append for batch")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ---------------------------------------------------------- STG_Work_ProductCrosswalk
def buildProductCrosswalk(stockItems: DataFrame, products: DataFrame) -> DataFrame:
    """Match OLTP stock items to Oracle products: GTIN/barcode first, then normalised name.

    Returns one row per (stock_item_id, product_id) candidate with `survivorship_rank`
    (1 = winning match for the stock item) and `match_status` in
    MATCHED | UNMATCHED | UNMATCHABLE.
    """
    oltp = stockItems.select(
        "stock_item_id",
        "stock_item_name",
        F.col("barcode"),
        rules.normalisedMatchKey(F.col("barcode"), F.col("stock_item_name")).alias("oltp_match_key"),
        rules.matchRuleCode(F.col("barcode")).alias("oltp_match_rule"),
    )
    oltp = oltp.withColumn("oltp_survivorship", rules.crosswalkSurvivorship(F.col("oltp_match_rule"), F.col("oltp_match_key")))
    ora = products.select(
        "product_id",
        "product_code",
        "product_description",
        rules.normalisedMatchKey(F.col("gtin"), F.col("product_description")).alias("ora_match_key"),
        rules.matchRuleCode(F.col("gtin")).alias("ora_match_rule"),
    )
    ora = ora.withColumn("ora_survivorship", rules.crosswalkSurvivorship(F.col("ora_match_rule"), F.col("ora_match_key")))
    candidates = oltp.join(
        ora,
        (oltp["oltp_match_key"] == ora["ora_match_key"])
        & (oltp["oltp_match_rule"] == ora["ora_match_rule"])
        & (oltp["oltp_survivorship"] != "UNMATCHABLE")
        & (ora["ora_survivorship"] != "UNMATCHABLE"),
        "left",
    )
    rankWindow = Window.partitionBy("stock_item_id").orderBy(
        F.when(F.col("oltp_match_rule") == "GTIN", 1).otherwise(2), F.col("product_id")
    )
    ranked = candidates.withColumn("survivorship_rank", F.row_number().over(rankWindow))
    return ranked.select(
        "stock_item_id",
        "stock_item_name",
        "barcode",
        "product_id",
        "product_code",
        F.col("oltp_match_key").alias("match_key"),
        F.col("oltp_match_rule").alias("match_rule_code"),
        F.when(F.col("product_id").isNotNull(), F.lit("MATCHED"))
        .when(F.col("oltp_survivorship") == "UNMATCHABLE", F.lit("UNMATCHABLE"))
        .otherwise(F.lit("UNMATCHED"))
        .alias("match_status"),
        F.when(F.col("product_id").isNotNull(), F.col("survivorship_rank")).otherwise(F.lit(None).cast("int")).alias("survivorship_rank"),
        (F.col("product_id").isNotNull() & (F.col("survivorship_rank") == 1)).alias("is_preferred_match"),
    )


def runStgWorkProductCrosswalk(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "STG_Work_ProductCrosswalk"
    run = startPackage(package)
    stockItems = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version") & (F.col("delete_flag") == "N"))
    products = readTable(spark, cfg.fqn(SILVER_PRODUCT))
    crosswalk = buildProductCrosswalk(stockItems, products)
    crosswalk = crosswalk.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    matched = crosswalk.where(F.col("match_status") == "MATCHED")
    unmatched = crosswalk.where(F.col("match_status") != "MATCHED")
    overwriteTable(matched, cfg.fqn(WORK_PRODUCT_CROSSWALK))
    run.rowsRejected = writeRejects(
        spark, cfg, unmatched, package, "work.ProductCrosswalk", "Crosswalk", "NO_CROSSWALK_MATCH",
        "Stock item has no GTIN or qualifying name match in the Oracle product master", "stock_item_id",
    )
    run.rowsInserted = readTable(spark, cfg.fqn(WORK_PRODUCT_CROSSWALK)).count()
    run.rowsRead = stockItems.count()
    endPackage(spark, cfg, run, "Succeeded", "work-table rebuild")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# -------------------------------------------------------- STG_Work_InventoryPosition
def buildInventoryPosition(movements: DataFrame, currentStockItems: DataFrame, windowStart: date, windowEnd: date) -> DataFrame:
    daily = movements.groupBy("stock_item_id", "warehouse_site_code", "movement_date").agg(
        F.sum("signed_quantity").cast("decimal(18,3)").alias("net_quantity"),
        F.sum(F.when(F.col("signed_quantity") > 0, F.col("signed_quantity")).otherwise(0)).cast("decimal(18,3)").alias("received_quantity"),
        F.sum(F.when(F.col("signed_quantity") < 0, -F.col("signed_quantity")).otherwise(0)).cast("decimal(18,3)").alias("issued_quantity"),
        F.count(F.lit(1)).cast("int").alias("movement_count"),
        F.max("movement_timestamp").alias("last_movement_at"),
    )
    running = Window.partitionBy("stock_item_id", "warehouse_site_code").orderBy("movement_date").rowsBetween(Window.unboundedPreceding, 0)
    daily = daily.withColumn("quantity_on_hand", F.sum("net_quantity").over(running).cast("decimal(18,3)"))
    windowed = daily.where((F.col("movement_date") >= F.lit(windowStart)) & (F.col("movement_date") <= F.lit(windowEnd)))
    items = currentStockItems.select(
        F.col("stock_item_id").alias("_id"),
        F.col("stock_item_name").alias("stock_item_name"),
        F.col("handling_class").alias("handling_class"),
        F.col("standard_unit_cost").alias("unit_cost"),
        F.col("bin_location").alias("bin_location_code"),
    )
    enriched = windowed.join(items, windowed["stock_item_id"] == items["_id"], "left").drop("_id")
    return enriched.select(
        "stock_item_id",
        "warehouse_site_code",
        F.col("movement_date").alias("position_date"),
        "net_quantity",
        "received_quantity",
        "issued_quantity",
        "movement_count",
        "quantity_on_hand",
        (F.col("quantity_on_hand") * F.coalesce(F.col("unit_cost"), F.lit(0))).cast("decimal(18,2)").alias("stock_value"),
        rules.stockPositionCode(F.col("net_quantity")).alias("stock_position_code"),
        rules.highChurnFlag(F.col("movement_count")).alias("high_churn_flag"),
        rules.plausiblePosition(F.col("net_quantity")).alias("is_plausible"),
        F.col("stock_item_name").isNull().alias("is_lookup_failure"),
        "stock_item_name",
        "handling_class",
        "bin_location_code",
        "last_movement_at",
    )


def runStgWorkInventoryPosition(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "STG_Work_InventoryPosition"
    run = startPackage(package)
    movements = readTable(spark, cfg.fqn(SILVER_STOCK_MOVEMENT))
    businessDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    windowStart = businessDate - timedelta(days=cfg.positionWindowDays)
    currentItems = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version"))
    position = buildInventoryPosition(movements, currentItems, windowStart, businessDate)
    position = position.withColumn("batch_id", F.lit(cfg.batchId).cast("long")).withColumn("business_date", F.lit(businessDate)).withColumn(
        "loaded_at_utc", F.current_timestamp()
    )
    implausible = position.where(~F.col("is_plausible"))
    lookupFailures = position.where(F.col("is_plausible") & F.col("is_lookup_failure"))
    good = position.where(F.col("is_plausible") & ~F.col("is_lookup_failure")).drop("is_plausible", "is_lookup_failure")
    run.rowsRejected = writeRejects(
        spark, cfg, implausible, package, "work.InventoryPositionDaily", "Work", "IMPLAUSIBLE_POSITION",
        "Net quantity outside +/-1,000,000", "stock_item_id",
    ) + writeRejects(
        spark, cfg, lookupFailures, package, "work.InventoryPositionDaily", "Work", "LOOKUP_FAILURE",
        "Stock item not found in stg.StockItem", "stock_item_id",
    )
    overwriteTable(good, cfg.fqn(WORK_INVENTORY_POSITION_DAILY))
    run.rowsInserted = readTable(spark, cfg.fqn(WORK_INVENTORY_POSITION_DAILY)).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    endPackage(spark, cfg, run, "Succeeded", f"rolling {cfg.positionWindowDays}-day rebuild to {businessDate}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


def runSilver(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, Dict[str, int]]:
    return {
        "STG_Load_Product": runStgLoadProduct(spark, cfg),
        "STG_Load_StockItem": runStgLoadStockItem(spark, cfg),
        "STG_Load_StockMovement": runStgLoadStockMovement(spark, cfg),
        "STG_Work_ProductCrosswalk": runStgWorkProductCrosswalk(spark, cfg),
        "STG_Work_InventoryPosition": runStgWorkInventoryPosition(spark, cfg),
    }
