"""Gold facts: FACT_Load_Movement (incremental), FACT_Load_StockHolding (snapshot),
FACT_Load_DailyInventorySnapshot (dense daily snapshot with carry-forward)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from product_inventory import rules
from product_inventory.config import PipelineConfig, farFutureLit
from product_inventory.control import (
    endPackage,
    getWatermark,
    queueLateArrivingMembers,
    setWatermark,
    startPackage,
    utcNow,
    writeRejects,
)
from product_inventory.dates import resolveBusinessDate
from product_inventory.gold_dimensions import GOLD_DIM_STOCK_ITEM, STOCK_ITEM_DIM_SCHEMA
from product_inventory.legacy import (
    readLegacyCustomerKeys,
    readLegacyDateKeys,
    readLegacySupplierKeys,
    readLegacyTransactionTypeKeys,
)
from product_inventory.scd import lookupAsOf
from product_inventory.silver import SILVER_STOCK_ITEM, SILVER_STOCK_MOVEMENT, WORK_INVENTORY_POSITION_DAILY
from product_inventory.tables import appendTable, readTable, readTableOrEmpty, replaceWhere, tableExists

GOLD_FACT_MOVEMENT = "gold_fact_movement"
GOLD_FACT_STOCK_HOLDING = "gold_fact_stock_holding"
GOLD_FACT_DAILY_INVENTORY_SNAPSHOT = "gold_fact_daily_inventory_snapshot"
UNKNOWN_STOCK_ITEM_KEY = 0


def _tsText(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parseTs(value: Optional[str]) -> Optional[datetime]:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None


# ------------------------------------------------------------------ FACT_Load_Movement
def resolveMovementKeys(
    movements: DataFrame,
    stockItemDim: DataFrame,
    customerKeys: DataFrame,
    supplierKeys: DataFrame,
    transactionTypeKeys: DataFrame,
    dateKeys: DataFrame,
) -> DataFrame:
    """Surrogate-key lookups of FACT_Load_Movement. Unknown stock items resolve to key 0
    and are flagged (`is_unknown_member`) for the late-arriving queue; unknown dates are
    flagged as source errors."""
    resolved = lookupAsOf(movements, stockItemDim, "stock_item_id", "wwi_stock_item_id", "movement_timestamp", "stock_item_key", UNKNOWN_STOCK_ITEM_KEY)
    resolved = resolved.join(customerKeys, resolved["customer_id"] == customerKeys["wwi_customer_id"], "left").drop("wwi_customer_id")
    resolved = resolved.join(supplierKeys, resolved["supplier_id"] == supplierKeys["wwi_supplier_id"], "left").drop("wwi_supplier_id")
    resolved = resolved.join(transactionTypeKeys, resolved["transaction_type_id"] == transactionTypeKeys["wwi_transaction_type_id"], "left").drop("wwi_transaction_type_id")
    dates = dateKeys.select(F.col("date_key").alias("_date_key"))
    resolved = resolved.join(dates, resolved["movement_date"] == dates["_date_key"], "left")
    return (
        resolved.withColumn("customer_key", F.coalesce(F.col("customer_key"), F.lit(0)).cast("long"))
        .withColumn("supplier_key", F.coalesce(F.col("supplier_key"), F.lit(0)).cast("long"))
        .withColumn("transaction_type_key", F.coalesce(F.col("transaction_type_key"), F.lit(0)).cast("long"))
        .withColumn("is_date_lookup_failure", F.col("_date_key").isNull())
        .withColumn("date_key", F.col("movement_date"))
        .drop("_date_key")
    )


def shapeMovementFact(resolved: DataFrame, unitCosts: DataFrame, lineageKey: int) -> DataFrame:
    costs = unitCosts.select(F.col("stock_item_id").alias("_cost_id"), F.col("standard_unit_cost").alias("_unit_cost"))
    joined = resolved.join(costs, resolved["stock_item_id"] == costs["_cost_id"], "left")
    signedQty = rules.signedFactQuantity(F.col("movement_type_code"), F.col("quantity_moved")).cast("decimal(18,3)")
    reversesKey = F.lit(None).cast("long")
    fromLocation = F.lit(None).cast("string")
    toLocation = F.lit(None).cast("string")
    return joined.select(
        F.col("date_key"),
        F.col("stock_item_key"),
        F.col("customer_key"),
        F.col("supplier_key"),
        F.col("transaction_type_key"),
        F.col("stock_item_transaction_id").alias("wwi_stock_item_transaction_id"),
        F.col("stock_item_id").alias("wwi_stock_item_id"),
        F.col("customer_id").alias("wwi_customer_id"),
        F.col("supplier_id").alias("wwi_supplier_id"),
        F.col("invoice_id").alias("wwi_invoice_id"),
        F.col("purchase_order_id").alias("wwi_purchase_order_id"),
        F.col("transaction_type_id").alias("wwi_transaction_type_id"),
        F.col("source_quantity").cast("int").alias("quantity"),
        F.col("quantity_moved"),
        signedQty.alias("signed_quantity"),
        (signedQty * F.coalesce(F.col("_unit_cost"), F.lit(0))).cast("decimal(18,2)").alias("signed_value"),
        F.col("movement_type_code"),
        F.col("movement_reason_code"),
        rules.movementReasonGroup(F.col("movement_reason_code")).alias("movement_reason_group"),
        F.col("warehouse_site_code"),
        F.col("counterparty_type_code"),
        F.col("movement_timestamp"),
        F.col("last_modified_at"),
        reversesKey.alias("reverses_movement_key"),
        (reversesKey.isNotNull() & (reversesKey > 0)).alias("is_reversal"),
        rules.isInterWarehouse(fromLocation, toLocation).alias("is_inter_warehouse"),
        F.col("is_unknown_member").alias("is_unknown_stock_item"),
        F.col("batch_id"),
        F.lit(lineageKey).cast("long").alias("lineage_key"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def inferredStockItemMembers(unknownMovements: DataFrame, dim: DataFrame, lineageKey: int) -> DataFrame:
    """Create inferred (placeholder) dimension members for business keys seen by the fact
    before the dimension; DIM_Load_StockItem enriches them on its next run."""
    maxKeyRow = dim.agg(F.max("stock_item_key")).collect()[0][0]
    maxKey = int(maxKeyRow) if maxKeyRow is not None else 0
    firstSeen = unknownMovements.groupBy("stock_item_id").agg(F.min("movement_timestamp").alias("first_seen"))
    firstSeen = firstSeen.withColumn("_rn", F.row_number().over(Window.orderBy("stock_item_id")))
    inferred = firstSeen.select(
        (F.lit(maxKey) + F.col("_rn")).cast("long").alias("stock_item_key"),
        F.col("stock_item_id").cast("int").alias("wwi_stock_item_id"),
        F.concat(F.lit("Inferred stock item "), F.col("stock_item_id")).alias("stock_item_name"),
        F.col("first_seen").alias("valid_from"),
    )
    template = dim.limit(0)
    for field in template.schema.fields:
        if field.name not in inferred.columns:
            default = {
                "valid_to": farFutureLit(),
                "is_current_row": F.lit(True),
                "row_version": F.lit(1),
                "is_inferred_member": F.lit(True),
                "is_reserved_member": F.lit(False),
                "lineage_key": F.lit(lineageKey),
                "product_category_code": F.lit("UNCLASS"),
                "brand_code": F.lit("UNBRANDED"),
                "size_code": F.lit("N/A"),
                "primary_supplier_id": F.lit(-1),
                "unit_price": F.lit(0),
            }.get(field.name, F.lit(None))
            inferred = inferred.withColumn(field.name, default.cast(field.dataType))
    return inferred.select(*[f.name for f in template.schema.fields])


def runFactLoadMovement(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "FACT_Load_Movement"
    run = startPackage(package)
    objectName = "Fact.Movement"
    now = utcNow()
    last = _parseTs(getWatermark(spark, cfg, "SQLSTG", objectName))
    windowFrom = datetime(1900, 1, 1) if (cfg.reloadFullHistory or last is None) else last
    windowTo = now
    staged = readTable(spark, cfg.fqn(SILVER_STOCK_MOVEMENT)).where(
        (F.col("last_modified_at") > F.lit(windowFrom)) & (F.col("last_modified_at") <= F.lit(windowTo))
    )
    dim = readTableOrEmpty(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM), STOCK_ITEM_DIM_SCHEMA)
    resolved = resolveMovementKeys(
        staged,
        dim,
        readLegacyCustomerKeys(spark, cfg),
        readLegacySupplierKeys(spark, cfg),
        readLegacyTransactionTypeKeys(spark, cfg),
        readLegacyDateKeys(spark, cfg),
    )
    unitCosts = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version")).select("stock_item_id", "standard_unit_cost")
    fact = shapeMovementFact(resolved, unitCosts, cfg.batchId)
    dateErrors = fact.where(F.col("date_key").isNull())
    run.rowsRejected = writeRejects(spark, cfg, dateErrors, package, "Fact.Movement", "Fact", "DATE_LOOKUP_FAILURE", "Movement date missing", "wwi_stock_item_transaction_id")
    fact = fact.where(F.col("date_key").isNotNull())
    unknown = resolved.where(F.col("is_unknown_member"))
    lateCount = queueLateArrivingMembers(spark, cfg, unknown, "Stock Item", "stock_item_id", package)
    if lateCount > 0 and tableExists(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM)):
        appendTable(inferredStockItemMembers(unknown, dim, cfg.batchId), cfg.fqn(GOLD_DIM_STOCK_ITEM), mergeSchema=False)
    target = cfg.fqn(GOLD_FACT_MOVEMENT)
    if cfg.reloadFullHistory or not tableExists(spark, target):
        fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    else:
        # corrections: a re-delivered transaction replaces its earlier fact row
        existing = readTable(spark, target)
        keep = existing.join(fact.select("wwi_stock_item_transaction_id"), "wwi_stock_item_transaction_id", "left_anti")
        keep.unionByName(fact).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target}__rebuild")
        spark.table(f"{target}__rebuild").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        spark.sql(f"DROP TABLE IF EXISTS {target}__rebuild")
    run.rowsInserted = readTable(spark, target).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = staged.count()
    setWatermark(spark, cfg, "SQLSTG", objectName, "TIMESTAMP", _tsText(windowFrom), _tsText(windowTo))
    endPackage(spark, cfg, run, "Succeeded", f"window ({_tsText(windowFrom)}, {_tsText(windowTo)}]; late-arriving={lateCount}")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected, "late_arriving": lateCount}


# -------------------------------------------------------------- FACT_Load_StockHolding
def shapeStockHoldingFact(currentStockItems: DataFrame, stockItemDim: DataFrame, snapshotDate: date, lineageKey: int) -> DataFrame:
    current = stockItemDim.where(F.col("is_current_row") & ~F.col("is_reserved_member")).select(
        F.col("wwi_stock_item_id").alias("_dim_id"), F.col("stock_item_key").alias("_dim_key")
    )
    joined = currentStockItems.join(current, currentStockItems["stock_item_id"] == current["_dim_id"], "left")
    available = rules.quantityAvailable(F.col("quantity_on_hand"), F.col("quantity_allocated")).cast("decimal(18,3)")
    daysSinceMovement = F.datediff(F.lit(snapshotDate), F.to_date(F.col("last_movement_when")))
    averageDailyIssues = F.coalesce(F.col("rule_average_daily_demand"), F.lit(0)).cast("decimal(18,4)")
    return joined.select(
        F.coalesce(F.col("_dim_key"), F.lit(UNKNOWN_STOCK_ITEM_KEY)).cast("long").alias("stock_item_key"),
        F.col("stock_item_id").alias("wwi_stock_item_id"),
        F.lit(snapshotDate).alias("as_at_date_key"),
        F.col("quantity_on_hand").cast("int").alias("quantity_on_hand"),
        F.col("bin_location"),
        F.col("last_stocktake_quantity").cast("int").alias("last_stocktake_quantity"),
        F.col("standard_unit_cost").alias("last_cost_price"),
        F.col("reorder_level").cast("int").alias("reorder_level"),
        F.col("target_stock_level").cast("int").alias("target_stock_level"),
        F.col("primary_warehouse_site_code").alias("warehouse_site_code"),
        F.col("region_code"),
        F.coalesce(F.col("quantity_allocated"), F.lit(0)).cast("decimal(18,3)").alias("quantity_allocated"),
        available.alias("quantity_available"),
        F.coalesce(F.col("quantity_in_transit"), F.lit(0)).cast("decimal(18,3)").alias("quantity_in_transit"),
        F.coalesce(F.col("quantity_on_order"), F.lit(0)).cast("decimal(18,3)").alias("quantity_on_order"),
        (F.col("quantity_on_hand") * F.col("standard_unit_cost")).cast("decimal(18,2)").alias("stock_value_at_cost"),
        rules.coverRatio(available, F.col("target_stock_level")).cast("decimal(18,4)").alias("cover_ratio"),
        rules.daysOfCover(F.col("quantity_on_hand"), F.col("quantity_allocated"), averageDailyIssues).alias("days_of_cover"),
        (F.col("quantity_on_hand") < F.coalesce(F.col("reorder_level"), F.lit(0))).alias("is_below_reorder_level"),
        rules.stockStatusCode(F.col("quantity_on_hand"), available, F.col("reorder_level")).alias("stock_status_code"),
        F.to_date(F.col("last_movement_when")).alias("last_movement_date_key"),
        F.to_date(F.col("last_counted_when")).alias("last_stocktake_date_key"),
        daysSinceMovement.alias("days_since_last_movement"),
        averageDailyIssues.alias("average_daily_issues"),
        F.col("_dim_key").isNull().alias("is_unknown_stock_item"),
        F.lit(lineageKey).cast("long").alias("lineage_key"),
        F.col("batch_id"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def runFactLoadStockHolding(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "FACT_Load_StockHolding"
    run = startPackage(package)
    snapshotDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    current = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version") & (F.col("delete_flag") == "N"))
    dim = readTableOrEmpty(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM), STOCK_ITEM_DIM_SCHEMA)
    fact = shapeStockHoldingFact(current, dim, snapshotDate, cfg.batchId)
    errors = fact.where(F.col("quantity_on_hand").isNull())
    run.rowsRejected = writeRejects(spark, cfg, errors, package, "Fact.Stock Holding", "Fact", "SOURCE_ERROR", "Holding without quantity", "wwi_stock_item_id")
    fact = fact.where(F.col("quantity_on_hand").isNotNull())
    unknown = fact.where(F.col("is_unknown_stock_item"))
    queueLateArrivingMembers(spark, cfg, unknown, "Stock Item", "wwi_stock_item_id", package)
    replaceWhere(spark, fact, cfg.fqn(GOLD_FACT_STOCK_HOLDING), f"as_at_date_key = '{snapshotDate.isoformat()}'")
    run.rowsInserted = readTable(spark, cfg.fqn(GOLD_FACT_STOCK_HOLDING)).where(F.col("as_at_date_key") == F.lit(snapshotDate)).count()
    run.rowsRead = current.count()
    endPackage(spark, cfg, run, "Succeeded", f"snapshot {snapshotDate} rebuilt")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ---------------------------------------------------- FACT_Load_DailyInventorySnapshot
SNAPSHOT_PRIOR_SCHEMA = StructType(
    [
        StructField("stock_item_id", IntegerType()),
        StructField("warehouse_site_code", StringType()),
        StructField("closing_quantity", DecimalType(18, 3)),
        StructField("last_movement_date", DateType()),
    ]
)


def buildDailyInventorySnapshot(
    snapshotDate: date,
    currentStockItems: DataFrame,
    movements: DataFrame,
    positions: DataFrame,
    priorSnapshot: DataFrame,
    stockItemDim: DataFrame,
    stockOutThreshold: int,
    lineageKey: int,
) -> DataFrame:
    """Dense item x site grain for one day. Items without movement carry the prior day's
    closing quantity forward (or the current holding on the very first snapshot)."""
    items = currentStockItems.select(
        "stock_item_id",
        F.col("primary_warehouse_site_code").alias("warehouse_site_code"),
        "quantity_on_hand",
        "quantity_allocated",
        "quantity_in_transit",
        "quantity_on_order",
        "reorder_level",
        "target_stock_level",
        "standard_unit_cost",
        "rule_average_daily_demand",
        "last_movement_when",
        "is_chiller_stock",
    )
    grain = (
        items.select("stock_item_id", "warehouse_site_code")
        .unionByName(positions.select("stock_item_id", "warehouse_site_code"))
        .unionByName(priorSnapshot.select("stock_item_id", "warehouse_site_code"))
        .where(F.col("warehouse_site_code").isNotNull())
        .distinct()
    )
    dayMoves = movements.where(F.col("movement_date") == F.lit(snapshotDate)).groupBy("stock_item_id", "warehouse_site_code").agg(
        F.sum(F.when(F.col("movement_type_code") == "RECEIPT", F.col("quantity_moved")).otherwise(0)).alias("received_quantity"),
        F.sum(F.when(F.col("movement_type_code").isin("ISSUE", "SALE"), F.col("quantity_moved")).otherwise(0)).alias("issued_quantity"),
        F.sum(F.when(F.col("movement_type_code") == "ADJUST", F.col("signed_quantity")).otherwise(0)).alias("adjusted_quantity"),
        F.sum(F.when(F.col("movement_type_code") == "SCRAP", F.col("quantity_moved")).otherwise(0)).alias("scrapped_quantity"),
        F.sum(F.when(F.col("movement_type_code") == "TRANSFER", F.col("signed_quantity")).otherwise(0)).alias("transferred_quantity"),
        F.sum("signed_quantity").alias("net_quantity"),
        F.count(F.lit(1)).alias("movement_count"),
        F.max("movement_date").alias("day_last_movement"),
    )
    trailing = movements.where(
        (F.col("movement_date") > F.lit(snapshotDate - timedelta(days=30))) & (F.col("movement_date") <= F.lit(snapshotDate))
    ).groupBy("stock_item_id", "warehouse_site_code").agg((F.sum(F.when(F.col("signed_quantity") < 0, -F.col("signed_quantity")).otherwise(0)) / 30).alias("trailing_daily_issues"))
    prior = priorSnapshot.select("stock_item_id", "warehouse_site_code", F.col("closing_quantity").alias("prior_closing"), F.col("last_movement_date").alias("prior_last_movement"))
    itemAttrs = items.drop("warehouse_site_code")
    base = (
        grain.join(itemAttrs, "stock_item_id", "left")
        .join(dayMoves, ["stock_item_id", "warehouse_site_code"], "left")
        .join(trailing, ["stock_item_id", "warehouse_site_code"], "left")
        .join(prior, ["stock_item_id", "warehouse_site_code"], "left")
    )
    net = F.coalesce(F.col("net_quantity"), F.lit(0)).cast("decimal(18,3)")
    opening = F.coalesce(F.col("prior_closing"), F.coalesce(F.col("quantity_on_hand"), F.lit(0)) - net).cast("decimal(18,3)")
    closing = (opening + net).cast("decimal(18,3)")
    allocated = F.coalesce(F.col("quantity_allocated"), F.lit(0)).cast("decimal(18,3)")
    available = (closing - allocated).cast("decimal(18,3)")
    avgIssues = F.coalesce(F.col("rule_average_daily_demand"), F.col("trailing_daily_issues"), F.lit(0)).cast("decimal(18,4)")
    lastMovement = F.coalesce(F.col("day_last_movement"), F.col("prior_last_movement"), F.to_date(F.col("last_movement_when")))
    daysSince = F.datediff(F.lit(snapshotDate), lastMovement)
    unitCost = F.coalesce(F.col("standard_unit_cost"), F.lit(0))
    stockValue = (closing * unitCost).cast("decimal(18,2)")
    dimCurrent = stockItemDim.where(F.col("is_current_row") & ~F.col("is_reserved_member")).select(
        F.col("wwi_stock_item_id").alias("_dim_id"), F.col("stock_item_key").alias("_dim_key")
    )
    withKeys = base.join(dimCurrent, base["stock_item_id"] == dimCurrent["_dim_id"], "left")
    daysCover = rules.daysOfCover(closing, allocated, avgIssues)
    return withKeys.select(
        F.lit(snapshotDate).alias("snapshot_date_key"),
        F.coalesce(F.col("_dim_key"), F.lit(UNKNOWN_STOCK_ITEM_KEY)).cast("long").alias("stock_item_key"),
        F.col("stock_item_id").alias("wwi_stock_item_id"),
        "warehouse_site_code",
        opening.alias("opening_quantity"),
        F.coalesce(F.col("received_quantity"), F.lit(0)).cast("decimal(18,3)").alias("received_quantity"),
        F.coalesce(F.col("issued_quantity"), F.lit(0)).cast("decimal(18,3)").alias("issued_quantity"),
        F.coalesce(F.col("adjusted_quantity"), F.lit(0)).cast("decimal(18,3)").alias("adjusted_quantity"),
        F.coalesce(F.col("scrapped_quantity"), F.lit(0)).cast("decimal(18,3)").alias("scrapped_quantity"),
        F.coalesce(F.col("transferred_quantity"), F.lit(0)).cast("decimal(18,3)").alias("transferred_quantity"),
        closing.alias("closing_quantity"),
        allocated.alias("allocated_quantity"),
        available.alias("available_quantity"),
        F.coalesce(F.col("quantity_in_transit"), F.lit(0)).cast("decimal(18,3)").alias("in_transit_quantity"),
        F.coalesce(F.col("quantity_on_order"), F.lit(0)).cast("decimal(18,3)").alias("on_order_quantity"),
        unitCost.cast("decimal(18,2)").alias("unit_cost"),
        stockValue.alias("stock_value"),
        (stockValue - rules.obsolescenceProvisionAmount(daysSince, stockValue)).cast("decimal(18,2)").alias("net_stock_value"),
        rules.obsolescenceProvisionAmount(daysSince, stockValue).alias("obsolescence_provision"),
        avgIssues.alias("average_daily_issues"),
        daysCover.alias("days_of_cover"),
        rules.coverBandCode(daysCover).alias("cover_band_code"),
        (closing < F.coalesce(F.col("reorder_level"), F.lit(0))).alias("is_below_reorder"),
        (closing <= F.lit(stockOutThreshold)).alias("is_stockout"),
        (closing > F.coalesce(F.col("target_stock_level"), F.lit(0)) * 2).alias("is_excess"),
        F.coalesce(F.col("movement_count"), F.lit(0)).cast("int").alias("movement_count"),
        F.col("movement_count").isNull().alias("is_carried_forward"),
        lastMovement.alias("last_movement_date"),
        daysSince.alias("days_since_last_movement"),
        rules.ageBucketCode(daysSince).alias("age_bucket_code"),
        F.when(daysSince > 90, closing).otherwise(F.lit(0)).cast("decimal(18,3)").alias("aged_quantity"),
        F.coalesce(F.col("is_chiller_stock"), F.lit(False)).alias("is_chiller_stock"),
        F.col("_dim_key").isNull().alias("is_unknown_stock_item"),
        F.lit(lineageKey).cast("long").alias("lineage_key"),
        F.current_timestamp().alias("loaded_at_utc"),
    )


def runFactLoadDailyInventorySnapshot(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "FACT_Load_DailyInventorySnapshot"
    run = startPackage(package)
    snapshotDate = resolveBusinessDate(spark, cfg, cfg.fqn(SILVER_STOCK_MOVEMENT))
    target = cfg.fqn(GOLD_FACT_DAILY_INVENTORY_SNAPSHOT)
    current = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("is_current_version") & (F.col("delete_flag") == "N"))
    movements = readTable(spark, cfg.fqn(SILVER_STOCK_MOVEMENT))
    positions = readTable(spark, cfg.fqn(WORK_INVENTORY_POSITION_DAILY))
    dim = readTableOrEmpty(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM), STOCK_ITEM_DIM_SCHEMA)
    if tableExists(spark, target):
        prior = (
            readTable(spark, target)
            .where(F.col("snapshot_date_key") == F.lit(snapshotDate - timedelta(days=1)))
            .select(F.col("wwi_stock_item_id").alias("stock_item_id"), "warehouse_site_code", "closing_quantity", "last_movement_date")
        )
    else:
        prior = spark.createDataFrame([], SNAPSHOT_PRIOR_SCHEMA)
    snapshot = buildDailyInventorySnapshot(snapshotDate, current, movements, positions, prior, dim, cfg.stockOutThreshold, cfg.batchId)
    unknown = snapshot.where(F.col("is_unknown_stock_item"))
    queueLateArrivingMembers(spark, cfg, unknown, "Stock Item", "wwi_stock_item_id", package)
    replaceWhere(spark, snapshot, target, f"snapshot_date_key = '{snapshotDate.isoformat()}'")
    retentionFloor = snapshotDate - timedelta(days=cfg.retentionDays)
    spark.sql(f"DELETE FROM {target} WHERE snapshot_date_key < '{retentionFloor.isoformat()}'")
    run.rowsInserted = readTable(spark, target).where(F.col("snapshot_date_key") == F.lit(snapshotDate)).count()
    run.rowsRead = run.rowsInserted
    endPackage(spark, cfg, run, "Succeeded", f"snapshot {snapshotDate}; retention {cfg.retentionDays} days")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": 0}


def runFacts(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, Dict[str, int]]:
    return {
        "FACT_Load_Movement": runFactLoadMovement(spark, cfg),
        "FACT_Load_StockHolding": runFactLoadStockHolding(spark, cfg),
        "FACT_Load_DailyInventorySnapshot": runFactLoadDailyInventorySnapshot(spark, cfg),
    }

