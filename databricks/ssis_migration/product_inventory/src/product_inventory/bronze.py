"""Bronze layer: the five EXT_* extract packages.

Legacy sources are read through Lakehouse Federation foreign catalogs; the bronze
tables mirror the `raw.*` landing tables of WideWorldImporters_Staging (snake_case)
plus the SSIS audit columns (batch_id, package_execution_id, loaded_at_utc,
source_system_code, source_row_number).
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from product_inventory import rules
from product_inventory.config import PipelineConfig
from product_inventory.control import (
    endPackage,
    getWatermark,
    resolveKeyWindow,
    resolveTimestampWindow,
    setWatermark,
    startPackage,
    utcNow,
    writeRejects,
)
from product_inventory.tables import overwriteTable, readTable, tableExists, writeBatch

BRONZE_ORA_PRODUCT_MASTER = "bronze_ora_product_master"
BRONZE_ORA_PRODUCT_HIERARCHY = "bronze_ora_product_hierarchy"
BRONZE_ORA_PRODUCT_CATEGORY = "bronze_ora_product_category"
BRONZE_SQL_STOCK_ITEM = "bronze_sql_stock_item"
BRONZE_SQL_STOCK_MOVEMENT = "bronze_sql_stock_movement"
BRONZE_SQL_STOCK_TRANSFER = "bronze_sql_stock_transfer"


def _audit(df: DataFrame, cfg: PipelineConfig, sourceSystemCode: str, orderCol: str) -> DataFrame:
    return (
        df.withColumn("source_system_code", F.lit(sourceSystemCode))
        .withColumn("batch_id", F.lit(cfg.batchId).cast("long"))
        .withColumn("package_execution_id", F.lit(cfg.batchId).cast("long"))
        .withColumn("loaded_at_utc", F.current_timestamp())
        .withColumn("source_row_number", F.row_number().over(Window.orderBy(F.col(orderCol))).cast("long"))
    )


def _tsText(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parseTs(value: Optional[str]) -> Optional[datetime]:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None


# ------------------------------------------------------------ EXT_ORA_ProductMaster
def shapeOracleProductMaster(products: DataFrame, categories: DataFrame, uomConversions: DataFrame, asOf: datetime) -> DataFrame:
    """Reproduce the EXT_ORA_ProductMaster source query + derived columns."""
    p = products.alias("p")
    c = categories.alias("c")
    u = uomConversions.where((F.col("primary_conv_flg") == "Y") & F.col("end_dt").isNull()).alias("u")
    joined = p.join(c, F.col("c.product_category_id") == F.col("p.product_category_id"), "left").join(
        u, F.col("u.product_id") == F.col("p.product_id"), "left"
    )
    asOfCol = F.lit(asOf)
    return joined.select(
        F.col("p.product_id").cast("long").alias("product_id"),
        F.col("p.item_nbr").alias("product_cd"),
        F.col("p.item_desc").alias("product_desc"),
        F.col("c.category_cd").alias("category_cd"),
        F.col("c.category_name").alias("category_desc"),
        F.col("p.brand_cd").alias("brand_cd"),
        rules.productActiveFlag(F.col("p.lifecycle_status_cd"), F.col("p.discontinued_dt"), asOfCol).alias("product_status_cd"),
        F.col("p.primary_uom_cd").alias("base_uom_cd"),
        F.coalesce(F.col("u.from_uom_cd"), F.col("p.sell_uom_cd")).alias("sell_uom_cd"),
        F.col("u.conv_factor").cast("decimal(18,8)").alias("sell_to_base_factor"),
        F.col("p.unit_cost_std").cast("decimal(18,5)").alias("standard_cost_amt"),
        F.col("p.list_price_amt").cast("decimal(18,5)").alias("list_price_amt"),
        F.col("p.cost_curr_cd").alias("cost_currency_cd"),
        F.col("p.hazmat_class_cd").alias("hazmat_class_cd"),
        F.col("p.chiller_flg").alias("chiller_flg"),
        F.col("p.shelf_life_days").cast("int").alias("shelf_life_days"),
        F.col("p.updated_dt").alias("last_update_dt"),
        rules.productHandlingClass(F.col("p.chiller_flg"), F.col("p.hazmat_class_cd")).alias("handling_class"),
        F.lit("N").alias("delete_flag"),
        # columns the downstream STG_Load_Product package reads from raw.OracleProductMaster
        F.coalesce(F.col("c.category_cd"), F.col("p.item_type_cd")).alias("prod_family_cd"),
        F.col("u.conv_factor").cast("decimal(18,4)").alias("pack_qty"),
        F.col("p.list_price_curr_cd").alias("list_price_ccy"),
        F.col("p.unit_weight_kg").cast("decimal(18,4)").alias("net_weight"),
        F.lit("KG").alias("weight_uom_cd"),
        F.col("p.hazmat_flg").alias("hazmat_flg"),
        F.when(F.col("p.discontinued_dt").isNotNull(), F.lit("Y")).otherwise(F.lit("N")).alias("discontinued_flg"),
        F.col("p.legacy_part_cd").alias("legacy_part_cd"),
        F.col("p.wwi_stock_item_id").cast("int").alias("wwi_stock_item_id"),
        F.col("p.deleted_flg").alias("source_deleted_flg"),
        F.col("p.source_sys").alias("erp_source_sys"),
    )


def runExtOraProductMaster(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "EXT_ORA_ProductMaster"
    run = startPackage(package)
    objectName = "WWI_MDM.PRODUCT_MASTER"
    now = utcNow()
    windowFrom, windowTo = resolveTimestampWindow(
        _parseTs(getWatermark(spark, cfg, "ORA_ERP", objectName)), cfg.reloadFullHistory, 0, now
    )
    products = readTable(spark, cfg.oracle("wwi_mdm", "product_master")).where(
        (F.col("updated_dt") >= F.lit(windowFrom)) & (F.col("updated_dt") < F.lit(windowTo))
    )
    shaped = shapeOracleProductMaster(
        products,
        readTable(spark, cfg.oracle("wwi_mdm", "product_category")),
        readTable(spark, cfg.oracle("wwi_mdm", "product_uom_conv")),
        now,
    )
    shaped = _audit(shaped, cfg, "ORA_ERP", "product_id")
    # "Convert ERP Numerics" error output -> rejects: products without a usable code/description
    invalid = shaped.where(F.col("product_cd").isNull() | F.col("product_desc").isNull())
    valid = shaped.where(F.col("product_cd").isNotNull() & F.col("product_desc").isNotNull())
    run.rowsRejected = writeRejects(
        spark, cfg, invalid, package, "raw.OracleProductMaster", "Extract", "ERP_NUMERIC_CONVERSION",
        "Product code or description missing in ERP extract", "product_id",
    )
    writeBatch(spark, valid, cfg.fqn(BRONZE_ORA_PRODUCT_MASTER), cfg.batchId, cfg.reloadFullHistory)
    run.rowsInserted = readTable(spark, cfg.fqn(BRONZE_ORA_PRODUCT_MASTER)).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    setWatermark(spark, cfg, "ORA_ERP", objectName, "TIMESTAMP", _tsText(windowFrom), _tsText(windowTo))
    # product categories are landed alongside the master so DIM_Load_ProductCategory has a source
    categories = _audit(
        readTable(spark, cfg.oracle("wwi_mdm", "product_category")).select(
            F.col("product_category_id").cast("long").alias("product_category_id"),
            F.col("category_cd"),
            F.col("category_name"),
            F.col("parent_category_id").cast("long").alias("parent_category_id"),
            F.col("category_level_nbr").cast("int").alias("category_level_nbr"),
            F.col("merch_group_cd"),
            F.col("default_tax_class_cd"),
            F.col("margin_target_pct").cast("decimal(5,2)").alias("margin_target_pct"),
            F.col("active_flg"),
            F.col("sort_order_nbr").cast("int").alias("sort_order_nbr"),
            F.col("updated_dt").alias("last_update_dt"),
        ),
        cfg,
        "ORA_ERP",
        "product_category_id",
    )
    overwriteTable(categories, cfg.fqn(BRONZE_ORA_PRODUCT_CATEGORY))
    endPackage(spark, cfg, run, "Succeeded", f"window [{_tsText(windowFrom)}, {_tsText(windowTo)})")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# --------------------------------------------------------- EXT_ORA_ProductHierarchy
def shapeOracleProductHierarchy(hierarchy: DataFrame) -> DataFrame:
    """Flatten the level-based PRODUCT_HIERARCHY rows into the node/path shape the SSIS
    CONNECT BY query produced (one row per node per hierarchy type, open rows only)."""
    open_ = hierarchy.where(F.col("end_dt").isNull())
    levels = []
    for level in (1, 2, 3, 4):
        parentCode = F.lit(None).cast("string") if level == 1 else F.col(f"level_{level - 1}_cd")
        pathParts = [F.col(f"level_{i}_cd") for i in range(1, level + 1)]
        levels.append(
            open_.where(F.col(f"level_{level}_cd").isNotNull()).select(
                F.col("hier_type_cd").alias("hierarchy_cd"),
                F.col(f"level_{level}_cd").alias("node_cd"),
                F.col(f"level_{level}_name").alias("node_desc"),
                parentCode.alias("parent_node_cd"),
                F.lit(level).alias("node_level"),
                F.concat(F.lit("/"), F.concat_ws("/", *pathParts)).alias("node_path"),
                F.col("level_1_cd").alias("level1_cd"),
                F.col("level_2_cd").alias("level2_cd"),
                F.col("level_3_cd").alias("level3_cd"),
                F.col("updated_dt").alias("last_update_dt"),
            )
        )
    nodes = levels[0]
    for extra in levels[1:]:
        nodes = nodes.unionByName(extra)
    nodes = nodes.groupBy("hierarchy_cd", "node_cd", "parent_node_cd", "node_level", "node_path", "level1_cd", "level2_cd", "level3_cd").agg(
        F.max("node_desc").alias("node_desc"), F.max("last_update_dt").alias("last_update_dt")
    )
    children = nodes.select(F.col("hierarchy_cd").alias("_h"), F.col("parent_node_cd").alias("_p")).where(F.col("_p").isNotNull()).distinct()
    withLeaf = nodes.join(children, (nodes["hierarchy_cd"] == children["_h"]) & (nodes["node_cd"] == children["_p"]), "left")
    return withLeaf.select(
        F.sha2(F.concat_ws("|", "hierarchy_cd", "node_path"), 256).alias("hierarchy_node_id"),
        F.sha2(F.concat_ws("|", "hierarchy_cd", F.regexp_replace(F.col("node_path"), "/[^/]+$", "")), 256).alias("parent_node_id"),
        "hierarchy_cd",
        "node_cd",
        "node_desc",
        "parent_node_cd",
        "node_level",
        "node_path",
        "level1_cd",
        "level2_cd",
        "level3_cd",
        F.when(F.col("_p").isNull(), F.lit("Y")).otherwise(F.lit("N")).alias("leaf_flg"),
        "last_update_dt",
        F.lit("HIERARCHY").alias("record_kind"),
    ).withColumn("parent_node_id", F.when(F.col("node_level") == 1, F.lit(None).cast("string")).otherwise(F.col("parent_node_id")))


def runExtOraProductHierarchy(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "EXT_ORA_ProductHierarchy"
    run = startPackage(package)
    shaped = shapeOracleProductHierarchy(readTable(spark, cfg.oracle("wwi_mdm", "product_hierarchy")))
    shaped = _audit(shaped, cfg, "ORA_ERP", "node_path")
    overwriteTable(shaped, cfg.fqn(BRONZE_ORA_PRODUCT_HIERARCHY))
    run.rowsInserted = readTable(spark, cfg.fqn(BRONZE_ORA_PRODUCT_HIERARCHY)).count()
    run.rowsRead = run.rowsInserted
    endPackage(spark, cfg, run, "Succeeded", "full truncate-and-load")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": 0}


# --------------------------------------------------------------- EXT_SQL_StockItems
def shapeSqlStockItems(stockItems: DataFrame, holdings: DataFrame, rules_: DataFrame, sites: DataFrame) -> DataFrame:
    si = stockItems.alias("si")
    sh = holdings.alias("sh")
    rr = rules_.where(~F.col("IsSuspended").cast("boolean")).alias("rr")
    ws = sites.alias("ws")
    joined = (
        si.join(sh, F.col("sh.StockItemID") == F.col("si.StockItemID"), "left")
        .join(rr, F.col("rr.StockItemID") == F.col("si.StockItemID"), "left")
        .join(ws, F.col("ws.WarehouseSiteID") == F.col("sh.PrimaryWarehouseSiteID"), "left")
    )
    return joined.select(
        F.col("si.StockItemID").cast("int").alias("stock_item_id"),
        F.col("si.StockItemName").alias("stock_item_name"),
        F.col("si.SupplierID").cast("int").alias("supplier_id"),
        F.col("si.ColorID").cast("int").alias("color_id"),
        F.col("si.UnitPackageID").cast("int").alias("unit_package_id"),
        F.col("si.OuterPackageID").cast("int").alias("outer_package_id"),
        F.col("si.Brand").alias("brand"),
        F.col("si.Size").alias("size"),
        F.col("si.LeadTimeDays").cast("int").alias("lead_time_days"),
        F.col("si.QuantityPerOuter").cast("int").alias("quantity_per_outer"),
        F.col("si.IsChillerStock").cast("boolean").alias("is_chiller_stock"),
        F.col("si.Barcode").alias("barcode"),
        F.col("si.TaxRate").cast("decimal(18,3)").alias("tax_rate"),
        F.col("si.UnitPrice").cast("decimal(18,2)").alias("unit_price"),
        F.col("si.RecommendedRetailPrice").cast("decimal(18,2)").alias("recommended_retail_price"),
        F.col("si.TypicalWeightPerUnit").cast("decimal(18,3)").alias("typical_weight_per_unit"),
        F.col("si.MarketingComments").alias("marketing_comments"),
        F.col("si.InternalComments").alias("internal_comments"),
        F.col("si.CustomFields").alias("custom_fields"),
        F.col("si.Tags").alias("tags"),
        F.col("si.SearchDetails").alias("search_details"),
        F.col("sh.QuantityOnHand").cast("int").alias("quantity_on_hand"),
        F.col("sh.BinLocation").alias("bin_location"),
        F.col("sh.LastStocktakeQuantity").cast("int").alias("last_stocktake_quantity"),
        F.col("sh.LastCostPrice").cast("decimal(18,2)").alias("last_cost_price"),
        F.col("sh.ReorderLevel").cast("int").alias("reorder_level"),
        F.col("sh.TargetStockLevel").cast("int").alias("target_stock_level"),
        F.col("sh.QuantityReservedAllSites").cast("decimal(18,3)").alias("quantity_allocated"),
        F.col("sh.QuantityInTransit").cast("decimal(18,3)").alias("quantity_in_transit"),
        F.col("sh.QuantityOnPurchaseOrder").cast("decimal(18,3)").alias("quantity_on_order"),
        F.col("sh.AbcClass").alias("abc_class"),
        F.col("sh.LastCountedWhen").alias("last_counted_when"),
        F.col("sh.LastMovementWhen").alias("last_movement_when"),
        F.col("sh.PrimaryWarehouseSiteID").cast("int").alias("primary_warehouse_site_id"),
        F.col("ws.SiteCode").alias("primary_warehouse_site_code"),
        F.col("ws.RegionCode").alias("region_code"),
        F.col("rr.PolicyCode").alias("replenishment_rule_code"),
        F.col("rr.AverageDailyDemand").cast("decimal(18,4)").alias("rule_average_daily_demand"),
        F.col("si.ValidFrom").alias("valid_from"),
        F.col("si.ValidTo").alias("valid_to"),
        F.col("si.LastEditedBy").cast("int").alias("last_edited_by"),
        F.col("si.ValidFrom").alias("last_edited_when"),
        rules.belowReorderFlag(F.col("sh.QuantityOnHand"), F.col("sh.ReorderLevel")).alias("below_reorder_flag"),
        rules.stockItemHandlingClass(F.col("si.IsChillerStock")).alias("handling_class"),
        F.lit("N").alias("delete_flag"),
    )


def runExtSqlStockItems(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "EXT_SQL_StockItems"
    run = startPackage(package)
    objectName = "Warehouse.StockItems"
    now = utcNow()
    windowFrom, windowTo = resolveTimestampWindow(
        _parseTs(getWatermark(spark, cfg, "WWI_OLTP", objectName)), cfg.reloadFullHistory, cfg.stockItemLookbackMinutes, now
    )
    current = readTable(spark, cfg.oltp("Warehouse", "StockItems"))
    # the system-versioned history is the source of the SCD2 versions on a full reload
    archive = readTable(spark, cfg.oltp("Warehouse", "StockItems_Archive")).select(*current.columns)
    versions = current.unionByName(archive) if cfg.reloadFullHistory else current
    versions = versions.where((F.col("ValidFrom") >= F.lit(windowFrom)) & (F.col("ValidFrom") < F.lit(windowTo)))
    shaped = shapeSqlStockItems(
        versions,
        readTable(spark, cfg.oltp("Warehouse", "StockItemHoldings")),
        readTable(spark, cfg.oltp("Warehouse", "ReplenishmentRules")),
        readTable(spark, cfg.oltp("Warehouse", "WarehouseSites")),
    )
    shaped = _audit(shaped, cfg, "WWI_OLTP", "stock_item_id")
    invalid = shaped.where(F.col("stock_item_name").isNull() | F.col("valid_from").isNull())
    valid = shaped.where(F.col("stock_item_name").isNotNull() & F.col("valid_from").isNotNull())
    run.rowsRejected = writeRejects(
        spark, cfg, invalid, package, "raw.SqlStockItem", "Extract", "SOURCE_ERROR",
        "Stock item without name or temporal validity", "stock_item_id",
    )
    # change-tracking delete detection is not expressible through federation: soft-delete
    # rows that disappeared from the live table instead (see README deviations)
    deletes = None
    target = cfg.fqn(BRONZE_SQL_STOCK_ITEM)
    if tableExists(spark, target):
        known = readTable(spark, target).where(F.col("delete_flag") == "N").select("stock_item_id").distinct()
        live = current.select(F.col("StockItemID").cast("int").alias("stock_item_id")).distinct()
        goneIds = known.join(live, "stock_item_id", "left_anti")
        latestKnown = readTable(spark, target).join(goneIds, "stock_item_id")
        latestKnown = latestKnown.withColumn("_rn", F.row_number().over(Window.partitionBy("stock_item_id").orderBy(F.col("valid_from").desc()))).where(F.col("_rn") == 1).drop("_rn")
        deletes = (
            latestKnown.withColumn("delete_flag", F.lit("Y"))
            .withColumn("valid_from", F.lit(now))
            .withColumn("batch_id", F.lit(cfg.batchId).cast("long"))
            .withColumn("package_execution_id", F.lit(cfg.batchId).cast("long"))
            .withColumn("loaded_at_utc", F.current_timestamp())
        )
    toWrite = valid if deletes is None else valid.unionByName(deletes.select(*valid.columns))
    writeBatch(spark, toWrite, target, cfg.batchId, cfg.reloadFullHistory)
    run.rowsInserted = readTable(spark, target).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    setWatermark(spark, cfg, "WWI_OLTP", objectName, "TIMESTAMP", _tsText(windowFrom), _tsText(windowTo))
    endPackage(spark, cfg, run, "Succeeded", f"window [{_tsText(windowFrom)}, {_tsText(windowTo)})")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ----------------------------------------------------------- EXT_SQL_StockMovements
def shapeSqlStockMovements(transactions: DataFrame, transactionTypes: DataFrame) -> DataFrame:
    m = transactions.alias("m")
    tt = transactionTypes.alias("tt")
    joined = m.join(tt, F.col("tt.TransactionTypeID") == F.col("m.TransactionTypeID"), "inner")
    return joined.select(
        F.col("m.StockItemTransactionID").cast("long").alias("stock_item_transaction_id"),
        F.col("m.StockItemID").cast("int").alias("stock_item_id"),
        F.col("m.TransactionTypeID").cast("int").alias("transaction_type_id"),
        F.col("tt.TransactionTypeName").alias("transaction_type_name"),
        F.col("m.CustomerID").cast("int").alias("customer_id"),
        F.col("m.InvoiceID").cast("int").alias("invoice_id"),
        F.col("m.SupplierID").cast("int").alias("supplier_id"),
        F.col("m.PurchaseOrderID").cast("int").alias("purchase_order_id"),
        F.lit(None).cast("string").alias("warehouse_site_code"),
        F.lit(None).cast("string").alias("bin_code"),
        F.col("m.TransactionOccurredWhen").alias("transaction_occurred_when"),
        F.col("m.Quantity").cast("decimal(18,3)").alias("quantity"),
        F.abs(F.col("m.Quantity")).cast("decimal(18,3)").alias("absolute_quantity"),
        rules.movementDirection(F.col("m.Quantity")).alias("movement_direction_code"),
        rules.movementClass(F.col("m.InvoiceID"), F.col("m.PurchaseOrderID")).alias("movement_class"),
        F.lit("EA").alias("uom_code"),
        F.col("m.LastEditedBy").cast("int").alias("last_edited_by"),
        F.col("m.LastEditedWhen").alias("last_edited_when"),
    )


def runExtSqlStockMovements(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "EXT_SQL_StockMovements"
    run = startPackage(package)
    objectName = "Warehouse.StockItemTransactions"
    source = readTable(spark, cfg.oltp("Warehouse", "StockItemTransactions"))
    lastKey = getWatermark(spark, cfg, "WWI_OLTP", objectName)
    maxKey = source.agg(F.max("StockItemTransactionID")).collect()[0][0]
    keyFrom, keyTo = resolveKeyWindow(int(lastKey) if lastKey else None, int(maxKey) if maxKey is not None else None, cfg.reloadFullHistory)
    window = source.where((F.col("StockItemTransactionID") > keyFrom) & (F.col("StockItemTransactionID") <= keyTo))
    shaped = shapeSqlStockMovements(window, readTable(spark, cfg.oltp("Application", "TransactionTypes")))
    shaped = _audit(shaped, cfg, "WWI_OLTP", "stock_item_transaction_id")
    invalid = shaped.where(F.col("stock_item_id").isNull() | F.col("transaction_occurred_when").isNull() | F.col("quantity").isNull())
    valid = shaped.where(F.col("stock_item_id").isNotNull() & F.col("transaction_occurred_when").isNotNull() & F.col("quantity").isNotNull())
    run.rowsRejected = writeRejects(
        spark, cfg, invalid, package, "raw.SqlStockMovement", "Extract", "CONSTRAINT_VIOLATION",
        "Movement missing stock item, timestamp or quantity", "stock_item_transaction_id",
    )
    target = cfg.fqn(BRONZE_SQL_STOCK_MOVEMENT)
    writeBatch(spark, valid, target, cfg.batchId, cfg.reloadFullHistory)
    run.rowsInserted = readTable(spark, target).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    setWatermark(spark, cfg, "WWI_OLTP", objectName, "KEY", str(keyFrom), str(keyTo))
    endPackage(spark, cfg, run, "Succeeded", f"key window ({keyFrom}, {keyTo}]")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ----------------------------------------------------------- EXT_SQL_StockTransfers
def shapeSqlStockTransfers(lines: DataFrame, transfers: DataFrame, sites: DataFrame, asOf: datetime, staleDays: int) -> DataFrame:
    tl = lines.alias("tl")
    t = transfers.alias("t")
    fs = sites.alias("fs")
    ts = sites.alias("ts")
    joined = (
        tl.join(t, F.col("t.StockTransferID") == F.col("tl.StockTransferID"), "inner")
        .join(fs, F.col("fs.WarehouseSiteID") == F.col("t.FromWarehouseSiteID"), "inner")
        .join(ts, F.col("ts.WarehouseSiteID") == F.col("t.ToWarehouseSiteID"), "inner")
        .where(F.coalesce(F.col("t.TransferStatus"), F.lit("")) != "CANC")
    )
    return joined.select(
        F.col("tl.StockTransferLineID").cast("long").alias("stock_transfer_line_id"),
        F.col("tl.StockTransferID").cast("long").alias("stock_transfer_id"),
        F.col("t.TransferReference").alias("transfer_reference"),
        F.col("tl.StockItemID").cast("int").alias("stock_item_id"),
        F.col("fs.SiteCode").alias("from_site_code"),
        F.col("ts.SiteCode").alias("to_site_code"),
        F.col("fs.RegionCode").alias("from_region_code"),
        F.col("ts.RegionCode").alias("to_region_code"),
        F.col("tl.QuantityDespatched").cast("decimal(18,3)").alias("transfer_quantity"),
        F.col("tl.QuantityReceived").cast("decimal(18,3)").alias("received_quantity"),
        F.col("t.TransferStatus").alias("transfer_status_code"),
        F.col("t.CarrierCode").alias("carrier_code"),
        F.col("t.DespatchedWhen").alias("dispatched_when"),
        F.col("t.ReceivedWhen").alias("received_when"),
        F.col("tl.UnitCostAtDespatch").cast("decimal(18,2)").alias("unit_cost_at_despatch"),
        F.col("tl.LastEditedWhen").alias("last_edited_when"),
        rules.transferInTransitQuantity(F.col("tl.QuantityDespatched"), F.col("tl.QuantityReceived")).cast("decimal(18,3)").alias("in_transit_quantity"),
        rules.staleTransitFlag(F.col("t.ReceivedWhen"), F.col("t.DespatchedWhen"), F.lit(asOf), staleDays).alias("stale_transit_flag"),
        F.lit("XFER").alias("movement_class"),
    )


def runExtSqlStockTransfers(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "EXT_SQL_StockTransfers"
    run = startPackage(package)
    objectName = "Warehouse.StockTransferLines"
    lines = readTable(spark, cfg.oltp("Warehouse", "StockTransferLines"))
    lastKey = getWatermark(spark, cfg, "WWI_OLTP", objectName)
    maxKey = lines.agg(F.max("StockTransferLineID")).collect()[0][0]
    keyFrom, keyTo = resolveKeyWindow(int(lastKey) if lastKey else None, int(maxKey) if maxKey is not None else None, cfg.reloadFullHistory)
    shaped = shapeSqlStockTransfers(
        lines.where(F.col("StockTransferLineID") > keyFrom),
        readTable(spark, cfg.oltp("Warehouse", "StockTransfers")),
        readTable(spark, cfg.oltp("Warehouse", "WarehouseSites")),
        utcNow(),
        cfg.staleTransitDays,
    )
    shaped = _audit(shaped, cfg, "WWI_OLTP", "stock_transfer_line_id")
    invalid = shaped.where(F.col("stock_item_id").isNull() | F.col("transfer_quantity").isNull())
    valid = shaped.where(F.col("stock_item_id").isNotNull() & F.col("transfer_quantity").isNotNull())
    run.rowsRejected = writeRejects(
        spark, cfg, invalid, package, "raw.SqlStockMovement", "Extract", "CONSTRAINT_VIOLATION",
        "Transfer line missing stock item or quantity", "stock_transfer_line_id",
    )
    target = cfg.fqn(BRONZE_SQL_STOCK_TRANSFER)
    writeBatch(spark, valid, target, cfg.batchId, cfg.reloadFullHistory)
    run.rowsInserted = readTable(spark, target).where(F.col("batch_id") == cfg.batchId).count()
    run.rowsRead = run.rowsInserted + run.rowsRejected
    setWatermark(spark, cfg, "WWI_OLTP", objectName, "KEY", str(keyFrom), str(keyTo))
    endPackage(spark, cfg, run, "Succeeded", f"key window ({keyFrom}, {keyTo}]")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


def runBronze(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, Dict[str, int]]:
    return {
        "EXT_ORA_ProductMaster": runExtOraProductMaster(spark, cfg),
        "EXT_ORA_ProductHierarchy": runExtOraProductHierarchy(spark, cfg),
        "EXT_SQL_StockItems": runExtSqlStockItems(spark, cfg),
        "EXT_SQL_StockMovements": runExtSqlStockMovements(spark, cfg),
        "EXT_SQL_StockTransfers": runExtSqlStockTransfers(spark, cfg),
    }
