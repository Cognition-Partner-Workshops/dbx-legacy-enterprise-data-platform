"""Gold dimensions: DIM_Load_StockItem (hybrid SCD2/SCD1) and DIM_Load_ProductCategory (SCD1)."""
from __future__ import annotations

from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from product_inventory import rules
from product_inventory.bronze import BRONZE_ORA_PRODUCT_CATEGORY
from product_inventory.config import FAR_FUTURE, PipelineConfig
from product_inventory.control import endPackage, startPackage, writeRejects
from product_inventory.scd import applyHybridScd2, applyScd1, reservedMemberRows
from product_inventory.silver import SILVER_PRODUCT, SILVER_STOCK_ITEM, WORK_PRODUCT_CROSSWALK
from product_inventory.tables import overwriteFromSelf, readTable, readTableOrEmpty, tableExists

GOLD_DIM_STOCK_ITEM = "gold_dim_stock_item"
GOLD_DIM_PRODUCT_CATEGORY = "gold_dim_product_category"

STOCK_ITEM_TYPE2 = [
    "stock_item_name",
    "brand_code",
    "size_code",
    "color_id",
    "primary_supplier_id",
    "barcode",
    "unit_price",
    "recommended_retail_price",
    "tax_rate",
    "quantity_per_outer",
    "lead_time_days",
    "is_chiller_stock",
    "typical_weight_per_unit",
    "unit_package_id",
    "outer_package_id",
    "product_category_code",
]
STOCK_ITEM_TYPE1 = ["marketing_comments", "search_details", "tags", "internal_comments"]

STOCK_ITEM_DIM_SCHEMA = StructType(
    [
        StructField("stock_item_key", LongType()),
        StructField("wwi_stock_item_id", IntegerType()),
        StructField("stock_item_name", StringType()),
        StructField("brand_code", StringType()),
        StructField("size_code", StringType()),
        StructField("color_id", IntegerType()),
        StructField("primary_supplier_id", IntegerType()),
        StructField("barcode", StringType()),
        StructField("unit_price", DecimalType(18, 2)),
        StructField("recommended_retail_price", DecimalType(18, 2)),
        StructField("tax_rate", DecimalType(18, 3)),
        StructField("quantity_per_outer", IntegerType()),
        StructField("lead_time_days", IntegerType()),
        StructField("is_chiller_stock", BooleanType()),
        StructField("typical_weight_per_unit", DecimalType(18, 3)),
        StructField("unit_package_id", IntegerType()),
        StructField("outer_package_id", IntegerType()),
        StructField("product_category_code", StringType()),
        StructField("marketing_comments", StringType()),
        StructField("search_details", StringType()),
        StructField("tags", StringType()),
        StructField("internal_comments", StringType()),
        StructField("standard_unit_cost", DecimalType(18, 2)),
        StructField("gross_margin_percent", DecimalType(18, 2)),
        StructField("price_band_code", StringType()),
        StructField("handling_code", StringType()),
        StructField("product_id", LongType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current_row", BooleanType()),
        StructField("row_version", IntegerType()),
        StructField("type1_hash", StringType()),
        StructField("type2_hash", StringType()),
        StructField("is_inferred_member", BooleanType()),
        StructField("is_reserved_member", BooleanType()),
        StructField("lineage_key", LongType()),
    ]
)

STOCK_ITEM_RESERVED_MEMBERS = [
    {"stock_item_key": 0, "wwi_stock_item_id": 0, "stock_item_name": "Unknown", "valid_from": datetime(2013, 1, 1)},
    {"stock_item_key": -1, "wwi_stock_item_id": -1, "stock_item_name": "Unknown", "valid_from": datetime(1900, 1, 1)},
    {"stock_item_key": -2, "wwi_stock_item_id": -2, "stock_item_name": "Not Applicable", "valid_from": datetime(1900, 1, 1)},
]


def _reservedDefaults(member: dict, lineage: int) -> dict:
    base = {
        "brand_code": "N/A",
        "size_code": "N/A",
        "primary_supplier_id": -1,
        "product_category_code": "UNCLASS",
        "unit_price": 0,
        "price_band_code": "P1",
        "handling_code": "AMBIENT",
        "valid_to": FAR_FUTURE,
        "is_current_row": True,
        "row_version": 1,
        "is_inferred_member": False,
        "is_reserved_member": True,
        "lineage_key": lineage,
    }
    base.update(member)
    return base


def ensureStockItemUnknownMembers(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """`Integration.EnsureUnknownMembers @DimensionName = 'Stock Item'`."""
    target = cfg.fqn(GOLD_DIM_STOCK_ITEM)
    existing = readTableOrEmpty(spark, target, STOCK_ITEM_DIM_SCHEMA)
    template = spark.createDataFrame([], STOCK_ITEM_DIM_SCHEMA)
    reserved = reservedMemberRows(template, [_reservedDefaults(m, cfg.batchId) for m in STOCK_ITEM_RESERVED_MEMBERS])
    missing = reserved.join(existing.select("stock_item_key"), "stock_item_key", "left_anti")
    return existing.unionByName(missing)


def shapeStockItemVersions(stagedStockItems: DataFrame, crosswalk: DataFrame, products: DataFrame) -> DataFrame:
    """stg.StockItem versions + crosswalked Oracle category -> dimension attribute rows."""
    xw = crosswalk.where(F.col("is_preferred_match")).select(F.col("stock_item_id").alias("_xw_id"), F.col("product_id").alias("_xw_product_id"))
    pr = products.select(F.col("product_id").alias("_p_id"), F.col("category_code").alias("_p_category"))
    joined = stagedStockItems.join(xw, stagedStockItems["stock_item_id"] == xw["_xw_id"], "left").join(pr, F.col("_xw_product_id") == pr["_p_id"], "left")
    return joined.select(
        F.col("stock_item_id").alias("wwi_stock_item_id"),
        "stock_item_name",
        "brand_code",
        "size_code",
        "color_id",
        F.col("supplier_id").alias("primary_supplier_id"),
        "barcode",
        "unit_price",
        "recommended_retail_price",
        "tax_rate",
        "quantity_per_outer",
        "lead_time_days",
        "is_chiller_stock",
        "typical_weight_per_unit",
        "unit_package_id",
        "outer_package_id",
        F.coalesce(F.col("_p_category"), F.lit("UNCLASS")).alias("product_category_code"),
        "marketing_comments",
        "search_details",
        "tags",
        "internal_comments",
        "standard_unit_cost",
        rules.grossMarginPercent(F.col("unit_price"), F.col("standard_unit_cost")).alias("gross_margin_percent"),
        rules.dimensionPriceBandCode(F.col("unit_price")).alias("price_band_code"),
        rules.handlingCode(F.col("is_chiller_stock"), F.col("quantity_per_outer")).alias("handling_code"),
        F.col("_xw_product_id").alias("product_id"),
        "valid_from",
    )


def runDimLoadStockItem(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "DIM_Load_StockItem"
    run = startPackage(package)
    existing = ensureStockItemUnknownMembers(spark, cfg)
    staged = readTable(spark, cfg.fqn(SILVER_STOCK_ITEM)).where(F.col("delete_flag") == "N")
    crosswalk = readTableOrEmpty(spark, cfg.fqn(WORK_PRODUCT_CROSSWALK), StructType([StructField("stock_item_id", IntegerType()), StructField("product_id", LongType()), StructField("is_preferred_match", BooleanType())]))
    products = readTableOrEmpty(spark, cfg.fqn(SILVER_PRODUCT), StructType([StructField("product_id", LongType()), StructField("category_code", StringType())]))
    incoming = shapeStockItemVersions(staged, crosswalk, products)
    rejected = incoming.where(F.col("stock_item_name").isNull() | F.col("valid_from").isNull())
    run.rowsRejected = writeRejects(
        spark, cfg, rejected, package, "Dimension.Stock Item", "Dimension", "SOURCE_ERROR", "Stock item version without name or validity", "wwi_stock_item_id"
    )
    incoming = incoming.where(F.col("stock_item_name").isNotNull() & F.col("valid_from").isNotNull())
    result = applyHybridScd2(existing, incoming, "wwi_stock_item_id", "stock_item_key", STOCK_ITEM_TYPE1, STOCK_ITEM_TYPE2, cfg.batchId)
    result = result.select(*[F.col(f.name).cast(f.dataType) for f in STOCK_ITEM_DIM_SCHEMA.fields])
    before = existing.count()
    overwriteFromSelf(spark, result, cfg.fqn(GOLD_DIM_STOCK_ITEM))
    after = readTable(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM)).count()
    run.rowsRead = incoming.count()
    run.rowsInserted = max(after - before, 0)
    run.rowsUpdated = readTable(spark, cfg.fqn(GOLD_DIM_STOCK_ITEM)).where(F.col("lineage_key") == cfg.batchId).count() - run.rowsInserted
    endPackage(spark, cfg, run, "Succeeded", "hybrid SCD2 (commercial) / SCD1 (marketing)")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


# ------------------------------------------------------------ DIM_Load_ProductCategory
CATEGORY_ATTRIBUTES = [
    "product_category_code",
    "product_category_name",
    "parent_category_code",
    "merchandise_group_code",
    "hierarchy_level",
    "is_leaf_category",
    "category_path",
    "reporting_rollup_code",
    "default_tax_class_code",
    "is_active",
]
CATEGORY_DIM_SCHEMA = StructType(
    [
        StructField("product_category_key", LongType()),
        StructField("wwi_product_category_id", LongType()),
        *[
            StructField(name, dtype)
            for name, dtype in [
                ("product_category_code", StringType()),
                ("product_category_name", StringType()),
                ("parent_category_code", StringType()),
                ("merchandise_group_code", StringType()),
                ("hierarchy_level", IntegerType()),
                ("is_leaf_category", BooleanType()),
                ("category_path", StringType()),
                ("reporting_rollup_code", StringType()),
                ("default_tax_class_code", StringType()),
                ("is_active", BooleanType()),
            ]
        ],
        StructField("row_hash", StringType()),
        StructField("is_reserved_member", BooleanType()),
        StructField("lineage_key", LongType()),
        StructField("valid_from", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("parent_category_key", LongType()),
    ]
)
CATEGORY_RESERVED_MEMBERS = [
    {"product_category_key": 0, "wwi_product_category_id": 0, "product_category_code": "UNKNOWN", "product_category_name": "Unknown"},
    {"product_category_key": -1, "wwi_product_category_id": -1, "product_category_code": "N/A", "product_category_name": "Not Applicable"},
]


def shapeProductCategories(rawCategories: DataFrame) -> DataFrame:
    c = rawCategories.alias("c")
    p = rawCategories.select(F.col("product_category_id").alias("_pid"), F.col("category_cd").alias("_pcode")).alias("p")
    children = rawCategories.select(F.col("parent_category_id").alias("_child_parent")).where(F.col("_child_parent").isNotNull()).distinct()
    joined = c.join(p, F.col("c.parent_category_id") == F.col("p._pid"), "left").join(
        children, F.col("c.product_category_id") == children["_child_parent"], "left"
    )
    return joined.select(
        F.col("c.product_category_id").alias("wwi_product_category_id"),
        F.upper(F.trim(F.col("c.category_cd"))).alias("product_category_code"),
        F.trim(F.col("c.category_name")).alias("product_category_name"),
        F.upper(F.trim(F.col("_pcode"))).alias("parent_category_code"),
        F.col("c.merch_group_cd").alias("merchandise_group_code"),
        F.col("c.category_level_nbr").cast("int").alias("hierarchy_level"),
        F.col("_child_parent").isNull().alias("is_leaf_category"),
        rules.categoryPath(F.col("_pcode"), F.col("c.category_cd")).alias("category_path"),
        rules.reportingRollupCode(F.col("c.merch_group_cd")).alias("reporting_rollup_code"),
        F.col("c.default_tax_class_cd").alias("default_tax_class_code"),
        (F.upper(F.coalesce(F.col("c.active_flg"), F.lit("Y"))) == "Y").alias("is_active"),
    )


def resolveParentKeys(dim: DataFrame) -> DataFrame:
    """Parent lookup against the dimension itself; missing parents resolve to -1."""
    parents = dim.select(F.col("product_category_code").alias("_pc"), F.col("product_category_key").alias("_pk"))
    resolved = dim.drop("parent_category_key").join(parents, dim["parent_category_code"] == parents["_pc"], "left")
    return resolved.withColumn(
        "parent_category_key",
        F.when(F.col("is_reserved_member"), F.lit(None).cast("long")).otherwise(F.coalesce(F.col("_pk"), F.lit(-1))).cast("long"),
    ).drop("_pc", "_pk")


def runDimLoadProductCategory(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, int]:
    package = "DIM_Load_ProductCategory"
    run = startPackage(package)
    target = cfg.fqn(GOLD_DIM_PRODUCT_CATEGORY)
    existing = readTableOrEmpty(spark, target, CATEGORY_DIM_SCHEMA)
    template = spark.createDataFrame([], CATEGORY_DIM_SCHEMA)
    reserved = reservedMemberRows(
        template,
        [
            {**m, "hierarchy_level": 0, "is_leaf_category": True, "category_path": m["product_category_code"], "reporting_rollup_code": "CORE", "is_active": True, "is_reserved_member": True, "lineage_key": cfg.batchId, "valid_from": datetime(1900, 1, 1)}
            for m in CATEGORY_RESERVED_MEMBERS
        ],
    )
    existing = existing.unionByName(reserved.join(existing.select("product_category_key"), "product_category_key", "left_anti"))
    incoming = shapeProductCategories(readTable(spark, cfg.fqn(BRONZE_ORA_PRODUCT_CATEGORY)))
    rejected = incoming.where(F.col("product_category_code").isNull() | (F.col("product_category_code") == ""))
    run.rowsRejected = writeRejects(spark, cfg, rejected, package, "Dimension.Product Category", "Dimension", "MISSING_CODE", "Category without code", "wwi_product_category_id")
    incoming = incoming.where(F.col("product_category_code").isNotNull() & (F.col("product_category_code") != ""))
    result = applyScd1(existing.drop("parent_category_key"), incoming, "wwi_product_category_id", "product_category_key", CATEGORY_ATTRIBUTES, cfg.batchId)
    result = resolveParentKeys(result.withColumn("parent_category_key", F.lit(None).cast("long")))
    result = result.select(*[F.col(f.name).cast(f.dataType) for f in CATEGORY_DIM_SCHEMA.fields])
    before = existing.count()
    if tableExists(spark, target):
        overwriteFromSelf(spark, result, target)
    else:
        result.write.format("delta").mode("overwrite").saveAsTable(target)
    after = readTable(spark, target).count()
    run.rowsRead = incoming.count()
    run.rowsInserted = max(after - before, 0)
    endPackage(spark, cfg, run, "Succeeded", "SCD1 with parent-key resolution")
    return {"rows_read": run.rowsRead, "rows_inserted": run.rowsInserted, "rows_rejected": run.rowsRejected}


def runDimensions(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, Dict[str, int]]:
    return {
        "DIM_Load_ProductCategory": runDimLoadProductCategory(spark, cfg),
        "DIM_Load_StockItem": runDimLoadStockItem(spark, cfg),
    }

