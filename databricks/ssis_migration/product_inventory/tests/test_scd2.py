from datetime import datetime

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

from product_inventory.config import FAR_FUTURE
from product_inventory.scd import applyHybridScd2, reservedMemberRows

DIM_SCHEMA = StructType(
    [
        StructField("stock_item_key", LongType()),
        StructField("wwi_stock_item_id", IntegerType()),
        StructField("stock_item_name", StringType()),
        StructField("unit_price", DecimalType(18, 2)),
        StructField("marketing_comments", StringType()),
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
INCOMING_COLS = ["wwi_stock_item_id", "stock_item_name", "unit_price", "marketing_comments", "valid_from"]
TYPE1 = ["marketing_comments"]
TYPE2 = ["stock_item_name", "unit_price"]


def _incoming(spark, rows):
    df = spark.createDataFrame(rows, INCOMING_COLS)
    return df.withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)")).withColumn(
        "wwi_stock_item_id", F.col("wwi_stock_item_id").cast("int")
    )


def _emptyDim(spark):
    template = spark.createDataFrame([], DIM_SCHEMA)
    return reservedMemberRows(
        template,
        [
            {
                "stock_item_key": 0,
                "wwi_stock_item_id": 0,
                "stock_item_name": "Unknown",
                "valid_from": datetime(1900, 1, 1),
                "valid_to": FAR_FUTURE,
                "is_current_row": True,
                "row_version": 1,
                "is_inferred_member": False,
                "is_reserved_member": True,
                "lineage_key": 0,
            }
        ],
    )


def _run(existing, incoming, lineage=1):
    return applyHybridScd2(existing, incoming, "wwi_stock_item_id", "stock_item_key", TYPE1, TYPE2, lineage)


def test_full_history_reload_versions_only_on_type2_change(spark):
    incoming = _incoming(
        spark,
        [
            (1, "Widget", 10.0, "old blurb", datetime(2013, 1, 1)),
            (1, "Widget", 10.0, "new blurb", datetime(2014, 1, 1)),  # type-1 only
            (1, "Widget", 12.5, "new blurb", datetime(2015, 1, 1)),  # type-2 change
            (2, "Gadget", 5.0, None, datetime(2013, 6, 1)),
        ],
    )
    result = _run(_emptyDim(spark), incoming).orderBy("wwi_stock_item_id", "valid_from").collect()
    assert [r.wwi_stock_item_id for r in result] == [0, 1, 1, 2]
    reserved, v1, v2, gadget = result
    assert reserved.is_reserved_member and reserved.stock_item_key == 0
    assert v1.row_version == 1 and not v1.is_current_row
    assert v1.valid_to == datetime(2014, 12, 31, 23, 59, 59)
    assert v1.marketing_comments == "new blurb"  # type-1 overwrite across versions
    assert v2.row_version == 2 and v2.is_current_row and v2.valid_to == FAR_FUTURE
    assert float(v2.unit_price) == 12.5
    assert gadget.row_version == 1 and gadget.is_current_row
    keys = {r.stock_item_key for r in result}
    assert len(keys) == 4 and min(k for k in keys if k > 0) == 1


def test_incremental_type2_change_closes_current_and_preserves_keys(spark):
    first = _run(_emptyDim(spark), _incoming(spark, [(1, "Widget", 10.0, "blurb", datetime(2013, 1, 1))]))
    second = _run(first, _incoming(spark, [(1, "Widget XL", 10.0, "blurb", datetime(2016, 1, 1))]), lineage=2)
    rows = second.where("wwi_stock_item_id = 1").orderBy("valid_from").collect()
    assert len(rows) == 2
    assert rows[0].stock_item_key == first.where("wwi_stock_item_id = 1").first().stock_item_key
    assert rows[0].valid_to == datetime(2015, 12, 31, 23, 59, 59) and not rows[0].is_current_row
    assert rows[1].row_version == 2 and rows[1].is_current_row and rows[1].lineage_key == 2
    assert rows[1].stock_item_key != rows[0].stock_item_key


def test_type1_only_change_overwrites_in_place(spark):
    first = _run(_emptyDim(spark), _incoming(spark, [(1, "Widget", 10.0, "blurb", datetime(2013, 1, 1))]))
    second = _run(first, _incoming(spark, [(1, "Widget", 10.0, "better blurb", datetime(2016, 1, 1))]))
    rows = second.where("wwi_stock_item_id = 1").collect()
    assert len(rows) == 1
    assert rows[0].marketing_comments == "better blurb" and rows[0].row_version == 1
    assert rows[0].valid_from == datetime(2013, 1, 1)


def test_unchanged_rows_are_idempotent(spark):
    incoming = _incoming(spark, [(1, "Widget", 10.0, "blurb", datetime(2013, 1, 1))])
    first = _run(_emptyDim(spark), incoming)
    second = _run(first, incoming)
    assert sorted(first.collect()) == sorted(second.collect())


def test_duplicate_versions_are_deduplicated(spark):
    incoming = _incoming(
        spark,
        [
            (1, "Widget", 10.0, "blurb", datetime(2013, 1, 1)),
            (1, "Widget", 10.0, "blurb", datetime(2013, 1, 1)),
            (1, "Widget", 10.0, "blurb", datetime(2013, 2, 1)),
        ],
    )
    result = _run(_emptyDim(spark), incoming)
    assert result.where("wwi_stock_item_id = 1").count() == 1
