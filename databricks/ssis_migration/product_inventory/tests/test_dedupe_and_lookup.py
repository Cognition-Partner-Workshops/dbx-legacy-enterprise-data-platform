from datetime import datetime

from pyspark.sql import functions as F

from product_inventory.scd import dedupeLatest, lookupAsOf


def test_dedupe_latest_keeps_last_version_per_key(spark):
    df = spark.createDataFrame(
        [
            (1, datetime(2026, 1, 1), "old"),
            (1, datetime(2026, 1, 3), "new"),
            (1, datetime(2026, 1, 2), "mid"),
            (2, datetime(2026, 1, 1), "only"),
        ],
        "stock_item_id int, valid_from timestamp, payload string",
    )
    out = {r["stock_item_id"]: r["payload"] for r in dedupeLatest(df, ["stock_item_id"], [F.col("valid_from")]).collect()}
    assert out == {1: "new", 2: "only"}


def test_as_of_lookup_uses_version_window_then_current_then_unknown(spark):
    dim = spark.createDataFrame(
        [
            (0, 0, datetime(1900, 1, 1), datetime(9999, 12, 31, 23, 59, 59), True, True),
            (10, 1, datetime(2020, 1, 1), datetime(2021, 12, 31, 23, 59, 59), False, False),
            (11, 1, datetime(2022, 1, 1), datetime(9999, 12, 31, 23, 59, 59), True, False),
        ],
        "stock_item_key long, wwi_stock_item_id int, valid_from timestamp, valid_to timestamp, is_current_row boolean, is_reserved_member boolean",
    )
    facts = spark.createDataFrame(
        [
            ("in_v1", 1, datetime(2020, 6, 1)),
            ("in_v2", 1, datetime(2023, 6, 1)),
            ("before_any", 1, datetime(2019, 6, 1)),
            ("unknown_item", 99, datetime(2023, 6, 1)),
        ],
        "label string, stock_item_id int, movement_timestamp timestamp",
    )
    out = {
        r["label"]: (r["stock_item_key"], r["is_unknown_member"])
        for r in lookupAsOf(facts, dim, "stock_item_id", "wwi_stock_item_id", "movement_timestamp", "stock_item_key").collect()
    }
    assert out["in_v1"] == (10, False)
    assert out["in_v2"] == (11, False)
    assert out["before_any"] == (11, False)  # falls back to the current row like the SSIS cache
    assert out["unknown_item"] == (0, True)  # late-arriving member -> unknown key 0
