from datetime import date

from pyspark.sql import functions as F

from inv_common import transforms
from conftest import SNAPSHOT_DATE


def _rows(df, key="StockItemId"):
    return {r[key]: r for r in df.collect()}


def test_snapshot_source_measures_and_age_bands(position, stockItem):
    src = _rows(transforms.buildDailySnapshotSource(position, stockItem, SNAPSHOT_DATE))
    assert set(src) == {1, 2, 3, 4, 5}, "only the requested snapshot date survives"
    assert src[1]["QuantityAvailable"] == 80 and float(src[1]["OnHandValue"]) == 250.0
    assert src[1]["AgeBandCode"] == "FRESH" and src[1]["IsExpiredChillerStock"] == 0
    assert src[2]["AgeBandCode"] == "D365P" and src[2]["QuantityAvailable"] == -5
    assert src[3]["AgeBandCode"] == "D180"
    assert src[4]["AgeBandCode"] == "D090" and src[4]["IsExpiredChillerStock"] == 1
    assert src[5]["AgeBandCode"] == "NEVER"


def test_snapshot_lookup_rejects_unknown_items_and_uses_current_dim_rows(position, stockItem, dimStockItem):
    matched, rejected = transforms.buildDailySnapshot(position, stockItem, dimStockItem, SNAPSHOT_DATE)
    m = _rows(matched)
    assert set(m) == {1, 2, 3, 4}
    assert m[4]["StockItemKey"] == 104, "expired dimension version must not be looked up"
    assert [r["StockItemId"] for r in rejected.collect()] == [5]
    assert "StockItemKey" not in rejected.columns


def test_snapshot_derived_measures(position, stockItem, dimStockItem):
    matched, _ = transforms.buildDailySnapshot(position, stockItem, dimStockItem, SNAPSHOT_DATE)
    m = _rows(matched)
    assert m[2]["DaysCoverAtCurrentRate"] == 0, "non-positive available -> 0"
    assert m[1]["DaysCoverAtCurrentRate"] == 80
    assert float(m[2]["ObsolescenceProvisionAmount"]) == 100.0, "D365P -> full on-hand value"
    assert float(m[3]["ObsolescenceProvisionAmount"]) == 80.0, "D180 -> half on-hand value (40*4/2)"
    assert float(m[1]["ObsolescenceProvisionAmount"]) == 0.0


def test_fact_mapping_partition_column_and_site_key(position, stockItem, dimStockItem, dimWarehouseSite):
    matched, _ = transforms.buildDailySnapshot(position, stockItem, dimStockItem, SNAPSHOT_DATE)
    fact = transforms.toFactDailyInventorySnapshot(matched, batchId=42, dimWarehouseSite=dimWarehouseSite)
    rows = _rows(fact, key="WWIStockItemID")
    assert all(r["SnapshotDateKey"] == SNAPSHOT_DATE and r["BatchId"] == 42 for r in rows.values())
    assert rows[1]["WarehouseSiteKey"] == 1 and rows[1]["RegionCode"] == "EU"
    assert rows[3]["WarehouseSiteKey"] == 2 and rows[3]["RegionCode"] == "APAC"
    assert rows[3]["DaysSinceLastMovement"] == (SNAPSHOT_DATE - date(2023, 8, 1)).days
    assert rows[1]["StockAgeBucketCode"] == "FRESH"
    assert fact.schema["QuantityOnHand"].dataType.simpleString() == "decimal(18,4)"


def test_deterministic_hash_is_order_independent(spark):
    a = spark.createDataFrame([(1, "x"), (2, "y"), (3, None)], "k int, v string")
    b = spark.createDataFrame([(3, None), (1, "x"), (2, "y")], "k int, v string").select("v", "k")
    ha = transforms.deterministicHash(a).first()
    hb = transforms.deterministicHash(b).first()
    assert ha["RowCount"] == 3 and ha["RowHashSum"] == hb["RowHashSum"]
    hc = transforms.deterministicHash(a.withColumn("v", F.coalesce("v", F.lit("z")))).first()
    assert hc["RowHashSum"] != ha["RowHashSum"]
