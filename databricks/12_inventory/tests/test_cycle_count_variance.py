from datetime import datetime

from inv_common import transforms


def _cycle_counts(spark):
    cols = "CycleCountId int, StockItemId int, WarehouseSiteCode string, BinLocationCode string, CountedQuantity int, CountedAtUtc timestamp, LoadBatchId bigint"
    rows = [
        (1, 1, "LDN", "A1", 99, datetime(2024, 3, 15, 8, 0), 7),   # -1 unit, -2.5 value -> AUTO
        (2, 2, "LDN", "A2", 10, datetime(2024, 3, 15, 8, 5), 7),   # equal -> excluded
        (3, 3, "SYD", "B1", 43, datetime(2024, 3, 15, 8, 10), 7),  # +3 units > tolerance 2 -> HOLD
        (4, 2, "LDN", "A2", 12, datetime(2024, 3, 15, 8, 15), 7),  # +2 units, 20 value -> AUTO
        (5, 3, "SYD", "ZZ", 2, datetime(2024, 3, 15, 8, 20), 7),   # no position -> system 0, +2 units 8 value -> AUTO
        (6, 1, "LDN", "A1", 50, datetime(2024, 3, 15, 8, 0), 8),   # other batch -> excluded
    ]
    return spark.createDataFrame(rows, cols)


def test_variance_set_tolerance_classification(spark, currentPosition, stockItem):
    df = transforms.buildCycleCountVariance(_cycle_counts(spark), currentPosition, stockItem, batchId=7, toleranceUnits=2, toleranceValue=50)
    rows = {r["CycleCountId"]: r for r in df.collect()}
    assert set(rows) == {1, 3, 4, 5}
    assert rows[1]["VarianceQuantity"] == -1 and float(rows[1]["VarianceValue"]) == -2.5 and rows[1]["CountStatusCode"] == "AUTO"
    assert rows[3]["VarianceQuantity"] == 3 and rows[3]["CountStatusCode"] == "HOLD"
    assert rows[4]["CountStatusCode"] == "AUTO" and float(rows[4]["VarianceValue"]) == 20.0
    assert rows[5]["SystemQuantity"] == 0 and rows[5]["CountStatusCode"] == "AUTO"
    assert transforms.measureVariances(df) == (3, 1)


def test_value_tolerance_holds_even_when_units_within(spark, currentPosition, stockItem):
    df = transforms.buildCycleCountVariance(_cycle_counts(spark), currentPosition, stockItem, batchId=7, toleranceUnits=2, toleranceValue=10)
    rows = {r["CycleCountId"]: r for r in df.collect()}
    assert rows[4]["CountStatusCode"] == "HOLD", "20.00 value variance above 10 tolerance"
    assert rows[1]["CountStatusCode"] == "AUTO"


def test_adjustment_movements_and_recount_queue(spark, currentPosition, stockItem, dimStockItem, dimWarehouseSite):
    variance = transforms.buildCycleCountVariance(_cycle_counts(spark), currentPosition, stockItem, 7, 2, 50)
    movements = transforms.buildAdjustmentMovements(variance, dimStockItem, dimWarehouseSite, batchId=7)
    rows = {r["StockTakeReference"]: r for r in movements.collect()}
    assert set(rows) == {"1", "4", "5"}, "only AUTO variances post"
    assert rows["1"]["MovementDirection"] == "-" and float(rows["1"]["Quantity"]) == -1
    assert rows["1"]["MovementReasonCode"] == "CYCLECOUNT" and rows["1"]["StockItemKey"] == 101
    assert rows["5"]["WarehouseSiteKey"] == 2 and rows["5"]["RegionCode"] == "APAC"
    assert len({r["NaturalKeyHash"] for r in rows.values()}) == 3
    held = transforms.heldRecounts(variance).collect()
    assert [r["BusinessKey"] for r in held] == ["3"]
    assert "supervisor recount" in held[0]["RejectReason"]
