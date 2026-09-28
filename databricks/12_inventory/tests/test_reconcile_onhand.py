from datetime import datetime
from decimal import Decimal

from inv_common import transforms
from conftest import NOW_UTC


def _holding(spark):
    cols = "WWIStockItemID int, WarehouseSiteCode string, QuantityOnHand decimal(18,4)"
    return spark.createDataFrame([(1, "LDN", Decimal(100)), (2, "LDN", Decimal(12)), (3, "SYD", Decimal(35)), (7, "NYC", Decimal(4))], cols)


def _moves(spark):
    cols = "StockItemId int, WarehouseSiteCode string, MovementAtUtc timestamp"
    return spark.createDataFrame([(3, "SYD", datetime(2024, 3, 15, 11, 50)), (2, "LDN", datetime(2024, 3, 15, 9, 0))], cols)


def _position(spark):
    cols = "StockItemId int, WarehouseSiteCode string, QuantityOnHand int"
    return spark.createDataFrame([(1, "LDN", 100), (2, "LDN", 10), (3, "SYD", 40), (4, "SYD", -3)], cols)


def test_classification(spark):
    df = transforms.buildOnHandComparison(_position(spark), _holding(spark), _moves(spark), 9, 30, "ALL", NOW_UTC)
    rows = {r["SourceKey"]: r for r in df.collect()}
    assert rows["LDN|1"]["VarianceStatus"] == "Matched" and float(rows["LDN|1"]["VarianceAmount"]) == 0
    assert rows["SYD|3"]["VarianceStatus"] == "Timing", "movement 10 minutes ago inside 30-minute window"
    assert rows["LDN|2"]["VarianceStatus"] == "Variance", "movement 3 hours ago is outside the window"
    assert float(rows["LDN|2"]["VarianceAmount"]) == -2
    assert rows["SYD|4"]["VarianceStatus"] == "Negative on hand"
    assert rows["|"]["VarianceStatus"] == "Variance" and float(rows["|"]["TargetAmount"]) == 4, "DW-only row from full outer join"
    assert all(r["BatchId"] == 9 and r["ObjectName"] == "Fact.Stock Holding" for r in rows.values())
    assert transforms.classifyDifferences(df) == (2, 1)


def test_site_scope_and_escalation(spark):
    df = transforms.buildOnHandComparison(_position(spark), _holding(spark), _moves(spark), 9, 30, "LDN", NOW_UTC)
    keys = {r["SourceKey"] for r in df.collect()}
    assert keys == {"LDN|1", "LDN|2"}
    esc = transforms.genuineDifferences(df).collect()
    assert [r["BusinessKey"] for r in esc] == ["LDN|2"]
    assert "does not agree" in esc[0]["RejectReason"]


def test_timing_window_boundary(spark):
    df = transforms.buildOnHandComparison(_position(spark), _holding(spark), _moves(spark), 9, 5, "ALL", NOW_UTC)
    rows = {r["SourceKey"]: r for r in df.collect()}
    assert rows["SYD|3"]["VarianceStatus"] == "Variance", "10-minute-old movement is outside a 5-minute window"
