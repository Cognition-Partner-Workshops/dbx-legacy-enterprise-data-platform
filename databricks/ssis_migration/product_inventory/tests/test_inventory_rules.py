from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from product_inventory import rules
from product_inventory.silver import buildInventoryPosition


def test_regional_safety_factor_and_replenishment_math(spark):
    df = spark.createDataFrame(
        [("APAC", 10.0, 7, 20, 0, 5, 12), ("EU", 10.0, 7, 500, 0, 0, 1), ("NA", 0.0, 7, 5, 0, 0, 6)],
        "region string, demand double, lead int, on_hand int, on_order int, allocated int, outer int",
    )
    out = df.select(
        "region",
        rules.regionalSafetyFactor(F.col("region")).alias("sf"),
        rules.reorderPoint(F.col("demand"), F.col("lead"), rules.regionalSafetyFactor(F.col("region"))).alias("rop"),
        rules.projectedAvailable(F.col("on_hand"), F.col("on_order"), F.col("allocated")).alias("proj"),
        rules.daysOfCover(F.col("on_hand"), F.col("allocated"), F.col("demand")).alias("cover"),
        rules.suggestedQuantity(
            rules.rawSuggestedQuantity(F.col("demand"), 21, rules.projectedAvailable(F.col("on_hand"), F.col("on_order"), F.col("allocated"))),
            F.col("outer"),
        ).alias("suggested"),
        rules.isStockoutRisk(rules.projectedAvailable(F.col("on_hand"), F.col("on_order"), F.col("allocated")), F.col("demand"), F.col("lead")).alias("risk"),
    )
    rows = {r["region"]: r for r in out.collect()}
    assert rows["APAC"]["sf"] == 1.5 and rows["EU"]["sf"] == 1.1 and rows["NA"]["sf"] == 1.25
    assert rows["APAC"]["rop"] == 105  # 10 * 7 * 1.5
    assert rows["APAC"]["proj"] == 15
    assert rows["APAC"]["cover"] == Decimal("1.50")
    assert rows["APAC"]["suggested"] == 204  # ceil((210 - 15) / 12) * 12
    assert rows["APAC"]["risk"] is True
    assert rows["EU"]["suggested"] == 0  # projected already covers 21 days
    assert rows["EU"]["risk"] is False
    assert rows["NA"]["cover"] == Decimal("999.00")  # zero-demand guard


def test_transfer_valuation_and_variance_classes(spark):
    df = spark.createDataFrame(
        [("EU", "APAC", None, 10.0), ("EU", "APAC", 12.5, 10.0), ("EU", "EU", 12.5, 10.0)],
        "from_region string, to_region string, transfer_price double, unit_cost double",
    )
    values = [r[0] for r in df.select(rules.transferMovementUnitValue(F.col("from_region"), F.col("to_region"), F.col("transfer_price"), F.col("unit_cost"))).collect()]
    assert values == [Decimal("10.80"), Decimal("12.50"), Decimal("10.00")]

    asOf = datetime(2026, 9, 30, 12, 0, 0)
    df = spark.createDataFrame(
        [("matched", 5, 5, None), ("negative", 5, -1, None), ("timing", 5, 4, datetime(2026, 9, 30, 11, 50)), ("variance", 5, 4, datetime(2026, 9, 29, 11, 50))],
        "label string, dw int, ops int, last_move timestamp",
    )
    out = {r["label"]: r["cls"] for r in df.select("label", rules.onHandVarianceClass(F.col("dw"), F.col("ops"), F.col("last_move"), F.lit(asOf), 30).alias("cls")).collect()}
    assert out == {"matched": "MATCHED", "negative": "NEGATIVE", "timing": "TIMING", "variance": "VARIANCE"}

    df = spark.createDataFrame([(0, 0.0), (2, 10.0), (3, 10.0), (2, 60.0)], "vq int, vv double")
    statuses = [r[0] for r in df.select(rules.cycleCountStatusCode(F.col("vq"), F.col("vv"), 2, 50.0)).collect()]
    assert statuses == ["AUTO", "AUTO", "HOLD", "HOLD"]


def test_inventory_position_window_running_balance_and_classification(spark):
    movements = spark.createDataFrame(
        [
            (1, "SYD01", date(2026, 1, 1), Decimal("100.000"), datetime(2026, 1, 1, 9)),
            (1, "SYD01", date(2026, 3, 1), Decimal("-40.000"), datetime(2026, 3, 1, 9)),
            (1, "SYD01", date(2026, 3, 2), Decimal("-60.000"), datetime(2026, 3, 2, 9)),
            (2, "SYD01", date(2026, 3, 2), Decimal("-5.000"), datetime(2026, 3, 2, 9)),
            (3, "SYD01", date(2026, 3, 2), Decimal("5000000.000"), datetime(2026, 3, 2, 9)),
        ],
        "stock_item_id int, warehouse_site_code string, movement_date date, signed_quantity decimal(18,3), movement_timestamp timestamp",
    )
    items = spark.createDataFrame(
        [(1, "Widget", "AMBIENT", Decimal("2.00"), "A-1"), (3, "Big", "AMBIENT", Decimal("1.00"), "B-1")],
        "stock_item_id int, stock_item_name string, handling_class string, standard_unit_cost decimal(18,2), bin_location string",
    )
    out = buildInventoryPosition(movements, items, date(2026, 2, 1), date(2026, 3, 31))
    rows = {(r["stock_item_id"], r["position_date"]): r for r in out.collect()}
    # the January opening movement is outside the window but still feeds the running balance
    assert (1, date(2026, 1, 1)) not in rows
    assert rows[(1, date(2026, 3, 1))]["quantity_on_hand"] == Decimal("60.000")
    assert rows[(1, date(2026, 3, 2))]["quantity_on_hand"] == Decimal("0.000")
    assert rows[(1, date(2026, 3, 2))]["stock_position_code"] == "NEGATIVE"  # net movement that day is negative
    assert rows[(1, date(2026, 3, 2))]["stock_value"] == Decimal("0.00")
    assert rows[(2, date(2026, 3, 2))]["is_lookup_failure"] is True  # stock item missing from staging
    assert rows[(3, date(2026, 3, 2))]["is_plausible"] is False  # outside +-1,000,000
    assert rows[(1, date(2026, 3, 1))]["high_churn_flag"] == "N"
