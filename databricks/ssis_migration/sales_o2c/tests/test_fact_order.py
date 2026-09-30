from decimal import Decimal

from sales_o2c.fact_order import computeOrderMeasures, splitHeld

SCHEMA = "order_line_id int, ordered_quantity int, picked_quantity int, unit_price_amount decimal(18,2), tax_rate_percent decimal(18,3), line_discount_percent decimal(9,4), backorder_order_id int, order_status_code string, line_status_code string"


def test_order_measures_backorder_and_fill_rate(spark):
    df = spark.createDataFrame(
        [(1, 10, 4, Decimal("2.50"), Decimal("15.000"), Decimal("10"), 555, None, None), (2, 4, 4, Decimal("2.50"), Decimal("15.000"), Decimal("0"), None, None, None), (3, 0, 0, Decimal("1.00"), Decimal("0"), Decimal("0"), None, "CANCELLED", None)],
        SCHEMA,
    )
    rows = {r.order_line_id: r for r in computeOrderMeasures(df).collect()}
    assert rows[1].ordered_gross_amount == Decimal("25.00") and rows[1].discount_amount == Decimal("2.50") and rows[1].ordered_net_amount == Decimal("22.50")
    assert rows[1].quantity_outstanding == 6 and rows[1].is_backordered and rows[1].fill_rate_percent == Decimal("40.0000")
    assert rows[1].total_excluding_tax == Decimal("25.00") and rows[1].tax_amount == Decimal("3.75") and rows[1].total_including_tax == Decimal("28.75")
    assert not rows[2].is_backordered and rows[2].fill_rate_percent == Decimal("100.0000")
    assert rows[3].fill_rate_percent == Decimal("0.0000") and rows[3].is_cancelled


def test_hold_and_retry_split(spark):
    df = spark.createDataFrame(
        [(1, 5, 7, 0), (2, 0, 7, 0), (3, 0, 7, 3), (4, 5, 0, 1)],
        "order_line_id int, customer_key int, stock_item_key int, hold_retry_count int",
    )
    ready, held = splitHeld(df, holdRetryLimit=3)
    assert sorted(r.order_line_id for r in ready.collect()) == [1, 3]  # 3 exhausted its retries -> loads against unknown member
    assert sorted(r.order_line_id for r in held.collect()) == [2, 4]
