from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from sales_o2c.dq import screenOrderLines, screenSaleLines
from sales_o2c.staging import regionCodeOf, splitOrderLines, splitSaleLines, taxRegimeOf, transformOrderLine


def _rawLine(spark, rows):
    return spark.createDataFrame(
        rows,
        "order_line_id int, order_id int, stock_item_id int, description string, package_type_name string, quantity int, unit_price decimal(18,2), "
        "tax_rate decimal(18,3), line_discount_amount decimal(18,2), line_discount_percent decimal(9,4), promotion_code string, picked_quantity int, "
        "picking_completed_when timestamp, line_status_code string, last_edited_when timestamp",
    )


def test_order_line_split_rejects_orphans_and_bad_numerics(spark):
    ts = datetime(2016, 1, 1)
    raw = _rawLine(spark, [
        (1, 10, 5, "ok", "Each", 3, Decimal("10.00"), Decimal("15.000"), Decimal("0"), Decimal("0"), None, 3, ts, "OPEN", ts),
        (2, 10, 5, "neg", "Each", -1, Decimal("10.00"), Decimal("15.000"), Decimal("0"), Decimal("0"), None, 0, None, "OPEN", ts),
        (3, 10, 5, "noprice", "Each", 1, None, Decimal("15.000"), Decimal("0"), Decimal("0"), None, 0, None, "OPEN", ts),
        (4, 99, 5, "orphan", "Each", 1, Decimal("10.00"), Decimal("15.000"), Decimal("0"), Decimal("0"), None, 0, None, "OPEN", ts),
    ])
    orders = spark.createDataFrame([("WWI_OLTP|10",)], "order_business_key string")
    valid, rejected = splitOrderLines(transformOrderLine(raw), orders)
    assert [r.order_line_id for r in valid.collect()] == [1]
    reasons = {r.order_line_id: r.reject_reason_code for r in rejected.collect()}
    assert reasons == {2: "NEG_QTY", 3: "BAD_NUMERIC", 4: "ORPHAN_LINE"}
    line = valid.first()
    assert line.extended_amount == Decimal("30.00") and line.line_tax_amount == Decimal("4.50") and line.picked_flag == "Y"


def test_region_and_regime_defaults(spark):
    df = spark.createDataFrame([(None,), (" eu ",), ("apac",)], "bill_to_region_code string")
    out = df.select(regionCodeOf(F.col("bill_to_region_code")).alias("r")).withColumn("t", taxRegimeOf(F.col("r"))).collect()
    assert [(r.r, r.t) for r in out] == [("NA", "SUT"), ("EU", "VAT"), ("APAC", "GST")]


def test_sale_line_split_on_tax_variance_and_zero_quantity(spark):
    df = spark.createDataFrame(
        [(1, 2, Decimal("0.01")), (2, 0, Decimal("0.00")), (3, 1, Decimal("0.03"))],
        "invoice_line_id int, quantity int, tax_variance_amount decimal(18,2)",
    )
    valid, rejected = splitSaleLines(df)
    assert [r.invoice_line_id for r in valid.collect()] == [1]
    assert {r.invoice_line_id: r.reject_reason_code for r in rejected.collect()} == {2: "ZERO_QTY", 3: "TAX_VARIANCE"}


def test_dq_order_line_screen_rules(spark):
    df = spark.createDataFrame(
        [(1, 5, Decimal("10.00"), Decimal("50.00")), (2, 0, Decimal("10.00"), Decimal("0.00")), (3, 5, Decimal("300000.00"), Decimal("1500000.00")), (4, 20000, Decimal("1.00"), Decimal("20000.00"))],
        "order_line_id int, ordered_quantity int, unit_price_amount decimal(18,2), extended_amount decimal(18,2)",
    )
    out = {r.order_line_id: r for r in screenOrderLines(df).collect()}
    assert out[1].dq_pass and out[1].reject_reason_code is None
    assert out[2].reject_reason_code == "DQ_OL_QTY_RANGE"
    assert out[3].reject_reason_code == "DQ_OL_PRICE_OUTLIER"
    assert out[4].reject_reason_code == "DQ_OL_QTY_RANGE"


def test_dq_invoice_line_screen_rules(spark):
    df = spark.createDataFrame(
        [
            (1, "NA", "SUT", Decimal("0.00"), Decimal("10.50"), 2),
            (2, "EU", "SUT", Decimal("0.00"), Decimal("10.50"), 2),
            (3, "NA", "SUT", Decimal("0.05"), Decimal("10.50"), 2),
            (4, "APAC", "GST", Decimal("0.00"), Decimal("10.50"), 0),
        ],
        "invoice_line_id int, region_code string, tax_regime_code string, tax_variance_amount decimal(18,2), net_amount decimal(18,2), minor_unit_digits int",
    )
    out = {r.invoice_line_id: r for r in screenSaleLines(df).collect()}
    assert out[1].dq_pass
    assert out[2].reject_reason_code == "DQ_IL_REGIME"
    assert out[3].reject_reason_code == "DQ_IL_TAX_VARIANCE"
    assert out[4].reject_reason_code == "DQ_IL_MINOR_UNIT"
