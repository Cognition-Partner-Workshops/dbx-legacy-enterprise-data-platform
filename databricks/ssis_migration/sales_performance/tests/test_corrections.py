from datetime import date, datetime

from conftest import d
from sales_performance.corrections import applySaleCorrections, classifyQueue, restateInPlace

FACT_SCHEMA = "sale_key bigint, invoice_number bigint, invoice_line_number int, customer_key bigint, stock_item_key bigint, salesperson_key bigint, city_key bigint, quantity decimal(18,3), extended_price decimal(19,4), tax_amount decimal(19,4), total_including_tax decimal(19,4), profit decimal(19,4), cost_amount decimal(19,4), net_amount decimal(19,4), is_reversal boolean, reverses_sale_key bigint, is_correction boolean, correction_lineage_key bigint"
QUEUE_SCHEMA = "queue_row_id bigint, fact_object_name string, fact_business_key string, dimension_name string, corrected_surrogate_key bigint, corrected_amount decimal(19,4), correction_type_code string, rekey_reason_code string, rekey_priority int, applied_flag boolean, created_at_utc timestamp"


def _fact(spark):
    rows = [
        (1, 1, 1, 10, 20, 30, 40, d(2), d(100), d(10), d(110), d(40), d(60), d(100), False, None, False, None),
        (2, 2, 1, 11, 21, 31, 41, d(1), d(50), d(5), d(55), d(20), d(30), d(50), False, None, False, None),
        (3, 3, 1, 12, 22, 32, 42, d(1), d(80), d(8), d(88), d(30), d(50), d(80), False, None, False, None),
    ]
    return spark.createDataFrame(rows, FACT_SCHEMA)


def _queue(spark):
    t = datetime(2016, 6, 1)
    rows = [
        (1, "Fact.Sale", "1|1", "Dimension.Customer", 99, None, "REKEY", "CUSTOMER_MERGE", 1, False, t),
        (2, "Fact.Sale", "2|1", None, None, d(120), "INCREASE", "PRICE_ADJUSTMENT", 2, False, t),
        (3, "Fact.Sale", "999|1", "Dimension.Customer", 99, None, "REKEY", "X", 2, False, t),
        (4, "Fact.Order", "7|2", "Dimension.Customer", 55, None, "REKEY", "CUSTOMER_MERGE", 2, False, t),
        (5, "Fact.Movement", "1|1", "Dimension.Customer", 55, None, "REKEY", "X", 3, False, t),
        (6, "Fact.Sale", "3|1", "Dimension.Customer", 99, None, "REKEY", "ALREADY", 1, True, t),
    ]
    return spark.createDataFrame(rows, QUEUE_SCHEMA)


def test_sale_corrections_reverse_and_replace_preserving_original(spark):
    supported, rejected = classifyQueue(_queue(spark))
    assert rejected.count() == 1 and rejected.first()["reject_reason_code"] == "UNSUPPORTED_TARGET"
    assert supported.filter("applied_flag").count() == 0
    newRows, reversedKeys, audit = applySaleCorrections(_fact(spark), supported.filter("fact_object_name = 'Fact.Sale'"), 77)
    rows = sorted(newRows.collect(), key=lambda r: r["sale_key"])
    assert len(rows) == 4 and {r["sale_key"] for r in rows} == {4, 5, 6, 7}
    byOrig = {}
    for r in rows:
        byOrig.setdefault(r["reverses_sale_key"], []).append(r)
    rev1 = [r for r in byOrig[1] if r["is_reversal"]][0]
    rep1 = [r for r in byOrig[1] if not r["is_reversal"]][0]
    assert str(rev1["extended_price"]) == "-100.0000" and str(rev1["quantity"]) == "-2.000" and str(rev1["profit"]) == "-40.0000"
    assert rep1["customer_key"] == 99 and str(rep1["extended_price"]) == "100.0000" and rep1["is_correction"]
    rep2 = [r for r in byOrig[2] if not r["is_reversal"]][0]
    assert str(rep2["extended_price"]) == "120.0000" and str(rep2["total_including_tax"]) == "125.0000" and rep2["customer_key"] == 11
    assert {r["sale_key"] for r in reversedKeys.collect()} == {1, 2}
    assert audit.count() == 2 and all(r["correction_style_code"] == "REVERSAL" for r in audit.collect())
    assert all(r["correction_lineage_key"] == 77 for r in rows)


def test_in_place_restatement_uses_natural_key(spark):
    orders = spark.createDataFrame(
        [(7, 2, 5, d(10), False), (7, 1, 5, d(20), False)],
        "order_number bigint, order_line_number int, customer_key bigint, extended_price decimal(19,4), is_restated boolean",
    )
    supported, _ = classifyQueue(_queue(spark))
    restated, audit = restateInPlace(
        orders, supported.filter("fact_object_name = 'Fact.Order'"), ["order_number", "order_line_number"], "extended_price", "Fact.Order"
    )
    rows = restated.collect()
    assert len(rows) == 1 and rows[0]["order_line_number"] == 2 and rows[0]["customer_key"] == 55 and rows[0]["is_restated"]
    assert audit.first()["fact_business_key"] == "7|2" and audit.first()["correction_style_code"] == "RESTATEMENT"
    assert date(2016, 6, 1)  # keep import meaningful for readers of the fixture timestamps
