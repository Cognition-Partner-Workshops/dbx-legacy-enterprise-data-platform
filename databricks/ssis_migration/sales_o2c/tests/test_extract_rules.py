from datetime import date, datetime
from decimal import Decimal

from sales_o2c.extract import deriveCustomerTransaction, deriveInvoice, deriveOrder


def test_order_backorder_flag_and_pick_cycle_hours(spark):
    df = spark.createDataFrame(
        [(1, None, datetime(2016, 1, 1, 8), datetime(2016, 1, 1, 12)), (2, 99, datetime(2016, 1, 1, 8), None)],
        "order_id int, backorder_order_id int, order_date timestamp, picking_completed_when timestamp",
    )
    rows = {r.order_id: r for r in deriveOrder(df).collect()}
    assert rows[1].backorder_flag == "N" and rows[1].pick_cycle_hours == 4
    assert rows[2].backorder_flag == "Y" and rows[2].pick_cycle_hours == -1
    assert rows[1].delete_flag == "N"


def test_invoice_effective_tax_rate_and_signed_total(spark):
    df = spark.createDataFrame(
        [(1, Decimal("100.00"), Decimal("15.00"), Decimal("115.00"), False), (2, Decimal("0.00"), Decimal("0.00"), Decimal("50.00"), True)],
        "invoice_id int, total_excluding_tax decimal(18,2), total_tax_amount decimal(18,2), total_including_tax decimal(18,2), is_credit_note boolean",
    )
    rows = {r.invoice_id: r for r in deriveInvoice(df).collect()}
    assert rows[1].effective_tax_rate == Decimal("0.150000") and rows[1].signed_total_including_tax == Decimal("115.00")
    assert rows[2].effective_tax_rate == Decimal("0.000000") and rows[2].signed_total_including_tax == Decimal("-50.00")


def test_customer_transaction_days_outstanding(spark):
    df = spark.createDataFrame(
        [(1, date(2016, 1, 1), None), (2, date(2016, 1, 1), date(2016, 1, 11))],
        "customer_transaction_id int, transaction_date date, finalization_date date",
    )
    rows = {r.customer_transaction_id: r for r in deriveCustomerTransaction(df, asOf="2016-02-01").collect()}
    assert rows[1].settled_flag == "N" and rows[1].days_outstanding == 31
    assert rows[2].settled_flag == "Y" and rows[2].days_outstanding == 10
    assert rows[1].record_kind == "ARTRAN"
