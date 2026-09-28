from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sales_lakehouse.gold import fact_order, fact_payment
from tests.gold_facts_fixtures import (
    allocation,
    defaultAllocations,
    isolatedConfig,
    rejected,
    seedDimensions,
    seedOrders,
    seedPayments,
    seedSales,
)


@pytest.fixture(scope="module")
def opCfg(spark, cfg):
    iso = isolatedConfig(spark, cfg, "op")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    seedOrders(spark, iso)
    seedPayments(spark, iso)
    fact_order.run(spark, iso)
    fact_payment.run(spark, iso)
    return iso


def _order(spark, opCfg, key):
    return spark.table(opCfg.fqn("gold", "fact_order")).filter(F.col("order_line_business_key") == key).collect()[0]


def test_order_open_quantity_flags_and_zero_qty_reject(spark, opCfg):
    l2 = _order(spark, opCfg, "ORD1-2")
    assert l2["quantity_open"] == Decimal("3.0000") and l2["is_open"] is True and l2["is_fully_picked"] is False
    assert l2["is_backordered"] is True and l2["backorder_reason_code"] == "NOSTOCK"
    assert l2["promised_delivery_date_key"] == date(2024, 3, 25)
    assert l2["flag_hold"] is True and l2["flag_backorder"] is True and l2["flag_split"] is False
    assert l2["customer_key"] == 10  # SCD2 as of order date 2024-03-10
    l1 = _order(spark, opCfg, "ORD1-1")
    assert l1["quantity_open"] == Decimal("0.0000") and l1["is_fully_picked"] is True
    ord2 = _order(spark, opCfg, "ORD2-1")
    assert ord2["is_credit_hold"] is True and ord2["is_despatch_blocked"] is True and ord2["customer_key"] == 11
    assert rejected(spark, opCfg, "FACT_ORDER_ZERO_QTY").count() == 1
    assert (
        spark.table(opCfg.fqn("gold", "fact_order")).filter(F.col("order_line_business_key") == "ORD3-1").count() == 0
    )


def test_order_merge_idempotent(spark, opCfg):
    before = spark.table(opCfg.fqn("gold", "fact_order")).count()
    fact_order.run(spark, opCfg)
    assert spark.table(opCfg.fqn("gold", "fact_order")).count() == before == 3


def test_payment_allocation_grain_unallocated_bucket_and_over_allocation_reject(spark, opCfg):
    fact = spark.table(opCfg.fqn("gold", "fact_payment"))
    keys = {r[0] for r in fact.select("payment_allocation_business_key").collect()}
    assert keys == {"A1", "A2", "PAY1|UNALLOC", "PAY3|UNALLOC"}
    assert rejected(spark, opCfg, "FACT_PAYMENT_OVER_ALLOC").count() == 1
    a1 = fact.filter(F.col("payment_allocation_business_key") == "A1").collect()[0]
    assert a1["days_to_pay"] == 5 and a1["invoice_number"] == "INV1" and a1["customer_key"] == 10
    assert a1["is_under_allocated"] is True and a1["is_fully_applied"] is False and a1["restatement_version"] == 0
    bucket = fact.filter(F.col("payment_allocation_business_key") == "PAY1|UNALLOC").collect()[0]
    assert bucket["unallocated_amount"] == Decimal("20.0000") and bucket["payment_status_code"] == "UNALLOC"
    unmatched = fact.filter(F.col("payment_allocation_business_key") == "PAY3|UNALLOC").collect()[0]
    assert unmatched["is_unallocated_receipt"] is True and unmatched["unallocated_amount"] == Decimal("30.0000")


def test_payment_merge_idempotent_then_restates_in_place(spark, opCfg):
    fqn = opCfg.fqn("gold", "fact_payment")
    fact_payment.run(spark, opCfg)
    fact = spark.table(fqn)
    assert fact.count() == 4
    assert fact.agg(F.max("restatement_version")).collect()[0][0] == 0

    allocations = [a for a in defaultAllocations() if a["payment_allocation_business_key"] != "A1"]
    allocations.append(allocation("A1", "PAY1", "INV1", "70", date(2024, 3, 20)))
    seedPayments(spark, opCfg, allocations)
    fact_payment.run(spark, opCfg)
    fact = spark.table(fqn)
    assert fact.count() == 4
    a1 = fact.filter(F.col("payment_allocation_business_key") == "A1").collect()[0]
    assert a1["allocated_amount"] == Decimal("70.0000") and a1["restatement_version"] == 1
    assert a1["restated_datetime"] is not None
    bucket = fact.filter(F.col("payment_allocation_business_key") == "PAY1|UNALLOC").collect()[0]
    assert bucket["unallocated_amount"] == Decimal("10.0000") and bucket["restatement_version"] == 1
    untouched = fact.filter(F.col("payment_allocation_business_key") == "PAY3|UNALLOC").collect()[0]
    assert untouched["restatement_version"] == 0
