from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from sales_o2c.fact_transaction import agingBucket, computeCustomerTransactionMeasures, computeTransactionMeasures, dueDate, transactionTypeCode


def test_due_date_by_region(spark):
    df = spark.createDataFrame([("NA", date(2016, 1, 15)), ("EU", date(2016, 1, 15)), ("APAC", date(2016, 1, 15))], "region_code string, transaction_date date")
    out = {r.region_code: r.d for r in df.select("region_code", dueDate(F.col("region_code"), F.col("transaction_date")).alias("d")).collect()}
    assert out == {"NA": date(2016, 2, 14), "EU": date(2016, 3, 1), "APAC": date(2016, 3, 15)}


def test_aging_buckets_are_region_specific(spark):
    df = spark.createDataFrame([(r, d) for r in ["NA", "EU", "APAC"] for d in [0, 15, 45, 75, 100, 130]], "region_code string, days int")
    out = {(r.region_code, r.days): r.b for r in df.select("region_code", "days", agingBucket(F.col("days"), F.col("region_code")).alias("b")).collect()}
    assert out[("NA", 0)] == "CURRENT" and out[("NA", 15)] == "1-30" and out[("NA", 45)] == "31-60" and out[("NA", 75)] == "61-90" and out[("NA", 100)] == "90+"
    assert out[("EU", 45)] == "31-60" and out[("EU", 75)] == "60+"
    assert out[("APAC", 45)] == "1-60" and out[("APAC", 100)] == "61-120" and out[("APAC", 130)] == "120+"


def test_signed_amount_and_past_due(spark):
    df = spark.createDataFrame(
        [(1, "INV", date(2016, 1, 1), Decimal("100.00"), Decimal("100.00"), "NA"), (2, "PAY", date(2016, 3, 1), Decimal("40.00"), Decimal("0.00"), "NA")],
        "id int, transaction_type_code string, due_date date, transaction_amount decimal(18,2), outstanding_balance decimal(18,2), region_code string",
    )
    rows = {r.id: r for r in computeCustomerTransactionMeasures(df, asOf="2016-02-01").collect()}
    assert rows[1].is_debit_transaction and rows[1].signed_amount == Decimal("100.00") and rows[1].is_past_due and rows[1].aging_bucket_code == "31-60"
    assert not rows[2].is_debit_transaction and rows[2].signed_amount == Decimal("-40.00") and not rows[2].is_past_due and rows[2].aging_bucket_code == "CURRENT"


def test_balance_check_and_ledger_side(spark):
    df = spark.createDataFrame(
        [(1, Decimal("100.00"), Decimal("15.00"), Decimal("115.00"), "CUSTOMER"), (2, Decimal("100.00"), Decimal("15.00"), Decimal("110.00"), "SUPPLIER")],
        "id int, amount_excluding_tax decimal(18,2), tax_amount decimal(18,2), transaction_amount decimal(18,2), party_type_code string",
    )
    rows = {r.id: r for r in computeTransactionMeasures(df).collect()}
    assert rows[1].is_balanced and rows[1].ledger_side_code == "AR"
    assert not rows[2].is_balanced and rows[2].balance_check_variance == Decimal("5.00") and rows[2].ledger_side_code == "AP"


def test_transaction_type_code_mapping(spark):
    df = spark.createDataFrame([("Customer Invoice",), ("Customer Payment Received",), ("Something Else",)], "n string")
    assert [r.c for r in df.select(transactionTypeCode(F.col("n")).alias("c")).collect()] == ["INV", "PAY", "OTH"]
