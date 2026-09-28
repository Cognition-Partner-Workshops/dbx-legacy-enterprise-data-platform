from datetime import date

import pytest

import sales_common as sc


def test_parse_helpers():
    assert sc.parseBool("True") and sc.parseBool("1") and not sc.parseBool("False")
    assert sc.parseBool("", default=True) is True
    assert sc.parseCsvList("de, at ,") == ["DE", "AT"]
    assert sc.monthPeriod(date(2024, 3, 5)) == "2024-03"
    assert sc.endOfMonth(date(2024, 2, 10)) == date(2024, 2, 29)
    assert sc.resolveBusinessDate("2024-03-05") == date(2024, 3, 5)


def test_restart_from_step_rule():
    assert not sc.shouldSkipForRestart("", "SLS_NA_Load_Commission")
    assert not sc.shouldSkipForRestart("Sales Mart", "SLS_NA_Load_Commission")
    assert not sc.shouldSkipForRestart("Facts", "SLS_NA_Load_Commission")
    assert sc.shouldSkipForRestart("Customer 360 Build", "SLS_NA_Load_Commission")
    assert sc.shouldSkipForRestart("Publish Reporting Layer", "SLS_Export_PartnerFeed")


def test_legacy_column_candidates():
    assert sc.legacyColumnCandidates("Customer Key") == ["CustomerKey", "Customer Key", "customer_key"]
    assert sc.legacyColumnCandidates("WWI Invoice ID") == ["WWIInvoiceID", "WWI Invoice ID", "wwi_invoice_id"]


def test_resolve_columns_first_candidate_wins_and_missing_raises(spark):
    df = spark.createDataFrame([("a", 1, 2)], "SaleLineBusinessKey string, BatchId int, Other int")
    out = sc.resolveColumns(df, {"SaleLineId": ["SaleLineId", "SaleLineBusinessKey"], "LoadBatchId": ["BatchId"],
                                 "LineTypeCode": ["LineTypeCode"]}, optional=("LineTypeCode",))
    assert out.columns == ["SaleLineId", "LoadBatchId", "LineTypeCode"]
    assert out.first()["SaleLineId"] == "a" and out.first()["LineTypeCode"] is None
    with pytest.raises(ValueError) as excinfo:
        sc.resolveColumns(df, {"CustomerId": ["CustomerId", "CustomerBusinessKey"]})
    assert "CustomerId" in str(excinfo.value)


def test_batch_filter_and_reload(spark):
    df = spark.createDataFrame([(6,), (7,), (7,)], "BatchId int")
    assert sc.batchFilter(df, "BatchId", 7, False).count() == 2
    assert sc.batchFilter(df, "BatchId", 7, True).count() == 3


def test_conform_to_schema_adds_missing_and_casts(spark):
    from pyspark.sql import types as T
    schema = T.StructType([T.StructField("A", T.StringType()), T.StructField("B", T.LongType()), T.StructField("C", T.DateType())])
    df = spark.createDataFrame([("x", "5")], "a string, B string")
    out = sc.conformToSchema(df, schema)
    assert out.columns == ["A", "B", "C"]
    row = out.first()
    assert row["A"] == "x" and row["B"] == 5 and row["C"] is None
