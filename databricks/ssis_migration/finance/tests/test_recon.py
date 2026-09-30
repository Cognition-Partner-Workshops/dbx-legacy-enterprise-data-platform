"""Recon checksum must be order-independent (rows and columns) and case-insensitive on column names."""

from decimal import Decimal

from finance.recon import EXCLUDED_COLS, checksum, sharedBusinessColumns


def test_checksum_ignores_row_and_column_order_and_case(spark):
    a = spark.createDataFrame(
        [(1, "x", Decimal("10.5")), (2, "y", None)], "id int, code string, amt decimal(18,5)"
    )
    b = spark.createDataFrame(
        [(None, "Y ", 2), (Decimal("10.5"), "x", 1)], "AMT decimal(15,5), CODE string, ID int"
    )
    cols = sharedBusinessColumns(a, b)
    assert cols == ["amt", "code", "id"]
    assert checksum(a, cols) == checksum(b, cols)


def test_checksum_detects_value_change(spark):
    a = spark.createDataFrame([(1, Decimal("10.5"))], "id int, amt decimal(18,5)")
    b = spark.createDataFrame([(1, Decimal("10.6"))], "id int, amt decimal(18,5)")
    assert checksum(a, ["amt", "id"]) != checksum(b, ["amt", "id"])


def test_shared_columns_exclude_technical_columns(spark):
    a = spark.createDataFrame([], "id int, batch_id bigint, scd_hash string, _rn int")
    assert sharedBusinessColumns(a, a) == ["id"]
    assert "batch_id" in EXCLUDED_COLS
