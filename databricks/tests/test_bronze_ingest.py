"""Bronze ingestion tests: schema, metadata, idempotency, quarantine, watermark, load_log."""
from __future__ import annotations

import csv
import dataclasses
import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_lakehouse.bronze import ingest
from sales_lakehouse.bronze.registry import BY_SOURCE_OBJECT
from sales_lakehouse.common.config import PipelineConfig

ORDER_HOLDS = BY_SOURCE_OBJECT["Sales.OrderHolds"]  # BIT + DATETIME2 + TINYINT
CURRENCY_CODE = BY_SOURCE_OBJECT["WWI_REF.CURRENCY_CODE"]  # Y/N flags + NUMBER(18,8)
ORDER_LINES = BY_SOURCE_OBJECT["Sales.OrderLines"]  # numeric-key incremental
FX_RATE_DAILY = BY_SOURCE_OBJECT["WWI_REF.FX_RATE_DAILY"]  # date watermark incremental


def writeCsv(cfg: PipelineConfig, source, rows: list[dict], rawLines: list[str] | None = None) -> str:
    """Write a full-width CSV for ``source``; missing keys become empty strings (= NULL)."""
    path = cfg.sourcePath(source.system, source.schema, source.table)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(source.columnNames)
        for row in rows:
            writer.writerow([row.get(c, "") for c in source.columnNames])
        for line in rawLines or []:
            handle.write(line + "\n")
    return path


def withBatch(cfg: PipelineConfig, batchId: int) -> PipelineConfig:
    return dataclasses.replace(cfg, batchId=batchId)


def bronze(spark, cfg, source):
    return spark.table(cfg.fqn("bronze", source.bronzeTable))


def loadLog(spark, cfg, sourceObject: str):
    return spark.table(cfg.fqn("quality", ingest.LOAD_LOG_TABLE)).filter(F.col("source_object") == sourceObject)


# --------------------------------------------------------------------------- #
# SQL Server: BIT, DATETIME2, corrupt row, metadata, idempotency
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def sqlServerLoad(spark, cfg):
    cfg = withBatch(cfg, 101)
    rows = [
        {"OrderHoldID": 1, "OrderID": 10, "HoldTypeCode": "CREDIT", "HoldReasonCode": "LIMIT", "PlacedWhen": "2024-03-01T08:15:30",
         "PlacedByPersonID": 3, "PlacedBySystem": "OLTP", "EscalationLevel": 2, "IsBlockingDespatch": 1},
        {"OrderHoldID": 2, "OrderID": 11, "HoldTypeCode": "MANUAL", "HoldReasonCode": "", "HoldNarrative": 'say "hi", twice',
         "PlacedWhen": "2024-03-02 09:00:00", "PlacedByPersonID": 4, "PlacedBySystem": "CSR", "ReleasedWhen": "2024-03-03T10:00:00.1234567",
         "EscalationLevel": 0, "IsBlockingDespatch": 0},
    ]
    # OrderHoldID is BIGINT and PlacedWhen DATETIME2 -> both unparseable -> corrupt.
    corrupt = "not-a-number,12,CREDIT,LIMIT,,not-a-date,3,OLTP,,,,,1,1"
    path = writeCsv(cfg, ORDER_HOLDS, rows, rawLines=[corrupt])
    results = ingest.run(spark, cfg, tables=[ORDER_HOLDS.bronzeTable])
    return cfg, path, results


def test_sqlserver_schema_matches_ddl(spark, sqlServerLoad):
    cfg, _, _ = sqlServerLoad
    df = bronze(spark, cfg, ORDER_HOLDS)
    assert df.columns == list(ORDER_HOLDS.columnNames) + list(ingest.METADATA_COLUMNS)
    types = dict(df.dtypes)
    assert types["IsBlockingDespatch"] == "boolean"
    assert types["PlacedWhen"] == "timestamp"
    assert types["OrderHoldID"] == "bigint"
    assert types["EscalationLevel"] == "tinyint"
    assert types["_batch_id"] == "bigint" and types["_load_ts"] == "timestamp"


def test_sqlserver_values_and_metadata(spark, sqlServerLoad):
    cfg, path, results = sqlServerLoad
    df = bronze(spark, cfg, ORDER_HOLDS)
    rows = {r["OrderHoldID"]: r for r in df.collect()}
    assert set(rows) == {1, 2}
    assert rows[1]["IsBlockingDespatch"] is True and rows[2]["IsBlockingDespatch"] is False
    assert rows[2]["HoldReasonCode"] is None  # empty string -> NULL
    assert rows[2]["HoldNarrative"] == 'say "hi", twice'
    assert rows[1]["PlacedWhen"].isoformat() == "2024-03-01T08:15:30"
    assert rows[2]["ReleasedWhen"].microsecond == 123456  # DATETIME2(7) truncated to micros
    for row in rows.values():
        assert row["_source_system"] == "SQLSERVER_WWI_OLTP"
        assert row["_source_object"] == "Sales.OrderHolds"
        assert row["_source_file"] == path
        assert row["_batch_id"] == 101
        assert row["_load_ts"] is not None
    assert results[0].rowsRead == 3 and results[0].rowsLoaded == 2 and results[0].rowsRejected == 1


def test_sqlserver_corrupt_row_quarantined(spark, sqlServerLoad):
    cfg, _, _ = sqlServerLoad
    rejected = (
        spark.table(cfg.fqn("quality", "rejected_rows"))
        .filter((F.col("rule_code") == "BRONZE_PARSE") & (F.col("batch_id") == 101))
        .filter(F.col("source_table") == cfg.fqn("bronze", ORDER_HOLDS.bronzeTable))
    )
    assert rejected.count() == 1
    assert "not-a-number" in rejected.first()["row_json"]


def test_sqlserver_load_log(spark, sqlServerLoad):
    cfg, _, _ = sqlServerLoad
    row = loadLog(spark, cfg, "Sales.OrderHolds").filter(F.col("batch_id") == 101).first()
    assert row is not None
    assert (row["rows_read"], row["rows_loaded"], row["rows_rejected"], row["status"]) == (3, 2, 1, "SUCCESS")
    assert row["started_at_utc"] <= row["finished_at_utc"]


def test_same_batch_rerun_is_idempotent_and_new_batch_appends(spark, sqlServerLoad):
    cfg, _, _ = sqlServerLoad
    ingest.run(spark, cfg, tables=["Sales.OrderHolds"])
    df = bronze(spark, cfg, ORDER_HOLDS)
    assert df.filter(F.col("_batch_id") == 101).count() == 2
    ingest.run(spark, withBatch(cfg, 102), tables=["Sales.OrderHolds"])
    df = bronze(spark, cfg, ORDER_HOLDS)
    assert df.filter(F.col("_batch_id") == 101).count() == 2
    assert df.filter(F.col("_batch_id") == 102).count() == 2
    assert df.count() == 4


def test_bad_bit_value_is_quarantined_not_coerced(spark, cfg):
    cfg = withBatch(cfg, 103)
    writeCsv(cfg, ORDER_HOLDS, [
        {"OrderHoldID": 5, "OrderID": 1, "HoldTypeCode": "X", "HoldReasonCode": "Y", "PlacedWhen": "2024-01-01", "PlacedByPersonID": 1, "PlacedBySystem": "S", "EscalationLevel": 1, "IsBlockingDespatch": "maybe"},
        {"OrderHoldID": 6, "OrderID": 1, "HoldTypeCode": "X", "HoldReasonCode": "Y", "PlacedWhen": "2024-01-01", "PlacedByPersonID": 1, "PlacedBySystem": "S", "EscalationLevel": 1, "IsBlockingDespatch": "true"},
    ])
    result = ingest.run(spark, cfg, tables=["Sales.OrderHolds"])[0]
    assert (result.rowsRead, result.rowsLoaded, result.rowsRejected) == (2, 1, 1)
    loaded = bronze(spark, cfg, ORDER_HOLDS).filter(F.col("_batch_id") == 103).collect()
    assert loaded[0]["OrderHoldID"] == 6 and loaded[0]["IsBlockingDespatch"] is True


# --------------------------------------------------------------------------- #
# Oracle: Y/N stays string, NUMBER(p,s) -> decimal, DATE -> timestamp
# --------------------------------------------------------------------------- #
def test_oracle_flags_stay_strings_and_numbers_are_decimal(spark, cfg):
    cfg = withBatch(cfg, 201)
    writeCsv(cfg, CURRENCY_CODE, [
        {"CURR_CD": "USD", "CURR_NUM_CD": "840", "CURR_NAME": "US Dollar", "MINOR_UNIT_DIGITS": 2, "ROUNDING_RULE_CD": "HALFUP", "REGION_CD": "NA",
         "EURO_LEGACY_FLG": "N", "ACTIVE_FLG": "Y", "TRADING_ALLOWED_FLG": "Y", "DISPLAY_SEQ_NBR": 1, "SOURCE_SYS": "ORA_ERP", "CREATED_BY": "SYS", "CREATED_DT": "2020-01-01"},
        {"CURR_CD": "DEM", "CURR_NUM_CD": "276", "CURR_NAME": "Deutsche Mark", "MINOR_UNIT_DIGITS": 2, "ROUNDING_RULE_CD": "HALFUP", "REGION_CD": "EU",
         "EURO_LEGACY_FLG": "Y", "EURO_FIXED_RATE": "1.95583000", "EURO_CONVERSION_DT": "1999-01-01", "ACTIVE_FLG": "N", "TRADING_ALLOWED_FLG": "N",
         "DISPLAY_SEQ_NBR": 99, "RETIRED_DT": "2002-02-28 23:59:59", "SOURCE_SYS": "ORA_ERP", "CREATED_BY": "SYS", "CREATED_DT": "2020-01-01"},
    ])
    result = ingest.run(spark, cfg, tables=["WWI_REF.CURRENCY_CODE"])[0]
    assert (result.rowsRead, result.rowsLoaded, result.rowsRejected) == (2, 2, 0)
    df = bronze(spark, cfg, CURRENCY_CODE)
    assert df.schema["ACTIVE_FLG"].dataType == T.StringType()
    assert df.schema["EURO_FIXED_RATE"].dataType == T.DecimalType(18, 8)
    assert df.schema["MINOR_UNIT_DIGITS"].dataType == T.DecimalType(1, 0)
    assert df.schema["RETIRED_DT"].dataType == T.TimestampType()
    rows = {r["CURR_CD"]: r for r in df.collect()}
    assert rows["USD"]["ACTIVE_FLG"] == "Y" and rows["DEM"]["ACTIVE_FLG"] == "N"
    assert str(rows["DEM"]["EURO_FIXED_RATE"]) == "1.95583000"
    assert rows["USD"]["EURO_FIXED_RATE"] is None
    assert rows["DEM"]["RETIRED_DT"].isoformat() == "2002-02-28T23:59:59"
    assert rows["USD"]["_source_system"] == "ORACLE_WWIGERP"
    assert rows["USD"]["_source_object"] == "WWI_REF.CURRENCY_CODE"


# --------------------------------------------------------------------------- #
# Incremental: numeric key watermark (Sales.OrderLines) run twice
# --------------------------------------------------------------------------- #
def orderLine(lineId: int, orderId: int) -> dict:
    return {"OrderLineID": lineId, "OrderID": orderId, "StockItemID": 7, "Description": f"line {lineId}", "PackageTypeID": 1,
            "Quantity": 2, "UnitPrice": "10.50", "TaxRate": "15.000", "PickedQuantity": 0, "LastEditedBy": 1,
            "LastEditedWhen": "2024-04-01T00:00:00"}


def test_incremental_numeric_watermark_moves(spark, cfg):
    first = withBatch(cfg, 301)
    writeCsv(first, ORDER_LINES, [orderLine(1, 1), orderLine(2, 1), orderLine(3, 2)])
    result = ingest.run(spark, first, tables=["Sales.OrderLines"])[0]
    assert result.rowsLoaded == 3
    watermark = spark.table(cfg.fqn("bronze", ingest.WATERMARK_TABLE)).filter(F.col("source_object") == "Sales.OrderLines").first()
    assert watermark["watermark_column"] == "OrderLineID"
    assert watermark["last_watermark_value"] == "3"
    assert watermark["previous_watermark_value"] is None
    assert watermark["last_run_batch_id"] == 301 and watermark["last_row_count"] == 3

    # Second extract: the file now also holds lines 4 and 5 -> only those load.
    second = withBatch(cfg, 302)
    writeCsv(second, ORDER_LINES, [orderLine(1, 1), orderLine(2, 1), orderLine(3, 2), orderLine(4, 3), orderLine(5, 3)])
    result = ingest.run(spark, second, tables=["Sales.OrderLines"])[0]
    assert (result.rowsRead, result.rowsLoaded) == (5, 2)
    df = bronze(spark, cfg, ORDER_LINES)
    assert sorted(r["OrderLineID"] for r in df.filter(F.col("_batch_id") == 302).collect()) == [4, 5]
    assert df.count() == 5
    watermark = spark.table(cfg.fqn("bronze", ingest.WATERMARK_TABLE)).filter(F.col("source_object") == "Sales.OrderLines").first()
    assert watermark["last_watermark_value"] == "5" and watermark["previous_watermark_value"] == "3"

    # Re-running batch 302 replays the same window instead of emptying the batch.
    result = ingest.run(spark, second, tables=["Sales.OrderLines"])[0]
    assert result.rowsLoaded == 2
    df = bronze(spark, cfg, ORDER_LINES)
    assert df.filter(F.col("_batch_id") == 302).count() == 2 and df.count() == 5
    watermark = spark.table(cfg.fqn("bronze", ingest.WATERMARK_TABLE)).filter(F.col("source_object") == "Sales.OrderLines").first()
    assert watermark["last_watermark_value"] == "5" and watermark["previous_watermark_value"] == "3"

    # Nothing new in the file -> zero rows, watermark unchanged.
    third = withBatch(cfg, 303)
    result = ingest.run(spark, third, tables=["Sales.OrderLines"])[0]
    assert result.rowsLoaded == 0
    watermark = spark.table(cfg.fqn("bronze", ingest.WATERMARK_TABLE)).filter(F.col("source_object") == "Sales.OrderLines").first()
    assert watermark["last_watermark_value"] == "5" and watermark["last_run_batch_id"] == 303


def fxRate(rateDate: str, toCurrency: str) -> dict:
    return {"FROM_CURR_CD": "USD", "TO_CURR_CD": toCurrency, "RATE_DT": rateDate, "RATE_TYPE_CD": "SPOT", "RATE": "0.92000000",
            "RATE_SOURCE_CD": "ECB", "FEED_REGION_CD": "EU", "INTERPOLATED_FLG": "N", "LOADED_TS": f"{rateDate} 06:00:00",
            "SUPERSEDED_FLG": "N", "SOURCE_SYS": "ORA_ERP", "CREATED_BY": "FXLOAD", "CREATED_DT": rateDate}


def test_incremental_timestamp_watermark(spark, cfg):
    first = withBatch(cfg, 401)
    writeCsv(first, FX_RATE_DAILY, [fxRate("2024-05-01", "EUR"), fxRate("2024-05-02", "EUR")])
    assert ingest.run(spark, first, tables=["WWI_REF.FX_RATE_DAILY"])[0].rowsLoaded == 2
    second = withBatch(cfg, 402)
    writeCsv(second, FX_RATE_DAILY, [fxRate("2024-05-01", "EUR"), fxRate("2024-05-02", "EUR"), fxRate("2024-05-02", "GBP"), fxRate("2024-05-03", "EUR")])
    result = ingest.run(spark, second, tables=["WWI_REF.FX_RATE_DAILY"])[0]
    # Same-day GBP row is not "> watermark" - the legacy date-window extract has the same blind spot.
    assert result.rowsLoaded == 1
    loaded = bronze(spark, cfg, FX_RATE_DAILY).filter(F.col("_batch_id") == 402).collect()
    assert loaded[0]["RATE_DT"].isoformat() == "2024-05-03T00:00:00"
    watermark = spark.table(cfg.fqn("bronze", ingest.WATERMARK_TABLE)).filter(F.col("source_object") == "WWI_REF.FX_RATE_DAILY").first()
    assert watermark["last_watermark_value"].startswith("2024-05-03 00:00:00")


# --------------------------------------------------------------------------- #
# Missing source file and table selection
# --------------------------------------------------------------------------- #
def test_missing_source_is_logged_not_fatal(spark, cfg):
    cfg = withBatch(cfg, 501)
    results = ingest.run(spark, cfg, tables=["Sales.BuyingGroups", "sqlserver_sales_customer_categories"])
    assert [r.status for r in results] == ["MISSING_SOURCE", "MISSING_SOURCE"]
    rows = loadLog(spark, cfg, "Sales.BuyingGroups").filter(F.col("batch_id") == 501).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "MISSING_SOURCE" and rows[0]["rows_read"] == 0
    assert cfg.sourcePath("sqlserver", "Sales", "BuyingGroups") in rows[0]["message"]
    assert not spark.catalog.tableExists(cfg.fqn("bronze", "sqlserver_sales_buying_groups"))


def test_unknown_table_name_rejected(spark, cfg):
    with pytest.raises(KeyError):
        ingest.run(spark, cfg, tables=["Sales.NoSuchTable"])


def test_full_run_covers_every_registry_entry(spark, cfg):
    cfg = withBatch(cfg, 601)
    results = ingest.run(spark, cfg)
    assert len(results) == len(BY_SOURCE_OBJECT)
    logged = spark.table(cfg.fqn("quality", ingest.LOAD_LOG_TABLE)).filter(F.col("batch_id") == 601)
    assert logged.count() == len(BY_SOURCE_OBJECT)
    assert {r.status for r in results} <= {"SUCCESS", "MISSING_SOURCE"}
    assert {r.sourceObject for r in results if r.status == "SUCCESS"} >= {"Sales.OrderHolds", "WWI_REF.CURRENCY_CODE"}
