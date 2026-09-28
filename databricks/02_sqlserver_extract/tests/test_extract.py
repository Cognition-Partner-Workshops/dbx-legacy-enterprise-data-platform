"""End-to-end package runs against local Delta with a fake JDBC reader and fake dbx_etl_common."""
from datetime import datetime, date
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from wwi_sqlserver_extract import extract, specs

NOW = datetime(2024, 3, 15, 10, 30, 0)


def sourceSchema(columns):
    return T.StructType([T.StructField(name, T._parse_datatype_string(dataType), True) for name, dataType in columns])


def sourceDf(spark, columns, rows):
    fields = [name for name, _ in columns]
    schema = sourceSchema(columns)
    data = []
    for row in rows:
        values = []
        for name, dataType in columns:
            value = row.get(name)
            if value is not None and dataType.startswith("decimal") and not isinstance(value, Decimal):
                value = Decimal(str(value))
            values.append(value)
        data.append(tuple(values))
    return spark.createDataFrame(data, schema).select(*fields)


class FakeReader:
    def __init__(self, spark, main, maxKey=0, deletes=None, version=0, failVersionQuery=False):
        self.spark = spark
        self.main = main
        self.maxKey = maxKey
        self.deletes = deletes
        self.version = version
        self.failVersionQuery = failVersionQuery
        self.sqls = []

    def __call__(self, sql):
        self.sqls.append(sql)
        if "CHANGETABLE" in sql:
            return self.deletes
        if "ChangeTrackingWatermark" in sql:
            if self.failVersionQuery:
                raise RuntimeError("Invalid column name 'ObjectName'")
            return self.spark.createDataFrame([(self.version,)], "LastSyncVersion bigint")
        if "CHANGE_TRACKING_MIN_VALID_VERSION" in sql:
            return self.spark.createDataFrame([(self.version,)], "MinValidVersion bigint")
        if "MAX(" in sql:
            return self.spark.createDataFrame([(self.maxKey,)], "MaxKey bigint")
        return self.main


def run(spark, dbutils, control, packageName, reader):
    from dbx_etl_common import naming, params

    return extract.runPackage(spark, dbutils, packageName, control=control, params=params, naming=naming,
                              sourceReader=reader, nowUtc=NOW)


def targetName(spec):
    return "wwi_test_bronze.%s" % spec.targetTable


def orderRows(ids, editedWhen=datetime(2024, 3, 14, 12)):
    return [dict(OrderID=i, CustomerID=1, OrderDate=datetime(2024, 3, 1, 9), PickingCompletedWhen=None, BackorderOrderID=None,
                 SalesChannelCode="DIRECT", LastEditedWhen=editedWhen) for i in ids]


def test_orders_numeric_key_bounded_with_delete_detection_and_merge(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_Orders")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Sales.Orders", "100", watermarkType="NumericKey")
    deletes = sourceDf(spark, spec.deleteDetection.columns, [dict(OrderID=7, ChangeOperation="D", ChangeVersion=55)])
    reader = FakeReader(spark, sourceDf(spark, spec.columns, orderRows([101, 102, 103])), maxKey=103, deletes=deletes, version=50)

    result = run(spark, dbutils, control, "EXT_SQL_Orders", reader)

    assert "o.OrderID > 100" in result.renderedSql and "o.OrderID <= 103" in result.renderedSql
    assert "CHANGETABLE(CHANGES Sales.Orders, 50)" in result.metrics["deleteSql"]
    assert (result.rowsRead, result.rowsInserted, result.rowsDeleted, result.rowsRejected) == (3, 4, 1, 0)
    assert (result.watermarkFrom, result.watermarkTo) == ("100", "103")
    landed = spark.table(targetName(spec))
    assert landed.count() == 4
    assert set(landed.columns) >= {"BatchId", "PackageExecutionId", "ExtractedAtUtc", "SourceSystemCode", "WatermarkFrom", "WatermarkTo",
                                   "BackorderFlag", "PickCycleHours", "DeleteFlag", "ChangeOperation", "ChangeVersion"}
    deleted = landed.filter("DeleteFlag = 'Y'").collect()
    assert len(deleted) == 1 and deleted[0]["OrderID"] == 7 and deleted[0]["ChangeVersion"] == 55
    audit = landed.filter("OrderID = 101").first()
    assert audit["BatchId"] == 42 and audit["SourceSystemCode"] == "WWI_OLTP" and audit["WatermarkFrom"] == "100" and audit["WatermarkTo"] == "103"
    assert audit["ExtractedAtUtc"] == NOW and audit["PackageExecutionId"] == 101

    setCalls = control.callsNamed("setWatermark")
    assert setCalls == [dict(sourceSystemCode="WWI_OLTP", objectName="Sales.Orders", watermarkTo="103", packageExecutionId=101, allowRewind=False)]
    start = control.callsNamed("logPackageStart")[0]
    assert start["packageName"] == "EXT_SQL_Orders" and start["projectName"] == "WWI_Extract_SqlServer" and start["batchId"] == 42
    end = control.callsNamed("logPackageEnd")[0]
    assert end["status"] == "Succeeded" and end["rowsRead"] == 3 and end["rowsDeleted"] == 1 and end["watermarkTo"] == "103"
    rowCount = control.callsNamed("logRowCount")[0]
    assert rowCount["objectName"] == "bronze.raw_sql_order" and rowCount["sourceRowCount"] == 4 and rowCount["targetRowCount"] == 4

    # Re-run for the same window: MERGE keeps the table idempotent and the watermark moves on.
    reader2 = FakeReader(spark, sourceDf(spark, spec.columns, orderRows([103, 104], editedWhen=datetime(2024, 3, 15))), maxKey=104,
                         deletes=deletes.limit(0), version=60)
    result2 = run(spark, dbutils, control, "EXT_SQL_Orders", reader2)
    assert "o.OrderID > 103" in result2.renderedSql
    assert result2.rowsInserted == 1 and result2.rowsUpdated == 1
    assert spark.table(targetName(spec)).count() == 5


def test_orders_reload_full_history_resets_lower_bound(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_Orders")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    dbutils.widgets.values["ReloadFullHistory"] = "True"
    control.registerWatermark("WWI_OLTP", "Sales.Orders", "100", watermarkType="NumericKey")
    reader = FakeReader(spark, sourceDf(spark, spec.columns, orderRows([1, 2])), maxKey=2,
                        deletes=sourceDf(spark, spec.deleteDetection.columns, []), version=0, failVersionQuery=True)
    result = run(spark, dbutils, control, "EXT_SQL_Orders", reader)
    assert "o.OrderID > 0" in result.renderedSql and result.watermarkFrom == "0"
    assert control.callsNamed("getWatermark")[0]["reloadFullHistory"] is True
    warnings = control.callsNamed("logError")
    assert len(warnings) == 1 and warnings[0]["errorSeverity"] == "Warning" and warnings[0]["sourceComponent"] == "Read Change Tracking Version"
    assert any("CHANGE_TRACKING_MIN_VALID_VERSION" in s for s in reader.sqls)


def test_stock_items_timestamp_watermark_with_lookback(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_StockItems")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Warehouse.StockItems", "2024-03-14T08:00:00", watermarkType="Timestamp", lookbackMinutes=240)
    rows = [dict(StockItemID=1, StockItemName="A", QuantityOnHand=2, ReorderLevel=5, IsChillerStock=True, LastEditedWhen=datetime(2024, 3, 14, 9))]
    reader = FakeReader(spark, sourceDf(spark, spec.columns, rows), deletes=sourceDf(spark, spec.deleteDetection.columns, []))
    result = run(spark, dbutils, control, "EXT_SQL_StockItems", reader)
    assert "N'2024-03-14T04:00:00.000'" in result.renderedSql
    assert result.watermarkTo == NOW.isoformat(timespec="milliseconds")
    assert "CHANGETABLE(CHANGES Warehouse.StockItems, 0)" in result.metrics["deleteSql"]
    row = spark.table(targetName(spec)).first()
    assert row["BelowReorderFlag"] == "Y" and row["HandlingClass"] == "CHILL" and row["DeleteFlag"] == "N"
    assert control.callsNamed("setWatermark")[0]["watermarkTo"] == result.watermarkTo


def test_stock_transfers_open_key_sets_watermark_from_extracted_max(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_StockTransfers")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Warehouse.StockTransferLines", "10", watermarkType="NumericKey")
    rows = [dict(StockTransferLineID=i, StockTransferID=1, StockItemID=1, TransferQuantity=5, ReceivedQuantity=0, TransferStatusCode="INTR",
                 DispatchedWhen=datetime(2024, 1, 1), ReceivedWhen=None, LastEditedWhen=datetime(2024, 3, 1)) for i in (11, 15, 12)]
    result = run(spark, dbutils, control, "EXT_SQL_StockTransfers", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    assert "tl.StockTransferLineID > 10" in result.renderedSql
    assert result.watermarkTo == "15" and control.callsNamed("setWatermark")[0]["watermarkTo"] == "15"
    assert spark.table(targetName(spec)).filter("StaleTransitFlag = 'Y'").count() == 3


def test_open_key_with_no_rows_does_not_move_watermark(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_CreditNotes")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Returns.CreditNoteLines", "77", watermarkType="NumericKey")
    result = run(spark, dbutils, control, "EXT_SQL_CreditNotes", reader=FakeReader(spark, sourceDf(spark, spec.columns, [])))
    assert result.rowsRead == 0 and result.watermarkTo is None
    assert control.callsNamed("setWatermark") == []
    assert control.callsNamed("logPackageEnd")[0]["status"] == "Succeeded"


def test_loyalty_ledger_binds_expiry_lookback(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_LoyaltyLedger")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Loyalty.LoyaltyPointsLedger", "500", watermarkType="NumericKey")
    rows = [dict(LoyaltyLedgerID="501", LoyaltyMemberID="1", ProgramCode="P", EntryTypeCode="EARN", PointsDelta="10", EntryWhen="2024-03-01T00:00:00",
                 LastEditedWhen="2024-03-01T00:00:00"),
            dict(LoyaltyLedgerID="499", LoyaltyMemberID="1", ProgramCode="P", EntryTypeCode="EXPIRE", PointsDelta="-10", EntryWhen="2024-01-01T00:00:00",
                 LastEditedWhen="2024-03-14T00:00:00")]
    result = run(spark, dbutils, control, "EXT_SQL_LoyaltyLedger", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    assert "l.LoyaltyLedgerID > CAST(500 AS bigint)" in result.renderedSql and "DATEADD(day, -1 * CAST(7 AS int)" in result.renderedSql
    assert result.watermarkTo == "501"


def test_web_sessions_date_window_replace_where_is_idempotent(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_WebSessions")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_WEB", "Ecommerce.WebSessions", "2024-03-14T00:00:00", watermarkType="DateWindow")
    rows = [dict(WebSessionID=i, SessionGuid="g%d" % i, RegionCode="EU", PageViewCount=1, HasCartActivity=False, HasCheckout=False,
                 AnalyticsConsentCode="DENIED", SessionStartedWhen=datetime(2024, 3, 14, 12, i), SessionEndedWhen=datetime(2024, 3, 14, 13)) for i in (1, 2)]
    rows.append(dict(WebSessionID=None, SessionGuid="orphan", RegionCode="NA", PageViewCount=3, HasCartActivity=True, HasCheckout=True,
                     AnalyticsConsentCode="GRANTED", SessionStartedWhen=datetime(2024, 3, 14, 15), SessionEndedWhen=datetime(2024, 3, 14, 16)))
    result = run(spark, dbutils, control, "EXT_SQL_WebSessions", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    assert "SessionStartedWhen >= CAST(N'2024-03-14' AS datetime2(7))" in result.renderedSql
    assert "SessionStartedWhen <  CAST(N'2024-03-15' AS datetime2(7))" in result.renderedSql
    assert result.rowsRead == 3 and result.rowsRejected == 0  # IgnoreFailure keeps the null-key row
    assert spark.table(targetName(spec)).count() == 3
    assert control.callsNamed("setWatermark")[-1]["watermarkTo"] == "2024-03-15"
    # restart of the same window (watermark not yet advanced) replaces instead of appending
    control.registerWatermark("WWI_WEB", "Ecommerce.WebSessions", "2024-03-14T00:00:00", watermarkType="DateWindow")
    run(spark, dbutils, control, "EXT_SQL_WebSessions", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows[:2])))
    assert spark.table(targetName(spec)).count() == 2
    # the window [today, today) is empty: nothing is read and the watermark is left alone
    setCalls = len(control.callsNamed("setWatermark"))
    result3 = run(spark, dbutils, control, "EXT_SQL_WebSessions", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    assert result3.rowsRead == 0 and result3.metrics["skippedEmptyWindow"] is True
    assert len(control.callsNamed("setWatermark")) == setCalls and spark.table(targetName(spec)).count() == 2


def test_full_reload_replaces_only_its_record_kind(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_Promotions")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    rows = [dict(PromotionID=1, PromotionCode="P1", RegionCode="EU", PromotionLineCount=4, RedemptionCount=1, IsActive=True)]
    run(spark, dbutils, control, "EXT_SQL_Promotions", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    # a row of another RecordKind sharing the table (legacy raw.SqlOrder layout) must survive the reload
    spark.sql("INSERT INTO %s (PromotionID, RecordKind, BatchId) VALUES (99, 'TERRITORY', 1)" % targetName(spec))
    rows2 = [dict(PromotionID=2, PromotionCode="P2", RegionCode="NA", PromotionLineCount=0, RedemptionCount=0, IsActive=False)]
    result = run(spark, dbutils, control, "EXT_SQL_Promotions", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows2)))
    landed = {r["RecordKind"]: r for r in spark.table(targetName(spec)).collect()}
    assert set(landed) == {"PROMOTION", "TERRITORY"}
    assert landed["PROMOTION"]["PromotionID"] == 2
    assert result.watermarkFrom is None and control.callsNamed("getWatermark") == [] and control.callsNamed("setWatermark") == []


def test_redirect_row_rejects_null_keys(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_OrderLines")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Sales.OrderLines", "0", watermarkType="NumericKey")
    rows = [dict(OrderLineID=1, OrderID=1, Quantity=2, PickedQuantity=1, ExtendedPrice=10, LineDiscountAmount=1, LastEditedWhen=NOW),
            dict(OrderLineID=None, OrderID=1, Quantity=2, PickedQuantity=2, ExtendedPrice=10, LineDiscountAmount=0, LastEditedWhen=NOW)]
    result = run(spark, dbutils, control, "EXT_SQL_OrderLines", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows), maxKey=1))
    assert (result.rowsRead, result.rowsInserted, result.rowsRejected) == (2, 1, 1)
    reject = control.callsNamed("logRejectedRecordSet")[0]
    assert reject["objectName"] == "raw.SqlOrderLine" and reject["rejectStage"] == "Extract" and reject["rejectedRowCount"] == 1
    assert control.callsNamed("logRowCount")[0]["rejectRowCount"] == 1
    landed = spark.table(targetName(spec)).first()
    assert landed["NetLineAmount"] == Decimal("9.00") and landed["ShortPickFlag"] == "Y"


def test_fail_component_raises_and_logs_failure(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_People")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    rows = [dict(PersonID=None, FullName="Ghost", IsSalesperson=False)]
    with pytest.raises(RuntimeError):
        run(spark, dbutils, control, "EXT_SQL_People", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows)))
    errors = control.callsNamed("logError")
    assert len(errors) == 1 and errors[0]["errorSeverity"] == "Error" and errors[0]["sourceName"] == "EXT_SQL_People"
    assert control.callsNamed("logPackageEnd")[0]["status"] == "Failed"
    assert not spark.catalog.tableExists(targetName(spec))


def test_invoice_split_counts_credit_notes(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_Invoices")
    spark.sql("DROP TABLE IF EXISTS %s" % targetName(spec))
    control.registerWatermark("WWI_OLTP", "Sales.Invoices", "0", watermarkType="NumericKey")
    rows = [dict(InvoiceID=1, IsCreditNote=False, TotalExcludingTax=100, TotalTaxAmount=15, TotalIncludingTax=115, LastEditedWhen=NOW),
            dict(InvoiceID=2, IsCreditNote=True, TotalExcludingTax=50, TotalTaxAmount=5, TotalIncludingTax=55, LastEditedWhen=NOW)]
    result = run(spark, dbutils, control, "EXT_SQL_Invoices", reader=FakeReader(spark, sourceDf(spark, spec.columns, rows), maxKey=2))
    assert result.metrics["CreditNotesCount"] == 1
    assert spark.table(targetName(spec)).count() == 2


def test_missing_source_column_fails_loudly(spark, dbutils, control):
    spec = specs.getPackage("EXT_SQL_Cities")
    df = sourceDf(spark, spec.columns, [dict(CityID=1, CityName="X", Continent="Europe")]).drop("Continent")
    with pytest.raises(ValueError, match="Continent"):
        run(spark, dbutils, control, "EXT_SQL_Cities", reader=FakeReader(spark, df))


def test_parse_numeric_watermark_tolerates_epoch_text():
    assert extract.parseNumericWatermark("1900-01-01T00:00:00") == 0
    assert extract.parseNumericWatermark(" 15 ") == 15
    assert extract.parseNumericWatermark(None) == 0
    with pytest.raises(ValueError):
        extract.validateTemporalLiteral("1 OR 1=1", "WatermarkFrom")
