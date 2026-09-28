from datetime import datetime
from decimal import Decimal

from inv_common import transforms
from conftest import NOW_UTC


def _movements(spark):
    cols = ("StockTransferId int, TransferReference string, StockItemId int, FromWarehouseSiteCode string, ToWarehouseSiteCode string, "
            "DespatchedAtUtc timestamp, ReceivedAtUtc timestamp, QuantityDespatched int, QuantityReceived int, TransferStatusCode string, "
            "CarrierCode string, MovementTypeCode string, LoadBatchId bigint")
    rows = [
        (1, "TR-1", 1, "LDN", "SYD", datetime(2024, 3, 1), None, 24, None, "INTRANSIT", "DHL", "TRANSFER", 5),        # cross region, priced, aged 14d
        (2, "TR-2", 2, "LDN", "MAN", datetime(2024, 3, 10), datetime(2024, 3, 12), 10, 10, "RECEIVED", "DHL", "TRANSFER", 5),  # same region
        (3, "TR-3", 3, "SYD", "NYC", datetime(2024, 3, 13), None, 6, 2, "PARTIAL", "UPS", "TRANSFER", 5),             # cross region, no price -> 1.08 uplift
        (4, "TR-4", 3, "SYD", "NYC", datetime(2024, 3, 13), datetime(2024, 3, 14), 0, 5, "RECEIVED", "UPS", "TRANSFER", 5),  # receipt-only leg
        (5, "TR-5", 1, "LDN", "SYD", datetime(2024, 3, 1), None, 1, None, "INTRANSIT", "DHL", "ISSUE", 5),            # not a transfer
        (6, "TR-6", 1, "LDN", "SYD", datetime(2024, 3, 1), None, 1, None, "INTRANSIT", "DHL", "TRANSFER", 6),         # other batch
    ]
    return spark.createDataFrame(rows, cols)


def _prices(spark):
    return spark.createDataFrame([(1, "EU", "APAC", Decimal("3.0000"))], "StockItemId int, FromRegionCode string, ToRegionCode string, TransferPrice decimal(18,4)")


def test_transfer_valuation_rules(spark, stockItem, warehouseSite):
    src = {r["StockTransferId"]: r for r in transforms.buildTransferSource(_movements(spark), stockItem, warehouseSite, _prices(spark), 5).collect()}
    assert set(src) == {1, 2, 3, 4}
    assert float(src[1]["MovementUnitValue"]) == 3.0, "cross-region transfer price wins"
    assert float(src[2]["MovementUnitValue"]) == 10.0, "same region -> standard cost"
    assert float(src[3]["MovementUnitValue"]) == 4.32, "cross-region without price -> cost * 1.08"
    assert src[1]["QuantityInTransit"] == 24 and src[3]["QuantityInTransit"] == 4 and src[2]["QuantityInTransit"] == 0


def test_derived_attributes_and_split(spark, stockItem, warehouseSite):
    df = transforms.deriveTransferAttributes(
        transforms.buildTransferSource(_movements(spark), stockItem, warehouseSite, _prices(spark), 5), NOW_UTC
    )
    rows = {r["StockTransferId"]: r for r in df.collect()}
    assert rows[1]["IsCrossRegion"] is True and rows[2]["IsCrossRegion"] is False
    assert float(rows[1]["IssueValue"]) == -72.0 and rows[1]["ReceiptValue"] is None
    assert float(rows[2]["ReceiptValue"]) == 100.0
    assert rows[1]["TransitDays"] == 14, "unreceived -> days to now"
    assert rows[2]["TransitDays"] == 2, "received -> despatch to receipt"
    despatched, receiptOnly = transforms.splitTransferLegs(df)
    assert {r["StockTransferId"] for r in despatched.collect()} == {1, 2, 3}
    assert {r["StockTransferId"] for r in receiptOnly.collect()} == {4}


def test_movement_legs_and_aged_escalation(spark, stockItem, warehouseSite, dimStockItem, dimWarehouseSite):
    despatched, _ = transforms.splitTransferLegs(transforms.deriveTransferAttributes(
        transforms.buildTransferSource(_movements(spark), stockItem, warehouseSite, _prices(spark), 5), NOW_UTC))
    legs = transforms.buildTransferMovements(despatched, dimStockItem, dimWarehouseSite, batchId=5).collect()
    byRef = {(r["TransferReference"], r["MovementReasonCode"]): r for r in legs}
    assert set(byRef) == {("TR-1", "XFER_ISSUE"), ("TR-2", "XFER_ISSUE"), ("TR-2", "XFER_RECEIPT"), ("TR-3", "XFER_ISSUE"), ("TR-3", "XFER_RECEIPT")}
    assert float(byRef[("TR-1", "XFER_ISSUE")]["Quantity"]) == -24 and byRef[("TR-1", "XFER_ISSUE")]["WarehouseSiteCode"] == "LDN"
    assert float(byRef[("TR-3", "XFER_RECEIPT")]["Quantity"]) == 2 and byRef[("TR-3", "XFER_RECEIPT")]["WarehouseSiteKey"] == 3
    assert byRef[("TR-1", "XFER_ISSUE")]["CostingMethodCode"] == "XFERP" and byRef[("TR-2", "XFER_ISSUE")]["CostingMethodCode"] == "STD"
    assert len({r["NaturalKeyHash"] for r in legs}) == 5
    aged = transforms.agedInTransit(despatched, alertDays=10).collect()
    assert [r["BusinessKey"] for r in aged] == ["TR-1"], "TR-3 is only 2 days in transit"
