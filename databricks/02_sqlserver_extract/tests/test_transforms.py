from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import types as T

from wwi_sqlserver_extract import transforms

NOW = datetime(2024, 3, 15, 10, 30, 0)


def rowsBy(df, key):
    return {r[key]: r.asDict() for r in df.collect()}


def test_invoice_tax_rate_and_signed_total(spark):
    schema = T.StructType([
        T.StructField("InvoiceID", T.IntegerType()),
        T.StructField("IsCreditNote", T.BooleanType()),
        T.StructField("TotalExcludingTax", T.DecimalType(18, 2)),
        T.StructField("TotalTaxAmount", T.DecimalType(18, 2)),
        T.StructField("TotalIncludingTax", T.DecimalType(18, 2)),
    ])
    df = spark.createDataFrame([
        (1, False, Decimal("100.00"), Decimal("15.00"), Decimal("115.00")),
        (2, True, Decimal("200.00"), Decimal("30.00"), Decimal("230.00")),
        (3, False, Decimal("0.00"), Decimal("0.00"), Decimal("0.00")),
    ], schema)
    out = rowsBy(transforms.deriveInvoices(df, NOW), "InvoiceID")
    assert out[1]["EffectiveTaxRate"] == Decimal("0.1500")
    assert out[1]["SignedTotalIncludingTax"] == Decimal("115.00")
    assert out[2]["SignedTotalIncludingTax"] == Decimal("-230.00")
    assert out[3]["EffectiveTaxRate"] == Decimal("0.0000")


def test_invoice_line_margin(spark):
    schema = T.StructType([
        T.StructField("InvoiceLineID", T.IntegerType()),
        T.StructField("ExtendedPrice", T.DecimalType(18, 2)),
        T.StructField("LineProfit", T.DecimalType(18, 2)),
    ])
    df = spark.createDataFrame([(1, Decimal("80.00"), Decimal("20.00")), (2, Decimal("0.00"), Decimal("-5.00"))], schema)
    out = rowsBy(transforms.deriveInvoiceLines(df, NOW), "InvoiceLineID")
    assert out[1]["GrossMarginPct"] == Decimal("0.2500") and out[1]["NegativeMarginFlag"] == "N"
    assert out[2]["GrossMarginPct"] == Decimal("0.0000") and out[2]["NegativeMarginFlag"] == "Y"


def test_customer_segment_consent_suppression(spark):
    df = spark.createDataFrame([
        Row(CustomerSegmentID=1, RegionCode="EU", ConsentStatusCode="OPTIN"),
        Row(CustomerSegmentID=2, RegionCode="EU", ConsentStatusCode="UNKNOWN"),
        Row(CustomerSegmentID=3, RegionCode="NA", ConsentStatusCode="OPTOUT"),
        Row(CustomerSegmentID=4, RegionCode="NA", ConsentStatusCode="UNKNOWN"),
    ])
    out = rowsBy(transforms.deriveCustomerSegments(df, NOW), "CustomerSegmentID")
    assert [out[i]["MarketableFlag"] for i in (1, 2, 3, 4)] == ["Y", "N", "N", "Y"]
    assert out[1]["RecordKind"] == "SEGMENT"


def test_order_flags_and_pick_cycle_hours(spark):
    df = spark.createDataFrame([
        Row(OrderID=1, BackorderOrderID=None, OrderDate=datetime(2024, 3, 1, 9, 50), PickingCompletedWhen=datetime(2024, 3, 1, 11, 5)),
        Row(OrderID=2, BackorderOrderID=7, OrderDate=datetime(2024, 3, 1, 9, 0), PickingCompletedWhen=None),
    ])
    out = rowsBy(transforms.deriveOrders(df, NOW), "OrderID")
    assert out[1]["BackorderFlag"] == "N" and out[1]["PickCycleHours"] == 2 and out[1]["DeleteFlag"] == "N"
    assert out[2]["BackorderFlag"] == "Y" and out[2]["PickCycleHours"] == -1


def test_stock_movement_class(spark):
    schema = T.StructType([
        T.StructField("StockItemTransactionID", T.IntegerType()),
        T.StructField("InvoiceID", T.IntegerType()),
        T.StructField("PurchaseOrderID", T.IntegerType()),
        T.StructField("Quantity", T.DecimalType(18, 3)),
    ])
    df = spark.createDataFrame([(1, None, None, Decimal("-4.000")), (2, None, 9, Decimal("4.000")), (3, 5, None, Decimal("-1.000"))], schema)
    out = rowsBy(transforms.deriveStockMovements(df, NOW), "StockItemTransactionID")
    assert [out[i]["MovementClass"] for i in (1, 2, 3)] == ["ADJ", "RCPT", "ISSUE"]
    assert out[1]["AbsoluteQuantity"] == Decimal("4.000")


def test_stock_transfer_stale_flag_uses_run_timestamp(spark):
    schema = T.StructType([
        T.StructField("StockTransferLineID", T.IntegerType()),
        T.StructField("TransferQuantity", T.DecimalType(18, 3)),
        T.StructField("ReceivedQuantity", T.DecimalType(18, 3)),
        T.StructField("DispatchedWhen", T.TimestampType()),
        T.StructField("ReceivedWhen", T.TimestampType()),
    ])
    df = spark.createDataFrame([
        (1, Decimal("10.000"), Decimal("0.000"), datetime(2024, 2, 1), None),
        (2, Decimal("10.000"), Decimal("4.000"), datetime(2024, 3, 10), None),
        (3, Decimal("10.000"), Decimal("10.000"), datetime(2024, 1, 1), datetime(2024, 1, 5)),
    ], schema)
    out = rowsBy(transforms.deriveStockTransfers(df, NOW), "StockTransferLineID")
    assert [out[i]["StaleTransitFlag"] for i in (1, 2, 3)] == ["Y", "N", "N"]
    assert out[2]["InTransitQuantity"] == Decimal("6.000") and out[2]["MovementClass"] == "XFER"


def test_customer_transaction_days_outstanding(spark):
    df = spark.createDataFrame([
        Row(CustomerTransactionID=1, TransactionDate=datetime(2024, 3, 1), FinalizationDate=None),
        Row(CustomerTransactionID=2, TransactionDate=datetime(2024, 3, 1), FinalizationDate=datetime(2024, 3, 4)),
    ])
    out = rowsBy(transforms.deriveCustomerTransactions(df, NOW), "CustomerTransactionID")
    assert out[1]["SettledFlag"] == "N" and out[1]["DaysOutstanding"] == 14
    assert out[2]["SettledFlag"] == "Y" and out[2]["DaysOutstanding"] == 3 and out[2]["RecordKind"] == "ARTRAN"


def test_city_region_and_postal_format(spark):
    df = spark.createDataFrame([Row(CityID=i, Continent=c) for i, c in enumerate(["North America", "Europe", "Asia", "Oceania", "Africa"])])
    out = rowsBy(transforms.deriveCities(df, NOW), "CityID")
    assert [out[i]["RegionCode"] for i in range(5)] == ["NA", "EU", "APAC", "APAC", "ROW"]
    assert [out[i]["PostalFormatCode"] for i in range(5)] == ["ZIP5_PLUS4", "ALPHANUM", "NUMERIC6", "NUMERIC6", "NUMERIC6"]


def test_supplier_duplicate_key_and_people_role(spark):
    supplier = spark.createDataFrame([Row(SupplierTransactionID=1, SupplierReference=" ab1 ", SupplierInvoiceNumber="inv9"),
                                      Row(SupplierTransactionID=2, SupplierReference=None, SupplierInvoiceNumber="x")])
    out = rowsBy(transforms.deriveSupplierTransactions(supplier, NOW), "SupplierTransactionID")
    assert out[1]["DuplicateCheckKey"] == "AB1|INV9" and out[2]["DuplicateCheckKey"] is None
    people = spark.createDataFrame([Row(PersonID=1, IsSalesperson=True), Row(PersonID=2, IsSalesperson=False)])
    out = rowsBy(transforms.derivePeople(people, NOW), "PersonID")
    assert out[1]["RoleCode"] == "SALES" and out[2]["RoleCode"] == "EMP" and out[1]["RecordKind"] == "PERSON"


def test_promotion_redemption_rate_and_territory_calendar(spark):
    promos = spark.createDataFrame([Row(PromotionID=1, RedemptionCount=1, PromotionLineCount=8), Row(PromotionID=2, RedemptionCount=3, PromotionLineCount=0)])
    out = rowsBy(transforms.derivePromotions(promos, NOW), "PromotionID")
    assert out[1]["RedemptionRatePct"] == Decimal("0.1250") and out[2]["RedemptionRatePct"] == Decimal("0.0000")
    terr = spark.createDataFrame([Row(SalesTerritoryID=i, RegionCode=r) for i, r in enumerate(["NA", "EU", "APAC"])])
    out = rowsBy(transforms.deriveSalesTerritories(terr, NOW), "SalesTerritoryID")
    assert [out[i]["FiscalCalendarCode"] for i in range(3)] == ["445", "CAL", "APR_MAR"]


def test_web_session_flags_shipment_and_return_derivations(spark):
    web = spark.createDataFrame([Row(WebSessionID=1, PageViewCount=1, HasCheckout=False), Row(WebSessionID=2, PageViewCount=5, HasCheckout=True)])
    out = rowsBy(transforms.deriveWebSessions(web, NOW), "WebSessionID")
    assert out[1]["BounceFlag"] == "Y" and out[1]["ConversionFlag"] == "N" and out[2]["BounceFlag"] == "N" and out[2]["ConversionFlag"] == "Y"
    ship = spark.createDataFrame([Row(ShipmentHeaderID=1, DispatchedWhen=datetime(2024, 3, 1, 8), DeliveredWhen=datetime(2024, 3, 2, 7, 59),
                                      OriginCountryCode="GB", DestinationCountryCode="FR")])
    out = rowsBy(transforms.deriveShipments(ship, NOW), "ShipmentHeaderID")
    assert out[1]["TransitHours"] == 23 and out[1]["CrossBorderFlag"] == "Y"
    ret = spark.createDataFrame([Row(ReturnLineID=1, ReturnedWhen=datetime(2024, 3, 1, 23), InspectedWhen=datetime(2024, 3, 2, 1), DispositionCode="RESTOCK")])
    out = rowsBy(transforms.deriveReturns(ret, NOW), "ReturnLineID")
    assert out[1]["DaysToInspect"] == 1 and out[1]["RestockableFlag"] == "Y"
