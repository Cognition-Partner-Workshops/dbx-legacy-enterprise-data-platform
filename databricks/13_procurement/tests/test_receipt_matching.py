from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

from procurement_lib import receipt_matching as rm


def _sources(spark):
    receipts = spark.createDataFrame([
        # matched: qty exact, price within absolute floor
        Row(ReceiptId=1, ReceiptNumber="R-1", PurchaseOrderNumber="PO-1", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 1, 9), ReceivedOuters=100, WarehouseSiteCode="W1", LoadBatchId=7),
        # GRNI: no invoice, 10 days old
        Row(ReceiptId=2, ReceiptNumber="R-2", PurchaseOrderNumber="PO-1", LineNumber=2, ReceivedAtUtc=datetime(2024, 3, 20, 9), ReceivedOuters=50, WarehouseSiteCode="W1", LoadBatchId=7),
        # GRNI: no invoice, 60 days old -> not accrued
        Row(ReceiptId=3, ReceiptNumber="R-3", PurchaseOrderNumber="PO-1", LineNumber=3, ReceivedAtUtc=datetime(2024, 1, 30, 9), ReceivedOuters=50, WarehouseSiteCode="W1", LoadBatchId=7),
        # QTYEXCEPT: price ok, quantity 10% off
        Row(ReceiptId=4, ReceiptNumber="R-4", PurchaseOrderNumber="PO-2", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 5, 9), ReceivedOuters=110, WarehouseSiteCode="W2", LoadBatchId=7),
        # PRICEEXCEPT: price 10% over, outside 1.00 floor and 3%
        Row(ReceiptId=5, ReceiptNumber="R-5", PurchaseOrderNumber="PO-2", LineNumber=2, ReceivedAtUtc=datetime(2024, 3, 5, 9), ReceivedOuters=100, WarehouseSiteCode="W2", LoadBatchId=7),
        # regional tolerance table: NA/CHEM qty tolerance 15% makes the 10% variance MATCHED
        Row(ReceiptId=6, ReceiptNumber="R-6", PurchaseOrderNumber="PO-3", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 5, 9), ReceivedOuters=110, WarehouseSiteCode="W3", LoadBatchId=7),
        # other batch
        Row(ReceiptId=7, ReceiptNumber="R-7", PurchaseOrderNumber="PO-1", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 5, 9), ReceivedOuters=1, WarehouseSiteCode="W1", LoadBatchId=6),
    ])
    poLines = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", LineNumber=1, OrderedOuters=100, ExpectedUnitPricePerOuter=Decimal("20.00"), CategoryCode="PACK"),
        Row(PurchaseOrderNumber="PO-1", LineNumber=2, OrderedOuters=50, ExpectedUnitPricePerOuter=Decimal("4.00"), CategoryCode="PACK"),
        Row(PurchaseOrderNumber="PO-1", LineNumber=3, OrderedOuters=50, ExpectedUnitPricePerOuter=Decimal("4.00"), CategoryCode="PACK"),
        Row(PurchaseOrderNumber="PO-2", LineNumber=1, OrderedOuters=100, ExpectedUnitPricePerOuter=Decimal("20.00"), CategoryCode="CHEM"),
        Row(PurchaseOrderNumber="PO-2", LineNumber=2, OrderedOuters=100, ExpectedUnitPricePerOuter=Decimal("100.00"), CategoryCode="CHEM"),
        Row(PurchaseOrderNumber="PO-3", LineNumber=1, OrderedOuters=100, ExpectedUnitPricePerOuter=Decimal("20.00"), CategoryCode="CHEM"),
    ])
    headers = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", SupplierId=100, RegionCode="EU"),
        Row(PurchaseOrderNumber="PO-2", SupplierId=101, RegionCode="EU"),
        Row(PurchaseOrderNumber="PO-3", SupplierId=101, RegionCode="NA"),
    ])
    invoices = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", PurchaseOrderLineNumber=1, InvoicedOuters=100, InvoicedUnitPrice=Decimal("20.90"), ApInvoiceNumber="INV-1"),
        Row(PurchaseOrderNumber="PO-2", PurchaseOrderLineNumber=1, InvoicedOuters=100, InvoicedUnitPrice=Decimal("20.00"), ApInvoiceNumber="INV-2"),
        Row(PurchaseOrderNumber="PO-2", PurchaseOrderLineNumber=2, InvoicedOuters=100, InvoicedUnitPrice=Decimal("110.00"), ApInvoiceNumber="INV-3"),
        Row(PurchaseOrderNumber="PO-3", PurchaseOrderLineNumber=1, InvoicedOuters=100, InvoicedUnitPrice=Decimal("20.00"), ApInvoiceNumber="INV-4"),
    ])
    tolerances = spark.createDataFrame([
        Row(RegionCode="NA", CategoryCode="CHEM", QuantityTolerancePercent=Decimal("15"), PriceTolerancePercent=Decimal("3"), PriceToleranceAbsolute=Decimal("1.00")),
    ])
    supplierKeys = spark.createDataFrame([Row(SupplierId=100, SupplierKey=1), Row(SupplierId=101, SupplierKey=2)])
    return receipts, poLines, headers, invoices, tolerances, supplierKeys


def test_three_way_match_outcomes(spark):
    receipts, poLines, headers, invoices, tolerances, supplierKeys = _sources(spark)
    df = rm.buildReceiptMatchInput(receipts, poLines, headers, invoices, tolerances, batchId=7)
    df = rm.evaluateMatchResult(rm.deriveMatchVariances(df), businessDate=date(2024, 3, 30))
    rows = {r.ReceiptId: r for r in df.collect()}

    assert set(rows) == {1, 2, 3, 4, 5, 6}
    assert rows[1].MatchResultCode == "MATCHED" and rows[1].PriceVariance == Decimal("0.90")
    assert rows[2].MatchResultCode == "GRNI" and rows[2].ReceiptAgeDays == 10 and rows[2].QuantityVariance == 50
    assert rows[3].MatchResultCode == "GRNI" and rows[3].ReceiptAgeDays == 60
    assert rows[4].MatchResultCode == "QTYEXCEPT" and rows[4].QuantityVariance == 10
    assert rows[5].MatchResultCode == "PRICEEXCEPT" and rows[5].PriceVariance == Decimal("10.00")
    assert rows[6].MatchResultCode == "MATCHED", "regional 15% quantity tolerance must apply"
    assert rows[1].QuantityTolerancePercent == Decimal("2.00") and rows[1].PriceToleranceAbsolute == Decimal("1.00")

    matched, grni, exceptions = rm.splitMatchOutcome(df)
    assert sorted(r.ReceiptId for r in matched.collect()) == [1, 6]
    assert sorted(r.ReceiptId for r in grni.collect()) == [2, 3]
    assert sorted(r.ReceiptId for r in exceptions.collect()) == [4, 5]
    assert rm.measureMatchOutcomes(grni, exceptions) == (2, 2)

    accruals = rm.buildAccruals(grni, supplierKeys, accrualCutoffDays=45).collect()
    assert [r.ReceiptId for r in accruals] == [2]
    assert accruals[0].AccrualAmount == Decimal("200.00") and accruals[0].MatchResultCode == "ACCRUED"

    fact = rm.toFactPurchaseReceipt(matched.join(supplierKeys, "SupplierId"), batchId=7)
    accrualFact = rm.toFactPurchaseReceipt(rm.buildAccruals(grni, supplierKeys, 45), batchId=7)
    assert fact.columns == accrualFact.columns
    union = fact.unionByName(accrualFact)
    assert union.count() == 3 and union.select("receipt_number", "receipt_line_number").distinct().count() == 3
