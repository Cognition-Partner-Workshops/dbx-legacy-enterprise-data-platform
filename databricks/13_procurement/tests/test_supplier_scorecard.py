from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

from procurement_lib import supplier_scorecard as sc


def _measures(spark):
    return spark.createDataFrame([
        Row(SupplierId=1, RegionCode="EU", OrderCount=20, OnTimeCount=19, QuantityAccurateCount=20, PriceAdherentCount=18, QualityRejectCount=1, InvoiceExceptionCount=0, WindowDays=90),
        Row(SupplierId=2, RegionCode="NA", OrderCount=10, OnTimeCount=9, QuantityAccurateCount=8, PriceAdherentCount=10, QualityRejectCount=0, InvoiceExceptionCount=2, WindowDays=90),
        Row(SupplierId=3, RegionCode="NA", OrderCount=10, OnTimeCount=7, QuantityAccurateCount=10, PriceAdherentCount=10, QualityRejectCount=0, InvoiceExceptionCount=0, WindowDays=90),
        Row(SupplierId=4, RegionCode="NA", OrderCount=10, OnTimeCount=2, QuantityAccurateCount=10, PriceAdherentCount=10, QualityRejectCount=0, InvoiceExceptionCount=0, WindowDays=90),
        Row(SupplierId=5, RegionCode="AP", OrderCount=3, OnTimeCount=3, QuantityAccurateCount=3, PriceAdherentCount=3, QualityRejectCount=0, InvoiceExceptionCount=0, WindowDays=90),
        Row(SupplierId=6, RegionCode="AP", OrderCount=0, OnTimeCount=0, QuantityAccurateCount=0, PriceAdherentCount=0, QualityRejectCount=0, InvoiceExceptionCount=0, WindowDays=90),
    ])


def test_score_bands_and_weighted_score(spark):
    weights = spark.createDataFrame([
        Row(RegionCode="EU", OnTimeWeight=Decimal("0.5"), AccuracyWeight=Decimal("0.2"), PriceWeight=Decimal("0.2"), QualityWeight=Decimal("0.1")),
    ])
    scored = {r.SupplierId: r for r in sc.scoreSuppliers(_measures(spark), weights, minimumOrdersForScore=5).collect()}

    # EU weights from the table: 95*0.5 + 100*0.2 + 90*0.2 + 95*0.1 = 95.0
    assert scored[1].OnTimePercent == Decimal("95.00") and scored[1].ScoreBandCode == "A"
    assert scored[1].SupplierScore == Decimal("95.00")
    # NA has no weight row -> defaults 0.4/0.2/0.2/0.2: 90*0.4 + 80*0.2 + 100*0.2 + 100*0.2 = 92
    assert scored[2].ScoreBandCode == "B" and scored[2].SupplierScore == Decimal("92.00")
    assert scored[3].ScoreBandCode == "C"
    assert scored[4].ScoreBandCode == "D"
    assert scored[5].ScoreBandCode == "NODATA", "below MinimumOrdersForScore"
    assert scored[6].ScoreBandCode == "NODATA" and scored[6].OnTimePercent == Decimal("0.00"), "zero orders must not divide by zero"
    assert sc.countUnscoredSuppliers(_measures(spark), 5) == 2


def test_build_scorecard_measures_over_window(spark):
    suppliers = spark.createDataFrame([Row(SupplierId=1, RegionCode="EU"), Row(SupplierId=2, RegionCode="EU")])
    headers = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", SupplierId=1, OrderDate=date(2024, 3, 1)),
        Row(PurchaseOrderNumber="PO-2", SupplierId=1, OrderDate=date(2024, 3, 2)),
        Row(PurchaseOrderNumber="PO-OLD", SupplierId=1, OrderDate=date(2023, 1, 1)),
    ])
    lines = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", LineNumber=1, OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("5.00"), PromisedDate=date(2024, 3, 10)),
        Row(PurchaseOrderNumber="PO-2", LineNumber=1, OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("5.00"), PromisedDate=date(2024, 3, 10)),
        Row(PurchaseOrderNumber="PO-OLD", LineNumber=1, OrderedOuters=10, ExpectedUnitPricePerOuter=Decimal("5.00"), PromisedDate=date(2023, 1, 10)),
    ])
    receipts = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 9, 12), ReceivedOuters=10, QualityStatusCode="OK"),
        Row(PurchaseOrderNumber="PO-2", LineNumber=1, ReceivedAtUtc=datetime(2024, 3, 12, 12), ReceivedOuters=9, QualityStatusCode="REJECT"),
    ])
    invoices = spark.createDataFrame([
        Row(PurchaseOrderNumber="PO-1", PurchaseOrderLineNumber=1, InvoicedUnitPrice=Decimal("5.00"), ApInvoiceNumber="INV-1"),
    ])
    rows = sc.buildScorecardMeasures(suppliers, headers, lines, receipts, invoices, date(2024, 3, 31), 90).collect()
    assert len(rows) == 1, "supplier 2 has no orders and PO-OLD is outside the window"
    r = rows[0]
    assert (r.SupplierId, r.RegionCode, r.OrderCount, r.OnTimeCount, r.QuantityAccurateCount) == (1, "EU", 2, 1, 1)
    assert (r.PriceAdherentCount, r.QualityRejectCount, r.InvoiceExceptionCount, r.WindowDays) == (2, 1, 1, 90)


def test_to_agg_supplier_performance_month_grain(spark):
    weights = spark.createDataFrame([], "RegionCode string, OnTimeWeight decimal(5,4), AccuracyWeight decimal(5,4), PriceWeight decimal(5,4), QualityWeight decimal(5,4)")
    scored = sc.scoreSuppliers(_measures(spark), weights, 5)
    keys = spark.createDataFrame([Row(SupplierId=i, SupplierKey=i * 10) for i in range(1, 7)])
    agg = sc.toAggSupplierPerformance(scored, keys, date(2024, 3, 17), batchId=7).collect()
    assert len(agg) == 6 and {r.calendar_month for r in agg} == {date(2024, 3, 1)}
    assert {r.supplier_key for r in agg} == {10, 20, 30, 40, 50, 60}
    assert all(r.refresh_batch_id == 7 for r in agg)
