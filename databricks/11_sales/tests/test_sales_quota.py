from datetime import date
from decimal import Decimal

import sales_quota as quota


def _money(v):
    return Decimal(str(v)).quantize(Decimal("0.01"))


def test_attainment_by_region_and_bands(spark):
    territories = quota.legacyTerritories(spark.createDataFrame(
        [("T-NA", "NA", True), ("T-EU", "EU", True), ("T-AP", "APAC", True), ("T-NOQ", "NA", True), ("T-OFF", "NA", False)],
        "SalesTerritoryCode string, RegionCode string, IsActive boolean"))
    quotas = quota.legacyQuotas(spark.createDataFrame(
        [("T-NA", "2024-03", _money(1000)), ("T-EU", "2024-03", _money(1000)), ("T-AP", "2024-03", _money(0)),
         ("T-NA", "2024-02", _money(5000))],
        "TerritoryCode string, QuotaPeriod string, QuotaAmount decimal(18,2)"))
    saleLines = quota.legacySaleLines(spark.createDataFrame(
        [("INV-1", date(2024, 3, 5), "T-NA", "NA", _money(1000), _money(80), _money(1000)),
         ("INV-2", date(2024, 3, 6), "T-NA", "NA", _money(100), _money(8), _money(100)),
         ("INV-3", date(2024, 2, 6), "T-NA", "NA", _money(9999), _money(0), _money(9999)),
         ("INV-4", date(2024, 3, 7), "T-EU", "EU", _money(1190), _money(190), _money(1000))],
        "SaleBusinessKey string, InvoiceDate date, SalesTerritoryCode string, RegionCode string, "
        "NetLineAmount decimal(18,2), TaxAmount decimal(18,2), NetAmount decimal(18,2)"))
    creditNotes = quota.legacyCreditNotes(spark.createDataFrame(
        [("INV-4", _money(150))], "AppliedToSaleBusinessKey string, NetAmount decimal(18,2)"))
    orderLines = quota.legacyOrderLines(spark.createDataFrame(
        [("T-AP", date(2024, 3, 30), _money(300)), ("T-AP", date(2024, 1, 2), _money(200))],
        "SalesTerritoryCode string, OrderDate date, NetLineAmount decimal(18,2)"))
    cal = quota.legacyFiscalCalendar(spark.createDataFrame(
        [(date(2024, 3, 30), "2024-P04")], "CalendarDate date, FiscalPeriod445 string"))

    assert quota.countTerritoriesWithoutQuota(territories, quotas, "2024-03") == 1  # T-NOQ (T-OFF inactive)

    work = quota.buildAttainmentByRegion(territories, quotas, saleLines, creditNotes, orderLines, cal, "2024-03")
    rows = {r["TerritoryCode"]: r for r in work.collect()}
    assert rows["T-NA"]["ActualAmount"] == Decimal("1188.00")  # (1000+80) + (100+8), Feb excluded
    assert rows["T-NA"]["MeasureBasisCode"] == "INVOICED"
    assert rows["T-EU"]["ActualAmount"] == Decimal("850.00")   # 1000 - 150 credit
    assert rows["T-EU"]["MeasureBasisCode"] == "NET_OF_CREDITS"
    # legacy LEFT JOIN to the 4-4-5 calendar restricts nothing: all order lines of the territory
    assert rows["T-AP"]["ActualAmount"] == Decimal("500.00")
    assert rows["T-AP"]["MeasureBasisCode"] == "ORDER_INTAKE"

    metrics = {r["TerritoryCode"]: r for r in quota.deriveAttainmentMetrics(work).collect()}
    assert metrics["T-NA"]["AttainmentPercent"] == Decimal("118.80") and metrics["T-NA"]["AttainmentBandCode"] == "AT"
    assert metrics["T-EU"]["AttainmentPercent"] == Decimal("85.00") and metrics["T-EU"]["AttainmentBandCode"] == "NEAR"
    assert metrics["T-AP"]["AttainmentPercent"] == Decimal("0.00") and metrics["T-AP"]["AttainmentBandCode"] == "NOQUOTA"


def test_band_edges(spark):
    df = spark.createDataFrame(
        [("A", _money(1200), _money(1000)), ("B", _money(1000), _money(1000)), ("C", _money(800), _money(1000)),
         ("D", _money(799.99), _money(1000))],
        "TerritoryCode string, ActualAmount decimal(18,2), QuotaAmount decimal(18,2)")
    bands = {r["TerritoryCode"]: r["AttainmentBandCode"] for r in quota.deriveAttainmentMetrics(df).collect()}
    assert bands == {"A": "OVER120", "B": "AT", "C": "NEAR", "D": "UNDER"}


def test_period_columns_from_quota_period(spark):
    df = spark.createDataFrame([("2024-03",), ("2024-P04",)], "QuotaPeriod string")
    out = {r["QuotaPeriod"]: r for r in quota.toRegionalSalesPerformance(df, 7).collect()}
    assert (out["2024-03"]["FiscalYear"], out["2024-03"]["FiscalPeriod"], out["2024-03"]["CalendarMonth"]) == (2024, 3, date(2024, 3, 1))
    assert (out["2024-P04"]["FiscalYear"], out["2024-P04"]["FiscalPeriod"], out["2024-P04"]["CalendarMonth"]) == (2024, 4, None)
    assert out["2024-03"]["RefreshBatchId"] == 7
