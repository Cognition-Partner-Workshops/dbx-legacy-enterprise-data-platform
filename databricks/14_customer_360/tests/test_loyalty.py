from datetime import date
from decimal import Decimal

from c360_lib import loyalty as L

AS_OF = date(2024, 6, 30)
LEDGER_SCHEMA = ("CustomerId int, RegionCode string, AccruedDate date, QualifyingAmount decimal(18,2), PointsAccrued decimal(18,2), "
                 "PointStatusCode string, SourceReference string, ExpiredAtUtc timestamp")


def test_expire_aged_points_by_region(spark):
    ledger = spark.createDataFrame([
        (1, "NA", date(2022, 6, 1), Decimal(10), Decimal(10), "ACTIVE", "I1", None),    # > 24m -> expired
        (1, "NA", date(2022, 7, 1), Decimal(10), Decimal(10), "ACTIVE", "I2", None),    # 24m exactly boundary: not < -> stays
        (2, "EU", date(2021, 7, 1), Decimal(10), Decimal(10), "ACTIVE", "I3", None),    # 36m boundary -> stays
        (2, "EU", date(2021, 6, 1), Decimal(10), Decimal(10), "ACTIVE", "I4", None),    # expired
        (3, "APAC", date(2022, 12, 1), Decimal(10), Decimal(10), "ACTIVE", "I5", None), # > 18m -> expired
        (4, "LATAM", date(2010, 1, 1), Decimal(10), Decimal(10), "ACTIVE", "I6", None), # never expires
        (1, "NA", date(2010, 1, 1), Decimal(10), Decimal(10), "REDEEMED", "I7", None),  # not ACTIVE -> untouched
    ], LEDGER_SCHEMA)
    out = {r.SourceReference: r for r in L.expireAgedPoints(ledger, AS_OF, expiredAtUtc="2024-06-30 00:00:00").collect()}
    assert out["I1"].PointStatusCode == "EXPIRED" and out["I1"].ExpiredAtUtc is not None
    assert out["I2"].PointStatusCode == "ACTIVE" and out["I3"].PointStatusCode == "ACTIVE"
    assert out["I4"].PointStatusCode == "EXPIRED" and out["I5"].PointStatusCode == "EXPIRED"
    assert out["I6"].PointStatusCode == "ACTIVE" and out["I7"].PointStatusCode == "REDEEMED"


def test_accrue_points_rules_and_dedupe(spark):
    sales = spark.createDataFrame([
        (1, "NA", "N1", date(2024, 6, 1), Decimal("120.99"), Decimal("100.00"), None),
        (2, "EU", "E1", date(2024, 6, 1), Decimal("120.00"), Decimal("100.50"), None),
        (3, "APAC", "A1", date(2024, 6, 1), Decimal("110.00"), Decimal("100.00"), Decimal("10.00")),
        (4, "LATAM", "L1", date(2024, 6, 1), Decimal("130.00"), Decimal("99.99"), None),
        (1, "NA", "DUP", date(2024, 6, 2), Decimal("50.00"), Decimal("40.00"), None),
    ], "CustomerId int, RegionCode string, InvoiceNumber string, InvoiceDate date, GrossAmount decimal(18,2), NetAmount decimal(18,2), GstAmount decimal(18,2)")
    ledger = spark.createDataFrame([(1, "NA", date(2024, 6, 2), Decimal(50), Decimal(50), "ACTIVE", "DUP", None)], LEDGER_SCHEMA)
    out = {r.SourceReference: r for r in L.accruePoints(sales, ledger).collect()}
    assert "DUP" not in out and len(out) == 4
    assert out["N1"].QualifyingAmount == Decimal("120.99") and out["N1"].PointsAccrued == Decimal("120.00")
    assert out["E1"].QualifyingAmount == Decimal("100.50") and out["E1"].PointsAccrued == Decimal("150.00")
    assert out["A1"].QualifyingAmount == Decimal("100.00") and out["A1"].PointsAccrued == Decimal("200.00")
    assert out["L1"].QualifyingAmount == Decimal("99.99") and out["L1"].PointsAccrued == Decimal("99.00")
    assert out["N1"].PointStatusCode == "ACTIVE" and out["N1"].AccruedDate == date(2024, 6, 1)


def test_tier_ladders_and_movement(spark):
    ledger = spark.createDataFrame([
        (1, "NA", date(2024, 1, 1), Decimal(0), Decimal(50000), "ACTIVE", "a", None),
        (1, "NA", date(2024, 1, 1), Decimal(0), Decimal(1000), "EXPIRED", "b", None),
        (2, "NA", date(2024, 1, 1), Decimal(0), Decimal(20000), "ACTIVE", "c", None),
        (3, "NA", date(2024, 1, 1), Decimal(0), Decimal(4999), "ACTIVE", "d", None),
        (4, "EU", date(2024, 1, 1), Decimal(0), Decimal(75000), "ACTIVE", "e", None),
        (5, "EU", date(2024, 1, 1), Decimal(0), Decimal(30000), "ACTIVE", "f", None),
        (6, "EU", date(2024, 1, 1), Decimal(0), Decimal(1), "ACTIVE", "g", None),
        (7, "APAC", date(2024, 1, 1), Decimal(0), Decimal(40000), "ACTIVE", "h", None),
        (8, "APAC", date(2024, 1, 1), Decimal(0), Decimal(15000), "ACTIVE", "i", None),
        (9, "LATAM", date(2024, 1, 1), Decimal(0), Decimal(100), "ACTIVE", "j", None),
    ], LEDGER_SCHEMA)
    current = spark.createDataFrame([(1, "NA", "PLATINUM"), (2, "NA", "SILVER")], "CustomerId int, RegionCode string, TierCode string")
    overlay = L.recalculateTierLadders(ledger, current)
    out = {r.CustomerId: r for r in overlay.collect()}
    assert [out[i].TierCode for i in range(1, 10)] == ["PLATINUM", "GOLD", "BASE", "PREMIER", "PLUS", "STANDARD", "DIAMOND", "JADE", "MEMBER"]
    assert out[1].ActivePoints == Decimal(50000) and out[1].ExpiredPoints == Decimal(1000)
    assert out[1].PreviousTierCode == "PLATINUM" and out[2].PreviousTierCode == "SILVER" and out[3].PreviousTierCode == "NONE"
    changes, expired = L.measureTierMovement(overlay)
    assert changes == 8 and expired == Decimal(1000)
    mv = {r.CustomerId: r.TierMovementCode for r in L.deriveTierMovement(overlay).collect()}
    assert mv[1] == "SAME" and mv[2] == "CHANGED" and mv[3] == "NEW"


def test_qualifying_sale_from_fact(spark):
    from datetime import datetime
    sale = spark.createDataFrame([
        (1, date(2024, 6, 1), 500, 110.0, 100.0, 10.0),
        (1, date(2024, 6, 1), 500, 55.0, 50.0, 5.0),
        (2, date(2024, 6, 1), 501, 220.0, 200.0, 20.0),
        (2, date(2024, 7, 1), 502, 220.0, 200.0, 20.0),  # after as-of -> excluded
    ], "CustomerKey int, InvoiceDateKey date, WWIInvoiceID int, TotalIncludingTax double, TotalExcludingTax double, TaxAmount double")
    dim = spark.createDataFrame([(1, 10, "APAC", datetime(9999, 12, 31)), (2, 20, "NA", datetime(9999, 12, 31))],
                                "CustomerKey int, WWICustomerID int, RegionCode string, ValidTo timestamp")
    out = {r.InvoiceNumber: r for r in L.buildLoyaltyQualifyingSale(sale, dim, AS_OF).collect()}
    assert set(out) == {"500", "501"}
    assert out["500"].GrossAmount == Decimal("165.00") and out["500"].NetAmount == Decimal("150.00") and out["500"].GstAmount == Decimal("15.00")
    assert out["501"].GstAmount is None and out["501"].CustomerId == 20
