from datetime import datetime

from c360_lib import segments as S


def frames(spark):
    profile = spark.createDataFrame([
        (1, 101, "EU", False), (2, 102, "NA", True), (3, 103, "NA", True), (4, 104, "NA", True),
        (5, 105, "NA", True), (6, 106, "NA", True), (7, 107, "NA", True), (8, 108, "NA", True), (9, 109, "EU", True),
    ], "CustomerKey int, CustomerId int, RegionCode string, IsContactable boolean")
    rolling = spark.createDataFrame([
        (1, 10, 10, 10, "ACTIVE"),
        (2, 8, 1, 1, "ACTIVE"),       # HIGH churn + monetary 8 -> AT_RISK_HIGH_VALUE
        (3, 7, 1, 1, "ACTIVE"),       # HIGH churn -> AT_RISK
        (4, 9, 8, 1, "INACTIVE"),     # CHAMPION beats DORMANT
        (5, 1, 3, 8, "ACTIVE"),       # NEW_PROMISING
        (6, 1, 5, 1, "ACTIVE"),       # LOYAL_PREMIUM via tier
        (7, 1, 5, 1, "INACTIVE"),     # DORMANT
        (8, 1, 5, 1, "ACTIVE"),       # CORE
    ], "CustomerKey int, MonetaryDecile int, FrequencyDecile int, RecencyDecile int, ActivityStatusCode string")
    churn = spark.createDataFrame([(1, "HIGH"), (2, "HIGH"), (3, "HIGH"), (4, "LOW")], "CustomerKey int, ChurnRiskBandCode string")
    loyalty = spark.createDataFrame([(106, "PLATINUM"), (104, "PREMIER"), (103, "DIAMOND")], "CustomerId int, TierCode string")
    return profile, rolling, churn, loyalty


def test_segment_precedence(spark):
    seg = S.assignSegments(*frames(spark), assignedAtUtc="2024-06-30 00:00:00")
    out = {r.CustomerId: r for r in seg.collect()}
    expected = {101: "SUPPRESSED", 102: "AT_RISK_HIGH_VALUE", 103: "AT_RISK", 104: "CHAMPION", 105: "NEW_PROMISING",
                106: "LOYAL_PREMIUM", 107: "DORMANT", 108: "CORE", 109: "CORE"}
    assert {k: v.SegmentCode for k, v in out.items()} == expected
    assert out[101].IsSuppressed is True and out[109].IsSuppressed is False
    assert all(r.SegmentModelVersion == "SEG-2021A" for r in out.values())
    assert S.assignSegments(*frames(spark), segmentModelVersion="SEG-X").first().SegmentModelVersion == "SEG-X"


def test_suppression_migration_and_movement(spark):
    seg = S.assignSegments(*frames(spark), assignedAtUtc="2024-06-30 00:00:00")
    assert S.dropSuppressedRows(seg, True).count() == 9
    kept = S.dropSuppressedRows(seg, False)
    assert kept.count() == 8 and kept.where("CustomerId = 101").count() == 0

    previous = spark.createDataFrame([
        (102, "CORE", datetime(2024, 5, 1)), (104, "CHAMPION", datetime(2024, 5, 1)), (999, "CORE", datetime(2024, 5, 1)),
    ], "CustomerId int, SegmentCode string, AssignedAtUtc timestamp")
    assert S.measureSegmentMigration(seg, previous) == (9, 1, 1)
    mv = {r.CustomerId: r.SegmentMovementCode for r in S.deriveSegmentMovement(seg, previous).collect()}
    assert mv[102] == "MOVED" and mv[104] == "SAME" and mv[101] == "NEW" and 999 not in mv
