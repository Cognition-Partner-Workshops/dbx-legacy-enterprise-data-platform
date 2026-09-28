"""C360_Publish_Segments business rules (segment assignment, suppression, movement)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_SEGMENT_MODEL_VERSION = "SEG-2021A"
PREMIUM_TIERS = ("PLATINUM", "PREMIER", "DIAMOND")


def assignSegments(profile: DataFrame, rollingMetric: DataFrame, churnFlag: DataFrame, loyaltyOverlay: DataFrame,
                   segmentModelVersion: str = DEFAULT_SEGMENT_MODEL_VERSION, assignedAtUtc=None) -> DataFrame:
    """Execute SQL Task 'Assign Segments' (CASE precedence preserved exactly)."""
    p = profile.select("CustomerKey", "CustomerId", "RegionCode", "IsContactable")
    m = rollingMetric.select("CustomerKey", "MonetaryDecile", "FrequencyDecile", "RecencyDecile", "ActivityStatusCode")
    ch = churnFlag.select("CustomerKey", "ChurnRiskBandCode")
    lo = loyaltyOverlay.select("CustomerId", "TierCode")
    euSuppressed = (F.col("RegionCode") == "EU") & (F.coalesce(F.col("IsContactable").cast("boolean"), F.lit(False)) == F.lit(False))
    segment = (
        F.when(euSuppressed, "SUPPRESSED")
        .when((F.col("ChurnRiskBandCode") == "HIGH") & (F.col("MonetaryDecile") >= 8), "AT_RISK_HIGH_VALUE")
        .when(F.col("ChurnRiskBandCode") == "HIGH", "AT_RISK")
        .when((F.col("MonetaryDecile") >= 9) & (F.col("FrequencyDecile") >= 8), "CHAMPION")
        .when((F.col("RecencyDecile") >= 8) & (F.col("FrequencyDecile") <= 3), "NEW_PROMISING")
        .when(F.col("TierCode").isin(*PREMIUM_TIERS), "LOYAL_PREMIUM")
        .when(F.col("ActivityStatusCode") == "INACTIVE", "DORMANT")
        .otherwise("CORE")
    )
    ts = F.lit(assignedAtUtc).cast("timestamp") if assignedAtUtc is not None else F.current_timestamp()
    return (
        p.join(m, "CustomerKey", "left").join(ch, "CustomerKey", "left").join(lo, "CustomerId", "left")
        .select(
            F.col("CustomerId"), F.col("RegionCode"),
            segment.alias("SegmentCode"),
            F.lit(segmentModelVersion).alias("SegmentModelVersion"),
            euSuppressed.alias("IsSuppressed"),
            ts.alias("AssignedAtUtc"),
        )
    )


def dropSuppressedRows(segments: DataFrame, publishSuppressedRows: bool) -> DataFrame:
    """Precedence constraints on @PublishSuppressedRows: when false, 'Drop Suppressed Rows'
    (DELETE ... WHERE IsSuppressed = 1) runs before the migration measure and publish."""
    if publishSuppressedRows:
        return segments
    return segments.where(F.col("IsSuppressed") == F.lit(False))


def measureSegmentMigration(segments: DataFrame, previous: DataFrame):
    """Execute SQL Task 'Measure Segment Migration' -> (SegmentCount, SuppressedCount, MigratedCount)."""
    segmentCount = segments.count()
    suppressedCount = segments.where(F.col("IsSuppressed") == F.lit(True)).count()
    migrated = (
        segments.alias("c").join(previous.alias("pr"), F.col("pr.CustomerId") == F.col("c.CustomerId"), "inner")
        .where(F.col("pr.SegmentCode") != F.col("c.SegmentCode")).count()
    )
    return segmentCount, suppressedCount, migrated


def deriveSegmentMovement(segments: DataFrame, previous: DataFrame) -> DataFrame:
    """Data Flow 'Publish Segments': Lookup Previous Segment (ignore no-match) ->
    Derive Segment Migration (NEW / SAME / MOVED)."""
    prev = previous.select("CustomerId", F.col("SegmentCode").alias("PreviousSegmentCode"))
    return (
        segments.join(prev, "CustomerId", "left")
        .withColumn("SegmentMovementCode",
                    F.when(F.col("PreviousSegmentCode").isNull(), "NEW")
                     .when(F.col("PreviousSegmentCode") == F.col("SegmentCode"), "SAME")
                     .otherwise("MOVED"))
    )
