"""C360_Build_LoyaltyOverlay business rules (point expiry, accrual, tier ladders, tier movement)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

DEFAULT_EXPIRY_MONTHS = {"NA": 24, "EU": 36, "APAC": 18}
PREMIUM_TIERS = ("PLATINUM", "PREMIER", "DIAMOND")


def expireAgedPoints(ledger: DataFrame, asOfDate, expiryMonths=None, expiredAtUtc=None) -> DataFrame:
    """Execute SQL Task 'Expire Aged Points': ACTIVE rows whose AccruedDate is older than the
    regional expiry (NA/EU/APAC only; other regions never expire) become EXPIRED."""
    months = dict(DEFAULT_EXPIRY_MONTHS, **(expiryMonths or {}))
    today = F.lit(asOfDate).cast("date")
    aged = F.lit(False)
    for region, m in months.items():
        aged = aged | ((F.col("RegionCode") == region) & (F.col("AccruedDate") < F.add_months(today, -int(m))))
    toExpire = (F.col("PointStatusCode") == "ACTIVE") & aged
    ts = F.lit(expiredAtUtc).cast("timestamp") if expiredAtUtc is not None else F.current_timestamp()
    return (
        ledger.withColumn("ExpiredAtUtc", F.when(toExpire, ts).otherwise(F.col("ExpiredAtUtc")))
        .withColumn("PointStatusCode", F.when(toExpire, F.lit("EXPIRED")).otherwise(F.col("PointStatusCode")))
    )


def buildLoyaltyQualifyingSale(factSale: DataFrame, dimCustomer: DataFrame, asOfDate) -> DataFrame:
    """Reconstructed work.LoyaltyQualifyingSale: one row per invoice with the amounts the
    accrual rules need (GrossAmount = Total Including Tax, NetAmount = Total Excluding Tax,
    GstAmount = Tax Amount for APAC invoices)."""
    today = F.lit(asOfDate).cast("date")
    cust = dimCustomer.select("CustomerKey", F.col("WWICustomerID").alias("CustomerId"), "RegionCode")
    return (
        factSale.where(F.col("InvoiceDateKey") <= today)
        .join(cust, "CustomerKey", "inner")
        .groupBy("CustomerId", "RegionCode", F.col("WWIInvoiceID").cast("string").alias("InvoiceNumber"))
        .agg(
            F.max("InvoiceDateKey").alias("InvoiceDate"),
            F.sum("TotalIncludingTax").cast(DecimalType(18, 2)).alias("GrossAmount"),
            F.sum("TotalExcludingTax").cast(DecimalType(18, 2)).alias("NetAmount"),
            F.sum(F.when(F.col("RegionCode") == "APAC", F.col("TaxAmount"))).cast(DecimalType(18, 2)).alias("GstAmount"),
        )
    )


def accruePoints(qualifyingSales: DataFrame, ledger: DataFrame) -> DataFrame:
    """Execute SQL Task 'Accrue Points': new ACTIVE ledger rows for invoices not yet in the
    ledger (NOT EXISTS on SourceReference = InvoiceNumber)."""
    dec = DecimalType(18, 2)
    gst = F.coalesce(F.col("GstAmount"), F.lit(0))
    qualifying = (
        F.when(F.col("RegionCode") == "NA", F.col("GrossAmount"))
        .when(F.col("RegionCode") == "EU", F.col("NetAmount"))
        .when(F.col("RegionCode") == "APAC", F.col("GrossAmount") - gst)
        .otherwise(F.col("NetAmount"))
    )
    points = (
        F.when(F.col("RegionCode") == "NA", F.floor(F.col("GrossAmount")))
        .when(F.col("RegionCode") == "EU", F.floor(F.col("NetAmount") * 1.5))
        .when(F.col("RegionCode") == "APAC", F.floor((F.col("GrossAmount") - gst) * 2))
        .otherwise(F.floor(F.col("NetAmount")))
    )
    existing = ledger.select(F.col("SourceReference").alias("InvoiceNumber")).distinct()
    return (
        qualifyingSales.join(existing, "InvoiceNumber", "left_anti")
        .select(
            F.col("CustomerId"), F.col("RegionCode"),
            F.col("InvoiceDate").alias("AccruedDate"),
            qualifying.cast(dec).alias("QualifyingAmount"),
            points.cast(dec).alias("PointsAccrued"),
            F.lit("ACTIVE").alias("PointStatusCode"),
            F.col("InvoiceNumber").alias("SourceReference"),
            F.lit(None).cast("timestamp").alias("ExpiredAtUtc"),
        )
    )


def tierCode(activePoints):
    return (
        F.when(F.col("RegionCode") == "NA",
               F.when(activePoints >= 50000, "PLATINUM").when(activePoints >= 20000, "GOLD")
                .when(activePoints >= 5000, "SILVER").otherwise("BASE"))
        .when(F.col("RegionCode") == "EU",
              F.when(activePoints >= 75000, "PREMIER").when(activePoints >= 30000, "PLUS").otherwise("STANDARD"))
        .otherwise(F.when(activePoints >= 40000, "DIAMOND").when(activePoints >= 15000, "JADE").otherwise("MEMBER"))
    )


def recalculateTierLadders(ledger: DataFrame, overlayCurrent: DataFrame) -> DataFrame:
    """Execute SQL Task 'Recalculate Tier Ladders' -> work.LoyaltyOverlay rows."""
    dec = DecimalType(18, 2)
    cur = overlayCurrent.select("CustomerId", F.col("TierCode").alias("CurrentTierCode"))
    active = F.sum(F.when(F.col("PointStatusCode") == "ACTIVE", F.col("PointsAccrued")).otherwise(F.lit(0)))
    expired = F.sum(F.when(F.col("PointStatusCode") == "EXPIRED", F.col("PointsAccrued")).otherwise(F.lit(0)))
    return (
        ledger.join(cur, "CustomerId", "left")
        .groupBy("CustomerId", "RegionCode")
        .agg(
            active.cast(dec).alias("ActivePoints"),
            expired.cast(dec).alias("ExpiredPoints"),
            F.max(F.coalesce(F.col("CurrentTierCode"), F.lit("NONE"))).alias("PreviousTierCode"),
        )
        .withColumn("TierCode", tierCode(F.col("ActivePoints")))
        .select("CustomerId", "RegionCode", "ActivePoints", "ExpiredPoints", "PreviousTierCode", "TierCode")
    )


def measureTierMovement(overlay: DataFrame):
    """Execute SQL Task 'Measure Tier Movement' -> (TierChangeCount, ExpiredPointsAmount)."""
    row = overlay.agg(
        F.coalesce(F.sum(F.when(F.col("TierCode") != F.col("PreviousTierCode"), 1).otherwise(0)), F.lit(0)).alias("TierChanges"),
        F.coalesce(F.sum("ExpiredPoints"), F.lit(0)).alias("ExpiredPoints"),
    ).collect()[0]
    return int(row["TierChanges"]), row["ExpiredPoints"]


def deriveTierMovement(overlay: DataFrame) -> DataFrame:
    """Data Flow 'Publish Loyalty Overlay' / Derived Column 'Derive Tier Movement'."""
    return overlay.withColumn(
        "TierMovementCode",
        F.when(F.col("TierCode") == F.col("PreviousTierCode"), "SAME")
         .when(F.col("PreviousTierCode") == "NONE", "NEW")
         .otherwise("CHANGED"),
    )
