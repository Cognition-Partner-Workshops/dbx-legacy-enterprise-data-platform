"""C360_Build_CustomerProfile business rules (address standardisation, identity graph,
regional consent, survivorship attributes, households)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

WITHHELD = "WITHHELD"
APAC_CONSENT_MAX_AGE_DAYS = 730
DEFAULT_MATCH_THRESHOLD = 85


def activeCustomer(asOfDate):
    """WHERE c.[Valid To] > SYSDATETIME() AND c.[WWI Customer ID] > 0 (SYSDATETIME -> BusinessDate)."""
    return (F.col("ValidTo") > F.lit(asOfDate).cast("timestamp")) & (F.col("WWICustomerID") > 0)


def buildAddressStandardisationInput(dimCustomer: DataFrame, asOfDate) -> DataFrame:
    """Reconstructs the rows of work.CustomerAddressStandardised from the current rows of
    Dimension.Customer (the legacy feeder of that work table is not in the repository)."""
    return (
        dimCustomer
        .where(activeCustomer(asOfDate))
        .select(
            F.col("WWICustomerID").alias("CustomerId"),
            F.col("RegionCode"),
            F.col("CountryCode"),
            F.col("PostalCode").alias("RawPostalCode"),
            F.col("PrimaryContactEmail").alias("RawEmail"),
            F.coalesce(F.col("VATRegistrationNumber"), F.col("GSTRegistrationNumber")).alias("TaxRegistrationNumber"),
            F.upper(F.regexp_replace(F.col("Customer"), r"[^A-Za-z0-9]", "")).alias("CustomerNameNormalised"),
            F.upper(F.regexp_replace(F.col("Customer"), r"[^A-Za-z0-9]", "")).alias("HouseholdNameKey"),
        )
    )


def standardiseAddresses(addr: DataFrame, standardisedAtUtc=None) -> DataFrame:
    """Execute SQL Task 'Standardise Addresses'.

    NA   -> LEFT(REPLACE(REPLACE(RawPostalCode,'-',''),' ',''),5)
    EU   -> CountryCode + '-' + UPPER(REPLACE(RawPostalCode,' ',''))
    APAC -> UPPER(REPLACE(RawPostalCode,' ',''))
    else -> UPPER(RawPostalCode);  StandardEmail = LOWER(LTRIM(RTRIM(RawEmail)))
    """
    naCode = F.substring(F.regexp_replace(F.col("RawPostalCode"), r"[- ]", ""), 1, 5)
    noSpaces = F.upper(F.regexp_replace(F.col("RawPostalCode"), " ", ""))
    standardPostal = (
        F.when(F.col("RegionCode") == "NA", naCode)
        .when(F.col("RegionCode") == "EU", F.concat(F.col("CountryCode"), F.lit("-"), noSpaces))
        .when(F.col("RegionCode") == "APAC", noSpaces)
        .otherwise(F.upper(F.col("RawPostalCode")))
    )
    ts = F.lit(standardisedAtUtc).cast("timestamp") if standardisedAtUtc is not None else F.current_timestamp()
    return (
        addr.withColumn("StandardPostalCode", standardPostal)
        .withColumn("StandardEmail", F.lower(F.trim(F.col("RawEmail"))))
        .withColumn("StandardisedAtUtc", ts)
    )


def resolveIdentityGraph(addr: DataFrame, matchThreshold: int = DEFAULT_MATCH_THRESHOLD, resolvedAtUtc=None) -> DataFrame:
    """Execute SQL Task 'Resolve Identity Graph' (the USING part of the MERGE plus the
    MatchScore >= @MatchThresholdScore filter). The MERGE target is rebuilt from this frame.

    Self-join a/b on b.CustomerId <= a.CustomerId and any of tax / email / (name+postal);
    SurvivingCustomerId = MIN(b.CustomerId); MatchScore = MAX(100 / 95 / 88 / 0).
    """
    a = addr.alias("a")
    b = addr.alias("b")
    joinCond = (F.col("b.CustomerId") <= F.col("a.CustomerId")) & (
        (F.col("a.TaxRegistrationNumber") == F.col("b.TaxRegistrationNumber"))
        | (F.col("a.StandardEmail") == F.col("b.StandardEmail"))
        | ((F.col("a.CustomerNameNormalised") == F.col("b.CustomerNameNormalised"))
           & (F.col("a.StandardPostalCode") == F.col("b.StandardPostalCode")))
    )
    score = (
        F.when(F.col("a.TaxRegistrationNumber") == F.col("b.TaxRegistrationNumber"), 100)
        .when(F.col("a.StandardEmail") == F.col("b.StandardEmail"), 95)
        .when((F.col("a.CustomerNameNormalised") == F.col("b.CustomerNameNormalised"))
              & (F.col("a.StandardPostalCode") == F.col("b.StandardPostalCode")), 88)
        .otherwise(0)
    )
    ts = F.lit(resolvedAtUtc).cast("timestamp") if resolvedAtUtc is not None else F.current_timestamp()
    return (
        a.join(b, joinCond)
        .groupBy(F.col("a.CustomerId").alias("CustomerId"))
        .agg(F.min("b.CustomerId").alias("SurvivingCustomerId"), F.max(score).alias("MatchScore"))
        .where(F.col("MatchScore") >= F.lit(int(matchThreshold)))
        .withColumn("ResolvedAtUtc", ts)
    )


def buildCustomerSalesSummary(factSale: DataFrame) -> DataFrame:
    """Reconstructed work.CustomerSalesSummary (lifetime figures per Customer Key), using the
    same invoice / revenue definitions as the C360 rolling-window SQL (WWI Invoice ID,
    Total Excluding Tax)."""
    return factSale.groupBy("CustomerKey").agg(
        F.min("InvoiceDateKey").alias("FirstOrderDate"),
        F.max("InvoiceDateKey").alias("LastOrderDate"),
        F.countDistinct("WWIInvoiceID").alias("LifetimeOrderCount"),
        F.sum("TotalExcludingTax").cast(DecimalType(18, 2)).alias("LifetimeNetAmount"),
    )


def buildCustomerPaymentSummary(factPayment: DataFrame) -> DataFrame:
    """Reconstructed work.CustomerPaymentSummary per Customer Key."""
    return factPayment.groupBy("CustomerKey").agg(
        F.max("PaymentDateKey").alias("LastPaymentDate"),
        F.sum(F.coalesce(F.col("UnallocatedAmount"), F.lit(0))).cast(DecimalType(18, 2)).alias("OpenBalanceAmount"),
        F.avg(F.col("DaysToPay").cast(DecimalType(18, 2))).cast(DecimalType(18, 2)).alias("AveragePaymentDays"),
    )


def buildProfileSource(dimCustomer: DataFrame, salesSummary: DataFrame, paymentSummary: DataFrame, asOfDate) -> DataFrame:
    """OLE DB Source 'Dimension Customer' (active, positive WWI customer id, left joins)."""
    c = dimCustomer.where(activeCustomer(asOfDate))
    return (
        c.join(salesSummary, "CustomerKey", "left")
        .join(paymentSummary, "CustomerKey", "left")
        .select(
            F.col("CustomerKey"),
            F.col("WWICustomerID").alias("CustomerId"),
            F.col("Customer").alias("CustomerName"),
            F.col("Category").alias("CustomerCategory"),
            F.col("BuyingGroup"),
            F.col("PostalCode"),
            F.col("RegionCode"),
            F.col("CountryCode"),
            F.col("PrimaryContactEmail").alias("ContactEmail"),
            F.col("PrimaryContactPhone").alias("ContactPhone"),
            F.col("MarketingConsentFlag"),
            F.col("ConsentCapturedOn").cast("date").alias("ConsentCapturedDate"),
            "FirstOrderDate", "LastOrderDate", "LifetimeOrderCount", "LifetimeNetAmount",
            "LastPaymentDate", "OpenBalanceAmount", "AveragePaymentDays",
        )
    )


def buildCustomerProfile(profileSource: DataFrame, identityGraph: DataFrame, asOfDate) -> DataFrame:
    """Data Flow 'Build Customer Profile': Lookup Surviving Identity (ignore no-match) ->
    Apply Regional Consent Rules -> Derive Profile Attributes."""
    today = F.lit(asOfDate).cast("date")
    ig = identityGraph.select("CustomerId", "SurvivingCustomerId", "MatchScore")
    df = profileSource.join(ig, "CustomerId", "left")

    isContactable = (
        F.when(F.col("RegionCode") == "EU",
               F.when(F.col("MarketingConsentFlag").isNull(), F.lit(False)).otherwise(F.col("MarketingConsentFlag").cast("boolean")))
        .when(F.col("RegionCode") == "APAC",
              F.when(F.col("ConsentCapturedDate").isNull(), F.lit(False))
               .otherwise(F.datediff(today, F.col("ConsentCapturedDate")) <= APAC_CONSENT_MAX_AGE_DAYS))
        .otherwise(F.lit(True))
    )
    contactEmail = F.when(
        (F.col("RegionCode") == "EU")
        & (F.col("MarketingConsentFlag").isNull() | (F.col("MarketingConsentFlag").cast("boolean") == F.lit(False))),
        F.lit(WITHHELD),
    ).otherwise(F.col("ContactEmail"))

    return (
        df.withColumn("IsContactable", isContactable)
        .withColumn("ContactEmail", contactEmail)
        .withColumn("MasterCustomerId", F.coalesce(F.col("SurvivingCustomerId"), F.col("CustomerId")))
        .withColumn("TenureDays", F.when(F.col("FirstOrderDate").isNull(), F.lit(0)).otherwise(F.datediff(today, F.col("FirstOrderDate"))))
        .withColumn(
            "AverageOrderValue",
            F.when(F.coalesce(F.col("LifetimeOrderCount"), F.lit(0)) == 0, F.lit(0).cast(DecimalType(18, 2)))
            .otherwise((F.col("LifetimeNetAmount") / F.col("LifetimeOrderCount")).cast(DecimalType(18, 2))),
        )
    )


def assignHouseholds(profile: DataFrame, addr: DataFrame) -> DataFrame:
    """Execute SQL Task 'Assign Households': DENSE_RANK() OVER (ORDER BY StandardPostalCode,
    HouseholdNameKey) joined back on CustomerId (inner join -> profiles without an address
    row are dropped, as in the legacy UPDATE ... INNER JOIN)."""
    w = Window.orderBy("StandardPostalCode", "HouseholdNameKey")
    households = addr.select("CustomerId", F.dense_rank().over(w).alias("HouseholdKey"))
    return profile.join(households, "CustomerId", "inner")


def countDuplicateClusters(identityGraph: DataFrame) -> int:
    return identityGraph.groupBy("SurvivingCustomerId").count().where(F.col("count") > 1).count()
