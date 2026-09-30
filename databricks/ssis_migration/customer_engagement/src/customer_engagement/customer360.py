"""C360_Build_* and C360_Publish_Segments: the Customer 360 build chain over the legacy DW facts."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from customer_engagement import regions, tables
from customer_engagement.config import CeConfig

IDENTITY_MATCH_THRESHOLD = 85
ROLLING_WINDOW_MONTHS = 12
INACTIVE_DAYS = 180
ORDER_GAP_MULTIPLIER = 2.0
BASKET_DECLINE_RATIO = 0.7
RETURN_RATE_THRESHOLD = 15.0
HIGH_RISK_MIN_SCORE = 60
MEDIUM_RISK_MIN_SCORE = 30
CHURN_RULE_SET_VERSION = "2024.1"
SEGMENT_MODEL_VERSION = "C360-SEG-2024.1"
OVERLAY_EXPIRY_MONTHS = {"NA": 24, "EU": 12, "APAC": 18}
PREMIUM_TIERS = ("PLATINUM", "GOLD", "PREMIER", "DIAMOND")


# --------------------------------------------------------------------------------------------
# Legacy DW views (the C360 packages read Fact.Sale / Fact.Payment / Dimension.Customer)
# --------------------------------------------------------------------------------------------


def saleView(sale: DataFrame) -> DataFrame:
    """Fact.Sale with the C360 column contract; falls back to the WWI columns where the SSIS-era
    columns ([Invoice Number], [Net Amount], [Gross Amount], [Region Code]) are unpopulated."""
    return sale.select(
        F.col("Customer Key").alias("CustomerKey"),
        F.coalesce(F.col("Invoice Number"), F.col("WWI Invoice ID").cast("string")).alias("InvoiceNumber"),
        F.col("Invoice Date Key").alias("InvoiceDate"),
        F.coalesce(F.col("Net Amount"), F.col("Total Excluding Tax")).cast("decimal(18,2)").alias("NetAmount"),
        F.coalesce(F.col("Gross Amount"), F.col("Total Including Tax")).cast("decimal(18,2)").alias("GrossAmount"),
        F.col("Total Excluding Tax").cast("decimal(18,2)").alias("TotalExcludingTax"),
        F.col("Tax Amount").cast("decimal(18,2)").alias("TaxAmount"),
        F.col("Quantity").cast("int").alias("Quantity"),
        F.col("Stock Item Key").alias("StockItemKey"),
        F.coalesce(F.col("Gross Margin Amount"), F.col("Profit")).cast("decimal(18,2)").alias("MarginAmount"),
        F.coalesce(F.col("Net Amount Reporting"), F.col("Total Excluding Tax Reporting"), F.col("Total Excluding Tax")).cast("decimal(18,2)").alias("NetAmountReporting"),
        F.coalesce(F.col("FX Rate To Reporting"), F.lit(1)).cast("decimal(19,9)").alias("FxRateToReporting"),
        F.coalesce(F.col("Correction Type Code"), F.lit("ORIG")).alias("CorrectionTypeCode"),
        F.upper(F.trim(F.col("Region Code"))).alias("SaleRegionCode"),
    )


def customerView(customer: DataFrame, defaultRegion: str) -> DataFrame:
    return customer.select(
        F.col("Customer Key").alias("CustomerKey"),
        F.col("WWI Customer ID").alias("CustomerId"),
        F.col("Customer").alias("CustomerName"),
        F.col("Category").alias("CategoryName"),
        F.col("Buying Group").alias("BuyingGroupName"),
        F.col("Postal Code").alias("PostalCode"),
        regions.normaliseRegion(F.col("Region Code"), defaultRegion).alias("RegionCode"),
        F.col("Country Code").alias("CountryCode"),
        F.col("Primary Contact Email").alias("ContactEmail"),
        F.col("Phone Number Standardized").alias("ContactPhone"),
        F.col("Marketing Consent Flag").alias("MarketingConsentFlag"),
        F.col("Consent Captured On").alias("ConsentCapturedDate"),
        F.coalesce(F.col("VAT Registration Number"), F.col("GST Registration Number")).alias("TaxRegistrationNumber"),
        F.coalesce(F.col("Is On Credit Hold"), F.lit(False)).alias("IsOnCreditHold"),
        F.col("Credit Limit Amount").alias("CreditLimitAmount"),
        F.col("Valid From").alias("ValidFrom"),
        F.col("Valid To").alias("ValidTo"),
        regions.isCurrentDimensionRow(F.col("Is Current Row"), F.col("Valid To")).alias("IsCurrentRow"),
        F.col("Customer Segment Key").alias("CustomerSegmentKey"),
        F.col("Erasure Requested On").alias("ErasureRequestedOn"),
        F.col("Account Manager Employee Key").alias("AccountManagerEmployeeKey"),
    )


# --------------------------------------------------------------------------------------------
# C360_Build_CustomerProfile
# --------------------------------------------------------------------------------------------


def standardiseAddresses(customers: DataFrame) -> DataFrame:
    """'Standardise Regional Addresses': postcode / e-mail / name normalisation per region."""
    postal = F.upper(F.trim(F.col("PostalCode")))
    naZip5 = F.regexp_extract(postal, r"^(\d{5})", 1)
    ukOutward = F.regexp_replace(postal, r"\s+", "")
    standardPostal = F.when(F.col("RegionCode") == "NA", F.when(naZip5 != "", naZip5).otherwise(postal)).when(F.col("RegionCode") == "EU", ukOutward).otherwise(postal)
    normalisedName = F.upper(F.trim(F.regexp_replace(F.col("CustomerName"), r"\s+", " ")))
    householdKey = F.regexp_replace(normalisedName, r"[^A-Z0-9]", "")
    return customers.select(
        "CustomerId",
        "CustomerKey",
        "RegionCode",
        "CountryCode",
        F.col("PostalCode").alias("RawPostalCode"),
        standardPostal.alias("StandardPostalCode"),
        F.lower(F.trim(F.col("ContactEmail"))).alias("StandardEmail"),
        F.upper(F.regexp_replace(F.coalesce(F.col("TaxRegistrationNumber"), F.lit("")), r"[^A-Z0-9]", "")).alias("TaxRegistrationNumber"),
        normalisedName.alias("CustomerNameNormalised"),
        householdKey.alias("HouseholdNameKey"),
        F.regexp_replace(F.coalesce(F.col("ContactPhone"), F.lit("")), r"[^0-9]", "").alias("PhoneDigits"),
    ).withColumn("TaxRegistrationNumber", regions.nullIfBlank(F.col("TaxRegistrationNumber")))


def resolveIdentityGraph(standardised: DataFrame, threshold: int = IDENTITY_MATCH_THRESHOLD) -> DataFrame:
    """'Resolve Customer Identity Graph': survivor = lowest CustomerId among rows sharing tax number,
    e-mail, or name+postcode; MatchScore = 100 / 95 / 90 for those match types."""
    matchCols = ["CustomerId", "TaxRegistrationNumber", "StandardEmail", "CustomerNameNormalised", "StandardPostalCode"]
    a = standardised.select(*[F.col(c).alias(f"a_{c}") for c in matchCols])
    b = standardised.select(*[F.col(c).alias(f"b_{c}") for c in matchCols])
    sameTax = F.col("a_TaxRegistrationNumber").isNotNull() & (F.col("a_TaxRegistrationNumber") == F.col("b_TaxRegistrationNumber"))
    sameEmail = F.col("a_StandardEmail").isNotNull() & (F.col("a_StandardEmail") == F.col("b_StandardEmail"))
    sameNamePostal = (
        (F.col("a_CustomerNameNormalised") == F.col("b_CustomerNameNormalised"))
        & F.col("a_StandardPostalCode").isNotNull()
        & (F.col("a_StandardPostalCode") == F.col("b_StandardPostalCode"))
    )
    score = F.when(sameTax, 100).when(sameEmail, 95).when(sameNamePostal, 90).otherwise(0)
    pairs = a.join(b, (F.col("b_CustomerId") <= F.col("a_CustomerId")) & (sameTax | sameEmail | sameNamePostal), "inner")
    graph = pairs.groupBy(F.col("a_CustomerId").alias("CustomerId")).agg(F.min("b_CustomerId").alias("SurvivingCustomerId"), F.max(score).alias("MatchScore"))
    return graph.where(F.col("MatchScore") >= threshold)


def summariseSales(sales: DataFrame) -> DataFrame:
    return sales.groupBy("CustomerKey").agg(
        F.min("InvoiceDate").alias("FirstOrderDate"),
        F.max("InvoiceDate").alias("LastOrderDate"),
        F.countDistinct("InvoiceNumber").cast("int").alias("LifetimeOrderCount"),
        F.sum("NetAmount").cast("decimal(18,2)").alias("LifetimeNetAmount"),
    )


def summarisePayments(payments: DataFrame) -> DataFrame:
    return payments.groupBy(F.col("Customer Key").alias("CustomerKey")).agg(
        F.max("Payment Date Key").alias("LastPaymentDate"),
        F.sum("Unallocated Amount").cast("decimal(18,2)").alias("OpenBalanceAmount"),
        F.avg("Days To Pay").cast("decimal(9,2)").alias("AveragePaymentDays"),
    )


def buildCustomerProfile(customers: DataFrame, salesSummary: DataFrame, paymentSummary: Optional[DataFrame], identityGraph: DataFrame, asOf: date, now) -> DataFrame:
    """'Derive Profile Attributes' + 'Assign Households' over current, real customers."""
    current = customers.where((F.col("ValidTo") > F.lit(now)) & (F.col("CustomerId") > 0))
    profile = current.join(salesSummary, on="CustomerKey", how="left")
    if paymentSummary is not None:
        profile = profile.join(paymentSummary, on="CustomerKey", how="left")
    else:
        profile = (
            profile.withColumn("LastPaymentDate", F.lit(None).cast("date"))
            .withColumn("OpenBalanceAmount", F.lit(None).cast("decimal(18,2)"))
            .withColumn("AveragePaymentDays", F.lit(None).cast("decimal(9,2)"))
        )
    profile = profile.join(identityGraph.select("CustomerId", "SurvivingCustomerId", "MatchScore"), on="CustomerId", how="left")
    std = standardiseAddresses(customers).select("CustomerId", "StandardPostalCode", "HouseholdNameKey")
    profile = profile.join(std, on="CustomerId", how="left")
    region = F.col("RegionCode")
    contactable = (
        F.when(region == "EU", F.coalesce(F.col("MarketingConsentFlag"), F.lit(False)))
        .when(
            region == "APAC",
            F.when(F.col("ConsentCapturedDate").isNull(), F.lit(False)).otherwise(F.datediff(F.lit(asOf), F.col("ConsentCapturedDate")) <= 730),
        )
        .otherwise(F.lit(True))
    )
    email = F.col("ContactEmail")
    masked = F.when(email.isNull(), F.lit(None).cast("string")).otherwise(F.concat(F.substring(email, 1, 1), F.lit("***"), F.regexp_extract(email, r"(@.*)$", 1)))
    orderCount = F.coalesce(F.col("LifetimeOrderCount"), F.lit(0))
    derived = (
        profile.withColumn("IsContactable", contactable)
        .withColumn("MaskedContactEmail", F.when(region == "EU", masked).otherwise(email))
        .withColumn("MasterCustomerId", F.coalesce(F.col("SurvivingCustomerId"), F.col("CustomerId")))
        .withColumn("TenureDays", F.when(F.col("FirstOrderDate").isNull(), F.lit(0)).otherwise(F.datediff(F.lit(asOf), F.col("FirstOrderDate"))))
        .withColumn(
            "AverageOrderValue",
            F.when(orderCount == 0, F.lit(0)).otherwise(F.col("LifetimeNetAmount") / orderCount).cast("decimal(18,2)"),
        )
        .withColumn("LifetimeOrderCount", orderCount)
        .withColumn("LifetimeNetAmount", F.coalesce(F.col("LifetimeNetAmount"), F.lit(0)).cast("decimal(18,2)"))
        .withColumn("ProfileAsOfDate", F.lit(asOf))
    )
    householdWindow = Window.orderBy("StandardPostalCode", "HouseholdNameKey")
    return derived.withColumn("HouseholdId", F.dense_rank().over(householdWindow).cast("bigint")).select(
        "CustomerKey",
        "CustomerId",
        "MasterCustomerId",
        "HouseholdId",
        "CustomerName",
        "CategoryName",
        "BuyingGroupName",
        "RegionCode",
        "CountryCode",
        "StandardPostalCode",
        "MaskedContactEmail",
        "IsContactable",
        "MarketingConsentFlag",
        "ConsentCapturedDate",
        "IsOnCreditHold",
        "CreditLimitAmount",
        "FirstOrderDate",
        "LastOrderDate",
        "LifetimeOrderCount",
        "LifetimeNetAmount",
        "AverageOrderValue",
        "TenureDays",
        "LastPaymentDate",
        "OpenBalanceAmount",
        "AveragePaymentDays",
        "MatchScore",
        "ProfileAsOfDate",
    )


def runBuildCustomerProfile(spark: SparkSession, cfg: CeConfig, asOf: date) -> Tuple[int, int]:
    now = tables.utcNow()
    customers = customerView(spark.table(cfg.dw("dimension", "Customer")), cfg.defaultRegion)
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    payments = tables.readTableOrNone(spark, cfg.dw("fact", "Payment"))
    standardised = standardiseAddresses(customers.where(F.col("CustomerId") > 0))
    tables.overwriteTable(standardised, cfg.table(tables.WORK_CUSTOMER_ADDRESS_STANDARDISED))
    graph = resolveIdentityGraph(spark.table(cfg.table(tables.WORK_CUSTOMER_ADDRESS_STANDARDISED)))
    tables.overwriteTable(graph, cfg.table(tables.WORK_CUSTOMER_IDENTITY_GRAPH))
    profile = buildCustomerProfile(
        customers,
        summariseSales(sales),
        summarisePayments(payments) if payments is not None else None,
        spark.table(cfg.table(tables.WORK_CUSTOMER_IDENTITY_GRAPH)),
        asOf,
        now,
    ).withColumn("LoadedAtUtc", F.lit(now).cast("timestamp"))
    tables.overwriteTable(profile, cfg.table(tables.GOLD_C360_CUSTOMER_PROFILE))
    count = spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_PROFILE)).count()
    tables.logPackageRun(spark, cfg.table(tables.ETL_PACKAGE_RUN), "C360_Build_CustomerProfile", cfg.runId, count, 0, f"as_of={asOf}")
    return count, 0


# --------------------------------------------------------------------------------------------
# C360_Build_LoyaltyOverlay
# --------------------------------------------------------------------------------------------


def buildQualifyingSales(sales: DataFrame, customers: DataFrame) -> DataFrame:
    """work.LoyaltyQualifyingSale: one row per invoice with the regional amount basis."""
    joined = sales.join(customers.select("CustomerKey", "CustomerId", "RegionCode"), on="CustomerKey", how="inner")
    return (
        joined.where(F.col("CustomerId") > 0)
        .groupBy("CustomerId", "RegionCode", "InvoiceNumber")
        .agg(
            F.min("InvoiceDate").alias("InvoiceDate"),
            F.sum("GrossAmount").cast("decimal(18,2)").alias("GrossAmount"),
            F.sum("NetAmount").cast("decimal(18,2)").alias("NetAmount"),
            F.sum(F.when(F.col("RegionCode") == "APAC", F.col("TaxAmount")).otherwise(F.lit(0))).cast("decimal(18,2)").alias("GstAmount"),
        )
    )


def qualifyingAmount(regionCol, gross, net, gst):  # type: ignore[no-untyped-def]
    return F.when(regionCol == "NA", gross).when(regionCol == "EU", net).when(regionCol == "APAC", gross - F.coalesce(gst, F.lit(0))).otherwise(net)


def pointsAccrued(regionCol, gross, net, gst):  # type: ignore[no-untyped-def]
    return (
        F.when(regionCol == "NA", F.floor(gross))
        .when(regionCol == "EU", F.floor(net * 1.5))
        .when(regionCol == "APAC", F.floor((gross - F.coalesce(gst, F.lit(0))) * 2))
        .otherwise(F.floor(net))
        .cast("int")
    )


def expireAgedPoints(ledger: DataFrame, asOf: date, now, expiryMonths=OVERLAY_EXPIRY_MONTHS) -> DataFrame:
    """'Expire Aged Points': ACTIVE rows older than the regional expiry window become EXPIRED."""
    cutoff = (
        F.when(F.col("RegionCode") == "NA", F.add_months(F.lit(asOf), -expiryMonths["NA"]))
        .when(F.col("RegionCode") == "EU", F.add_months(F.lit(asOf), -expiryMonths["EU"]))
        .when(F.col("RegionCode") == "APAC", F.add_months(F.lit(asOf), -expiryMonths["APAC"]))
    )
    expiring = (F.col("PointStatusCode") == "ACTIVE") & cutoff.isNotNull() & (F.col("AccruedDate") < cutoff)
    return ledger.withColumn("ExpiredAtUtc", F.when(expiring, F.lit(now).cast("timestamp")).otherwise(F.col("ExpiredAtUtc"))).withColumn(
        "PointStatusCode", F.when(expiring, F.lit("EXPIRED")).otherwise(F.col("PointStatusCode"))
    )


def accrueNewPoints(ledger: Optional[DataFrame], qualifying: DataFrame, now) -> DataFrame:
    """'Accrue New Points': insert one ledger row per qualifying invoice not yet in the ledger."""
    candidates = qualifying.select(
        "CustomerId",
        "RegionCode",
        F.col("InvoiceNumber").alias("SourceReference"),
        F.col("InvoiceDate").alias("AccruedDate"),
        qualifyingAmount(F.col("RegionCode"), F.col("GrossAmount"), F.col("NetAmount"), F.col("GstAmount")).cast("decimal(18,2)").alias("QualifyingAmount"),
        pointsAccrued(F.col("RegionCode"), F.col("GrossAmount"), F.col("NetAmount"), F.col("GstAmount")).alias("PointsAccrued"),
        F.lit("ACTIVE").alias("PointStatusCode"),
        F.lit(None).cast("timestamp").alias("ExpiredAtUtc"),
        F.lit(now).cast("timestamp").alias("CreatedAtUtc"),
    )
    if ledger is None:
        return candidates
    return candidates.join(ledger.select("CustomerId", "SourceReference"), on=["CustomerId", "SourceReference"], how="left_anti")


def buildLoyaltyOverlay(ledger: DataFrame, previousOverlay: Optional[DataFrame], asOf: date) -> DataFrame:
    """'Aggregate Loyalty Position' + 'Recalculate Tier Ladders' + tier movement."""
    position = ledger.groupBy("CustomerId", "RegionCode").agg(
        F.sum(F.when(F.col("PointStatusCode") == "ACTIVE", F.col("PointsAccrued")).otherwise(0)).cast("int").alias("ActivePoints"),
        F.sum(F.when(F.col("PointStatusCode") == "EXPIRED", F.col("PointsAccrued")).otherwise(0)).cast("int").alias("ExpiredPoints"),
        F.sum("QualifyingAmount").cast("decimal(18,2)").alias("LifetimeQualifyingAmount"),
        F.max("AccruedDate").alias("LastAccrualDate"),
    )
    if previousOverlay is not None:
        prev = previousOverlay.groupBy("CustomerId").agg(F.max(F.coalesce(F.col("TierCode"), F.lit("NONE"))).alias("PreviousTierCode"))
        position = position.join(prev, on="CustomerId", how="left")
    position = position.withColumn("PreviousTierCode", F.coalesce(F.col("PreviousTierCode") if previousOverlay is not None else F.lit(None), F.lit("NONE")))
    position = position.withColumn("TierCode", regions.overlayTierCode(F.col("RegionCode"), F.col("ActivePoints")))
    return position.withColumn(
        "TierMovementCode",
        F.when(F.col("PreviousTierCode") == "NONE", "NEW").when(F.col("PreviousTierCode") == F.col("TierCode"), "UNCHANGED").otherwise("CHANGED"),
    ).withColumn("OverlayAsOfDate", F.lit(asOf))


def runBuildLoyaltyOverlay(spark: SparkSession, cfg: CeConfig, asOf: date) -> Tuple[int, int]:
    now = tables.utcNow()
    customers = customerView(spark.table(cfg.dw("dimension", "Customer")), cfg.defaultRegion).where(F.col("IsCurrentRow"))
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    qualifying = buildQualifyingSales(sales, customers)
    ledgerFqn = cfg.table(tables.WORK_LOYALTY_POINT_LEDGER)
    ledger = tables.readTableOrNone(spark, ledgerFqn)
    newRows = accrueNewPoints(ledger, qualifying, now)
    combined = newRows if ledger is None else ledger.select(*newRows.columns).unionByName(newRows)
    updated = expireAgedPoints(combined, asOf, now)
    ledgerCount = updated.count()
    tables.overwriteTable(updated, ledgerFqn)
    overlayFqn = cfg.table(tables.GOLD_C360_LOYALTY_OVERLAY)
    previous = tables.readTableOrNone(spark, overlayFqn)
    overlay = buildLoyaltyOverlay(spark.table(ledgerFqn), previous, asOf).withColumn("LoadedAtUtc", F.lit(now).cast("timestamp"))
    count = overlay.count()
    tables.overwriteTable(overlay, overlayFqn)
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "C360_Build_LoyaltyOverlay",
        cfg.runId,
        count,
        0,
        f"ledger_rows={ledgerCount} as_of={asOf}",
    )
    return count, 0


# --------------------------------------------------------------------------------------------
# C360_Build_RollingMetrics
# --------------------------------------------------------------------------------------------


def fourFourFivePeriod(asOf: date) -> Tuple[date, date]:
    """Latest complete 4-4-5 period (calendar-year anchored, 4/4/5-week quarters) ending on or before asOf."""
    yearStart = date(asOf.year, 1, 1)
    lengths = [4, 4, 5] * 4
    start = yearStart
    periods = []
    for weeks in lengths:
        end = start + timedelta(weeks=weeks) - timedelta(days=1)
        periods.append((start, end))
        start = end + timedelta(days=1)
    complete = [p for p in periods if p[1] <= asOf]
    if complete:
        return complete[-1]
    prevYear = date(asOf.year - 1, 1, 1)
    start = prevYear
    prevPeriods = []
    for weeks in lengths:
        end = start + timedelta(weeks=weeks) - timedelta(days=1)
        prevPeriods.append((start, end))
        start = end + timedelta(days=1)
    return prevPeriods[-1]


def buildRollingMetrics(sales: DataFrame, customers: DataFrame, asOf: date, windowMonths: int = ROLLING_WINDOW_MONTHS, inactiveDays: int = INACTIVE_DAYS) -> DataFrame:
    """'Aggregate Rolling Window' + APAC 4-4-5 realignment + 'Derive Rolling Ratios' + regional deciles."""
    windowEnd = asOf
    windowStart = (F.add_months(F.lit(asOf), -windowMonths)).cast("date")
    apacStart, apacEnd = fourFourFivePeriod(asOf)
    cust = customers.where(F.col("IsCurrentRow") & (F.col("CustomerId") > 0)).select("CustomerKey", "CustomerId", "RegionCode")
    joined = sales.join(cust, on="CustomerKey", how="inner").where((F.col("InvoiceDate") >= windowStart) & (F.col("InvoiceDate") <= F.lit(windowEnd)))
    agg = joined.groupBy("CustomerKey", "CustomerId", "RegionCode").agg(
        F.countDistinct("InvoiceNumber").cast("int").alias("OrderCount"),
        F.sum("TotalExcludingTax").cast("decimal(18,2)").alias("NetRevenue"),
        F.sum(F.when(F.col("Quantity") < 0, F.abs(F.col("TotalExcludingTax"))).otherwise(F.lit(0))).cast("decimal(18,2)").alias("ReturnAmount"),
        F.countDistinct("StockItemKey").cast("int").alias("DistinctItemCount"),
        F.max("InvoiceDate").alias("LastOrderDate"),
    )
    realigned = agg.withColumn("WindowStartDate", F.when(F.col("RegionCode") == "APAC", F.lit(apacStart)).otherwise(windowStart)).withColumn(
        "WindowEndDate", F.when(F.col("RegionCode") == "APAC", F.lit(apacEnd)).otherwise(F.lit(windowEnd))
    )
    orderCount = F.col("OrderCount")
    ratios = (
        realigned.withColumn("AverageBasketAmount", F.when(orderCount == 0, F.lit(0)).otherwise(F.col("NetRevenue") / orderCount).cast("decimal(18,2)"))
        .withColumn(
            "ReturnRatePercent",
            F.when(F.col("NetRevenue") == 0, F.lit(0)).otherwise(F.col("ReturnAmount") * 100 / F.col("NetRevenue")).cast("decimal(18,2)"),
        )
        .withColumn(
            "DaysSinceLastOrder",
            F.when(F.col("LastOrderDate").isNull(), F.lit(9999)).otherwise(F.datediff(F.col("WindowEndDate"), F.col("LastOrderDate"))),
        )
        .withColumn("OrdersPerMonth", (orderCount.cast("decimal(18,4)") / F.lit(windowMonths)).cast("decimal(18,4)"))
        .withColumn("ActivityStatusCode", F.when(F.col("DaysSinceLastOrder") > inactiveDays, "INACTIVE").otherwise("ACTIVE"))
        .withColumn("RollingWindowMonths", F.lit(windowMonths))
    )
    byRegion = Window.partitionBy("RegionCode")
    return (
        ratios.withColumn("RecencyDecile", F.ntile(10).over(byRegion.orderBy(F.col("DaysSinceLastOrder").desc())))
        .withColumn("FrequencyDecile", F.ntile(10).over(byRegion.orderBy("OrderCount")))
        .withColumn("MonetaryDecile", F.ntile(10).over(byRegion.orderBy("NetRevenue")))
        .withColumn("MetricAsOfDate", F.lit(asOf))
    )


def runBuildRollingMetrics(spark: SparkSession, cfg: CeConfig, asOf: date) -> Tuple[int, int]:
    now = tables.utcNow()
    customers = customerView(spark.table(cfg.dw("dimension", "Customer")), cfg.defaultRegion)
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    metrics = buildRollingMetrics(sales, customers, asOf).withColumn("LoadedAtUtc", F.lit(now).cast("timestamp"))
    tables.overwriteTable(metrics, cfg.table(tables.GOLD_C360_CUSTOMER_ROLLING_METRIC))
    count = spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_ROLLING_METRIC)).count()
    tables.logPackageRun(spark, cfg.table(tables.ETL_PACKAGE_RUN), "C360_Build_RollingMetrics", cfg.runId, count, 0, f"as_of={asOf}")
    return count, 0


# --------------------------------------------------------------------------------------------
# C360_Build_ChurnFlags
# --------------------------------------------------------------------------------------------


def priorWindowBasket(sales: DataFrame, asOf: date, windowMonths: int = ROLLING_WINDOW_MONTHS) -> DataFrame:
    """Average basket in the window immediately before the rolling window (feeds PriorAverageBasketAmount)."""
    priorStart = F.add_months(F.lit(asOf), -2 * windowMonths).cast("date")
    priorEnd = F.add_months(F.lit(asOf), -windowMonths).cast("date")
    prior = (
        sales.where((F.col("InvoiceDate") >= priorStart) & (F.col("InvoiceDate") < priorEnd))
        .groupBy("CustomerKey")
        .agg(F.countDistinct("InvoiceNumber").alias("priorOrders"), F.sum("TotalExcludingTax").alias("priorNet"))
    )
    return prior.select(
        "CustomerKey",
        F.when(F.col("priorOrders") == 0, F.lit(0)).otherwise(F.col("priorNet") / F.col("priorOrders")).cast("decimal(18,2)").alias("PriorAverageBasketAmount"),
    )


def buildChurnFeatureSet(profile: DataFrame, rolling: DataFrame, priorBasket: DataFrame, overlay: Optional[DataFrame]) -> DataFrame:
    """work.ChurnFeatureSet assembled from the profile, rolling metrics, prior-window basket and loyalty overlay."""
    features = (
        rolling.select(
            "CustomerKey",
            "CustomerId",
            "RegionCode",
            "DaysSinceLastOrder",
            "AverageBasketAmount",
            "ReturnRatePercent",
            "MonetaryDecile",
            "FrequencyDecile",
            "RecencyDecile",
            "ActivityStatusCode",
        )
        .join(
            profile.select("CustomerKey", "FirstOrderDate", "LastOrderDate", "LifetimeOrderCount", "IsOnCreditHold", "IsContactable"),
            on="CustomerKey",
            how="left",
        )
        .join(priorBasket, on="CustomerKey", how="left")
    )
    gap = F.when(
        F.coalesce(F.col("LifetimeOrderCount"), F.lit(0)) > 1,
        F.datediff(F.col("LastOrderDate"), F.col("FirstOrderDate")) / (F.col("LifetimeOrderCount") - 1),
    ).otherwise(F.lit(0.0))
    features = features.withColumn("AverageOrderGapDays", gap.cast("decimal(18,2)")).withColumn(
        "PriorAverageBasketAmount", F.coalesce(F.col("PriorAverageBasketAmount"), F.lit(0)).cast("decimal(18,2)")
    )
    if overlay is not None:
        tiers = overlay.groupBy("CustomerId").agg(F.max("TierCode").alias("TierCode"), F.max("PreviousTierCode").alias("PreviousTierCode"))
        features = features.join(tiers, on="CustomerId", how="left")
    else:
        features = features.withColumn("TierCode", F.lit(None).cast("string")).withColumn("PreviousTierCode", F.lit(None).cast("string"))
    return (
        features.withColumn("TierCode", F.coalesce(F.col("TierCode"), F.lit("NONE")))
        .withColumn("PreviousTierCode", F.coalesce(F.col("PreviousTierCode"), F.lit("NONE")))
        .withColumn("IsOnCreditHold", F.coalesce(F.col("IsOnCreditHold"), F.lit(False)))
    )


def scoreChurn(features: DataFrame, orderGapMultiplier: float = ORDER_GAP_MULTIPLIER, ruleSetVersion: str = CHURN_RULE_SET_VERSION) -> DataFrame:
    """'Score Churn Rules': the five weighted rules and the risk band, thresholds as encoded in the package."""
    scored = (
        features.withColumn(
            "RuleOrderGapScore",
            F.when((F.col("AverageOrderGapDays") > 0) & (F.col("DaysSinceLastOrder") > F.col("AverageOrderGapDays") * orderGapMultiplier), 30).otherwise(0),
        )
        .withColumn(
            "RuleBasketDeclineScore",
            F.when(
                (F.col("PriorAverageBasketAmount") > 0) & (F.col("AverageBasketAmount") < F.col("PriorAverageBasketAmount") * BASKET_DECLINE_RATIO),
                20,
            ).otherwise(0),
        )
        .withColumn("RuleReturnsScore", F.when(F.col("ReturnRatePercent") > RETURN_RATE_THRESHOLD, 15).otherwise(0))
        .withColumn(
            "RuleTierLapseScore",
            F.when((F.col("TierCode") != F.col("PreviousTierCode")) & (F.col("PreviousTierCode") != "NONE"), 15).otherwise(0),
        )
        .withColumn("RuleCreditHoldScore", F.when(F.col("IsOnCreditHold"), 25).otherwise(0))
    )
    total = F.col("RuleOrderGapScore") + F.col("RuleBasketDeclineScore") + F.col("RuleReturnsScore") + F.col("RuleTierLapseScore") + F.col("RuleCreditHoldScore")
    return (
        scored.withColumn("ChurnRiskScore", total.cast("int"))
        .withColumn(
            "ChurnRiskBand",
            F.when(F.col("ChurnRiskScore") >= HIGH_RISK_MIN_SCORE, "HIGH").when(F.col("ChurnRiskScore") >= MEDIUM_RISK_MIN_SCORE, "MEDIUM").otherwise("LOW"),
        )
        .withColumn("IsHighRisk", F.col("ChurnRiskBand") == "HIGH")
        .withColumn("RuleSetVersion", F.lit(ruleSetVersion))
    )


def runBuildChurnFlags(spark: SparkSession, cfg: CeConfig, asOf: date) -> Tuple[int, int]:
    now = tables.utcNow()
    profile = spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_PROFILE))
    rolling = spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_ROLLING_METRIC))
    overlay = tables.readTableOrNone(spark, cfg.table(tables.GOLD_C360_LOYALTY_OVERLAY))
    sales = saleView(spark.table(cfg.dw("fact", "Sale")))
    features = buildChurnFeatureSet(profile, rolling, priorWindowBasket(sales, asOf), overlay)
    flags = scoreChurn(features).withColumn("ScoredAtUtc", F.lit(now).cast("timestamp")).withColumn("ScoreAsOfDate", F.lit(asOf))
    tables.overwriteTable(flags, cfg.table(tables.GOLD_C360_CUSTOMER_CHURN_FLAG))
    saved = spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_CHURN_FLAG))
    highRisk = saved.where(F.col("IsHighRisk")).select(
        "CustomerId",
        "RegionCode",
        "ChurnRiskScore",
        F.lit("CHURN_HIGH_RISK").alias("OutreachReasonCode"),
        F.lit(now).cast("timestamp").alias("QueuedAtUtc"),
        F.lit("PENDING").alias("QueueStatusCode"),
    )
    queued = tables.appendInsertOnly(spark, highRisk, cfg.table(tables.WORK_CUSTOMER_OUTREACH_QUEUE), ["CustomerId", "OutreachReasonCode"])
    count = saved.count()
    tables.logPackageRun(spark, cfg.table(tables.ETL_PACKAGE_RUN), "C360_Build_ChurnFlags", cfg.runId, count, 0, f"high_risk_queued={queued} as_of={asOf}")
    return count, 0


# --------------------------------------------------------------------------------------------
# C360_Publish_Segments
# --------------------------------------------------------------------------------------------


def assignSegments(
    profile: DataFrame,
    rolling: DataFrame,
    churn: DataFrame,
    overlay: Optional[DataFrame],
    previousSegments: Optional[DataFrame],
    modelVersion: str = SEGMENT_MODEL_VERSION,
    publishSuppressed: bool = False,
) -> DataFrame:
    """'Assign Segments' precedence + suppression + segment movement against the previous publish."""
    base = (
        profile.select("CustomerKey", "CustomerId", "RegionCode", "IsContactable")
        .join(
            rolling.select("CustomerKey", "RecencyDecile", "FrequencyDecile", "MonetaryDecile", "ActivityStatusCode"),
            on="CustomerKey",
            how="left",
        )
        .join(churn.select("CustomerKey", "ChurnRiskBand", "ChurnRiskScore"), on="CustomerKey", how="left")
    )
    if overlay is not None:
        base = base.join(overlay.groupBy("CustomerId").agg(F.max("TierCode").alias("TierCode")), on="CustomerId", how="left")
    else:
        base = base.withColumn("TierCode", F.lit(None).cast("string"))
    highChurn = F.col("ChurnRiskBand") == "HIGH"
    segment = (
        F.when((F.col("RegionCode") == "EU") & (~F.coalesce(F.col("IsContactable"), F.lit(False))), "SUPPRESSED")
        .when(highChurn & (F.col("MonetaryDecile") >= 8), "AT_RISK_HIGH_VALUE")
        .when(highChurn, "AT_RISK")
        .when((F.col("MonetaryDecile") >= 9) & (F.col("FrequencyDecile") >= 8), "CHAMPION")
        .when((F.col("RecencyDecile") >= 8) & (F.col("FrequencyDecile") <= 3), "NEW_PROMISING")
        .when(F.col("TierCode").isin(*PREMIUM_TIERS), "LOYAL_PREMIUM")
        .when(F.col("ActivityStatusCode") == "INACTIVE", "DORMANT")
        .otherwise("CORE")
    )
    assigned = base.withColumn("SegmentCode", segment).withColumn("IsSuppressed", F.col("SegmentCode") == "SUPPRESSED")
    if not publishSuppressed:
        assigned = assigned.where(~F.col("IsSuppressed"))
    if previousSegments is not None:
        prev = previousSegments.select("CustomerId", F.col("SegmentCode").alias("PreviousSegmentCode"))
        assigned = assigned.join(prev, on="CustomerId", how="left")
    else:
        assigned = assigned.withColumn("PreviousSegmentCode", F.lit(None).cast("string"))
    return assigned.withColumn(
        "SegmentMovementCode",
        F.when(F.col("PreviousSegmentCode").isNull(), "NEW").when(F.col("PreviousSegmentCode") == F.col("SegmentCode"), "UNCHANGED").otherwise("CHANGED"),
    ).withColumn("SegmentModelVersion", F.lit(modelVersion))


def runPublishSegments(spark: SparkSession, cfg: CeConfig, asOf: date, publishSuppressed: bool = False) -> Tuple[int, int]:
    now = tables.utcNow()
    segFqn = cfg.table(tables.GOLD_C360_CUSTOMER_SEGMENT)
    previous = tables.readTableOrNone(spark, segFqn)
    if previous is not None:
        tables.overwriteTable(previous.select("CustomerId", "SegmentCode", "AssignedAtUtc"), cfg.table(tables.WORK_CUSTOMER_SEGMENT_PREVIOUS))
        previous = spark.table(cfg.table(tables.WORK_CUSTOMER_SEGMENT_PREVIOUS))
    segments = (
        assignSegments(
            spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_PROFILE)),
            spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_ROLLING_METRIC)),
            spark.table(cfg.table(tables.GOLD_C360_CUSTOMER_CHURN_FLAG)),
            tables.readTableOrNone(spark, cfg.table(tables.GOLD_C360_LOYALTY_OVERLAY)),
            previous,
            publishSuppressed=publishSuppressed,
        )
        .withColumn("AssignedAtUtc", F.lit(now).cast("timestamp"))
        .withColumn("SegmentAsOfDate", F.lit(asOf))
    )
    count = segments.count()
    tables.overwriteTable(segments, segFqn)
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "C360_Publish_Segments",
        cfg.runId,
        count,
        0,
        f"publish_suppressed={publishSuppressed} as_of={asOf}",
    )
    return count, 0
