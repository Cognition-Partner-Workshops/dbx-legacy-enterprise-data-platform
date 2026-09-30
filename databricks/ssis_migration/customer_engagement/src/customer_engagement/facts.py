"""FACT_Load_LoyaltyPoints and FACT_Load_WebSession: silver -> gold transaction facts."""

from __future__ import annotations

from datetime import date
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from customer_engagement import tables
from customer_engagement.config import CeConfig

LOYALTY_WATERMARK = "Fact.Loyalty Points"
WEB_SESSION_WATERMARK = "Fact.Web Session"
UNKNOWN_CUSTOMER_KEY = 0
POINT_CASH_VALUE = {"NA": 0.0100, "EU": 0.0085, "APAC": 0.0060}


def currentCustomerKeys(customerDim: DataFrame) -> DataFrame:
    """Lookup Customer Key: Dimension.Customer WHERE [Is Current Row] = 1 keyed on [WWI Customer ID]."""
    return (
        customerDim.where(F.col("Is Current Row"))
        .select(F.col("Customer Key").alias("CustomerKey"), F.col("WWI Customer ID").cast("string").alias("CustomerBusinessKey"))
        .dropDuplicates(["CustomerBusinessKey"])
    )


def stagePointsFromLedger(silverLedger: DataFrame) -> DataFrame:
    """Project the conformed ledger onto the stg.LoyaltyPoints contract the fact package reads."""
    return silverLedger.select(
        F.col("LoyaltyEntryId").alias("LoyaltyEventBusinessKey"),
        F.col("CustomerId").cast("string").alias("CustomerBusinessKey"),
        F.col("ProgramCode").alias("LoyaltyProgramCode"),
        F.col("EntryTypeCode").alias("EventTypeCode"),
        F.col("EntryDate").alias("EventDate"),
        F.abs(F.col("PointsDelta")).cast("int").alias("PointsQuantity"),
        F.lit(0).cast("decimal(18,2)").alias("QualifyingSpendAmount"),
        F.when(F.col("RegionCode") == "NA", "USD").when(F.col("RegionCode") == "EU", "EUR").otherwise("AUD").alias("TransactionCurrency"),
        F.col("RegionCode"),
        F.col("SourceInvoiceId").cast("string").alias("SourceInvoiceNumber"),
        F.col("LastModifiedAt"),
    )


def deriveLoyaltyPointMeasures(
    stgPoints: DataFrame, customerKeys: DataFrame, dateDim: DataFrame, expiryMonthsNa: int, expiryMonthsEu: int, lineageKey: int
) -> Tuple[DataFrame, DataFrame]:
    """Lookups + 'Derive Points Measures' + 'Route Loyalty Events'. Returns (fact rows, held rows)."""
    joined = stgPoints.join(customerKeys, on="CustomerBusinessKey", how="left")
    dates = dateDim.select(F.col("Date").alias("EventDate"), F.col("Date").alias("EventDateKey")).distinct()
    joined = joined.join(dates, on="EventDate", how="left")
    region = F.col("RegionCode")
    cashValue = (
        F.when(region == "NA", F.lit(POINT_CASH_VALUE["NA"])).when(region == "EU", F.lit(POINT_CASH_VALUE["EU"])).otherwise(F.lit(POINT_CASH_VALUE["APAC"])).cast("decimal(18,4)")
    )
    derived = (
        joined.withColumn("CustomerKeyResolved", F.coalesce(F.col("CustomerKey"), F.lit(UNKNOWN_CUSTOMER_KEY)))
        .withColumn(
            "SignedPoints",
            F.when(F.col("EventTypeCode").isin("REDEEM", "EXPIRE"), -F.col("PointsQuantity")).otherwise(F.col("PointsQuantity")),
        )
        .withColumn("PointCashValue", cashValue)
        .withColumn(
            "EarnRatePerCurrencyUnit",
            F.when(region == "NA", F.lit(1.0)).when(region == "EU", F.lit(0.8)).when(F.col("LoyaltyProgramCode") == "DBL", F.lit(2.0)).otherwise(F.lit(1.2)).cast("decimal(9,4)"),
        )
        .withColumn("LiabilityAmount", (F.col("PointsQuantity") * cashValue).cast("decimal(18,2)"))
        .withColumn(
            "ExpiryDate",
            F.when(region == "EU", F.add_months(F.col("EventDate"), expiryMonthsEu)).otherwise(F.add_months(F.col("EventDate"), expiryMonthsNa)),
        )
        .withColumn("SourceInvoiceDegenerate", F.col("SourceInvoiceNumber"))
        .withColumn("LineageKey", F.lit(lineageKey).cast("bigint"))
        .withColumn(
            "RouteCode",
            F.when(F.col("CustomerKey").isNull(), "UNMATCHED_CUSTOMER")
            .when(F.col("EventTypeCode") == "REDEEM", "REDEMPTIONS")
            .when(F.col("EventTypeCode").isin("ADJ", "ADJUST", "GOODWILL"), "ADJUSTMENTS")
            .otherwise("EARNINGS"),
        )
        .withColumn("MovementReference", F.col("LoyaltyEventBusinessKey").cast("string"))
        .withColumn("LoyaltyAccountNumber", F.concat_ws("-", F.col("CustomerBusinessKey"), F.col("LoyaltyProgramCode")))
    )
    held = derived.where(F.col("RouteCode") == "UNMATCHED_CUSTOMER").withColumn("RejectReasonCode", F.lit("LOOKUP_CUSTOMER_NO_MATCH"))
    return derived.where(F.col("RouteCode") != "UNMATCHED_CUSTOMER"), held


def generateExpiryRows(fact: DataFrame, asOf: date, lineageKey: int) -> DataFrame:
    """Integration.LoadFactLoyaltyPoints @GenerateExpiryRows: EXPIRE rows for earnings past their expiry date."""
    earned = fact.where((F.col("SignedPoints") > 0) & F.col("ExpiryDate").isNotNull() & (F.col("ExpiryDate") <= F.lit(asOf)))
    alreadyExpired = fact.where(F.col("EventTypeCode") == "EXPIRE").select("LoyaltyAccountNumber", F.col("MovementReference").alias("existingRef"))
    generated = earned.join(
        alreadyExpired,
        (earned["LoyaltyAccountNumber"] == alreadyExpired["LoyaltyAccountNumber"]) & (F.concat(F.lit("EXP-"), earned["MovementReference"]) == alreadyExpired["existingRef"]),
        "left_anti",
    )
    return (
        generated.withColumn("MovementReference", F.concat(F.lit("EXP-"), F.col("MovementReference")))
        .withColumn("EventTypeCode", F.lit("EXPIRE"))
        .withColumn("RouteCode", F.lit("EXPIRIES"))
        .withColumn("EventDate", F.col("ExpiryDate"))
        .withColumn("EventDateKey", F.col("ExpiryDate"))
        .withColumn("SignedPoints", -F.col("PointsQuantity"))
        .withColumn("LiabilityAmount", -F.col("LiabilityAmount"))
        .withColumn("LineageKey", F.lit(lineageKey).cast("bigint"))
        .select(*fact.columns)
    )


def recomputeRunningBalance(fact: DataFrame) -> DataFrame:
    """Points Balance After = SUM(signed points) OVER (PARTITION BY account ORDER BY date, reference)."""
    w = Window.partitionBy("LoyaltyAccountNumber").orderBy("EventDate", "MovementReference").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return fact.withColumn("PointsBalanceAfter", F.sum("SignedPoints").over(w).cast("int"))


def runFactLoyaltyPoints(spark: SparkSession, cfg: CeConfig, expiryMonthsNa: int = 24, expiryMonthsEu: int = 36) -> Tuple[int, int]:
    wmTable = cfg.table(tables.ETL_WATERMARK)
    target = cfg.table(tables.GOLD_FACT_LOYALTY_POINTS)
    last = tables.getWatermark(spark, wmTable, LOYALTY_WATERMARK, "")
    silver = spark.table(cfg.table(tables.SILVER_LOYALTY_LEDGER))
    stg = stagePointsFromLedger(silver)
    if last:
        stg = stg.where(F.col("LastModifiedAt") > F.lit(last).cast("timestamp"))
    customerKeys = currentCustomerKeys(spark.table(cfg.dw("dimension", "Customer")))
    dateDim = spark.table(cfg.dw("dimension", "Date"))
    lineageKey = cfg.batchId
    fact, held = deriveLoyaltyPointMeasures(stg, customerKeys, dateDim, expiryMonthsNa, expiryMonthsEu, lineageKey)
    fact = fact.withColumn("LoadDatetime", F.lit(tables.utcNow()).cast("timestamp"))
    keyCols = ["LoyaltyAccountNumber", "MovementReference", "EventDate"]
    inserted = tables.appendInsertOnly(spark, fact, target, keyCols)
    full = spark.table(target)
    expiries = generateExpiryRows(full, date.today(), lineageKey).withColumn("LoadDatetime", F.lit(tables.utcNow()).cast("timestamp"))
    expired = tables.appendInsertOnly(spark, expiries, target, keyCols)
    rebalanced = recomputeRunningBalance(spark.table(target))
    rebalanced.count()
    tables.overwriteTable(rebalanced, target)
    heldCount = held.count()
    if heldCount:
        tables.appendTable(held.withColumn("LoadDatetime", F.lit(tables.utcNow()).cast("timestamp")), cfg.table(tables.GOLD_FACT_LOYALTY_POINTS_REJECTS))
    maxModified = stg.agg(F.max("LastModifiedAt")).first()[0]
    if maxModified is not None:
        tables.setWatermark(spark, wmTable, LOYALTY_WATERMARK, "incremental_fact", str(maxModified), cfg.runId)
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "FACT_Load_LoyaltyPoints",
        cfg.runId,
        inserted + expired,
        heldCount,
        f"expiry_rows={expired} watermark_from={last!r}",
    )
    return inserted + expired, heldCount


def deriveWebSessionFact(stgSessions: DataFrame, customerKeys: DataFrame, minimumSessionSeconds: int, lineageKey: int) -> Tuple[DataFrame, DataFrame]:
    """Dedup on session key, customer lookup (ignore failure), derived attributes and routing."""
    dedup = stgSessions.dropDuplicates(["SessionBusinessKey"])
    joined = dedup.join(customerKeys, on="CustomerBusinessKey", how="left")
    ua = F.lower(F.coalesce(F.col("UserAgentFamily"), F.lit("")))
    region = F.col("RegionCode")
    consent = F.coalesce(F.col("ConsentGivenFlag"), F.lit("N"))
    noConsent = consent != "Y"
    derived = (
        joined.withColumn("IsAnonymous", F.col("CustomerKey").isNull())
        .withColumn(
            "CustomerKeyResolved",
            F.when(region.isin("EU", "APAC") & noConsent, F.lit(UNKNOWN_CUSTOMER_KEY)).otherwise(F.coalesce(F.col("CustomerKey"), F.lit(UNKNOWN_CUSTOMER_KEY))),
        )
        .withColumn(
            "IsLikelyBot",
            ua.contains("bot") | ua.contains("spider") | ua.contains("crawler") | (F.col("PageViewCount") > 500),
        )
        .withColumn(
            "IsBounce",
            (F.col("PageViewCount") <= 1) | (F.col("SessionDurationSeconds") < F.lit(minimumSessionSeconds)),
        )
        .withColumn(
            "PagesPerMinute",
            F.when(F.col("SessionDurationSeconds") == 0, F.lit(0.0))
            .otherwise(F.col("PageViewCount").cast("double") / F.col("SessionDurationSeconds").cast("double") * 60)
            .cast("decimal(9,4)"),
        )
        .withColumn("SessionIdDegenerate", F.col("SessionBusinessKey"))
        .withColumn(
            "VisitorBusinessKey",
            F.when((region == "APAC") & noConsent & F.col("VisitorBusinessKey").isNotNull(), F.sha2(F.col("VisitorBusinessKey"), 256)).otherwise(F.col("VisitorBusinessKey")),
        )
        .withColumn("PseudonymisedFlag", (region == "APAC") & noConsent)
        .withColumn(
            "ConsentBasisCode",
            F.when(region == "EU", "GDPR-CONSENT").when(region == "APAC", "APAC-CONSENT").otherwise("NA-OPTOUT"),
        )
        .withColumn(
            "PurgeAfterDate",
            F.when(region == "EU", F.add_months(F.to_date("SessionStartedAt"), 14))
            .when(region == "APAC", F.add_months(F.to_date("SessionStartedAt"), 6))
            .otherwise(F.add_months(F.to_date("SessionStartedAt"), 36)),
        )
        .withColumn("LineageKey", F.lit(lineageKey).cast("bigint"))
        .withColumn(
            "RouteCode",
            F.when(F.col("SessionEndedAt").isNull(), "SESSION_OPEN")
            .when(F.col("IsLikelyBot"), "BOT_TRAFFIC")
            .when(F.col("IsBounce"), "BOUNCES")
            .when(F.col("IsAnonymous"), "ANONYMOUS_SESSIONS")
            .otherwise("IDENTIFIED_SESSIONS"),
        )
    )
    rejects = derived.where(F.col("RouteCode").isin("BOT_TRAFFIC", "SESSION_OPEN")).withColumn("RejectReasonCode", F.col("RouteCode"))
    return derived.where(~F.col("RouteCode").isin("BOT_TRAFFIC", "SESSION_OPEN")), rejects


def runFactWebSession(spark: SparkSession, cfg: CeConfig, minimumSessionSeconds: int = 3) -> Tuple[int, int]:
    wmTable = cfg.table(tables.ETL_WATERMARK)
    target = cfg.table(tables.GOLD_FACT_WEB_SESSION)
    last = tables.getWatermark(spark, wmTable, WEB_SESSION_WATERMARK, "")
    stg = spark.table(cfg.table(tables.SILVER_WEB_SESSION))
    if last:
        stg = stg.where(F.col("LastModifiedAt") > F.lit(last).cast("timestamp"))
    customerKeys = currentCustomerKeys(spark.table(cfg.dw("dimension", "Customer")))
    fact, rejects = deriveWebSessionFact(stg, customerKeys, minimumSessionSeconds, cfg.batchId)
    fact = fact.withColumn("LoadDatetime", F.lit(tables.utcNow()).cast("timestamp"))
    inserted = tables.appendInsertOnly(spark, fact, target, ["SessionBusinessKey"])
    rejected = rejects.count()
    if rejected:
        tables.appendTable(rejects.withColumn("LoadDatetime", F.lit(tables.utcNow()).cast("timestamp")), cfg.table(tables.GOLD_FACT_WEB_SESSION_REJECTS))
    maxModified = stg.agg(F.max("LastModifiedAt")).first()[0]
    if maxModified is not None:
        tables.setWatermark(spark, wmTable, WEB_SESSION_WATERMARK, "incremental_fact", str(maxModified), cfg.runId)
    tables.logPackageRun(spark, cfg.table(tables.ETL_PACKAGE_RUN), "FACT_Load_WebSession", cfg.runId, inserted, rejected, f"watermark_from={last!r}")
    return inserted, rejected
