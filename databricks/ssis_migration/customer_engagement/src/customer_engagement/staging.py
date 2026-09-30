"""STG_Load_LoyaltyLedger and STG_Load_WebSession: raw landing -> conformed silver tables."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_engagement import regions, tables
from customer_engagement.config import CeConfig

LOYALTY_WATERMARK = "stg.LoyaltyLedger"
WEB_SESSION_WATERMARK = "stg.WebSession"
MAX_SESSION_SECONDS = 86400


def _tierReference(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        regions.STAGING_TIER_REFERENCE,
        "LoyaltyTierCode STRING, TierName STRING, TierDiscountPercent DOUBLE",
    ).withColumn("TierDiscountPercent", F.col("TierDiscountPercent").cast("decimal(9,4)"))


def typeLoyaltyEntries(raw: DataFrame, defaultRegion: str) -> DataFrame:
    """'Type Loyalty Entries' derived column: defaults, signed-delta split and regional expiry."""
    region = regions.normaliseRegion(
        F.coalesce(regions.nullIfBlank(F.col("RegionCode")), regions.regionFromProgram(F.col("ProgramCode"))),
        defaultRegion,
    )
    delta = F.coalesce(F.col("PointsDelta").cast("int"), F.lit(0))
    entryDate = F.to_date(F.substring(F.col("EntryWhen"), 1, 10))
    typed = raw.select(
        F.col("LoyaltyLedgerID").cast("bigint").alias("LoyaltyEntryId"),
        F.col("CustomerID").cast("int").alias("CustomerId"),
        F.col("LoyaltyMemberID").cast("bigint").alias("LoyaltyMemberId"),
        F.upper(F.trim(F.coalesce(F.col("EntryTypeCode"), F.lit("ADJ")))).alias("EntryTypeCode"),
        F.upper(F.trim(F.coalesce(regions.nullIfBlank(F.col("ProgramCode")), F.lit("BASE")))).alias("ProgramCode"),
        region.alias("RegionCode"),
        F.upper(F.trim(F.col("TierCode"))).alias("SourceTierCode"),
        delta.alias("PointsDelta"),
        F.when(delta < 0, F.lit(0)).otherwise(delta).alias("PointsEarned"),
        F.when(delta > 0, F.lit(0)).otherwise(-delta).alias("PointsRedeemed"),
        F.col("PointsBalanceAfter").cast("int").alias("PointsBalanceAfter"),
        F.col("SourceInvoiceID").cast("bigint").alias("SourceInvoiceId"),
        F.col("RedemptionReference").alias("RedemptionReference"),
        entryDate.alias("EntryDate"),
        F.to_timestamp(F.regexp_replace(F.col("EntryWhen"), "T", " ")).alias("EntryWhen"),
        F.to_date(F.substring(F.col("ExpiryDate"), 1, 10)).alias("SourceExpiryDate"),
        F.to_timestamp(F.regexp_replace(F.col("LastEditedWhen"), "T", " ")).alias("LastEditedWhen"),
        F.col("BatchId").alias("SourceBatchId"),
    )
    return typed.withColumn("ExpiryDate", regions.stagingExpiryDefault(F.col("RegionCode"), F.col("EntryDate"), F.col("SourceExpiryDate")))


def aggregateCustomerPoints(typed: DataFrame) -> DataFrame:
    """'Aggregate Customer Points' + 'Derive Loyalty Tier'."""
    agg = typed.groupBy("CustomerId", "ProgramCode", "RegionCode").agg(
        F.sum("PointsEarned").cast("int").alias("TotalPointsEarned"),
        F.sum("PointsRedeemed").cast("int").alias("TotalPointsRedeemed"),
        F.count("LoyaltyEntryId").cast("int").alias("EntryCount"),
    )
    net = F.col("TotalPointsEarned") - F.col("TotalPointsRedeemed")
    return agg.withColumn("NetPointsBalance", net.cast("int")).withColumn("LoyaltyTierCode", regions.stagingTierCode(F.col("NetPointsBalance")))


def conformLoyaltyLedger(raw: DataFrame, tierReference: DataFrame, defaultRegion: str, loadedAt: datetime, batchId: int) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Returns (row-level conformed ledger, customer/programme balances, tier-lookup rejects)."""
    typed = typeLoyaltyEntries(raw, defaultRegion)
    balances = aggregateCustomerPoints(typed)
    withTier = balances.join(tierReference, on="LoyaltyTierCode", how="left")
    rejects = withTier.where(F.col("TierName").isNull()).withColumn("RejectReasonCode", F.lit("UNKNOWN_TIER"))
    matched = withTier.where(F.col("TierName").isNotNull())
    ledger = typed.join(
        matched.select(
            "CustomerId",
            "ProgramCode",
            "RegionCode",
            "TotalPointsEarned",
            "TotalPointsRedeemed",
            "EntryCount",
            "NetPointsBalance",
            "LoyaltyTierCode",
            "TierName",
            "TierDiscountPercent",
        ),
        on=["CustomerId", "ProgramCode", "RegionCode"],
        how="inner",
    )
    meta = (
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.lit(loadedAt).cast("timestamp").alias("LoadedAtUtc"),
        F.lit(loadedAt).cast("timestamp").alias("LastModifiedAt"),
        F.lit("WWI_OLTP").alias("SourceSystemCode"),
        F.lit("VALID").alias("DqStatusCode"),
    )
    return ledger.select("*", *meta), matched.select("*", *meta[:2]), rejects.select("*", *meta[:2])


def _landingPagePath(urlCol) -> F.Column:  # type: ignore[name-defined]
    """LOWER(LEFT(TOKEN(REPLACE(url,'://',' '),' ',2),200)) with a path passthrough for scheme-less URLs."""
    afterScheme = F.regexp_extract(urlCol, r"://([^ ]*)", 1)
    path = F.when(urlCol.isNull(), F.lit("/")).when(urlCol.contains("://"), afterScheme).otherwise(urlCol)
    return F.lower(F.substring(F.trim(path), 1, 200))


def conformWebSession(raw: DataFrame, countryRef: Optional[DataFrame], defaultRegion: str, loadedAt: datetime, batchId: int) -> Tuple[DataFrame, DataFrame]:
    """'Lookup Country Region', 'Apply Consent Rules' and 'Screen Web Session'. Returns (valid, rejects)."""
    started = F.to_timestamp(F.col("SessionStartedWhen"))
    ended = F.to_timestamp(F.col("SessionEndedWhen"))
    mapped = raw.select(
        F.col("WebSessionID").alias("SessionGuidRaw"),
        F.col("CustomerID").cast("int").alias("CustomerID"),
        F.col("AnonymousVisitorKey").alias("VisitorBusinessKey"),
        F.lit(None).cast("string").alias("ChannelCode"),
        F.col("DeviceCategory").alias("DeviceTypeCode"),
        F.upper(F.trim(F.col("CountryCode"))).alias("CountryCode"),
        F.col("BrowserFamily").alias("UserAgentText"),
        F.col("LandingPageUrl").alias("LandingPageUrl"),
        F.col("PageViewCount").cast("int").alias("PageViewCountRaw"),
        (F.unix_timestamp(ended) - F.unix_timestamp(started)).cast("int").alias("DurationSecondsRaw"),
        F.col("ConsentCategories").alias("ConsentFlag"),
        started.alias("SessionStartWhen"),
        ended.alias("SessionEndedAt"),
        F.when(F.upper(F.col("CartCreatedFlag")) == "Y", F.lit(1)).otherwise(F.lit(0)).alias("CartAddCount"),
        F.col("CampaignCode"),
        F.col("BatchId").alias("SourceBatchId"),
    )
    if countryRef is not None:
        ref = countryRef.select(F.upper(F.trim(F.col("CountryCode"))).alias("CountryCode"), F.col("RegionCode").alias("RefRegionCode")).dropDuplicates(["CountryCode"])
        mapped = mapped.join(ref, on="CountryCode", how="left")
    else:
        mapped = mapped.withColumn("RefRegionCode", F.lit(None).cast("string"))

    region = regions.regionFromCountry(F.col("CountryCode"), F.col("RefRegionCode"), defaultRegion)
    consentGiven = regions.consentGivenFlag(F.col("ConsentFlag"))
    duration = F.coalesce(F.col("DurationSecondsRaw"), F.lit(0))
    derived = (
        mapped.withColumn("SessionGuid", F.upper(F.trim(F.col("SessionGuidRaw"))))
        .withColumn("RegionCode", region)
        .withColumn("ConsentGivenFlag", consentGiven)
        .withColumn(
            "CustomerId",
            F.when((F.col("RegionCode") == "EU") & (F.col("ConsentGivenFlag") != "Y"), F.lit(-1)).otherwise(F.coalesce(F.col("CustomerID"), F.lit(-1))),
        )
        .withColumn(
            "UserAgentFamily",
            F.when(F.col("ConsentGivenFlag") != "Y", F.lit("SUPPRESSED")).otherwise(F.substring(F.trim(F.coalesce(F.col("UserAgentText"), F.lit("UNKNOWN"))), 1, 40)),
        )
        .withColumn("LandingPagePath", _landingPagePath(F.col("LandingPageUrl")))
        .withColumn("ChannelCode", F.upper(F.trim(F.coalesce(F.col("ChannelCode"), F.lit("WEB")))))
        .withColumn("DeviceTypeCode", F.upper(F.trim(F.coalesce(F.col("DeviceTypeCode"), F.lit("UNKNOWN")))))
        .withColumn("PageViewCount", F.coalesce(F.col("PageViewCountRaw"), F.lit(0)))
        .withColumn("DurationSeconds", F.when(duration < 0, F.lit(0)).otherwise(duration))
        .withColumn("BounceFlag", F.when(F.col("PageViewCount") <= 1, F.lit("Y")).otherwise(F.lit("N")))
    )
    validKey = F.length(F.col("SessionGuid")) == 36
    implausible = F.col("DurationSeconds") > MAX_SESSION_SECONDS
    reason = F.when(implausible, F.lit("IMPLAUSIBLE_DURATION")).when(~validKey | F.col("SessionGuid").isNull(), F.lit("MALFORMED_SESSION_KEY"))
    screened = derived.withColumn("RejectReasonCode", reason)
    meta = [
        F.lit(batchId).cast("bigint").alias("BatchId"),
        F.lit(loadedAt).cast("timestamp").alias("LoadedAtUtc"),
        F.lit(loadedAt).cast("timestamp").alias("LastModifiedAt"),
        F.lit("WWI_WEB").alias("SourceSystemCode"),
    ]
    valid = screened.where(F.col("RejectReasonCode").isNull()).select(
        F.col("SessionGuid").alias("SessionBusinessKey"),
        F.col("VisitorBusinessKey"),
        F.when(F.col("CustomerId") > 0, F.col("CustomerId").cast("string")).alias("CustomerBusinessKey"),
        F.col("CustomerId"),
        F.col("SessionStartWhen").alias("SessionStartedAt"),
        F.col("SessionEndedAt"),
        F.col("PageViewCount"),
        F.col("CartAddCount"),
        F.col("DurationSeconds").alias("SessionDurationSeconds"),
        F.col("ChannelCode"),
        F.col("DeviceTypeCode"),
        F.col("UserAgentFamily"),
        F.col("CountryCode").alias("CountryIsoCode"),
        F.col("RegionCode"),
        F.col("ConsentGivenFlag"),
        F.col("LandingPagePath"),
        F.col("CampaignCode"),
        F.col("BounceFlag"),
        F.lit("VALID").alias("DqStatusCode"),
        *meta,
    )
    rejects = screened.where(F.col("RejectReasonCode").isNotNull()).select(
        F.col("SessionGuid").alias("BusinessKey"),
        "RejectReasonCode",
        "CustomerId",
        "RegionCode",
        "DurationSeconds",
        "SessionStartWhen",
        *meta[:2],
    )
    return valid, rejects


def _rawSource(spark: SparkSession, cfg: CeConfig, legacyName: str, bronzeName: str) -> DataFrame:
    if cfg.stagingSourceMode == "bronze":
        return spark.table(cfg.table(bronzeName))
    return spark.table(cfg.staging("raw", legacyName))


def _windowFilter(df: DataFrame, dateCol: F.Column, lastValue: str) -> DataFrame:  # type: ignore[name-defined]
    return df.where(dateCol > F.lit(lastValue).cast("timestamp")) if lastValue else df


def runStgLoyaltyLedger(spark: SparkSession, cfg: CeConfig) -> Tuple[int, int]:
    wmTable = cfg.table(tables.ETL_WATERMARK)
    last = tables.getWatermark(spark, wmTable, LOYALTY_WATERMARK, "")
    raw = _rawSource(spark, cfg, "sqlloyaltyledger", tables.BRONZE_LOYALTY_LEDGER)
    raw = _windowFilter(raw, F.to_timestamp(F.regexp_replace(F.col("EntryWhen"), "T", " ")), last)
    now = tables.utcNow()
    ledger, balances, rejects = conformLoyaltyLedger(raw, _tierReference(spark), cfg.defaultRegion, now, cfg.batchId)
    inserted = tables.appendInsertOnly(spark, ledger, cfg.table(tables.SILVER_LOYALTY_LEDGER), ["LoyaltyEntryId"])
    # Balances are a snapshot of the customer/programme position after this load.
    fullLedger = spark.table(cfg.table(tables.SILVER_LOYALTY_LEDGER))
    snapshot = aggregateCustomerPoints(fullLedger).join(_tierReference(spark), on="LoyaltyTierCode", how="left")
    tables.overwriteTable(snapshot.withColumn("LoadedAtUtc", F.lit(now).cast("timestamp")), cfg.table(tables.SILVER_LOYALTY_CUSTOMER_BALANCE))
    rejected = rejects.count()
    if rejected:
        tables.appendTable(rejects, cfg.table(tables.SILVER_LOYALTY_LEDGER_REJECTS))
    maxEntry = ledger.agg(F.max("EntryWhen")).first()[0]
    if maxEntry is not None:
        tables.setWatermark(spark, wmTable, LOYALTY_WATERMARK, "incremental_append", str(maxEntry), cfg.runId)
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "STG_Load_LoyaltyLedger",
        cfg.runId,
        inserted,
        rejected,
        f"source_mode={cfg.stagingSourceMode} watermark_from={last!r}",
    )
    return inserted, rejected


def runStgWebSession(spark: SparkSession, cfg: CeConfig) -> Tuple[int, int]:
    wmTable = cfg.table(tables.ETL_WATERMARK)
    last = tables.getWatermark(spark, wmTable, WEB_SESSION_WATERMARK, "")
    raw = _rawSource(spark, cfg, "sqlwebsession", tables.BRONZE_WEB_SESSION)
    raw = _windowFilter(raw, F.to_timestamp(F.col("SessionStartedWhen")), last)
    countryRef = tables.readTableOrNone(spark, cfg.staging("ref", "country"))
    now = tables.utcNow()
    valid, rejects = conformWebSession(raw, countryRef, cfg.defaultRegion, now, cfg.batchId)
    inserted = tables.appendInsertOnly(spark, valid, cfg.table(tables.SILVER_WEB_SESSION), ["SessionBusinessKey"])
    rejected = rejects.count()
    if rejected:
        tables.appendTable(rejects, cfg.table(tables.SILVER_WEB_SESSION_REJECTS))
    maxStart = valid.agg(F.max("SessionStartedAt")).first()[0]
    if maxStart is not None:
        tables.setWatermark(spark, wmTable, WEB_SESSION_WATERMARK, "incremental_append", str(maxStart), cfg.runId)
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "STG_Load_WebSession",
        cfg.runId,
        inserted,
        rejected,
        f"source_mode={cfg.stagingSourceMode} watermark_from={last!r}",
    )
    return inserted, rejected
