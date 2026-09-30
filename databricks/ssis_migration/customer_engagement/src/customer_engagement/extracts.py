"""EXT_SQL_LoyaltyLedger and EXT_SQL_WebSessions: OLTP -> bronze landing (raw.Sql* shape)."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_engagement import tables
from customer_engagement.config import CeConfig

SOURCE_SYSTEM_OLTP = "WWI_OLTP"
SOURCE_SYSTEM_WEB = "WWI_WEB"
LEDGER_WATERMARK = "Loyalty.LoyaltyPointsLedger"
SESSION_WATERMARK = "Ecommerce.WebSessions"


def auditColumns(df: DataFrame, sourceSystemCode: str, batchId: int, loadedAt: datetime) -> DataFrame:
    """SSIS ``audit_derivations``: BatchId, PackageExecutionId, LoadedAtUtc, SourceSystemCode, SourceRowNumber."""
    return (
        df.withColumn("BatchId", F.lit(batchId).cast("bigint"))
        .withColumn("PackageExecutionId", F.lit(None).cast("bigint"))
        .withColumn("LoadedAtUtc", F.lit(loadedAt).cast("timestamp"))
        .withColumn("SourceSystemCode", F.lit(sourceSystemCode))
        .withColumn("SourceRowNumber", F.monotonically_increasing_id().cast("bigint"))
    )


def transformLoyaltyLedgerExtract(
    ledger: DataFrame,
    members: DataFrame,
    programs: DataFrame,
    tiers: DataFrame,
    lastLedgerId: int,
    lookbackDays: int,
    now: datetime,
    batchId: int,
) -> DataFrame:
    """Numeric-key incremental plus a lookback re-read of rows the expiry sweep touched."""
    lookbackFrom = now - timedelta(days=lookbackDays)
    selected = ledger.where((F.col("LoyaltyLedgerID") > F.lit(lastLedgerId)) | (F.col("ExpiredWhen") >= F.lit(lookbackFrom)))
    m = members.alias("m")
    p = programs.alias("p")
    t = tiers.alias("t")
    joined = (
        selected.alias("l")
        .join(m, F.col("m.LoyaltyMemberID") == F.col("l.LoyaltyMemberID"), "inner")
        .join(p, F.col("p.LoyaltyProgramID") == F.col("m.LoyaltyProgramID"), "inner")
        .join(t, F.col("t.LoyaltyTierID") == F.col("m.CurrentTierID"), "left")
    )
    out = joined.select(
        F.col("l.LoyaltyLedgerID").cast("string").alias("LoyaltyLedgerID"),
        F.col("l.LoyaltyMemberID").cast("string").alias("LoyaltyMemberID"),
        F.col("m.CustomerID").cast("string").alias("CustomerID"),
        F.col("p.ProgramCode").alias("ProgramCode"),
        F.col("t.TierCode").alias("TierCode"),
        F.col("l.EntryTypeCode").alias("EntryTypeCode"),
        F.col("l.PointsDelta").cast("string").alias("PointsDelta"),
        F.col("l.PointsRemaining").cast("string").alias("PointsBalanceAfter"),
        F.col("l.SourceInvoiceID").cast("string").alias("SourceInvoiceID"),
        F.col("l.SourceReference").alias("RedemptionReference"),
        F.date_format(F.col("l.EntryWhen"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("EntryWhen"),
        F.date_format(F.col("l.ExpiresOnDate"), "yyyy-MM-dd").alias("ExpiryDate"),
        F.trim(F.col("p.RegionCode")).alias("RegionCode"),
        F.date_format(F.coalesce(F.col("l.ExpiredWhen"), F.col("l.EntryWhen")), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("LastEditedWhen"),
    )
    return auditColumns(out, SOURCE_SYSTEM_OLTP, batchId, now)


def transformWebSessionExtract(sessions: DataFrame, carts: DataFrame, windowStart: datetime, windowEnd: datetime, batchId: int, now: datetime) -> DataFrame:
    """Date-window extract; EU sessions without analytics consent lose fingerprint and referrer."""
    inWindow = sessions.where((F.col("StartedWhen") >= F.lit(windowStart)) & (F.col("StartedWhen") < F.lit(windowEnd)))
    cartAgg = carts.groupBy("WebSessionID").agg(
        F.count("*").alias("cartCount"),
        F.max("ConvertedOrderID").alias("convertedOrderId"),
    )
    joined = inWindow.alias("ws").join(cartAgg.alias("ch"), on="WebSessionID", how="left")
    region = F.upper(F.trim(F.col("ws.RegionCode")))
    consent = F.upper(F.trim(F.col("ws.ConsentStateCode")))
    euSuppressed = (region == "EU") & (~consent.isin("GRANTED", "OPTIN", "EXPR", "EINW") | consent.isNull())
    duration = F.coalesce(
        F.col("ws.DurationSeconds"),
        (F.unix_timestamp("ws.EndedWhen") - F.unix_timestamp("ws.StartedWhen")).cast("int"),
    )
    hasCheckout = F.col("ch.convertedOrderId").isNotNull()
    out = joined.select(
        F.col("ws.SessionGuid").cast("string").alias("WebSessionID"),
        F.col("ws.CustomerID").cast("string").alias("CustomerID"),
        F.when(euSuppressed, F.lit(None).cast("string")).otherwise(F.sha2(F.coalesce(F.col("ws.IpAddressText"), F.col("ws.SessionGuid")), 256)).alias("AnonymousVisitorKey"),
        F.date_format(F.col("ws.StartedWhen"), "yyyy-MM-dd HH:mm:ss").alias("SessionStartedWhen"),
        F.date_format(F.col("ws.EndedWhen"), "yyyy-MM-dd HH:mm:ss").alias("SessionEndedWhen"),
        F.col("ws.LandingPageUrl").alias("LandingPageUrl"),
        F.when(euSuppressed, F.lit(None).cast("string")).otherwise(F.col("ws.ReferrerUrl")).alias("ReferrerUrl"),
        F.col("ws.CampaignCode").alias("CampaignCode"),
        F.col("ws.DeviceCategory").alias("DeviceCategory"),
        F.col("ws.BrowserFamily").alias("BrowserFamily"),
        F.col("ws.CountryISO3").alias("CountryCode"),
        F.col("ws.PageViewCount").cast("string").alias("PageViewCount"),
        F.when(F.col("ch.cartCount").isNull(), F.lit("N")).otherwise(F.lit("Y")).alias("CartCreatedFlag"),
        F.when(hasCheckout, F.lit("Y")).otherwise(F.lit("N")).alias("OrderPlacedFlag"),
        F.col("ch.convertedOrderId").cast("string").alias("OrderID"),
        F.col("ws.ConsentStateCode").alias("ConsentCategories"),
        F.date_format(F.coalesce(F.col("ws.EndedWhen"), F.col("ws.StartedWhen")), "yyyy-MM-dd HH:mm:ss").alias("LastEditedWhen"),
        region.alias("RegionCode"),
        duration.alias("SessionDurationSeconds"),
        F.when(F.coalesce(F.col("ws.PageViewCount"), F.lit(0)) <= 1, F.lit("Y")).otherwise(F.lit("N")).alias("BounceFlag"),
        F.when(hasCheckout, F.lit("Y")).otherwise(F.lit("N")).alias("ConversionFlag"),
    )
    return auditColumns(out, SOURCE_SYSTEM_WEB, batchId, now)


def runExtLoyaltyLedger(spark: SparkSession, cfg: CeConfig, lookbackDays: int = 7) -> Tuple[int, int]:
    target = cfg.table(tables.BRONZE_LOYALTY_LEDGER)
    wmTable = cfg.table(tables.ETL_WATERMARK)
    lastId = int(tables.getWatermark(spark, wmTable, LEDGER_WATERMARK, "0"))
    now = tables.utcNow()
    ledger = spark.table(cfg.oltp("loyalty", "loyaltypointsledger"))
    members = spark.table(cfg.oltp("loyalty", "loyaltymembers"))
    programs = spark.table(cfg.oltp("loyalty", "loyaltyprograms"))
    tiers = spark.table(cfg.oltp("loyalty", "loyaltytiers"))
    out = transformLoyaltyLedgerExtract(ledger, members, programs, tiers, lastId, lookbackDays, now, cfg.batchId)
    lookbackFrom = (now - timedelta(days=lookbackDays)).strftime("%Y-%m-%d %H:%M:%S")
    tables.deleteWhere(spark, target, f"try_cast(LastEditedWhen AS TIMESTAMP) >= TIMESTAMP '{lookbackFrom}'")
    if not tables.tableExists(spark, target):
        tables.overwriteTable(out, target)
    else:
        tables.appendTable(out, target)
    maxId = ledger.agg(F.max("LoyaltyLedgerID")).first()[0]
    tables.setWatermark(spark, wmTable, LEDGER_WATERMARK, "incremental_key", str(maxId or lastId), cfg.runId)
    inserted = out.count()
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "EXT_SQL_LoyaltyLedger",
        cfg.runId,
        inserted,
        0,
        f"lastLedgerId={lastId} lookbackDays={lookbackDays}",
    )
    return inserted, 0


def resolveSessionWindow(spark: SparkSession, cfg: CeConfig, sessions: DataFrame) -> Tuple[datetime, datetime]:
    """Explicit window when given, otherwise from the watermark to now (full history on first run)."""
    wmTable = cfg.table(tables.ETL_WATERMARK)
    now = tables.utcNow()
    if cfg.windowStart != "auto":
        start = datetime.fromisoformat(cfg.windowStart)
    else:
        last = tables.getWatermark(spark, wmTable, SESSION_WATERMARK, "")
        if last:
            start = datetime.fromisoformat(last)
        else:
            first = sessions.agg(F.min("StartedWhen")).first()[0]
            start = (first or now).replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.fromisoformat(cfg.windowEnd) if cfg.windowEnd != "auto" else now
    return start, end


def runExtWebSessions(spark: SparkSession, cfg: CeConfig) -> Tuple[int, int]:
    target = cfg.table(tables.BRONZE_WEB_SESSION)
    wmTable = cfg.table(tables.ETL_WATERMARK)
    sessions = spark.table(cfg.oltp("ecommerce", "websessions"))
    carts = spark.table(cfg.oltp("ecommerce", "cartheaders"))
    start, end = resolveSessionWindow(spark, cfg, sessions)
    now = tables.utcNow()
    out = transformWebSessionExtract(sessions, carts, start, end, cfg.batchId, now)
    tables.deleteWhere(
        spark,
        target,
        f"try_cast(SessionStartedWhen AS TIMESTAMP) >= TIMESTAMP '{start:%Y-%m-%d %H:%M:%S}' AND try_cast(SessionStartedWhen AS TIMESTAMP) < TIMESTAMP '{end:%Y-%m-%d %H:%M:%S}'",
    )
    if not tables.tableExists(spark, target):
        tables.overwriteTable(out, target)
    else:
        tables.appendTable(out, target)
    tables.setWatermark(spark, wmTable, SESSION_WATERMARK, "date_window", end.isoformat(sep=" "), cfg.runId)
    inserted = out.count()
    tables.logPackageRun(
        spark,
        cfg.table(tables.ETL_PACKAGE_RUN),
        "EXT_SQL_WebSessions",
        cfg.runId,
        inserted,
        0,
        f"window={start.isoformat()}..{end.isoformat()}",
    )
    return inserted, 0
