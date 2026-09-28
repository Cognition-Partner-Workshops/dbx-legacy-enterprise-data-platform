"""Outbound partner sales feed - replaces SSIS ``SLS_Export_PartnerFeed``.

``exportPartnerFeed(spark, cfg, outDir, region)`` writes ``<outDir>/partner_feed_<REGION>_<yyyyMMdd>.csv``
plus ``<outDir>/partner_feed_<REGION>_<yyyyMMdd>.manifest.json``.  Nothing is
pushed anywhere: the legacy "outbound share" copy is out of scope (stub).

Column list is the legacy work.PartnerFeedRow contract, in order:
PartnerCode, InvoiceNumber, InvoiceDate, CustomerReference, StockItemCode,
Quantity, NetAmount, SettlementCurrencyCode, RegionCode.
"""
from __future__ import annotations

import datetime as dt
import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import readTable
from sales_lakehouse.gold.inputs import (
    DIM_STOCK_ITEM_SCHEMA,
    FX_RATE_TYPE_AVERAGE,
    LEGACY_REGION_CURRENCY,
    notReversal,
    readOrEmpty,
)

FEED_COLUMNS = [
    "PartnerCode", "InvoiceNumber", "InvoiceDate", "CustomerReference", "StockItemCode",
    "Quantity", "NetAmount", "SettlementCurrencyCode", "RegionCode",
]
REDACTED = "REDACTED"
LOOKBACK_DAYS = 1  # legacy: Invoice Date Key >= DATEADD(DAY, -1, today)


def _shareConsent(customer: DataFrame) -> DataFrame:
    """Dimension.Customer [Share Consent Flag] equivalent.

    dim_customer carries marketing_consent_flag; a customer who has requested
    erasure never has a sharing consent whatever the flag says.
    """
    consent = F.coalesce(F.col("marketing_consent_flag"), F.lit(False)) & F.col("erasure_requested_on").isNull()
    return customer.filter(F.col("is_current_row") == True).select(  # noqa: E712
        "customer_key",
        F.col("region_code").alias("customer_region_code"),
        F.col("source_customer_reference").alias("customer_reference"),
        consent.alias("share_consent_flag"),
    )


def buildPartnerFeed(
    sale: DataFrame,
    customer: DataFrame,
    channel: DataFrame,
    stockItem: DataFrame,
    fxRate: DataFrame,
    region: str,
    asOfDate: dt.date,
    suppressUnconsentedEuRows: bool = True,
    lookbackDays: int = LOOKBACK_DAYS,
) -> DataFrame:
    """Rows of the regional feed (already filtered / redacted / restated)."""
    settlementCurrency = LEGACY_REGION_CURRENCY[region]
    cutoff = asOfDate - dt.timedelta(days=lookbackDays)
    s = notReversal(sale).filter((F.col("region_code") == region) & (F.col("invoice_date_key") >= F.lit(cutoff)))
    partner = channel.select(
        "sales_channel_key",
        F.coalesce(F.col("partner_name"), F.col("sales_channel_code"), F.lit("DIRECT")).alias("PartnerCode"),
    )
    items = stockItem.select("stock_item_key", F.col("wwi_stock_item_id").cast("string").alias("item_code"))
    # LEGACY QUIRK: partner restatement uses the AVERAGE rate table; a missing rate
    # silently falls back to 1.0 (amount passes through unconverted).
    fx = (
        fxRate.filter((F.col("rate_type_code") == FX_RATE_TYPE_AVERAGE) & (F.col("to_currency_code") == settlementCurrency))
        .groupBy("from_currency_code")
        .agg(F.max_by("conversion_rate", "effective_date").alias("settlement_rate"))
        .withColumnRenamed("from_currency_code", "transaction_currency_code")
    )
    rows = (
        s.join(_shareConsent(customer), "customer_key", "inner")
        .join(partner, "sales_channel_key", "left")
        .join(items, "stock_item_key", "left")
        .join(fx, "transaction_currency_code", "left")
    )
    # LEGACY QUIRK (EU only): customers without a sharing consent are first redacted, then the
    # REDACTED rows are deleted - other regions ignore consent entirely.
    isEuNoConsent = (F.col("customer_region_code") == "EU") & (~F.col("share_consent_flag"))
    rows = rows.withColumn(
        "CustomerReference", F.when(isEuNoConsent, F.lit(REDACTED)).otherwise(F.col("customer_reference"))
    )
    if suppressUnconsentedEuRows:
        rows = rows.filter(~((F.col("customer_region_code") == "EU") & (F.col("CustomerReference") == REDACTED)))
    rate = F.when(F.col("transaction_currency_code") == settlementCurrency, F.lit(1.0)).otherwise(
        F.coalesce(F.col("settlement_rate"), F.lit(1.0))
    )
    return rows.select(
        F.coalesce(F.col("PartnerCode"), F.lit("DIRECT")).alias("PartnerCode"),
        F.col("invoice_number").alias("InvoiceNumber"),
        F.col("invoice_date_key").alias("InvoiceDate"),
        "CustomerReference",
        F.coalesce(F.col("item_code"), F.col("stock_item_key").cast("string")).alias("StockItemCode"),
        F.col("quantity").cast("int").alias("Quantity"),
        F.round(F.col("total_excluding_tax") * rate, 2).cast("decimal(18,2)").alias("NetAmount"),
        F.lit(settlementCurrency).alias("SettlementCurrencyCode"),
        F.col("region_code").alias("RegionCode"),
    ).orderBy("PartnerCode", "InvoiceNumber")


def writeFeedFiles(feed: DataFrame, outDir: str, region: str, asOfDate: dt.date, batchId: int) -> tuple[str, str]:
    """Write a single CSV + manifest json; returns (csvPath, manifestPath)."""
    os.makedirs(outDir, exist_ok=True)
    stem = f"partner_feed_{region}_{asOfDate.strftime('%Y%m%d')}"
    csvPath = os.path.join(outDir, f"{stem}.csv")
    manifestPath = os.path.join(outDir, f"{stem}.manifest.json")
    rows = feed.select(*FEED_COLUMNS).collect()
    with open(csvPath, "w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(FEED_COLUMNS) + "\n")
        for row in rows:
            fh.write(",".join(_csvField(row[c]) for c in FEED_COLUMNS) + "\n")
    manifest = {
        "region": region,
        "as_of_date": asOfDate.isoformat(),
        "batch_id": batchId,
        "file": os.path.basename(csvPath),
        "row_count": len(rows),
        "columns": FEED_COLUMNS,
        "settlement_currency_code": LEGACY_REGION_CURRENCY[region],
        "net_amount_total": str(sum((row["NetAmount"] for row in rows), start=0)),
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    with open(manifestPath, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    return csvPath, manifestPath


def _csvField(value: object) -> str:
    if value is None:
        return ""
    text = value.isoformat() if isinstance(value, (dt.date, dt.datetime)) else str(value)
    if any(ch in text for ch in (",", '"', "\n")):
        return '"' + text.replace('"', '""') + '"'
    return text


def exportPartnerFeed(
    spark: SparkSession,
    cfg: PipelineConfig,
    outDir: str,
    region: str,
    asOfDate: dt.date | None = None,
    suppressUnconsentedEuRows: bool = True,
    lookbackDays: int = LOOKBACK_DAYS,
) -> tuple[str, str]:
    asOf = asOfDate or dt.date.today()
    sale = readTable(spark, cfg, "gold", "fact_sale")
    customer = readTable(spark, cfg, "silver", "dim_customer")
    channel = readTable(spark, cfg, "silver", "dim_sales_channel")
    fxRate = readTable(spark, cfg, "silver", "ref_fx_rate")
    stockItem = readOrEmpty(spark, cfg, "silver", "dim_stock_item", DIM_STOCK_ITEM_SCHEMA)
    feed = buildPartnerFeed(sale, customer, channel, stockItem, fxRate, region, asOf, suppressUnconsentedEuRows, lookbackDays)
    return writeFeedFiles(feed, outDir, region, asOf, cfg.batchId)


def run(spark: SparkSession, cfg: PipelineConfig, outDir: str | None = None) -> None:
    target = outDir or os.path.join(cfg.mockDataRoot, "outbound", "partner_feed")
    for region in LEGACY_REGION_CURRENCY:
        exportPartnerFeed(spark, cfg, target, region)
