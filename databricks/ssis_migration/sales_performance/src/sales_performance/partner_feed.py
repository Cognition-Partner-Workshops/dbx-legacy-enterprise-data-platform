"""SLS_Export_PartnerFeed: outbound partner_feed_YYYYMMDD.csv into the group's landing volume."""

import csv
import os
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_performance.commissions import writeMetrics
from sales_performance.common import readTable, saveTable, withAudit
from sales_performance.sale_line import CUSTOMER_TABLE, FACT_SALE_TABLE, FX_TABLE, STOCK_ITEM_TABLE

PACKAGE = "SLS_Export_PartnerFeed"
FEED_TABLE = "gold_partner_feed_row"
PARTNER_TABLE = "silver_dim_partner"
FEED_COLUMNS = [
    "partner_code",
    "invoice_number",
    "invoice_date",
    "customer_reference",
    "stock_item_code",
    "quantity",
    "net_amount",
    "settlement_currency_code",
    "region_code",
]


def derivePartners(customers: DataFrame) -> DataFrame:
    """Dimension.Partner does not exist in the legacy warehouse; partners are the buying groups
    (the retail chains WWI sells through). Settlement currency defaults to USD."""
    return (
        customers.filter(F.col("buying_group").isNotNull())
        .select(
            F.upper(F.regexp_replace(F.col("buying_group"), "[^A-Za-z0-9]", "")).alias("partner_code"),
            F.col("buying_group").alias("partner_name"),
            F.lit("USD").alias("settlement_currency_code"),
        )
        .dropDuplicates(["partner_code"])
    )


def buildPartnerFeed(
    factSale: DataFrame,
    customers: DataFrame,
    stockItems: DataFrame,
    partners: DataFrame,
    fxRates: DataFrame,
    fromDate,
    partnerScope: str = "ALL",
    suppressUnconsentedEuRows: bool = True,
) -> DataFrame:
    cust = customers.select(
        "customer_key",
        F.col("buying_group"),
        F.coalesce(F.col("wwi_customer_id").cast("string"), F.lit("")).alias("customer_reference"),
        F.upper(F.regexp_replace(F.col("buying_group"), "[^A-Za-z0-9]", "")).alias("partner_code"),
    )
    consent = F.lit(False)
    if "share_consent_flag" in customers.columns:
        cust = cust.withColumn("share_consent_flag", F.coalesce(F.col("share_consent_flag").cast("boolean"), F.lit(False)))
        consent = F.col("share_consent_flag")
    items = stockItems.select("stock_item_key", F.col("wwi_stock_item_id").cast("string").alias("stock_item_code"))
    fx = fxRates.filter(F.col("rate_type_code") == "AVERAGE").select(
        F.col("currency_code").alias("fx_currency_code"), F.col("quote_currency_code").alias("fx_quote"), "conversion_rate"
    )
    df = (
        factSale.filter(~F.col("is_reversal") & (F.col("invoice_date") >= F.lit(fromDate)))
        .join(cust, "customer_key", "inner")
        .join(items, "stock_item_key", "inner")
        .join(partners.select("partner_code", "settlement_currency_code"), "partner_code", "inner")
    )
    df = df.join(fx, (df["currency_code"] == fx["fx_currency_code"]) & (df["settlement_currency_code"] == fx["fx_quote"]), "left")
    if partnerScope and partnerScope.upper() != "ALL":
        df = df.filter(F.col("partner_code") == partnerScope.upper())
    isEu = F.col("region_code") == "EU"
    df = df.withColumn("customer_reference", F.when(isEu & ~consent, F.lit("REDACTED")).otherwise(F.col("customer_reference")))
    if suppressUnconsentedEuRows:
        df = df.filter(~(isEu & (F.col("customer_reference") == "REDACTED")))
    return df.select(
        "partner_code",
        "invoice_number",
        "invoice_date",
        "customer_reference",
        "stock_item_code",
        "quantity",
        (F.col("extended_price") * F.coalesce(F.col("conversion_rate"), F.lit(1))).cast("decimal(19,4)").alias("net_amount"),
        "settlement_currency_code",
        "region_code",
    )


def writeFeedFile(feed: DataFrame, outboundDir: str, asOf: date) -> str:
    os.makedirs(outboundDir, exist_ok=True)
    path = os.path.join(outboundDir, f"partner_feed_{asOf.strftime('%Y%m%d')}.csv")
    rows = feed.select(*FEED_COLUMNS).orderBy("partner_code", "invoice_number", "stock_item_code").collect()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(FEED_COLUMNS)
        for r in rows:
            writer.writerow([r[c] for c in FEED_COLUMNS])
    return path


def runPartnerFeed(
    spark: SparkSession,
    batchId: int,
    outboundDir: str,
    partnerScope: str = "ALL",
    suppressUnconsentedEuRows: bool = True,
    feedFromDate: str = None,
):
    fact = readTable(spark, FACT_SALE_TABLE)
    customers = readTable(spark, CUSTOMER_TABLE)
    items = readTable(spark, STOCK_ITEM_TABLE)
    partners = derivePartners(customers)
    saveTable(withAudit(partners, PACKAGE, batchId), PARTNER_TABLE)
    if feedFromDate:
        fromDate = date.fromisoformat(feedFromDate)
        asOf = fromDate
    else:
        maxDate = fact.agg(F.max("invoice_date")).collect()[0][0]
        asOf = maxDate
        fromDate = date.fromordinal(maxDate.toordinal() - 1)
    feed = withAudit(
        buildPartnerFeed(fact, customers, items, partners, readTable(spark, FX_TABLE), fromDate, partnerScope, suppressUnconsentedEuRows),
        PACKAGE,
        batchId,
    )
    feed = feed.withColumn("feed_as_of_date", F.lit(asOf))
    saveTable(feed, FEED_TABLE)
    path = writeFeedFile(feed, outboundDir, asOf)
    metrics = {"export_row_count": feed.count()}
    writeMetrics(spark, PACKAGE, metrics, batchId)
    metrics["file_path"] = path
    return metrics
