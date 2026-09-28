"""SLS_Export_PartnerFeed: outbound partner sales feed.

Partners receive only the lines for their own accounts, EU rows without a
sharing consent are redacted (and dropped when SuppressUnconsentedEuRows), and
amounts are restated into the partner's settlement currency. The legacy
package names the file partner_feed_YYYYMMDD.csv (code page 1252) and keeps a
copy in work.PartnerFeedArchive; the .dtsx contains no Flat File Destination,
so header/delimiter follow the SSIS flat-file defaults (see mapping doc).
"""
from __future__ import annotations

import csv
import io
import os
import shutil
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from sales_common import MONEY, legacyColumnCandidates, resolveColumns
from sales_schemas import PARTNER_FEED_COLUMNS

FEED_NAME = "partner_feed"
FEED_ENCODING = "cp1252"          # DefaultCodePage 1252 on the legacy components
FEED_DELIMITER = ","
FEED_LINE_TERMINATOR = "\r\n"     # SSIS flat-file default row delimiter
FEED_OBJECT_NAME = "file:partner_feed.csv"

FACT_SALE_COLUMNS = {
    "CustomerKey": legacyColumnCandidates("Customer Key"),
    "StockItemKey": legacyColumnCandidates("Stock Item Key"),
    "InvoiceNumber": legacyColumnCandidates("WWI Invoice ID"),
    "InvoiceDate": legacyColumnCandidates("Invoice Date Key"),
    "Quantity": ["Quantity"],
    "TotalExcludingTax": legacyColumnCandidates("Total Excluding Tax"),
    "CurrencyCode": legacyColumnCandidates("Currency Code") + legacyColumnCandidates("Transaction Currency Code"),
}
DIM_CUSTOMER_COLUMNS = {
    "CustomerKey": legacyColumnCandidates("Customer Key"),
    "PartnerKey": legacyColumnCandidates("Partner Key"),
    "RegionCode": legacyColumnCandidates("Region Code"),
    "ShareConsentFlag": legacyColumnCandidates("Share Consent Flag"),
    "CustomerReference": legacyColumnCandidates("Customer Reference") + legacyColumnCandidates("Source Customer Reference"),
}
DIM_STOCK_ITEM_COLUMNS = {
    "StockItemKey": legacyColumnCandidates("Stock Item Key"),
    "StockItemCode": legacyColumnCandidates("Stock Item Code"),
}
DIM_PARTNER_COLUMNS = {
    "PartnerKey": legacyColumnCandidates("Partner Key"),
    "PartnerCode": legacyColumnCandidates("Partner Code"),
    "SettlementCurrencyCode": legacyColumnCandidates("Settlement Currency Code"),
}
FX_REVALUATION_COLUMNS = {
    "CurrencyCode": ["CurrencyCode", "FromCurrencyCode"],
    "QuoteCurrencyCode": ["QuoteCurrencyCode", "ToCurrencyCode"],
    "RateTypeCode": ["RateTypeCode"],
    "ConversionRate": ["ConversionRate", "Rate"],
}


def legacyFactSale(df: DataFrame) -> DataFrame:
    return resolveColumns(df, FACT_SALE_COLUMNS)


def legacyDimCustomer(df: DataFrame) -> DataFrame:
    return resolveColumns(df, DIM_CUSTOMER_COLUMNS, optional=("ShareConsentFlag",))


def legacyDimStockItem(df: DataFrame) -> DataFrame:
    return resolveColumns(df, DIM_STOCK_ITEM_COLUMNS)


def legacyDimPartner(df: DataFrame) -> DataFrame:
    return resolveColumns(df, DIM_PARTNER_COLUMNS)


def legacyFxRevaluationRates(df: DataFrame) -> DataFrame:
    return resolveColumns(df, FX_REVALUATION_COLUMNS)


def buildPartnerFeedRows(factSale: DataFrame, dimCustomer: DataFrame, dimStockItem: DataFrame,
                         dimPartner: DataFrame, fxRates: DataFrame | None, partnerScope: str,
                         fromInvoiceDate: date) -> DataFrame:
    """'Build Partner Feed Rows' (INSERT INTO work.PartnerFeedRow ... FROM Fact.Sale ...)."""
    c = dimCustomer.select(F.col("CustomerKey").alias("c_CustomerKey"), "PartnerKey",
                           F.col("RegionCode"), "ShareConsentFlag", "CustomerReference")
    si = dimStockItem.select(F.col("StockItemKey").alias("si_StockItemKey"), "StockItemCode")
    pa = dimPartner.select(F.col("PartnerKey").alias("pa_PartnerKey"), "PartnerCode", "SettlementCurrencyCode")
    s = factSale.where(F.col("InvoiceDate").cast("date") >= F.lit(fromInvoiceDate))
    df = (s.join(c, s["CustomerKey"] == c["c_CustomerKey"], "inner")
           .join(si, s["StockItemKey"] == si["si_StockItemKey"], "inner")
           .join(pa, c["PartnerKey"] == pa["pa_PartnerKey"], "inner"))
    if fxRates is not None:
        fx = (fxRates.where(F.col("RateTypeCode") == "AVERAGE")
                     .select(F.col("CurrencyCode").alias("fx_CurrencyCode"),
                             F.col("QuoteCurrencyCode").alias("fx_QuoteCurrencyCode"),
                             F.col("ConversionRate"))
                     .dropDuplicates(["fx_CurrencyCode", "fx_QuoteCurrencyCode"]))
        df = df.join(fx, (df["CurrencyCode"] == fx["fx_CurrencyCode"])
                     & (df["SettlementCurrencyCode"] == fx["fx_QuoteCurrencyCode"]), "left")
    else:
        df = df.withColumn("ConversionRate", F.lit(None).cast(MONEY))
    scope = (partnerScope or "ALL").strip()
    if scope.upper() != "ALL":
        df = df.where(F.col("PartnerCode") == F.lit(scope))
    redacted = (F.col("RegionCode") == "EU") & (~F.coalesce(F.col("ShareConsentFlag").cast("boolean"), F.lit(False)))
    return df.select(
        "PartnerCode",
        F.col("InvoiceNumber").cast("string").alias("InvoiceNumber"),
        F.col("InvoiceDate").cast("date").alias("InvoiceDate"),
        F.when(redacted, F.lit("REDACTED")).otherwise(F.col("CustomerReference")).alias("CustomerReference"),
        "StockItemCode",
        "Quantity",
        (F.col("TotalExcludingTax") * F.coalesce(F.col("ConversionRate"), F.lit(1))).cast(MONEY).alias("NetAmount"),
        "SettlementCurrencyCode",
        "RegionCode",
    )


def suppressUnconsentedEuRows(rows: DataFrame) -> DataFrame:
    """DELETE FROM work.PartnerFeedRow WHERE RegionCode = 'EU' AND CustomerReference = 'REDACTED'."""
    return rows.where(~((F.col("RegionCode") == "EU") & (F.col("CustomerReference") == "REDACTED")))


def orderedFeedRows(rows: DataFrame) -> DataFrame:
    """SELECT <feed columns> FROM work.PartnerFeedRow ORDER BY PartnerCode, InvoiceNumber."""
    return rows.select(*PARTNER_FEED_COLUMNS).orderBy("PartnerCode", "InvoiceNumber")


def outboundFileName(businessDate: date) -> str:
    """'partner_feed_' + YYYYMMDD + '.csv' (legacy uses GETDATE(); here BusinessDate)."""
    return "%s_%s.csv" % (FEED_NAME, businessDate.strftime("%Y%m%d"))


def archiveRelativePath(fileName: str, businessDate: date) -> str:
    """config/landing-zone.yaml archive layout: archive/{feed}/{yyyy}/{MM}/{original_filename}."""
    return os.path.join("archive", FEED_NAME, businessDate.strftime("%Y"), businessDate.strftime("%m"), fileName)


def _formatValue(column: str, value) -> str:
    if value is None:
        return ""
    if column == "InvoiceDate":
        return value.strftime("%Y-%m-%d") if hasattr(value, "strftime") else str(value)
    if column == "Quantity":
        dec = Decimal(str(value))
        return str(int(dec)) if dec == dec.to_integral_value() else str(dec.normalize())
    if column == "NetAmount":
        return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return str(value)


def renderFeed(rows: Iterable, columns=PARTNER_FEED_COLUMNS) -> str:
    """Render the feed exactly as the legacy flat file: header row, comma delimited,
    no text qualifier, CRLF row delimiter."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=FEED_DELIMITER, lineterminator=FEED_LINE_TERMINATOR,
                        quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    count = 0
    for row in rows:
        record = row.asDict() if hasattr(row, "asDict") else dict(row)
        writer.writerow([_formatValue(c, record.get(c)) for c in columns])
        count += 1
    return buffer.getvalue()


def writeFeedFiles(content: str, volumeRoot: str, fileName: str, businessDate: date) -> tuple[str, str]:
    """Write the outbound file and its archive copy under the UC Volume root."""
    outboundPath = os.path.join(volumeRoot, "outbound", FEED_NAME, fileName)
    archivePath = os.path.join(volumeRoot, archiveRelativePath(fileName, businessDate))
    for path in (outboundPath, archivePath):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(outboundPath, "w", encoding=FEED_ENCODING, newline="") as handle:
        handle.write(content)
    shutil.copyfile(outboundPath, archivePath)
    return outboundPath, archivePath


def toArchiveRows(rows: DataFrame, fileName: str, batchId: int, packageExecutionId) -> DataFrame:
    return (rows.withColumn("BatchId", F.lit(int(batchId)).cast("long"))
                .withColumn("ExportFileName", F.lit(fileName))
                .withColumn("ExportedAtUtc", F.current_timestamp())
                .withColumn("PackageExecutionId", F.lit(packageExecutionId).cast("long")))
