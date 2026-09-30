"""ING_FILE_PartnerSales_{NA,EU,APAC}: partner sales file ingestion into the bronze raw table.

Each feed is parsed with its own layout (record markers, date format, decimal separator,
tax treatment), validated with the package's conditional-split rules, reconciled against the
footer control record, and then archived (all controls matched) or quarantined.
"""

import csv
import os
import shutil
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_performance import config
from sales_performance.common import saveTable, withAudit

RAW_TABLE = "bronze_raw_file_partner_sales"
REJECT_TABLE = "bronze_rejected_file_row"
REGISTER_TABLE = "bronze_inbound_file_register"

MONEY = T.DecimalType(19, 4)
QTY = T.DecimalType(18, 3)


@dataclass(frozen=True)
class FeedSpec:
    regionCode: str
    packageName: str
    folder: str
    filePattern: str
    encoding: str
    delimiter: str
    header: bool
    columns: tuple


NA_COLUMNS = (
    "record_type",
    "partner_code",
    "store_number",
    "transaction_number",
    "transaction_date_text",
    "product_code",
    "upc",
    "quantity_text",
    "unit_price_text",
    "state_tax_text",
    "county_tax_text",
    "line_total_text",
    "currency_code",
    "state_code",
    "postal_code",
    "footer_record_count_text",
)
EU_COLUMNS = (
    "record_type",
    "partner_code",
    "outlet_code",
    "receipt_number",
    "transaction_date_text",
    "article_number",
    "ean",
    "quantity_text",
    "gross_amount_text",
    "vat_rate_text",
    "vat_registration_number",
    "currency_code",
    "country_code",
    "postal_code",
    "consent_flag",
    "footer_amount_text",
)
APAC_COLUMNS = (
    "record_marker",
    "partner_code",
    "outlet_code",
    "outlet_name",
    "slip_number",
    "transaction_date_text",
    "item_code",
    "quantity_text",
    "net_amount_text",
    "gst_amount_text",
    "currency_code",
    "country_code",
    "postal_district",
    "footer_total_text",
)

FEEDS = {
    "NA": FeedSpec("NA", "ING_FILE_PartnerSales_NA", "inbound/partner/na", "partner_sales_na_", "windows-1252", ",", True, NA_COLUMNS),
    # The generator docstring says "semicolon" but the shipped .dtsx flat-file connection manager and
    # config/landing-zone.yaml both use "," with a double-quote text qualifier (comma decimals are
    # quoted); the persisted artefacts win. See README "EU delimiter".
    "EU": FeedSpec("EU", "ING_FILE_PartnerSales_EU", "inbound/partner/eu", "partner_sales_eu_", "UTF-8", ",", True, EU_COLUMNS),
    # config/landing-zone.yaml (and the CodePage="28591" persisted in the shipped .dtsx) win over the
    # generator docstring's "code page 932"; see README "APAC encoding".
    "APAC": FeedSpec(
        "APAC", "ING_FILE_PartnerSales_APAC", "inbound/partner/apac", "partner_sales_apac_", "ISO-8859-1", "\t", False, APAC_COLUMNS
    ),
}

RAW_SCHEMA = """
    region_code string, source_system_code string, record_type string, partner_code string,
    partner_outlet_code string, partner_order_ref string, sale_date_text string, transaction_date date,
    customer_ref string, item_ref string, barcode string, quantity_text string, quantity decimal(18,3),
    unit_price decimal(19,4), amount_text string, gross_amount decimal(19,4), tax_amount decimal(19,4),
    net_amount decimal(19,4), tax_rate_percent decimal(9,4), tax_treatment_code string,
    currency_text string, country_text string, country_code string, state_code string, postal_code string,
    marketable_flag string, outlet_name string, source_file_name string, source_row_number bigint
"""

REJECT_SCHEMA = """
    region_code string, package_name string, source_file_name string, source_row_number bigint,
    reject_reason_code string, reject_reason_text string, raw_record string
"""


def countryNameMap():
    return F.create_map(*[x for k, (name, _, _) in config.COUNTRY_MAP.items() for x in (F.lit(k), F.lit(name))])


def _cleanNumber(col):
    return F.regexp_replace(F.trim(col), "[,$ ]", "")


def _euNumber(col):
    return F.regexp_replace(F.regexp_replace(F.trim(col), "\\.", ""), ",", ".")


def _splitOutsideQuotes(delimiter: str):
    """Split on the delimiter only when it is outside a double-quoted field (SSIS TextQualifier)."""
    return F.split(F.col("value"), "\\Q" + delimiter + '\\E(?=(?:[^"]*"[^"]*")*[^"]*$)', -1)


def _unquote(col):
    return F.regexp_replace(F.regexp_replace(F.trim(col), '^"(.*)"$', "$1"), '""', '"')


def readFeedFile(spark: SparkSession, spec: FeedSpec, path: str) -> DataFrame:
    """Read one landed file as all-text columns, keeping the raw record and its row number."""
    lines = (
        spark.read.format("csv")
        .option("encoding", spec.encoding)
        .option("sep", "\u0001")
        .option("quote", "")
        .option("header", "false")
        .schema("value string")
        .load(path)
        .withColumn("_pos", F.monotonically_increasing_id())
    )
    lines = lines.withColumn("source_row_number", F.row_number().over(Window.orderBy("_pos")))
    if spec.header:
        lines = lines.filter(F.col("source_row_number") > 1)
    lines = lines.filter(F.length(F.trim(F.col("value"))) > 0)
    parts = _splitOutsideQuotes(spec.delimiter)
    cols = [_unquote(parts.getItem(i)).alias(name) for i, name in enumerate(spec.columns)]
    return lines.select(
        F.col("value").alias("raw_record"),
        F.col("source_row_number"),
        F.size(parts).alias("field_count"),
        *cols,
    ).withColumn("source_file_name", F.lit(os.path.basename(path)))


def _typedRecordFilter(spec: FeedSpec, df: DataFrame):
    if spec.regionCode == "NA":
        return F.col("record_type") == "D", F.col("record_type") == "T", F.col("record_type").isin("D", "H", "T")
    if spec.regionCode == "EU":
        return F.col("record_type") == "2", F.col("record_type") == "9", F.col("record_type").isin("1", "2", "9")
    isDetail = ~F.col("record_marker").isin("#HEAD", "#TOTAL")
    return isDetail, F.col("record_marker") == "#TOTAL", F.lit(True)


def parseNaDetail(df: DataFrame) -> DataFrame:
    dateText = F.trim(F.col("transaction_date_text"))
    return df.select(
        F.lit("NA").alias("region_code"),
        F.lit(config.SOURCE_SYSTEM_PARTNER).alias("source_system_code"),
        F.col("record_type"),
        F.col("partner_code"),
        F.col("store_number").alias("partner_outlet_code"),
        F.col("transaction_number").alias("partner_order_ref"),
        dateText.alias("sale_date_text"),
        F.to_date(dateText, "MM/dd/yyyy").alias("transaction_date"),
        F.col("store_number").alias("customer_ref"),
        F.col("product_code").alias("item_ref"),
        F.col("upc").alias("barcode"),
        F.col("quantity_text"),
        _cleanNumber(F.col("quantity_text")).cast(QTY).alias("quantity"),
        _cleanNumber(F.col("unit_price_text")).cast(MONEY).alias("unit_price"),
        F.col("line_total_text").alias("amount_text"),
        _cleanNumber(F.col("line_total_text")).cast(MONEY).alias("gross_amount"),
        (_cleanNumber(F.col("state_tax_text")).cast(MONEY) + _cleanNumber(F.col("county_tax_text")).cast(MONEY)).alias("tax_amount"),
        F.lit(None).cast(MONEY).alias("net_amount"),
        F.lit(None).cast(T.DecimalType(9, 4)).alias("tax_rate_percent"),
        F.lit("SALESTAX").alias("tax_treatment_code"),
        F.col("currency_code").alias("currency_text"),
        F.when(F.upper(F.col("currency_code")) == "CAD", "CANADA").otherwise("UNITED STATES").alias("country_text"),
        F.when(F.upper(F.col("currency_code")) == "CAD", "CA").otherwise("US").alias("country_code"),
        F.col("state_code"),
        F.col("postal_code"),
        F.lit(None).cast("string").alias("marketable_flag"),
        F.lit(None).cast("string").alias("outlet_name"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.col("raw_record"),
    ).withColumn(
        "is_valid",
        (F.length(F.trim(F.col("partner_code"))) > 0)
        & (F.length(F.trim(F.col("partner_order_ref"))) > 0)
        & (F.col("quantity") > 0)
        & (F.col("gross_amount") >= 0)
        & F.col("transaction_date").isNotNull()
        & F.col("tax_amount").isNotNull(),
    )


def parseEuDetail(df: DataFrame) -> DataFrame:
    dateText = F.trim(F.col("transaction_date_text"))
    gross = _euNumber(F.col("gross_amount_text")).cast(MONEY)
    vatRate = _euNumber(F.col("vat_rate_text")).cast(T.DecimalType(9, 4))
    net = (gross / (F.lit(1) + vatRate / F.lit(100))).cast(MONEY)
    return df.select(
        F.lit("EU").alias("region_code"),
        F.lit(config.SOURCE_SYSTEM_PARTNER).alias("source_system_code"),
        F.col("record_type"),
        F.col("partner_code"),
        F.col("outlet_code").alias("partner_outlet_code"),
        F.col("receipt_number").alias("partner_order_ref"),
        dateText.alias("sale_date_text"),
        F.to_date(dateText, "dd/MM/yyyy").alias("transaction_date"),
        F.col("vat_registration_number").alias("customer_ref"),
        F.col("article_number").alias("item_ref"),
        F.col("ean").alias("barcode"),
        F.col("quantity_text"),
        _euNumber(F.col("quantity_text")).cast(QTY).alias("quantity"),
        F.lit(None).cast(MONEY).alias("unit_price"),
        F.col("gross_amount_text").alias("amount_text"),
        gross.alias("gross_amount"),
        (gross - net).alias("tax_amount"),
        net.alias("net_amount"),
        vatRate.alias("tax_rate_percent"),
        F.lit("VAT").alias("tax_treatment_code"),
        F.col("currency_code").alias("currency_text"),
        F.coalesce(countryNameMap()[F.upper(F.col("country_code"))], F.upper(F.col("country_code"))).alias("country_text"),
        F.upper(F.col("country_code")).alias("country_code"),
        F.lit(None).cast("string").alias("state_code"),
        F.col("postal_code"),
        F.when(F.upper(F.col("consent_flag")).isin("J", "Y"), "Y").otherwise("N").alias("marketable_flag"),
        F.lit(None).cast("string").alias("outlet_name"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.col("raw_record"),
    ).withColumn(
        "is_valid",
        (F.length(F.trim(F.col("customer_ref"))) >= 8)
        & (F.length(F.trim(F.col("partner_order_ref"))) > 0)
        & (F.col("quantity") != 0)
        & (F.col("tax_rate_percent") >= 0)
        & F.col("transaction_date").isNotNull()
        & F.col("gross_amount").isNotNull(),
    )


def parseApacDetail(df: DataFrame) -> DataFrame:
    dateText = F.trim(F.col("transaction_date_text"))
    net = _cleanNumber(F.col("net_amount_text")).cast(MONEY)
    gst = _cleanNumber(F.col("gst_amount_text")).cast(MONEY)
    return df.select(
        F.lit("APAC").alias("region_code"),
        F.lit(config.SOURCE_SYSTEM_PARTNER).alias("source_system_code"),
        F.col("record_marker").alias("record_type"),
        F.col("partner_code"),
        F.col("outlet_code").alias("partner_outlet_code"),
        F.col("slip_number").alias("partner_order_ref"),
        dateText.alias("sale_date_text"),
        F.to_date(dateText, "yyyy/MM/dd").alias("transaction_date"),
        F.col("outlet_code").alias("customer_ref"),
        F.col("item_code").alias("item_ref"),
        F.lit(None).cast("string").alias("barcode"),
        F.col("quantity_text"),
        _cleanNumber(F.col("quantity_text")).cast(QTY).alias("quantity"),
        F.lit(None).cast(MONEY).alias("unit_price"),
        F.col("net_amount_text").alias("amount_text"),
        (net + gst).alias("gross_amount"),
        gst.alias("tax_amount"),
        net.alias("net_amount"),
        F.when(net != 0, (gst / net * 100).cast(T.DecimalType(9, 4))).alias("tax_rate_percent"),
        F.lit("GST").alias("tax_treatment_code"),
        F.col("currency_code").alias("currency_text"),
        F.coalesce(countryNameMap()[F.upper(F.col("country_code"))], F.upper(F.col("country_code"))).alias("country_text"),
        F.upper(F.col("country_code")).alias("country_code"),
        F.lit(None).cast("string").alias("state_code"),
        F.when(F.length(F.trim(F.col("postal_district"))) < 6, F.lpad(F.trim(F.col("postal_district")), 6, "0"))
        .otherwise(F.trim(F.col("postal_district")))
        .alias("postal_code"),
        F.lit(None).cast("string").alias("marketable_flag"),
        F.col("outlet_name"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.col("raw_record"),
    ).withColumn(
        "is_valid",
        (F.length(F.trim(F.col("partner_order_ref"))) > 0)
        & (F.length(F.trim(F.col("item_ref"))) > 0)
        & (F.col("quantity") > 0)
        & (F.col("tax_amount") >= 0)
        & F.col("transaction_date").isNotNull()
        & F.col("net_amount").isNotNull(),
    )


PARSERS = {"NA": parseNaDetail, "EU": parseEuDetail, "APAC": parseApacDetail}


def footerControl(spec: FeedSpec, footer: DataFrame):
    """Return (controlName, controlValue) carried by the feed's footer record."""
    if footer.count() == 0:
        return None, None
    row = footer.limit(1).collect()[0]
    if spec.regionCode == "NA":
        text = row["footer_record_count_text"] or row["partner_code"]
        try:
            return "detail_row_count", int(_pyClean(text))
        except ValueError:
            return "detail_row_count", None
    if spec.regionCode == "EU":
        text = row["footer_amount_text"] or row["partner_code"]
        try:
            return "gross_amount_sum", float(_pyClean(text).replace(".", "").replace(",", "."))
        except ValueError:
            return "gross_amount_sum", None
    text = row["footer_total_text"] or row["partner_code"]
    try:
        return "net_amount_sum", float(_pyClean(text))
    except ValueError:
        return "net_amount_sum", None


def _pyClean(text):
    return (text or "").strip().replace(" ", "").replace("$", "")


def processFeedFile(spark: SparkSession, spec: FeedSpec, path: str, batchId: int):
    """Parse, validate and reconcile a single file. Returns (validDf, rejectDf, summary)."""
    lines = readFeedFile(spark, spec, path)
    isDetail, isFooter, isKnown = _typedRecordFilter(spec, lines)
    expectedFields = len(spec.columns)
    wellFormed = F.col("field_count") >= expectedFields - 1
    detail = lines.filter(isDetail & isKnown & wellFormed)
    unknown = lines.filter(~isKnown | (isDetail & ~wellFormed))
    footer = lines.filter(isFooter & isKnown)

    parsed = PARSERS[spec.regionCode](detail)
    valid = parsed.filter(F.col("is_valid")).drop("is_valid", "raw_record")
    malformed = parsed.filter(~F.col("is_valid") | F.col("is_valid").isNull())

    rejects = malformed.select(
        F.lit(spec.regionCode).alias("region_code"),
        F.lit(spec.packageName).alias("package_name"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.lit("MALFORMED" if spec.regionCode != "EU" else "VAT_MISSING").alias("reject_reason_code"),
        F.lit(f"{spec.regionCode} partner detail row failed validation.").alias("reject_reason_text"),
        F.col("raw_record"),
    ).unionByName(
        unknown.select(
            F.lit(spec.regionCode).alias("region_code"),
            F.lit(spec.packageName).alias("package_name"),
            F.col("source_file_name"),
            F.col("source_row_number"),
            F.lit("UNKNOWN_RECORD_TYPE").alias("reject_reason_code"),
            F.lit("Record type/marker not recognised or field count wrong.").alias("reject_reason_text"),
            F.col("raw_record"),
        )
    )

    detailCount = parsed.count()
    validCount = valid.count()
    malformedCount = detailCount - validCount
    unknownCount = unknown.count()
    controlName, controlValue = footerControl(spec, footer)
    if controlName == "detail_row_count":
        landed = float(detailCount)
    elif controlName == "gross_amount_sum":
        landed = float(parsed.agg(F.coalesce(F.sum("gross_amount"), F.lit(0))).collect()[0][0])
    else:
        landed = float(parsed.agg(F.coalesce(F.sum("net_amount"), F.lit(0))).collect()[0][0])
    controlsMatch = controlValue is not None and abs(landed - float(controlValue)) < 0.005 and unknownCount == 0
    summary = dict(
        file_name=os.path.basename(path),
        detail_rows=detailCount,
        valid_rows=validCount,
        malformed_rows=malformedCount,
        unknown_rows=unknownCount,
        control_name=controlName,
        control_value=controlValue,
        landed_value=landed,
        controls_match=bool(controlsMatch),
    )
    return withAudit(valid, spec.packageName, batchId), withAudit(rejects, spec.packageName, batchId), summary


def listInboundFiles(rootPath: str, spec: FeedSpec):
    folder = os.path.join(rootPath, spec.folder)
    if not os.path.isdir(folder):
        return []
    return sorted(
        os.path.join(folder, f) for f in os.listdir(folder) if f.startswith(spec.filePattern) and os.path.isfile(os.path.join(folder, f))
    )


def moveFile(path: str, destinationDir: str):
    """Archive/quarantine move implemented as copy + delete so it also works on /Volumes FUSE paths."""
    os.makedirs(destinationDir, exist_ok=True)
    target = os.path.join(destinationDir, os.path.basename(path))
    if os.path.exists(target):
        os.remove(target)
    shutil.copyfile(path, target)
    os.remove(path)
    return target


def alreadyProcessed(spark: SparkSession, fileName: str) -> bool:
    fullName = config.tableName(REGISTER_TABLE)
    if not spark.catalog.tableExists(fullName):
        return False
    return spark.table(fullName).filter((F.col("file_name") == fileName) & (F.col("file_status") == "Processed")).count() > 0


def ingestFeed(spark: SparkSession, regionCode: str, rootPath: str, batchId: int, moveFiles: bool = True):
    """Run one ING_FILE_PartnerSales_* package over every file in its drop folder."""
    spec = FEEDS[regionCode]
    results = []
    for path in listInboundFiles(rootPath, spec):
        fileName = os.path.basename(path)
        if alreadyProcessed(spark, fileName):
            continue
        valid, rejects, summary = processFeedFile(spark, spec, path, batchId)
        if summary["controls_match"]:
            saveTable(valid, RAW_TABLE, mode="append")
            status = "Processed"
            destination = os.path.join(rootPath, "archive", spec.folder.split("/", 1)[1])
        else:
            status = "Quarantined"
            destination = os.path.join(rootPath, "quarantine", spec.folder.split("/", 1)[1])
        if rejects.limit(1).count() > 0:
            saveTable(rejects, REJECT_TABLE, mode="append")
        if moveFiles:
            moveFile(path, destination)
        register = spark.createDataFrame(
            [
                (
                    regionCode,
                    spec.packageName,
                    fileName,
                    status,
                    summary["detail_rows"],
                    summary["valid_rows"],
                    summary["malformed_rows"],
                    summary["unknown_rows"],
                    summary["control_name"],
                    None if summary["control_value"] is None else float(summary["control_value"]),
                    float(summary["landed_value"]),
                    summary["controls_match"],
                    batchId,
                )
            ],
            "region_code string, package_name string, file_name string, file_status string, detail_rows bigint, "
            "valid_rows bigint, malformed_rows bigint, unknown_rows bigint, control_name string, control_value double, "
            "landed_value double, controls_match boolean, batch_id bigint",
        ).withColumn("registered_at_utc", F.current_timestamp())
        saveTable(register, REGISTER_TABLE, mode="append")
        summary["file_status"] = status
        results.append(summary)
    ensureTables(spark)
    return results


def ensureTables(spark: SparkSession):
    for name, schema in ((RAW_TABLE, RAW_SCHEMA), (REJECT_TABLE, REJECT_SCHEMA)):
        if not spark.catalog.tableExists(config.tableName(name)):
            empty = withAudit(spark.createDataFrame([], schema), "init", 0)
            saveTable(empty, name)


def _pyDecimal(text, euStyle=False):
    from decimal import Decimal, InvalidOperation

    t = (text or "").strip().replace(" ", "").replace("$", "")
    if euStyle:
        t = t.replace(".", "").replace(",", ".")
    else:
        t = t.replace(",", "")
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def _pyDate(text, fmt):
    from datetime import datetime

    try:
        return datetime.strptime((text or "").strip(), fmt).date()
    except ValueError:
        return None


def pythonParseFile(spec: FeedSpec, path: str):
    """Independent, Spark-free re-implementation of the feed rules used by the recon task.

    Returns the (file_name, partner_code, partner_order_ref, item_ref, quantity, gross_amount,
    transaction_date) tuples that the package should have landed for the file: the valid detail
    rows when the footer control reconciles and no unknown records exist, otherwise nothing."""
    from decimal import Decimal

    with open(path, encoding=spec.encoding, newline="") as fh:
        records = [row for row in csv.reader(fh, delimiter=spec.delimiter, quotechar='"') if any(x.strip() for x in row)]
    if spec.header:
        records = records[1:]
    n = len(spec.columns)
    valid, detailCount, unknown, control, sumGross, sumNet = [], 0, 0, None, Decimal(0), Decimal(0)
    fileName = os.path.basename(path)
    for row in records:
        parts = [p.strip() for p in row]
        marker = parts[0] if parts else ""
        isFooter = marker in ("T", "9", "#TOTAL") or marker == "#HEAD"
        if len(parts) < n - 1 and not isFooter:
            unknown += 1
            continue
        parts += [""] * (n - len(parts))
        rec = dict(zip(spec.columns, parts))
        if spec.regionCode == "NA":
            rt = rec["record_type"]
            if rt not in ("D", "H", "T"):
                unknown += 1
                continue
            if rt == "T":
                control = _pyDecimal(rec["footer_record_count_text"] or rec["partner_code"])
                continue
            if rt != "D":
                continue
            detailCount += 1
            qty, gross = _pyDecimal(rec["quantity_text"]), _pyDecimal(rec["line_total_text"])
            tax = _pyDecimal(rec["state_tax_text"]), _pyDecimal(rec["county_tax_text"])
            d = _pyDate(rec["transaction_date_text"], "%m/%d/%Y")
            if (
                rec["partner_code"]
                and rec["transaction_number"]
                and qty is not None
                and qty > 0
                and gross is not None
                and gross >= 0
                and d
                and None not in tax
            ):
                valid.append((fileName, rec["partner_code"], rec["transaction_number"], rec["product_code"], qty, gross, d))
        elif spec.regionCode == "EU":
            rt = rec["record_type"]
            if rt not in ("1", "2", "9"):
                unknown += 1
                continue
            if rt == "9":
                control = _pyDecimal(rec["footer_amount_text"] or rec["partner_code"], euStyle=True)
                continue
            if rt != "2":
                continue
            detailCount += 1
            qty, gross, rate = (
                _pyDecimal(rec["quantity_text"], True),
                _pyDecimal(rec["gross_amount_text"], True),
                _pyDecimal(rec["vat_rate_text"], True),
            )
            d = _pyDate(rec["transaction_date_text"], "%d/%m/%Y")
            if gross is not None:
                sumGross += gross
            if (
                len(rec["vat_registration_number"]) >= 8
                and rec["receipt_number"]
                and qty is not None
                and qty != 0
                and rate is not None
                and rate >= 0
                and d
                and gross is not None
            ):
                valid.append((fileName, rec["partner_code"], rec["receipt_number"], rec["article_number"], qty, gross, d))
        else:
            marker = rec["record_marker"]
            if marker == "#HEAD":
                continue
            if marker == "#TOTAL":
                control = _pyDecimal(rec["footer_total_text"] or rec["partner_code"])
                continue
            detailCount += 1
            qty, net, gst = _pyDecimal(rec["quantity_text"]), _pyDecimal(rec["net_amount_text"]), _pyDecimal(rec["gst_amount_text"])
            d = _pyDate(rec["transaction_date_text"], "%Y/%m/%d")
            if net is not None:
                sumNet += net
            if (
                rec["slip_number"]
                and rec["item_code"]
                and qty is not None
                and qty > 0
                and gst is not None
                and gst >= 0
                and d
                and net is not None
            ):
                valid.append((fileName, rec["partner_code"], rec["slip_number"], rec["item_code"], qty, net + gst, d))
    landed = {"NA": Decimal(detailCount), "EU": sumGross, "APAC": sumNet}[spec.regionCode]
    if control is None or unknown or abs(landed - control) >= Decimal("0.005"):
        return []
    return valid
