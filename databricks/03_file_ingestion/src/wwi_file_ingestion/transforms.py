"""Data Flow transformations of the ING_FILE_* packages as DataFrame operations.

Each ``parse*`` function takes the output of ``lines.splitColumns`` (one row per
line, one string column per connection-manager column) and returns the same
rows with:

* ``RecordClass`` - which Conditional Split output the row took
  (``Detail`` / ``Header`` / ``Footer`` / ``Control`` / ``Unknown`` / ``ColumnCount``)
* the Derived Column outputs, typed as the SSIS expressions typed them
* ``RejectReasonCode`` / ``RejectReason`` - NULL for rows the package loaded,
  otherwise the err.RejectedFileRow reason of the branch that received the row

Reject precedence per row: COLUMN_COUNT (Flat File Source error output) ->
UNKNOWN_RECORD_TYPE (split default output) -> CONVERSION (a derived cast that
failed) -> the package's own validation reason. Header rows are dropped
(``RecordClass = Header``) and footer rows are kept with reason ``FOOTER`` so
the Foreach loop's control-total tasks can read them back, which is how
ING_FILE_PartnerSales_NA reads its footer count from err.RejectedFileRow.
"""

from __future__ import annotations

from typing import Callable, Dict, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import feeds
from .lines import emptyText, nonEmptyText

MONEY = "decimal(19,4)"  # DT_CY
QUANTITY = "decimal(18,3)"
RATE = "decimal(18,8)"
VAT_RATE = "decimal(9,4)"

REASON_COLUMN_COUNT = "COLUMN_COUNT"
REASON_UNKNOWN_RECORD = "UNKNOWN_RECORD_TYPE"
REASON_CONVERSION = "CONVERSION"
REASON_FOOTER = "FOOTER"
REASON_FX_UNKNOWN_PAIR = "FX_UNKNOWN_PAIR"
REASON_EMPTY_LINE = "EMPTY_LINE"
REASON_QUARANTINED = "QUARANTINED"

FX_TOLERANCE_BASIS_POINTS = 500

CLASS_DETAIL = "Detail"
CLASS_HEADER = "Header"
CLASS_FOOTER = "Footer"
CLASS_CONTROL = "Control"
CLASS_UNKNOWN = "Unknown"
CLASS_COLUMN_COUNT = "ColumnCount"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def tryCast(df: DataFrame, name: str, expression: Column, dataType: str) -> DataFrame:
    """SSIS explicit cast without ANSI failures: NULL when the text does not convert."""
    tmp = "_cast_" + name
    return (
        df.withColumn(tmp, expression)
        .withColumn(name, F.expr("try_cast(%s AS %s)" % (tmp, dataType)))
        .drop(tmp)
    )


def conversionFailed(*pairs: Sequence) -> Column:
    """True when any (sourceText, typedValue) pair produced no typed value.

    A derived-column cast that yields NULL is the row the SSIS component would
    have raised a conversion error for; an empty string is a conversion error
    for numeric / date casts in SSIS too, so NULL typed value is the whole test.
    """
    failed = F.lit(False)
    for _textCol, typedCol in pairs:
        failed = failed | F.col(typedCol).isNull()
    return failed


def _isoFromParts(text: Column, yearPos: int, monthPos: int, dayPos: int) -> Column:
    return F.concat(
        F.substring(text, yearPos, 4), F.lit("-"),
        F.substring(text, monthPos, 2), F.lit("-"),
        F.substring(text, dayPos, 2),
    )


def _classifyColumnCount(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "_columnCountOk", F.col("ActualColumnCount") == F.col("ExpectedColumnCount")
    )


def _finish(df: DataFrame, validCondition: Column, validationReason: str, validationText: str) -> DataFrame:
    """Assign RecordClass / RejectReasonCode following the split precedence."""
    detail = F.col("RecordClass") == CLASS_DETAIL
    reason = (
        F.when(F.col("RecordClass") == CLASS_COLUMN_COUNT, F.lit(REASON_COLUMN_COUNT))
        .when(F.col("RecordClass") == CLASS_UNKNOWN, F.lit(REASON_UNKNOWN_RECORD))
        .when(F.col("RecordClass") == CLASS_FOOTER, F.lit(REASON_FOOTER))
        .when(F.col("RecordClass") == CLASS_HEADER, F.lit(None).cast("string"))
        .when(F.col("RecordClass") == CLASS_CONTROL, F.lit(None).cast("string"))
        .when(detail & F.col("_conversionFailed"), F.lit(REASON_CONVERSION))
        .when(detail & ~validCondition, F.lit(validationReason))
        .otherwise(F.lit(None).cast("string"))
    )
    text = (
        F.when(reason == REASON_COLUMN_COUNT, F.lit("Line does not carry the expected number of delimited columns."))
        .when(reason == REASON_UNKNOWN_RECORD, F.lit("Record type not recognised by the split."))
        .when(reason == REASON_FOOTER, F.lit("Footer / control record retained for control-total reconciliation."))
        .when(reason == REASON_CONVERSION, F.lit("A typed column could not be converted from its text value."))
        .when(reason == validationReason, F.lit(validationText))
        .otherwise(F.lit(None).cast("string"))
    )
    return (
        df.withColumn("RejectReasonCode", reason)
        .withColumn("RejectReason", text)
        .drop("_columnCountOk", "_conversionFailed")
    )


def _recordClass(df: DataFrame, detail: Column, header: Column, footer: Column) -> DataFrame:
    return df.withColumn(
        "RecordClass",
        F.when(~F.col("_columnCountOk"), F.lit(CLASS_COLUMN_COUNT))
        .when(detail, F.lit(CLASS_DETAIL))
        .when(header, F.lit(CLASS_HEADER))
        .when(footer, F.lit(CLASS_FOOTER))
        .otherwise(F.lit(CLASS_UNKNOWN)),
    )


def _audit(df: DataFrame, spec: feeds.FeedSpec) -> DataFrame:
    """audit_derivations(): SourceSystemCode, RegionCode, SourceFileName, ExtractedAtUtc."""
    return (
        df.withColumn("SourceSystemCode", F.lit(spec.sourceSystemCode))
        .withColumn("RegionCode", F.lit(spec.regionCode))
        .withColumn("SourceFileName", F.col("FileName"))
        .withColumn("ExtractedAtUtc", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# ING_FILE_PartnerSales_NA
# ---------------------------------------------------------------------------


def parsePartnerSalesNa(df: DataFrame) -> DataFrame:
    spec = feeds.PARTNER_SALES_NA
    df = _classifyColumnCount(df)
    df = _recordClass(
        df,
        detail=F.col("RecordType") == "D",
        header=F.col("RecordType") == "H",
        footer=F.col("RecordType") == "T",
    )
    text = F.col("TransactionDateText")
    df = tryCast(df, "TransactionDate", _isoFromParts(text, 7, 1, 4), "date")
    df = tryCast(df, "Quantity", F.col("QuantityText"), QUANTITY)
    df = tryCast(df, "UnitPrice", F.col("UnitPriceText"), MONEY)
    df = tryCast(df, "_stateTax", F.col("StateTaxText"), MONEY)
    df = tryCast(df, "_countyTax", F.col("CountyTaxText"), MONEY)
    df = df.withColumn("TaxAmount", (F.col("_stateTax") + F.col("_countyTax")).cast(MONEY))
    df = tryCast(df, "LineTotal", F.col("LineTotalText"), MONEY)
    df = df.withColumn("TaxTreatmentCode", F.lit("SALESTAX"))
    df = _audit(df, spec)
    df = df.withColumn(
        "_conversionFailed",
        conversionFailed(
            ("TransactionDateText", "TransactionDate"), ("QuantityText", "Quantity"),
            ("UnitPriceText", "UnitPrice"), ("StateTaxText", "_stateTax"),
            ("CountyTaxText", "_countyTax"), ("LineTotalText", "LineTotal"),
        ),
    ).drop("_stateTax", "_countyTax")
    valid = (
        nonEmptyText(F.col("PartnerCode")) & nonEmptyText(F.col("TransactionNumber"))
        & (F.col("Quantity") > 0) & (F.col("LineTotal") >= 0)
    )
    return _finish(df, valid, spec.rejectReasonCode, spec.rejectReason)


# ---------------------------------------------------------------------------
# ING_FILE_PartnerSales_EU
# ---------------------------------------------------------------------------


def _decimalComma(col: Column) -> Column:
    return F.regexp_replace(col, ",", ".")


def parsePartnerSalesEu(df: DataFrame) -> DataFrame:
    spec = feeds.PARTNER_SALES_EU
    df = _classifyColumnCount(df)
    df = _recordClass(
        df,
        detail=F.col("RecordType") == "2",
        header=F.col("RecordType") == "1",
        footer=F.col("RecordType") == "9",
    )
    text = F.col("TransactionDateText")
    df = tryCast(df, "TransactionDate", _isoFromParts(text, 7, 4, 1), "date")
    df = tryCast(df, "Quantity", _decimalComma(F.col("QuantityText")), QUANTITY)
    df = tryCast(df, "GrossAmount", _decimalComma(F.col("GrossAmountText")), MONEY)
    df = tryCast(df, "VatRate", _decimalComma(F.col("VatRateText")), VAT_RATE)
    df = df.withColumn("TaxTreatmentCode", F.lit("VAT"))
    df = df.withColumn(
        "MarketableFlag",
        F.when((F.col("ConsentFlag") == "J") | (F.col("ConsentFlag") == "Y"), F.lit("Y")).otherwise(F.lit("N")),
    )
    # Back Out VAT: NetAmount = Gross / (1 + VatRate/100); VatAmount = Gross - NetAmount
    divisor = F.lit(1) + F.col("VatRate") / F.lit(100)
    df = df.withColumn("NetAmount", (F.col("GrossAmount") / divisor).cast(MONEY))
    df = df.withColumn("VatAmount", (F.col("GrossAmount") - (F.col("GrossAmount") / divisor)).cast(MONEY))
    df = _audit(df, spec)
    df = df.withColumn(
        "_conversionFailed",
        conversionFailed(
            ("TransactionDateText", "TransactionDate"), ("QuantityText", "Quantity"),
            ("GrossAmountText", "GrossAmount"), ("VatRateText", "VatRate"),
        ),
    )
    valid = (
        (F.coalesce(F.length(F.trim(F.col("VatRegistrationNumber"))), F.lit(0)) >= 8)
        & nonEmptyText(F.col("ReceiptNumber"))
        & (F.col("Quantity") != 0) & (F.col("VatRate") >= 0)
    )
    return _finish(df, valid, spec.rejectReasonCode, spec.rejectReason)


# ---------------------------------------------------------------------------
# ING_FILE_PartnerSales_APAC
# ---------------------------------------------------------------------------


def parsePartnerSalesApac(df: DataFrame) -> DataFrame:
    spec = feeds.PARTNER_SALES_APAC
    df = _classifyColumnCount(df)
    marker = F.col("RecordMarker")
    df = _recordClass(
        df,
        detail=(marker != "#TOTAL") & (marker != "#HEAD"),
        header=marker == "#HEAD",
        footer=marker == "#TOTAL",
    )
    df = tryCast(df, "TransactionDate", F.regexp_replace(F.col("TransactionDateText"), "/", "-"), "date")
    df = tryCast(df, "Quantity", F.col("QuantityText"), QUANTITY)
    df = tryCast(df, "NetAmount", F.col("NetAmountText"), MONEY)
    df = tryCast(df, "GstAmount", F.col("GstAmountText"), MONEY)
    df = df.withColumn("GrossAmount", (F.col("NetAmount") + F.col("GstAmount")).cast(MONEY))
    df = df.withColumn("TaxTreatmentCode", F.lit("GST"))
    # RIGHT("000000" + TRIM(PostalDistrict), 6): zero-pad, never trim the district
    df = df.withColumn("PostalCode", F.substring(F.concat(F.lit("000000"), F.trim(F.col("PostalDistrict"))), -6, 6))
    df = _audit(df, spec)
    df = df.withColumn(
        "_conversionFailed",
        conversionFailed(
            ("TransactionDateText", "TransactionDate"), ("QuantityText", "Quantity"),
            ("NetAmountText", "NetAmount"), ("GstAmountText", "GstAmount"),
        ),
    )
    valid = (
        nonEmptyText(F.col("SlipNumber")) & nonEmptyText(F.col("ItemCode"))
        & (F.col("Quantity") > 0) & (F.col("GstAmount") >= 0)
    )
    return _finish(df, valid, spec.rejectReasonCode, spec.rejectReason)


# ---------------------------------------------------------------------------
# ING_FILE_CarrierScan
# ---------------------------------------------------------------------------


def parseCarrierScan(df: DataFrame) -> DataFrame:
    spec = feeds.CARRIER_SCAN
    df = _classifyColumnCount(df)
    # No record types: every well-formed line is a scan event.
    df = df.withColumn(
        "RecordClass",
        F.when(~F.col("_columnCountOk"), F.lit(CLASS_COLUMN_COUNT)).otherwise(F.lit(CLASS_DETAIL)),
    )
    ts = F.col("ScanTimestampText")
    # (DT_DBTIMESTAMP)SUBSTRING(text,1,19): the local wall-clock part of the ISO string.
    df = tryCast(df, "ScanTimestampUtc", F.regexp_replace(F.substring(ts, 1, 19), "T", " "), "timestamp")
    # (DT_I4)SUBSTRING(text,21,2) * 60 + (DT_I4)SUBSTRING(text,24,2): the package
    # reads the offset digits only; the sign at position 20 is not consulted.
    df = tryCast(df, "_offsetHours", F.substring(ts, 21, 2), "int")
    df = tryCast(df, "_offsetMinutes", F.substring(ts, 24, 2), "int")
    df = df.withColumn("ScanOffsetMinutes", (F.col("_offsetHours") * 60 + F.col("_offsetMinutes")).cast("int"))
    df = df.withColumn("ExceptionFlag", F.when(nonEmptyText(F.col("ExceptionReasonCode")), "Y").otherwise("N"))
    df = df.withColumn(
        "DeliveredFlag",
        F.when((F.col("ScanStatusCode") == "DLV") | (F.col("ScanStatusCode") == "POD"), "Y").otherwise("N"),
    )
    df = _audit(df, spec)
    df = df.withColumn(
        "_conversionFailed",
        conversionFailed(("ScanTimestampText", "ScanTimestampUtc"), ("ScanTimestampText", "ScanOffsetMinutes")),
    ).drop("_offsetHours", "_offsetMinutes")
    valid = (
        nonEmptyText(F.col("TrackingNumber")) & nonEmptyText(F.col("ScanStatusCode"))
        & (F.coalesce(F.length(F.trim(ts)), F.lit(0)) >= 19)
    )
    return _finish(df, valid, spec.rejectReasonCode, spec.rejectReason)


def countDuplicateScans(landedDf: DataFrame) -> int:
    """Count Duplicate Scans: groups of TrackingNumber/ScanStatusCode/ScanTimestampUtc seen more than once.

    Duplicates are counted, never rejected - the package only records the number.
    """
    dupes = (
        landedDf.groupBy("TrackingNumber", "ScanStatusCode", "ScanTimestampUtc")
        .count()
        .where(F.col("count") > 1)
    )
    return dupes.count()


# ---------------------------------------------------------------------------
# ING_FILE_SupplierCatalog
# ---------------------------------------------------------------------------


def parseSupplierCatalog(df: DataFrame) -> DataFrame:
    spec = feeds.SUPPLIER_CATALOG
    df = _classifyColumnCount(df)
    df = _recordClass(
        df,
        detail=F.col("RecordType") == "DTL",
        header=F.col("RecordType") == "HDR",
        footer=F.col("RecordType") == "TRL",
    )
    fromText = F.col("EffectiveFromText")
    toText = F.col("EffectiveToText")
    df = tryCast(df, "EffectiveFromDate", _isoFromParts(fromText, 1, 5, 7), "date")
    df = tryCast(
        df,
        "EffectiveToDate",
        F.when(F.length(F.trim(toText)) == 8, _isoFromParts(toText, 1, 5, 7)).otherwise(F.lit("9999-12-31")),
        "date",
    )
    df = tryCast(df, "ListPrice", F.col("ListPriceText"), MONEY)
    df = tryCast(df, "NetPrice", F.col("NetPriceText"), MONEY)
    df = tryCast(df, "PackSize", F.col("PackSizeText"), QUANTITY)
    df = tryCast(df, "LeadTimeDays", F.col("LeadTimeDaysText"), "int")
    df = df.withColumn("HazardousFlag", F.when(nonEmptyText(F.col("HazardClassCode")), "Y").otherwise("N"))
    df = _audit(df, spec)
    df = df.withColumn(
        "_conversionFailed",
        conversionFailed(
            ("EffectiveFromText", "EffectiveFromDate"), ("EffectiveToText", "EffectiveToDate"),
            ("ListPriceText", "ListPrice"), ("NetPriceText", "NetPrice"),
            ("PackSizeText", "PackSize"), ("LeadTimeDaysText", "LeadTimeDays"),
        ),
    )
    valid = (
        nonEmptyText(F.col("SupplierItemCode")) & (F.col("NetPrice") >= 0)
        & (F.col("ListPrice") >= F.col("NetPrice"))
        & (F.coalesce(F.length(F.trim(fromText)), F.lit(0)) == 8)
    )
    return _finish(df, valid, spec.rejectReasonCode, spec.rejectReason)


def priceChecksum(landedDf: DataFrame) -> int:
    """Compute Price Checksum: SUM(CAST(NetPrice * 100 AS bigint)) % 1000000 over the landed rows."""
    row = landedDf.agg(
        F.coalesce(F.sum((F.col("NetPrice") * 100).cast("bigint")) % 1000000, F.lit(0)).alias("checksum")
    ).collect()[0]
    return int(row["checksum"])


# ---------------------------------------------------------------------------
# ING_FILE_FxOverride
# ---------------------------------------------------------------------------


def parseFxOverride(df: DataFrame) -> DataFrame:
    """Parse Override + Four-eyes derivation; the published-rate lookup is applied by ``applyPublishedRate``."""
    spec = feeds.FX_OVERRIDE
    df = _classifyColumnCount(df)
    df = df.withColumn(
        "RecordClass",
        F.when(~F.col("_columnCountOk"), F.lit(CLASS_COLUMN_COUNT))
        .when(F.col("RecordType") == "FXO", F.lit(CLASS_DETAIL))
        .otherwise(F.lit(CLASS_CONTROL)),
    )
    df = tryCast(df, "RateDate", F.col("RateDateText"), "date")
    df = tryCast(df, "OverrideRate", F.col("OverrideRateText"), RATE)
    df = df.withColumn("RatePairCode", F.concat(F.col("FromCurrencyCode"), F.lit("/"), F.col("ToCurrencyCode")))
    df = df.withColumn(
        "FourEyesFlag",
        F.when(
            nonEmptyText(F.col("ApprovedByUser")) & (F.col("ApprovedByUser") != F.col("RequestedByUser")), "Y"
        ).otherwise("N"),
    )
    df = _audit(df, spec)
    return df.withColumn(
        "_conversionFailed",
        conversionFailed(("RateDateText", "RateDate"), ("OverrideRateText", "OverrideRate")),
    )


def applyPublishedRate(df: DataFrame, publishedRatesDf: DataFrame,
                       toleranceBasisPoints: int = FX_TOLERANCE_BASIS_POINTS) -> DataFrame:
    """Lookup Published Rate (no match -> FX_UNKNOWN_PAIR), Measure Deviation, Validate Override.

    ``publishedRatesDf`` must carry ``RatePairCode``, ``RateDate`` and ``PublishedRate``
    (the SPOT rates of raw.OracleFxRate). Precedence exactly as the data flow:
    unknown pair rows leave at the lookup, then the approval gate refuses a row
    unless FourEyesFlag = Y, an approval ticket is present and the deviation is
    within the tolerance band. Refused rows keep PublishedRate so the reject
    carries the rate the override was measured against.
    """
    spec = feeds.FX_OVERRIDE
    published = (
        publishedRatesDf.select("RatePairCode", "RateDate", "PublishedRate")
        .dropDuplicates(["RatePairCode", "RateDate"])
    )
    joined = df.join(published, on=["RatePairCode", "RateDate"], how="left")
    joined = joined.withColumn(
        "DeviationBasisPoints",
        F.when(F.col("PublishedRate").isNull(), F.lit(None).cast("int"))
        .when(F.col("PublishedRate") == 0, F.lit(0))
        .otherwise((F.abs(F.col("OverrideRate") - F.col("PublishedRate")) / F.col("PublishedRate") * 10000).cast("int")),
    )
    detail = F.col("RecordClass") == CLASS_DETAIL
    approved = (
        (F.col("FourEyesFlag") == "Y") & nonEmptyText(F.col("ApprovalTicketNumber"))
        & (F.col("DeviationBasisPoints") <= toleranceBasisPoints)
    )
    joined = joined.withColumn(
        "_unknownPair", detail & ~F.col("_conversionFailed") & F.col("PublishedRate").isNull()
    )
    unknownPair = F.col("_unknownPair")
    finished = _finish(joined, approved, spec.rejectReasonCode, spec.rejectReason)
    return (
        finished.withColumn(
            "RejectReasonCode",
            F.when(unknownPair, F.lit(REASON_FX_UNKNOWN_PAIR)).otherwise(F.col("RejectReasonCode")),
        ).withColumn(
            "RejectReason",
            F.when(unknownPair, F.lit("No published SPOT rate for the currency pair on the rate date."))
            .otherwise(F.col("RejectReason")),
        ).drop("_unknownPair")
    )


# ---------------------------------------------------------------------------
# ING_FILE_QuarantineMalformed
# ---------------------------------------------------------------------------


def originFeedCode(fileName: Column) -> Column:
    expr = F.lit("UNKNOWN")
    for needle, code in reversed(feeds.ORIGIN_FEED_CODES):
        expr = F.when(F.instr(fileName, needle) > 0, F.lit(code)).otherwise(expr)
    return expr


def classifyQuarantinedRows(df: DataFrame) -> DataFrame:
    """Classify Quarantined Row + Split Replayable Rows.

    Every line is recorded (RecordClass = Detail, no RejectReasonCode of its own
    beyond the classification); ``ReplayEligibleFlag`` = N marks the unreadable
    branch (empty or NUL-bearing lines).
    """
    spec = feeds.QUARANTINE_MALFORMED
    raw = F.col("RawLine")
    df = df.withColumn("OriginFeedCode", originFeedCode(F.col("FileName")))
    df = df.withColumn(
        "RejectReasonCode",
        F.when(emptyText(raw), F.lit(REASON_EMPTY_LINE)).otherwise(F.lit(REASON_QUARANTINED)),
    )
    df = df.withColumn(
        "ReplayEligibleFlag",
        F.when(nonEmptyText(raw) & (F.instr(raw, "\x00") == 0), "Y").otherwise("N"),
    )
    df = df.withColumn("RecordClass", F.lit(CLASS_DETAIL))
    df = df.withColumn(
        "RejectReason",
        F.when(F.col("RejectReasonCode") == REASON_EMPTY_LINE, F.lit("Empty line in a quarantined file."))
        .otherwise(F.lit(spec.rejectReason)),
    )
    return _audit(df, spec)


PARSERS: Dict[str, Callable[[DataFrame], DataFrame]] = {
    feeds.PARTNER_SALES_NA.packageName: parsePartnerSalesNa,
    feeds.PARTNER_SALES_EU.packageName: parsePartnerSalesEu,
    feeds.PARTNER_SALES_APAC.packageName: parsePartnerSalesApac,
    feeds.CARRIER_SCAN.packageName: parseCarrierScan,
    feeds.SUPPLIER_CATALOG.packageName: parseSupplierCatalog,
    feeds.FX_OVERRIDE.packageName: parseFxOverride,
    feeds.QUARANTINE_MALFORMED.packageName: classifyQuarantinedRows,
}
