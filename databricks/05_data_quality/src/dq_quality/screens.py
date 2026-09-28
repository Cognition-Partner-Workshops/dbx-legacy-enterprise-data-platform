"""Row-level screening logic for the DQ_*_Screen packages as pure DataFrame functions.

Each function mirrors one legacy Data Flow Task: the Derived Column expressions are
translated one-for-one into Spark column expressions, Lookups become left joins whose
no-match rows are collected as rejects, Conditional Splits keep their output order (first
matching output wins, the last one is the default output) and Aggregates become groupBy.

Null handling: SSIS raises an error row when a boolean expression evaluates to NULL; Spark
treats a NULL predicate as "not matched", so such rows fall through to the default output
of the split (the reject branch) instead of aborting the data flow.

Every function returns a :class:`ScreenOutput`; ``rejected`` always carries
``RejectReasonCode`` and ``RejectBranch`` so the caller can route rows into the mapped
``silver.err_*`` table and register them through ``control.logRejectedRecordSet``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Optional, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

LOOKUP_MISS_REASON = "LOOKUP_MISS"

CUSTOMER_REASON_CODES = ("DQ_CUST_NAME_NULL", "DQ_CUST_CONSENT", "DQ_CUST_CREDIT_NEG", "DQ_CUST_COUNTRY")
SUPPLIER_REASON_CODES = ("DQ_SUPP_TERMS_NULL", "DQ_SUPP_TAXID_DUP")
ORDER_LINE_REASON_CODES = ("DQ_OL_QTY_RANGE", "DQ_OL_PRICE_OUTLIER", "DQ_OL_EXTENSION")
INVOICE_LINE_REASON_CODES = ("DQ_IL_TAX_MISMATCH", "DQ_IL_REGIME", "DQ_IL_MINOR_UNIT", "DQ_IL_CURRENCY")
PAYMENT_REASON_CODES = ("DQ_PAY_ORPHAN", "DQ_PAY_FUTURE", "DQ_PAY_VALUE_DATE", "DQ_PAY_METHOD_OUTLIER")
FILE_REASON_CODES = ("DQ_FILE_DELIMITER", "DQ_FILE_DATE", "DQ_FILE_AMOUNT")
REFERENTIAL_ORDER_REASON = "DQ_REF_ORDERLINE"
REFERENTIAL_SALE_REASON = "DQ_REF_SALELINE"
RECON_BREACH_REASON = "DQ_RECON_BREACH"

EXPECTED_FILE_DELIMITERS = 8  # nine pipe-delimited interface fields


@dataclass
class ScreenOutput:
    passed: DataFrame
    rejected: DataFrame
    branches: dict = field(default_factory=dict)
    summary: Optional[DataFrame] = None


def yn(condition: Column) -> Column:
    """SSIS ``cond ? "Y" : "N"``; a NULL condition yields ``"N"``."""
    return F.when(condition, F.lit("Y")).otherwise(F.lit("N"))


def isBlank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def conditionalSplit(df: DataFrame, outputs: Sequence[tuple[str, Column]], defaultOutput: str) -> dict:
    """SSIS Conditional Split: outputs are tested in order, the remainder is the default output."""
    branches = {}
    remaining = df
    for name, condition in outputs:
        branches[name] = remaining.filter(condition)
        remaining = remaining.filter(~F.coalesce(condition, F.lit(False)))
    branches[defaultOutput] = remaining
    return branches


def lookup(df: DataFrame, lookupDf: DataFrame, keyColumns: Sequence[str], valueColumns: Sequence[str],
           lookupName: str) -> tuple[DataFrame, DataFrame]:
    """SSIS Lookup with "Redirect rows to no match output": returns ``(matched, noMatch)``."""
    reference = lookupDf.select(*keyColumns, *valueColumns).dropDuplicates(list(keyColumns))
    marker = "__%s_hit" % lookupName.replace(" ", "_")
    joined = df.join(reference.withColumn(marker, F.lit(True)), list(keyColumns), "left")
    matched = joined.filter(F.col(marker)).drop(marker)
    noMatch = (joined.filter(F.col(marker).isNull()).drop(marker, *valueColumns)
               .withColumn("RejectBranch", F.lit("%s: Lookup No Match Output" % lookupName))
               .withColumn("LookupName", F.lit(lookupName))
               .withColumn("LookupColumnName", F.lit(",".join(keyColumns)))
               .withColumn("LookupValue", F.concat_ws("|", *[F.col(c).cast("string") for c in keyColumns])))
    return matched, noMatch


def _tagBranches(branches: dict, skip: Sequence[str]) -> list:
    tagged = []
    for name, df in branches.items():
        if name in skip:
            continue
        tagged.append(df.withColumn("RejectBranch", F.lit(name)))
    return tagged


def unionAll(frames: Sequence[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


# ---------------------------------------------------------------------------
# DQ_Customer_Screen - DFT Screen Customer
# ---------------------------------------------------------------------------

def deriveCustomerFlags(df: DataFrame) -> DataFrame:
    """``Evaluate Customer Rules`` derived column."""
    nameMissing = isBlank(F.col("CustomerName"))
    taxIdMissing = F.col("TaxRegistrationNumber").isNull() | (F.length(F.trim(F.col("TaxRegistrationNumber"))) < 6)
    isEu = F.col("RegionCode") == "EU"
    consentUndecided = (F.col("MarketingConsentFlag") != "Y") & (F.col("MarketingConsentFlag") != "N")
    consentBreach = (F.when(isEu & consentUndecided, F.lit("Y"))
                     .when(isEu & (F.col("RetentionMonths") > 24), F.lit("Y"))
                     .otherwise(F.lit("N")))
    creditImplausible = (F.col("CreditLimitAmount") < 0) | (F.col("CreditLimitAmount") > 100000000)
    reason = (F.when(nameMissing, F.lit("DQ_CUST_NAME_NULL"))
              .when(isEu & consentUndecided, F.lit("DQ_CUST_CONSENT"))
              .when(F.col("CreditLimitAmount") < 0, F.lit("DQ_CUST_CREDIT_NEG"))
              .otherwise(F.lit("DQ_CUST_COUNTRY")))
    return (df.withColumn("NameMissingFlag", yn(nameMissing))
            .withColumn("TaxIdMissingFlag", yn(taxIdMissing))
            .withColumn("ConsentBreachFlag", consentBreach)
            .withColumn("CreditImplausibleFlag", yn(creditImplausible))
            .withColumn("RejectReasonCode", reason))


def screenCustomer(customerDf: DataFrame, countryDf: DataFrame) -> ScreenOutput:
    """``countryDf`` = ``SELECT CountryCode, RegionCode AS ReferenceRegionCode FROM ref.Country WHERE IsActive``."""
    flagged = deriveCustomerFlags(customerDf)
    matched, unknownCountry = lookup(flagged, countryDf, ["CountryCode"], ["ReferenceRegionCode"],
                                     "Lookup Valid Country (Full Cache)")
    branches = conditionalSplit(matched, [
        ("Passes All Rules", (F.col("NameMissingFlag") == "N") & (F.col("ConsentBreachFlag") == "N")
         & (F.col("CreditImplausibleFlag") == "N") & (F.col("RegionCode") == F.col("ReferenceRegionCode"))),
        ("Completeness Failure", (F.col("NameMissingFlag") == "Y") | (F.col("TaxIdMissingFlag") == "Y")),
        ("Consent Failure", F.col("ConsentBreachFlag") == "Y"),
    ], "Region Mismatch")
    rejected = unionAll(_tagBranches(branches, ["Passes All Rules"]) + [unknownCountry])
    return ScreenOutput(branches["Passes All Rules"], rejected, {**branches, "Unknown Country": unknownCountry})


# ---------------------------------------------------------------------------
# DQ_Supplier_Screen - DFT Screen Supplier
# ---------------------------------------------------------------------------

def deriveSupplierFlags(df: DataFrame) -> DataFrame:
    """``Normalize Tax Identifier`` derived column."""
    normalized = (F.when(F.col("TaxIdentifier").isNull(), F.lit("NONE"))
                  .otherwise(F.upper(F.regexp_replace(F.regexp_replace(F.trim(F.col("TaxIdentifier")), "-", ""), " ", ""))))
    termsMissing = isBlank(F.col("PaymentTermsCode"))
    df = df.withColumn("NormalizedTaxId", normalized)
    taxShapeInvalid = (F.col("RegionCode") == "EU") & (
        (F.length(F.col("NormalizedTaxId")) < 8)
        | (F.substring(F.col("NormalizedTaxId"), 1, 2) != F.substring(F.upper(F.trim(F.col("CountryCode"))), 1, 2)))
    return (df.withColumn("TermsMissingFlag", yn(termsMissing))
            .withColumn("TaxShapeInvalidFlag", yn(taxShapeInvalid))
            .withColumn("RejectReasonCode", F.when(termsMissing, F.lit("DQ_SUPP_TERMS_NULL"))
                        .otherwise(F.lit("DQ_SUPP_TAXID_DUP"))))


def screenSupplier(supplierDf: DataFrame) -> ScreenOutput:
    flagged = deriveSupplierFlags(supplierDf)
    groups = (flagged.groupBy("NormalizedTaxId")
              .agg(F.count("SupplierCode").alias("SupplierCount"), F.min("SupplierCode").alias("FirstSupplierCode")))
    groupBranches = conditionalSplit(groups, [
        ("Unique Supplier", F.col("SupplierCount") == 1),
        ("Duplicate Tax Identifier", (F.col("SupplierCount") > 1) & (F.col("NormalizedTaxId") != "NONE")),
    ], "Missing Tax Identifier")
    passed = flagged.join(groupBranches["Unique Supplier"], "NormalizedTaxId", "inner")
    rejectGroups = unionAll(_tagBranches(groupBranches, ["Unique Supplier"]))
    rejected = flagged.join(rejectGroups, "NormalizedTaxId", "inner")
    return ScreenOutput(passed, rejected, groupBranches, summary=groups)


# ---------------------------------------------------------------------------
# DQ_OrderLine_Screen - DFT Screen Order Line
# ---------------------------------------------------------------------------

def deriveOrderLineFlags(df: DataFrame) -> DataFrame:
    quantityOutOfRange = (F.col("Quantity") <= 0) | (F.col("Quantity") > 10000)
    priceOutlier = F.col("UnitPriceAmount") > 250000
    mismatch = F.round(F.abs(F.col("ExtendedAmount") - (F.col("Quantity") * F.col("UnitPriceAmount"))), 2)
    reason = (F.when(quantityOutOfRange, F.lit("DQ_OL_QTY_RANGE"))
              .when(priceOutlier, F.lit("DQ_OL_PRICE_OUTLIER"))
              .otherwise(F.lit("DQ_OL_EXTENSION")))
    return (df.withColumn("QuantityOutOfRangeFlag", yn(quantityOutOfRange))
            .withColumn("PriceOutlierFlag", yn(priceOutlier))
            .withColumn("ExtensionMismatchAmount", mismatch.cast("decimal(18,2)"))
            .withColumn("RejectReasonCode", reason))


def screenOrderLine(orderLineDf: DataFrame, customerDf: DataFrame) -> ScreenOutput:
    """``customerDf`` = ``SELECT CustomerId, CustomerCode FROM stg.Customer`` (uncached lookup)."""
    matched, noCustomer = lookup(orderLineDf, customerDf, ["CustomerId"], ["CustomerCode"],
                                 "Lookup Order Customer (No Cache)")
    noCustomer = noCustomer.withColumn("RejectReasonCode", F.lit(LOOKUP_MISS_REASON))
    flagged = deriveOrderLineFlags(matched)
    branches = conditionalSplit(flagged, [
        ("Passes All Rules", (F.col("QuantityOutOfRangeFlag") == "N") & (F.col("PriceOutlierFlag") == "N")
         & (F.col("ExtensionMismatchAmount") <= F.lit(0.01).cast("decimal(18,2)"))),
        ("Invalid Quantity", F.col("QuantityOutOfRangeFlag") == "Y"),
        ("Price Outlier", F.col("PriceOutlierFlag") == "Y"),
    ], "Extension Mismatch")
    rejected = unionAll(_tagBranches(branches, ["Passes All Rules"]))
    return ScreenOutput(branches["Passes All Rules"], rejected,
                        {**branches, "Customer Lookup Failure": noCustomer})


# ---------------------------------------------------------------------------
# DQ_InvoiceLine_Screen - DFT Screen Invoice Line
# ---------------------------------------------------------------------------

def deriveInvoiceLineFlags(df: DataFrame) -> DataFrame:
    regimeMismatch = (((F.col("RegionCode") == "EU") & (F.col("TaxRegimeCode") != "VAT"))
                      | ((F.col("RegionCode") == "APAC") & (F.col("TaxRegimeCode") != "GST"))
                      | ((F.col("RegionCode") == "NA") & (F.col("TaxRegimeCode") != "SUT")))
    taxMismatch = F.col("TaxVarianceAmount") > F.lit(0.02).cast("decimal(18,2)")
    minorUnitBreach = (F.col("MinorUnitDigits") == 0) & (F.col("NetAmount") != F.round(F.col("NetAmount"), 0))
    reason = (F.when(taxMismatch, F.lit("DQ_IL_TAX_MISMATCH"))
              .when((F.col("RegionCode") == "EU") & (F.col("TaxRegimeCode") != "VAT"), F.lit("DQ_IL_REGIME"))
              .otherwise(F.lit("DQ_IL_MINOR_UNIT")))
    return (df.withColumn("RegimeMismatchFlag", yn(regimeMismatch))
            .withColumn("TaxMismatchFlag", yn(taxMismatch))
            .withColumn("MinorUnitBreachFlag", yn(minorUnitBreach))
            .withColumn("RejectReasonCode", reason))


def screenInvoiceLine(saleLineDf: DataFrame, currencyDf: DataFrame) -> ScreenOutput:
    """``currencyDf`` = ``SELECT CurrencyCode AS SaleCurrencyCode, CurrencyName, MinorUnitDigits FROM ref.Currency WHERE IsActive``."""
    matched, unknownCurrency = lookup(saleLineDf, currencyDf, ["SaleCurrencyCode"],
                                      ["CurrencyName", "MinorUnitDigits"], "Lookup Currency Domain (Full Cache)")
    unknownCurrency = unknownCurrency.withColumn("RejectReasonCode", F.lit("DQ_IL_CURRENCY"))
    flagged = deriveInvoiceLineFlags(matched)
    branches = conditionalSplit(flagged, [
        ("Passes All Rules", (F.col("TaxMismatchFlag") == "N") & (F.col("RegimeMismatchFlag") == "N")
         & (F.col("MinorUnitBreachFlag") == "N")),
        ("Tax Mismatch", F.col("TaxMismatchFlag") == "Y"),
        ("Regime Mismatch", F.col("RegimeMismatchFlag") == "Y"),
    ], "Minor Unit Breach")
    rejected = unionAll(_tagBranches(branches, ["Passes All Rules"]) + [unknownCurrency])
    return ScreenOutput(branches["Passes All Rules"], rejected, {**branches, "Unknown Currency": unknownCurrency})


# ---------------------------------------------------------------------------
# DQ_Payment_Screen - DFT Screen Payment
# ---------------------------------------------------------------------------

LARGE_PAYMENT_AMOUNT = 5000000


def derivePaymentFlags(df: DataFrame, asOfDate: Optional[Column] = None) -> DataFrame:
    today = asOfDate if asOfDate is not None else F.current_date()
    orphan = F.col("MatchTypeCode") == "UNMATCHED"
    future = F.col("PaymentDate") > today
    valueBefore = F.col("ValueDate") < F.col("PaymentDate")
    large = F.col("PaymentAmount") > LARGE_PAYMENT_AMOUNT
    reason = (F.when(orphan, F.lit("DQ_PAY_ORPHAN"))
              .when(future, F.lit("DQ_PAY_FUTURE"))
              .otherwise(F.lit("DQ_PAY_VALUE_DATE")))
    return (df.withColumn("OrphanFlag", yn(orphan))
            .withColumn("FutureDatedFlag", yn(future))
            .withColumn("ValueDateBeforePaymentFlag", yn(valueBefore))
            .withColumn("LargePaymentFlag", yn(large))
            .withColumn("RejectReasonCode", reason))


def screenPayment(paymentDf: DataFrame, matchedDf: DataFrame, asOfDate: Optional[Column] = None) -> ScreenOutput:
    """``matchedDf`` = ``SELECT PaymentNumber, MatchTypeCode FROM work.PaymentMatched``.

    The legacy flow aggregates per (PaymentMethodCode, PaymentCurrencyCode) after the derived
    column and routes the *groups*; ``summary`` holds those groups, ``passed`` the plausible
    groups and ``rejected`` the individual payments above the large-payment limit inside an
    outlier group (the rows the group-level rule actually indicts).
    """
    withMatch = (paymentDf.join(matchedDf.select("PaymentNumber", "MatchTypeCode").dropDuplicates(["PaymentNumber"]),
                                "PaymentNumber", "left")
                 .withColumn("MatchTypeCode", F.coalesce(F.col("MatchTypeCode"), F.lit("UNMATCHED"))))
    flagged = derivePaymentFlags(withMatch, asOfDate)
    summary = (flagged.groupBy("PaymentMethodCode", "PaymentCurrencyCode")
               .agg(F.sum("PaymentAmount").alias("TotalPaidAmount"),
                    F.max("PaymentAmount").alias("MaxPaymentAmount"),
                    F.count("PaymentNumber").alias("PaymentCount")))
    groupBranches = conditionalSplit(summary, [
        ("Plausible Method Total", (F.col("PaymentCount") > 0) & (F.col("MaxPaymentAmount") <= LARGE_PAYMENT_AMOUNT)),
    ], "Method Total Outlier")
    outlierGroups = groupBranches["Method Total Outlier"].select("PaymentMethodCode", "PaymentCurrencyCode")
    rejected = (flagged.join(outlierGroups, ["PaymentMethodCode", "PaymentCurrencyCode"], "inner")
                .filter(F.col("LargePaymentFlag") == "Y")
                .withColumn("RejectReasonCode", F.lit("DQ_PAY_METHOD_OUTLIER"))
                .withColumn("RejectBranch", F.lit("Method Total Outlier")))
    return ScreenOutput(groupBranches["Plausible Method Total"], rejected, {**groupBranches, "Flagged Payments": flagged},
                        summary=summary)


# ---------------------------------------------------------------------------
# DQ_File_Screen - DFT Screen Partner File
# ---------------------------------------------------------------------------

def deriveFileRowFlags(df: DataFrame) -> DataFrame:
    delimiterBreach = F.col("DelimiterCount") != EXPECTED_FILE_DELIMITERS
    dateText = F.col("SaleDateText")
    unparsableDate = (dateText.isNull() | (F.length(F.trim(dateText)) < 8)
                      | ((F.instr(dateText, "/") == 0) & (F.instr(dateText, "-") == 0)))
    amountText = F.col("AmountText")
    unparsableAmount = amountText.isNull() | (F.length(F.regexp_replace(F.trim(amountText), r"[,$.]", "")) == 0)
    highBit = F.instr(F.col("RawLine"), "\uFFFD") > 0
    reason = (F.when(delimiterBreach, F.lit("DQ_FILE_DELIMITER"))
              .when(dateText.isNull(), F.lit("DQ_FILE_DATE"))
              .otherwise(F.lit("DQ_FILE_AMOUNT")))
    return (df.withColumn("DelimiterBreachFlag", yn(delimiterBreach))
            .withColumn("UnparsableDateFlag", yn(unparsableDate))
            .withColumn("UnparsableAmountFlag", yn(unparsableAmount))
            .withColumn("HighBitFlag", yn(highBit))
            .withColumn("RejectReasonCode", reason))


def screenFileRows(fileRowDf: DataFrame) -> ScreenOutput:
    flagged = deriveFileRowFlags(fileRowDf)
    branches = conditionalSplit(flagged, [
        ("Well Formed Row", (F.col("DelimiterBreachFlag") == "N") & (F.col("UnparsableDateFlag") == "N")
         & (F.col("UnparsableAmountFlag") == "N") & (F.col("HighBitFlag") == "N")),
        ("Malformed Delimiters", F.col("DelimiterBreachFlag") == "Y"),
        ("Unparsable Date", F.col("UnparsableDateFlag") == "Y"),
    ], "Unparsable Amount")
    rejected = unionAll(_tagBranches(branches, ["Well Formed Row"]))
    return ScreenOutput(branches["Well Formed Row"], rejected, branches)


def reconstructFileRow(df: DataFrame, interfaceColumns: Sequence[str]) -> DataFrame:
    """Rebuild ``RawLine`` / ``DelimiterCount`` from the already-parsed bronze row.

    The legacy screen counted pipes in the raw line. bronze.raw_file_partner_sales holds the
    parsed fields, so the line is reassembled from the agreed interface columns: a missing
    (NULL) field drops a delimiter, an embedded pipe inside a field adds one - the two
    defects the legacy check was written to catch.
    """
    rawLine = F.concat_ws("|", *[F.col(c) for c in interfaceColumns])
    presentFields = reduce(lambda a, b: a + b, [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in interfaceColumns])
    embeddedPipes = reduce(lambda a, b: a + b, [
        F.coalesce(F.length(F.col(c)) - F.length(F.regexp_replace(F.col(c), r"\|", "")), F.lit(0))
        for c in interfaceColumns])
    return (df.withColumn("RawLine", rawLine)
            .withColumn("DelimiterCount", F.greatest(presentFields - 1, F.lit(0)) + embeddedPipes))


# ---------------------------------------------------------------------------
# DQ_Referential_Screen - DFT Order Line Referential / DFT Sale Line Referential
# ---------------------------------------------------------------------------

def lookupChain(df: DataFrame, lookups: Sequence[tuple[str, DataFrame, Sequence[str], Sequence[str]]],
                reasonCode: str) -> ScreenOutput:
    """Consecutive full-cache lookups; each no-match output is a reject branch."""
    current = df
    rejects = []
    branches = {}
    for name, lookupDf, keys, values in lookups:
        current, noMatch = lookup(current, lookupDf, keys, values, name)
        noMatch = noMatch.withColumn("RejectReasonCode", F.lit(LOOKUP_MISS_REASON))
        rejects.append(noMatch)
        branches[name] = noMatch
    passed = current.withColumn("RejectReasonCode", F.lit(reasonCode))
    return ScreenOutput(passed, unionAll(rejects), branches)


def screenReferentialOrder(orderKeysDf: DataFrame, stockItemDf: DataFrame, packageTypeDf: DataFrame) -> ScreenOutput:
    return lookupChain(orderKeysDf, [
        ("Lookup Stock Item Key (Full Cache)", stockItemDf, ["StockItemId"], ["StockItemName"]),
        ("Lookup Package Type (Full Cache)", packageTypeDf, ["PackageTypeCode"], ["PackageTypeName"]),
    ], REFERENTIAL_ORDER_REASON)


def screenReferentialSale(saleKeysDf: DataFrame, currencyDf: DataFrame, territoryDf: DataFrame) -> ScreenOutput:
    return lookupChain(saleKeysDf, [
        ("Lookup Sale Currency (Full Cache)", currencyDf, ["SaleCurrencyCode"], ["CurrencyName"]),
        ("Lookup Sales Territory (Partial Cache)", territoryDf, ["SalesTerritoryCode"], ["SalesTerritoryName"]),
    ], REFERENTIAL_SALE_REASON)


# ---------------------------------------------------------------------------
# DQ_Reject_Reprocess - DFT Reprocess Rejects
# ---------------------------------------------------------------------------

MAX_RETRY_COUNT = 5
ABANDON_AFTER_DAYS = 30


def prepareReprocess(rejectDf: DataFrame, stockItemDf: DataFrame, nowUtc: Optional[Column] = None) -> ScreenOutput:
    """``Prepare Retry`` + ``Re-Lookup Stock Item (No Cache)`` + ``Route Reprocess Outcome``.

    Input columns: RejectedRowId, ObjectName, BusinessKey, RejectReasonCode, RetryCount,
    FirstRejectedAtUtc, PayloadJson. Outputs: ``passed`` = "Resolved Now", branches
    "Aged Out" and the stock-item no-match output ("Still Unresolved").
    """
    now = nowUtc if nowUtc is not None else F.current_timestamp()
    prepared = (rejectDf
                .withColumn("RetryCount", F.when(F.col("RetryCount").isNull(), F.lit(1)).otherwise(F.col("RetryCount") + 1))
                .withColumn("AgeDays", F.datediff(F.to_date(now), F.to_date(F.col("FirstRejectedAtUtc"))))
                .withColumn("StockItemId", F.split(F.col("BusinessKey"), r"\|").getItem(1))
                .withColumn("AbandonFlag", yn(F.datediff(F.to_date(now), F.to_date(F.col("FirstRejectedAtUtc"))) > ABANDON_AFTER_DAYS)))
    matched, unresolved = lookup(prepared, stockItemDf, ["StockItemId"], ["StockItemName"], "Re-Lookup Stock Item (No Cache)")
    branches = conditionalSplit(matched, [("Resolved Now", F.col("AbandonFlag") == "N")], "Aged Out")
    rejected = unionAll([branches["Aged Out"].withColumn("RejectBranch", F.lit("Aged Out")), unresolved])
    return ScreenOutput(branches["Resolved Now"], rejected, {**branches, "Still Unresolved": unresolved})
