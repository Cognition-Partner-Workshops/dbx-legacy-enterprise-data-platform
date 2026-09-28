"""Regional customer conditioning - the divergence between DIM_NA / DIM_EU / DIM_APAC_Load_Customer.

Each function takes the conformed `silver.stg_customer` (+ address / tax) rows for
one region and returns `(validDf, rejectedDf)`. The rules are the derived-column
expressions and conditional splits of the three generated packages plus the
region-specific branches of Integration.usp_MigrateStagedCustomerDataV2; the
baseline that must keep holding is validation/runtime/04_regional_divergence.sql:

* NA  - 5-digit ZIP + ZIP+4, USD credit limit, digits-only phone, tax jurisdiction
        = state|county|city, credit hold forces IsTaxExempt = 0, consent basis
        IMPLIED_OPT_OUT / source IMPORT, 7-year retention.
* EU  - erasure rows pseudonymised (all versions), postcode upper-cased without
        spaces, VAT number normalised & upper-cased, EUR credit limit, reverse
        charge for cross-border B2B with a valid VAT number, no explicit opt-in
        => consent flags 0, 6-year retention.
* APAC - postcode may stay NULL for HKG / ARE / PAN, SGD credit limit, phone
        normalised with a leading '+', GST number normalised, GST treatment
        derived from registration + business-number type, local-script name
        kept, country-derived postal format, channel-specific consent, 5-year
        retention.
"""

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REGION_RETENTION_YEARS = {"NA": 7, "EU": 6, "APAC": 5}
REGION_CREDIT_CURRENCY = {"NA": "USD", "EU": "EUR", "APAC": "SGD"}
NO_POSTCODE_COUNTRIES = ("HKG", "ARE", "PAN", "HK", "AE", "PA")
EU_REVERSE_CHARGE_HOME = "DEU"   # the EU hub entity; cross-border = customer country <> hub country
REJECT_REASON_COLUMN = "RejectReasonCode"

def _requiredCommon():
    return [
        ("MISSING_BUSINESS_KEY", F.col("CustomerBusinessKey").isNull() | (F.trim(F.col("CustomerBusinessKey")) == "")),
        ("MISSING_NAME", F.col("CustomerName").isNull() | (F.trim(F.col("CustomerName")) == "")),
    ]


def _digits(col):
    return F.regexp_replace(F.coalesce(col, F.lit("")), "[^0-9]", "")


def _creditBand(col):
    return (
        F.when(col.isNull(), F.lit("NONE"))
        .when(col >= 1000000, F.lit("PLATINUM"))
        .when(col >= 250000, F.lit("GOLD"))
        .when(col >= 50000, F.lit("SILVER"))
        .otherwise(F.lit("BRONZE"))
    )


def _common(df: DataFrame, regionCode: str) -> DataFrame:
    retention = REGION_RETENTION_YEARS[regionCode]
    return (
        df.withColumn("RegionCode", F.lit(regionCode))
        .withColumn("Customer", F.coalesce(F.col("CustomerNameStandardized"), F.col("CustomerName")))
        .withColumn("BillToCustomer", F.coalesce(F.col("BillToCustomerName"), F.col("CustomerLegalName"), F.col("CustomerName")))
        .withColumn("Category", F.col("CustomerCategoryCode"))
        .withColumn("BuyingGroup", F.col("BuyingGroupName"))
        .withColumn("PrimaryContact", F.col("PrimaryContactName"))
        .withColumn("AccountStatusCode", F.coalesce(F.col("CustomerStatusCode"), F.lit("ACT")))
        .withColumn("CreditLimitBand", _creditBand(F.col("CreditLimitAmount")))
        .withColumn("IsOnCreditHold", F.coalesce(F.col("IsOnCreditHold").cast("boolean"), F.lit(False)))
        .withColumn("RetentionYears", F.coalesce(F.col("RetentionYears").cast("int"), F.lit(retention)))
        .withColumn("RetentionExpiryDate",
                    F.coalesce(F.col("RetentionExpiryDate").cast("date"),
                               F.add_months(F.coalesce(F.col("LastActivityDate"), F.col("AccountOpenedDate")).cast("date"), 12 * retention)))
        .withColumn("CountryCode", F.upper(F.trim(F.col("PrimaryCountryCode"))))
        .withColumn("PostalCode", F.col("PostalCodeRaw"))
        .withColumn("WebsiteURL", F.lower(F.trim(F.col("WebsiteUrl"))))
    )


def _split(df: DataFrame, rules) -> Tuple[DataFrame, DataFrame]:
    reason = F.lit(None).cast("string")
    for code, predicate in reversed(rules):
        reason = F.when(predicate, F.lit(code)).otherwise(reason)
    tagged = df.withColumn(REJECT_REASON_COLUMN, reason)
    return tagged.where(F.col(REJECT_REASON_COLUMN).isNull()).drop(REJECT_REASON_COLUMN), tagged.where(F.col(REJECT_REASON_COLUMN).isNotNull())


# --------------------------------------------------------------------------------------------

def conditionNaCustomers(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    d = _common(df, "NA")
    zipDigits = _digits(F.col("PostalCodeRaw"))
    d = (
        d.withColumn("PostalCodeStandardized", F.when(F.length(zipDigits) >= 5, F.substring(zipDigits, 1, 5)).otherwise(F.lit(None)))
        .withColumn("ZIPPlusFour", F.when(F.length(zipDigits) == 9, F.concat(F.substring(zipDigits, 1, 5), F.lit("-"), F.substring(zipDigits, 6, 4)))
                    .otherwise(F.col("PostalCodeStandardized")))
        .withColumn("PostalFormatCode", F.lit("US-ZIP"))
        .withColumn("CreditLimitCurrencyCode", F.coalesce(F.col("CreditLimitCurrencyCode"), F.lit(REGION_CREDIT_CURRENCY["NA"])))
        .withColumn("PhoneNumberStandardized", F.when(F.length(_digits(F.col("PhoneNumber"))) >= 10, F.substring(_digits(F.col("PhoneNumber")), -10, 10)).otherwise(F.lit(None)))
        .withColumn("SalesTaxJurisdictionCode",
                    F.concat_ws("|", F.upper(F.coalesce(F.col("StateProvinceCode"), F.lit("XX"))),
                                F.upper(F.coalesce(F.col("CountyName"), F.lit(""))), F.upper(F.coalesce(F.col("CityName"), F.lit(""))))
                    .alias("SalesTaxJurisdictionCode"))
        .withColumn("IsTaxExempt", F.when(F.col("IsOnCreditHold"), F.lit(False)).otherwise(F.coalesce(F.col("IsTaxExempt").cast("boolean"), F.lit(False))))
        .withColumn("ConsentBasisCode", F.coalesce(F.col("ConsentBasisCode"), F.lit("IMPLIED_OPT_OUT")))
        .withColumn("ConsentSourceCode", F.coalesce(F.col("ConsentSourceCode"), F.lit("IMPORT")))
        .withColumn("MarketingConsentFlag", F.coalesce(F.col("MarketingConsentFlag").cast("boolean"), F.lit(True)))
        .withColumn("ProfilingConsentFlag", F.coalesce(F.col("ProfilingConsentFlag").cast("boolean"), F.lit(True)))
        .withColumn("IsPseudonymized", F.lit(False))
        .withColumn("IsReverseChargeEligible", F.lit(False))
    )
    rules = _requiredCommon() + [
        ("INVALID_ZIP", F.col("PostalCodeStandardized").isNull()),
        ("INVALID_COUNTRY", ~F.col("CountryCode").isin("USA", "CAN", "MEX", "US", "CA", "MX")),
        ("NEGATIVE_CREDIT_LIMIT", F.col("CreditLimitAmount") < 0),
    ]
    return _split(d, rules)


def conditionEuCustomers(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    d = _common(df, "EU")
    vat = F.upper(F.regexp_replace(F.coalesce(F.col("TaxRegistrationNumber"), F.lit("")), "[^A-Za-z0-9]", ""))
    erased = F.col("ErasureRequestedOn").isNotNull()
    pseudo = F.concat(F.lit("ERASED-"), F.sha2(F.col("CustomerBusinessKey"), 256).substr(1, 16))
    d = (
        d.withColumn("PostalCodeStandardized", F.upper(F.regexp_replace(F.coalesce(F.col("PostalCodeRaw"), F.lit("")), "\\s", "")))
        .withColumn("PostalCodeStandardized", F.when(F.col("PostalCodeStandardized") == "", F.lit(None)).otherwise(F.col("PostalCodeStandardized")))
        .withColumn("ZIPPlusFour", F.lit(None).cast("string"))
        .withColumn("PostalFormatCode", F.lit("EU-POSTCODE"))
        .withColumn("VATRegistrationNumber", F.when(vat == "", F.lit(None)).otherwise(vat))
        .withColumn("VATValidationStatus",
                    F.when(F.col("VATRegistrationNumber").isNull(), F.lit("NONE"))
                    .when(F.coalesce(F.col("TaxRegistrationValidFlag").cast("boolean"), F.lit(False)), F.lit("VALID"))
                    .otherwise(F.lit("UNVERIFIED")))
        .withColumn("CreditLimitCurrencyCode", F.coalesce(F.col("CreditLimitCurrencyCode"), F.lit(REGION_CREDIT_CURRENCY["EU"])))
        .withColumn("PhoneNumberStandardized", F.when(_digits(F.col("PhoneNumber")) == "", F.lit(None)).otherwise(F.concat(F.lit("+"), _digits(F.col("PhoneNumber")))))
        .withColumn("IsReverseChargeEligible",
                    (F.col("VATValidationStatus") == "VALID") & (F.col("CountryCode") != F.lit(EU_REVERSE_CHARGE_HOME))
                    & F.coalesce(F.col("IsB2B").cast("boolean"), F.lit(True)))
        .withColumn("IsTaxExempt", F.coalesce(F.col("IsTaxExempt").cast("boolean"), F.lit(False)))
        .withColumn("ConsentBasisCode", F.coalesce(F.col("ConsentBasisCode"), F.lit("EXPLICIT_OPT_IN")))
        .withColumn("ConsentSourceCode", F.coalesce(F.col("ConsentSourceCode"), F.lit("WEB")))
        .withColumn("MarketingConsentFlag", F.when(F.col("ConsentBasisCode") == "EXPLICIT_OPT_IN", F.coalesce(F.col("MarketingConsentFlag").cast("boolean"), F.lit(False))).otherwise(F.lit(False)))
        .withColumn("ProfilingConsentFlag", F.when(F.col("ConsentBasisCode") == "EXPLICIT_OPT_IN", F.coalesce(F.col("ProfilingConsentFlag").cast("boolean"), F.lit(False))).otherwise(F.lit(False)))
        .withColumn("IsPseudonymized", erased)
        .withColumn("Customer", F.when(erased, pseudo).otherwise(F.col("Customer")))
        .withColumn("BillToCustomer", F.when(erased, pseudo).otherwise(F.col("BillToCustomer")))
        .withColumn("PrimaryContact", F.when(erased, F.lit(None)).otherwise(F.col("PrimaryContact")))
        .withColumn("PhoneNumberStandardized", F.when(erased, F.lit(None)).otherwise(F.col("PhoneNumberStandardized")))
        .withColumn("WebsiteURL", F.when(erased, F.lit(None)).otherwise(F.col("WebsiteURL")))
        .withColumn("PostalCode", F.when(erased, F.lit(None)).otherwise(F.col("PostalCode")))
        .withColumn("PostalCodeStandardized", F.when(erased, F.lit(None)).otherwise(F.col("PostalCodeStandardized")))
        .withColumn("MarketingConsentFlag", F.when(erased, F.lit(False)).otherwise(F.col("MarketingConsentFlag")))
        .withColumn("ProfilingConsentFlag", F.when(erased, F.lit(False)).otherwise(F.col("ProfilingConsentFlag")))
    )
    rules = _requiredCommon() + [
        ("MISSING_POSTCODE", F.col("PostalCodeStandardized").isNull() & ~erased),
        ("INVALID_VAT_FORMAT", F.col("VATRegistrationNumber").isNotNull() & ~F.col("VATRegistrationNumber").rlike("^[A-Z]{2}[A-Z0-9]{2,12}$")),
        ("NEGATIVE_CREDIT_LIMIT", F.col("CreditLimitAmount") < 0),
    ]
    return _split(d, rules)


def conditionApacCustomers(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    d = _common(df, "APAC")
    gst = F.upper(F.regexp_replace(F.coalesce(F.col("TaxRegistrationNumber"), F.lit("")), "[^A-Za-z0-9]", ""))
    noPostcode = F.col("CountryCode").isin(*NO_POSTCODE_COUNTRIES)
    phoneDigits = _digits(F.col("PhoneNumber"))
    d = (
        d.withColumn("PostalCodeStandardized",
                     F.when(noPostcode, F.lit(None)).otherwise(F.upper(F.regexp_replace(F.coalesce(F.col("PostalCodeRaw"), F.lit("")), "\\s", ""))))
        .withColumn("PostalCodeStandardized", F.when(F.col("PostalCodeStandardized") == "", F.lit(None)).otherwise(F.col("PostalCodeStandardized")))
        .withColumn("ZIPPlusFour", F.lit(None).cast("string"))
        .withColumn("PostalFormatCode",
                    F.when(noPostcode, F.lit("NONE"))
                    .when(F.col("CountryCode").isin("AUS", "AU", "NZL", "NZ"), F.lit("APAC-4"))
                    .when(F.col("CountryCode").isin("SGP", "SG", "IND", "IN", "JPN", "JP", "CHN", "CN", "KOR", "KR"), F.lit("APAC-6"))
                    .otherwise(F.lit("APAC-FREE")))
        .withColumn("CreditLimitCurrencyCode", F.coalesce(F.col("CreditLimitCurrencyCode"), F.lit(REGION_CREDIT_CURRENCY["APAC"])))
        .withColumn("PhoneNumberStandardized", F.when(phoneDigits == "", F.lit(None)).otherwise(F.concat(F.lit("+"), phoneDigits)))
        .withColumn("GSTRegistrationNumber", F.when(gst == "", F.lit(None)).otherwise(gst))
        .withColumn("BusinessNumberType",
                    F.coalesce(F.col("BusinessNumberType"),
                               F.when(F.col("CountryCode").isin("AUS", "AU"), F.lit("ABN"))
                               .when(F.col("CountryCode").isin("NZL", "NZ"), F.lit("NZBN"))
                               .when(F.col("CountryCode").isin("SGP", "SG"), F.lit("UEN"))
                               .when(F.col("CountryCode").isin("IND", "IN"), F.lit("GSTIN"))
                               .otherwise(F.lit("OTHER"))))
        .withColumn("GSTTreatmentCode",
                    F.when(F.col("GSTRegistrationNumber").isNull(), F.lit("UNREGISTERED"))
                    .when(F.col("BusinessNumberType").isin("ABN", "NZBN", "UEN", "GSTIN"), F.lit("REGISTERED"))
                    .otherwise(F.lit("REGISTERED_OTHER")))
        .withColumn("LocalScriptName", F.col("LocalScriptName"))
        .withColumn("IsTaxExempt", F.coalesce(F.col("IsTaxExempt").cast("boolean"), F.lit(False)))
        .withColumn("IsReverseChargeEligible", F.lit(False))
        .withColumn("ConsentBasisCode", F.coalesce(F.col("ConsentBasisCode"), F.lit("CHANNEL_SPECIFIC")))
        .withColumn("ConsentSourceCode", F.coalesce(F.col("ConsentSourceCode"), F.lit("CHANNEL")))
        .withColumn("MarketingConsentFlag", F.coalesce(F.col("MarketingConsentFlag").cast("boolean"), F.lit(False)))
        .withColumn("ProfilingConsentFlag", F.coalesce(F.col("ProfilingConsentFlag").cast("boolean"), F.col("MarketingConsentFlag")))
        .withColumn("IsPseudonymized", F.lit(False))
    )
    rules = _requiredCommon() + [
        ("MISSING_POSTCODE", F.col("PostalCodeStandardized").isNull() & ~noPostcode),
        ("INVALID_GST_NUMBER", F.col("GSTRegistrationNumber").isNotNull() & (F.length(F.col("GSTRegistrationNumber")) < 8)),
        ("NEGATIVE_CREDIT_LIMIT", F.col("CreditLimitAmount") < 0),
    ]
    return _split(d, rules)


CONDITIONERS = {"NA": conditionNaCustomers, "EU": conditionEuCustomers, "APAC": conditionApacCustomers}


def conditionCustomers(df: DataFrame, regionCode: str) -> Tuple[DataFrame, DataFrame]:
    try:
        return CONDITIONERS[regionCode](df)
    except KeyError:
        raise ValueError(f"Unsupported RegionCode {regionCode!r}; expected one of {sorted(CONDITIONERS)}")


CUSTOMER_SOURCE_COLUMNS = [
    "CustomerBusinessKey", "WWICustomerID", "SourceSystemCode", "CustomerName", "CustomerLegalName",
    "CustomerNameStandardized", "BillToCustomerName", "BuyingGroupName", "CustomerCategoryCode",
    "CustomerStatusCode", "PrimaryContactName", "PhoneNumber", "WebsiteUrl", "PostalCodeRaw",
    "StateProvinceCode", "CountyName", "CityName", "PrimaryCountryCode", "CreditLimitAmount",
    "CreditLimitCurrencyCode", "CreditLimitAmountUsd", "IsOnCreditHold", "IsTaxExempt",
    "TaxRegistrationNumber", "TaxRegistrationValidFlag", "IsB2B", "BusinessNumberType", "LocalScriptName",
    "PaymentTermsCode", "StandardDiscountPercentage", "ConsentBasisCode", "ConsentSourceCode",
    "MarketingConsentFlag", "ProfilingConsentFlag", "ErasureRequestedOn", "RetentionYears",
    "RetentionExpiryDate", "AccountOpenedDate", "LastActivityDate", "SourceModifiedDate", "FiscalYearStartMonth",
]


def withMissingSourceColumns(df: DataFrame) -> DataFrame:
    """Add any customer source column the staging table does not carry as NULL so the rules can run."""
    existing = {c.lower() for c in df.columns}
    for c in CUSTOMER_SOURCE_COLUMNS:
        if c.lower() not in existing:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df
