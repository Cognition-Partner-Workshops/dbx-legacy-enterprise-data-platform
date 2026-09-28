from decimal import Decimal

from pyspark.sql import functions as F

from wwi_dimensions import regional

COLS = ("CustomerBusinessKey STRING, CustomerName STRING, PostalCodeRaw STRING, PrimaryCountryCode STRING, PhoneNumber STRING, "
        "CreditLimitAmount DECIMAL(18,2), CreditLimitCurrencyCode STRING, IsOnCreditHold BOOLEAN, IsTaxExempt BOOLEAN, "
        "TaxRegistrationNumber STRING, TaxRegistrationValidFlag BOOLEAN, StateProvinceCode STRING, CountyName STRING, CityName STRING, "
        "ErasureRequestedOn DATE, MarketingConsentFlag BOOLEAN, ConsentBasisCode STRING")


def _df(spark, rows):
    return regional.withMissingSourceColumns(spark.createDataFrame(rows, COLS))


def _one(df, key):
    return df.where(F.col("CustomerBusinessKey") == key).collect()[0]


def test_na_rules(spark):
    valid, rejected = regional.conditionNaCustomers(_df(spark, [
        ("NA-1", "Acme", "12345-6789", "USA", "(212) 555-0100", Decimal("300000"), None, True, True, None, None, "NY", "New York", "New York", None, None, None),
        ("NA-2", "Bad Zip", "12", "USA", None, Decimal("10"), None, False, False, None, None, "NY", None, None, None, None, None),
        ("NA-3", "Abroad", "75001", "FRA", None, Decimal("10"), None, False, False, None, None, None, None, None, None, None, None),
    ]))
    r = _one(valid, "NA-1")
    assert r["PostalCodeStandardized"] == "12345" and r["ZIPPlusFour"] == "12345-6789"
    assert r["CreditLimitCurrencyCode"] == "USD" and r["PhoneNumberStandardized"] == "2125550100"
    assert r["SalesTaxJurisdictionCode"] == "NY|NEW YORK|NEW YORK"
    assert r["IsTaxExempt"] is False                 # credit hold forces exemption off
    assert r["ConsentBasisCode"] == "IMPLIED_OPT_OUT" and r["ConsentSourceCode"] == "IMPORT"
    assert r["RetentionYears"] == 7 and r["CreditLimitBand"] == "GOLD" and r["RegionCode"] == "NA"
    rej = {x["CustomerBusinessKey"]: x["RejectReasonCode"] for x in rejected.collect()}
    assert rej == {"NA-2": "INVALID_ZIP", "NA-3": "INVALID_COUNTRY"}


def test_eu_rules(spark):
    from datetime import date
    valid, rejected = regional.conditionEuCustomers(_df(spark, [
        ("EU-1", "Müller GmbH", "sw1a 1aa", "GBR", "+44 20 7946 0958", Decimal("1000"), None, False, False, "gb 123 4567 89", True, None, None, None, None, True, None),
        ("EU-2", "Erased Person", "10115", "DEU", "030 1234", Decimal("0"), None, False, False, None, None, None, None, None, date(2026, 1, 1), True, "EXPLICIT_OPT_IN"),
        ("EU-3", "No postcode", None, "FRA", None, Decimal("0"), None, False, False, None, None, None, None, None, None, None, None),
        ("EU-4", "Bad VAT", "75001", "FRA", None, Decimal("0"), None, False, False, "1", False, None, None, None, None, None, None),
    ]))
    r = _one(valid, "EU-1")
    assert r["PostalCodeStandardized"] == "SW1A1AA" and r["VATRegistrationNumber"] == "GB123456789"
    assert r["CreditLimitCurrencyCode"] == "EUR" and r["VATValidationStatus"] == "VALID" and r["IsReverseChargeEligible"] is True
    assert r["MarketingConsentFlag"] is True and r["RetentionYears"] == 6
    e = _one(valid, "EU-2")
    assert e["IsPseudonymized"] is True and e["Customer"].startswith("ERASED-") and e["PhoneNumberStandardized"] is None
    assert e["MarketingConsentFlag"] is False and e["PostalCodeStandardized"] is None
    rej = {x["CustomerBusinessKey"]: x["RejectReasonCode"] for x in rejected.collect()}
    assert rej == {"EU-3": "MISSING_POSTCODE", "EU-4": "INVALID_VAT_FORMAT"}


def test_apac_rules(spark):
    valid, rejected = regional.conditionApacCustomers(_df(spark, [
        ("AP-1", "Tan Pte", "0 18956", "SGP", "65 6123 4567", Decimal("500"), None, False, False, "2019 12345 K", None, None, None, None, None, None, None),
        ("AP-2", "HK Trading", None, "HKG", None, Decimal("500"), None, False, False, None, None, None, None, None, None, None, None),
        ("AP-3", "No code", None, "AUS", None, Decimal("500"), None, False, False, None, None, None, None, None, None, None, None),
    ]))
    r = _one(valid, "AP-1")
    assert r["PostalCodeStandardized"] == "018956" and r["PostalFormatCode"] == "APAC-6"
    assert r["CreditLimitCurrencyCode"] == "SGD" and r["PhoneNumberStandardized"] == "+6561234567"
    assert r["GSTRegistrationNumber"] == "201912345K" and r["BusinessNumberType"] == "UEN" and r["GSTTreatmentCode"] == "REGISTERED"
    assert r["RetentionYears"] == 5 and r["ConsentBasisCode"] == "CHANNEL_SPECIFIC"
    h = _one(valid, "AP-2")
    assert h["PostalCodeStandardized"] is None and h["PostalFormatCode"] == "NONE" and h["GSTTreatmentCode"] == "UNREGISTERED"
    rej = {x["CustomerBusinessKey"]: x["RejectReasonCode"] for x in rejected.collect()}
    assert rej == {"AP-3": "MISSING_POSTCODE"}
