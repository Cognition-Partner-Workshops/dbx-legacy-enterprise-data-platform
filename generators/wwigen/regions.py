"""Regional divergence: NA, EU and APAC are genuinely different shops.

Twenty years of acquisitions left three operating regions that never agreed
on anything. This module holds those disagreements in one place so every
generated table can be consistently wrong in the same way the real estate is:

* tax is sales tax (NA), VAT (EU) or GST (APAC), with different bases,
  registration identifiers and rounding;
* fiscal calendars start in January (NA), April (EU) and July (APAC);
* currencies differ, and so does the FX convention - NA books in the
  transaction currency, EU triangulates through EUR at the daily rate, APAC
  restates at a monthly average rate;
* address, postal and phone formats are per country, not per region;
* consent and retention rules differ (opt-out with a 7-year retention in NA,
  explicit opt-in with 3-year retention in EU, express-consent flags with a
  5-year retention in APAC);
* the status, classification and reason code sets are entirely different
  strings for the same business meaning, which is what the reference-data
  translation layer exists to reconcile.
"""

from __future__ import annotations

from dataclasses import dataclass

REGIONS = ("NA", "EU", "APAC")


@dataclass(frozen=True)
class CountryProfile:
    code: str
    name: str
    currency: str
    dial_prefix: str
    postal_style: str
    address_style: str
    tax_rate: float
    tax_label: str


COUNTRIES = {
    "NA": (
        CountryProfile("US", "United States", "USD", "+1", "zip5", "us", 0.0725, "SALES_TAX"),
        CountryProfile("CA", "Canada", "CAD", "+1", "ca_alnum", "us", 0.1300, "HST"),
        CountryProfile("MX", "Mexico", "MXN", "+52", "zip5", "us", 0.1600, "IVA"),
    ),
    "EU": (
        CountryProfile("GB", "United Kingdom", "GBP", "+44", "gb_alnum", "eu", 0.2000, "VAT"),
        CountryProfile("DE", "Germany", "EUR", "+49", "zip5", "eu", 0.1900, "VAT"),
        CountryProfile("FR", "France", "EUR", "+33", "zip5", "eu", 0.2000, "VAT"),
        CountryProfile("NL", "Netherlands", "EUR", "+31", "nl_alnum", "eu", 0.2100, "VAT"),
        CountryProfile("IE", "Ireland", "EUR", "+353", "ie_eircode", "eu", 0.2300, "VAT"),
    ),
    "APAC": (
        CountryProfile("AU", "Australia", "AUD", "+61", "zip4", "apac", 0.1000, "GST"),
        CountryProfile("NZ", "New Zealand", "NZD", "+64", "zip4", "apac", 0.1500, "GST"),
        CountryProfile("SG", "Singapore", "SGD", "+65", "zip6", "apac", 0.0900, "GST"),
        CountryProfile("JP", "Japan", "JPY", "+81", "jp_split", "apac", 0.1000, "CT"),
    ),
}

# Fiscal year start month per region. The EU entities were folded in from a
# UK-headquartered acquisition and kept their April year end; APAC came from
# the Australian distributor and kept July.
FISCAL_START_MONTH = {"NA": 1, "EU": 4, "APAC": 7}

# The regional reporting currency every local amount is restated into.
REPORTING_CURRENCY = {"NA": "USD", "EU": "EUR", "APAC": "AUD"}

# How each region converts. This drives which FX rate row the ETL has to find.
FX_CONVENTION = {
    "NA": "TXN_DAILY",       # rate on the transaction date, no triangulation
    "EU": "EUR_TRIANGULATE",  # local -> EUR -> reporting, both legs daily
    "APAC": "MONTH_AVG",      # monthly average rate, restated at period close
}

# Customer classification codes. Same three commercial tiers, three vocabularies.
CUSTOMER_CLASS_CODES = {
    "NA": ("A", "B", "C", "H"),
    "EU": ("K1", "K2", "K3", "KX"),
    "APAC": ("T1", "T2", "T3", "TS"),
}

CUSTOMER_STATUS_CODES = {
    "NA": ("ACT", "INA", "HLD", "CLS"),
    "EU": ("AKT", "INA", "SPR", "GES"),
    "APAC": ("ACTV", "INAC", "SUSP", "CLSD"),
}

ORDER_STATUS_CODES = {
    "NA": ("OPN", "PCK", "SHP", "INV", "CAN"),
    "EU": ("O", "P", "V", "F", "S"),
    "APAC": ("10", "20", "30", "40", "90"),
}

RETURN_REASON_CODES = {
    "NA": ("DMG", "WRNG", "LATE", "QTY", "NOR"),
    "EU": ("R01", "R02", "R03", "R04", "R09"),
    "APAC": ("DAMG", "MISM", "DELY", "OVER", "OTHR"),
}

PAYMENT_METHOD_CODES = {
    "NA": ("ACH", "CHK", "CC", "WIRE"),
    "EU": ("SEPA", "BACS", "CARD", "SWIFT"),
    "APAC": ("BECS", "CHQ", "CARD", "TT"),
}

# Consent and retention. GDPR-shaped in the EU, opt-out in NA, express consent
# for electronic marketing in APAC.
CONSENT_MODEL = {
    "NA": {"basis": "OPT_OUT", "marketing_default": "Y", "retention_months": 84,
           "consent_code_set": ("OPTOUT", "OPTIN", "DNC")},
    "EU": {"basis": "OPT_IN", "marketing_default": "N", "retention_months": 36,
           "consent_code_set": ("EINW", "WIDER", "KEINE")},
    "APAC": {"basis": "EXPRESS", "marketing_default": "N", "retention_months": 60,
             "consent_code_set": ("EXPR", "INFER", "WITHDRAWN")},
}

# Legacy source-system identifiers as they appear in ref.SOURCE_SYSTEM_REF.
SOURCE_SYSTEM_CODE = {
    "NA": "ERPNA",
    "EU": "ERPEU",
    "APAC": "ERPAP",
}

# Warehouse sites, used by inventory movements and shipment origins.
SITES = {
    "NA": ("CHI01", "DAL02", "NJ03", "TOR04"),
    "EU": ("MAN01", "HAM02", "LYO03", "ROT04"),
    "APAC": ("SYD01", "MEL02", "SIN03", "OSA04"),
}


def countries(region: str):
    return COUNTRIES[region]


def fiscal_period(region: str, when) -> str:
    """Fiscal period label ``FYnnnn-Pnn`` under the region's own calendar."""
    start_month = FISCAL_START_MONTH[region]
    offset = (when.month - start_month) % 12
    fiscal_year = when.year if when.month >= start_month else when.year - 1
    if start_month == 1:
        fiscal_year = when.year
    return "FY%04d-P%02d" % (fiscal_year + (1 if start_month != 1 else 0), offset + 1)


def tax_treatment(region: str, country: CountryProfile, net_amount: float,
                  customer_is_registered: bool) -> tuple:
    """Return ``(tax_code, tax_rate, tax_amount)`` under the regional rules.

    NA applies destination sales tax to everything and does not care about
    registration. EU zero-rates a registered cross-border business customer
    (reverse charge) and rounds to two decimals per line. APAC applies GST to
    everything but rounds the tax at the invoice level, which is why the line
    sums do not always tie - the reconciliation packages expect that.
    """
    if region == "NA":
        rate = country.tax_rate
        return ("%s-%s" % (country.tax_label, country.code), rate, _round2(net_amount * rate))
    if region == "EU":
        if customer_is_registered:
            return ("VAT-RC-%s" % country.code, 0.0, 0.0)
        rate = country.tax_rate
        return ("VAT-%s-STD" % country.code, rate, _round2(net_amount * rate))
    rate = country.tax_rate
    # Deliberately truncating, not rounding: the APAC ledger has done this
    # since the distributor's original system and finance reconciles the
    # rounding difference in a separate journal.
    return ("%s%s" % (country.tax_label, country.code), rate, _truncate2(net_amount * rate))


def _round2(value: float) -> float:
    return float(int(value * 100 + (0.5 if value >= 0 else -0.5))) / 100.0


def _truncate2(value: float) -> float:
    return float(int(value * 100)) / 100.0


def postal_code(style: str, draw: int) -> str:
    """Format a postal code in the country's own shape."""
    letters = "ABCDEFGHJKLMNPRSTVWXYZ"
    if style == "zip5":
        return "%05d" % (draw % 100000)
    if style == "zip4":
        return "%04d" % (draw % 10000)
    if style == "zip6":
        return "%06d" % (draw % 1000000)
    if style == "ca_alnum":
        return "%s%d%s %d%s%d" % (letters[draw % 22], (draw // 3) % 10,
                                  letters[(draw // 7) % 22], (draw // 11) % 10,
                                  letters[(draw // 13) % 22], (draw // 17) % 10)
    if style == "gb_alnum":
        return "%s%s%d %d%s%s" % (letters[draw % 22], letters[(draw // 5) % 22],
                                  (draw // 9) % 100 // 10, (draw // 23) % 10,
                                  letters[(draw // 29) % 22], letters[(draw // 31) % 22])
    if style == "nl_alnum":
        return "%04d %s%s" % (1000 + draw % 8999, letters[draw % 22], letters[(draw // 3) % 22])
    if style == "ie_eircode":
        return "%s%02d %s%s%s%s" % ("DACHKNPTVWX"[draw % 11], draw % 100,
                                    letters[(draw // 3) % 22], letters[(draw // 5) % 22],
                                    letters[(draw // 7) % 22], letters[(draw // 11) % 22])
    if style == "jp_split":
        return "%03d-%04d" % (draw % 1000, (draw // 7) % 10000)
    return "%05d" % (draw % 100000)


def phone_number(country: CountryProfile, draw: int) -> str:
    """Phone formats differ per country, and the NA one keeps its brackets."""
    if country.code in ("US", "CA"):
        return "%s (%03d) 555-%04d" % (country.dial_prefix, 200 + draw % 700, draw % 10000)
    if country.code == "JP":
        return "%s-%d-%04d-%04d" % (country.dial_prefix, 1 + draw % 9,
                                    (draw // 3) % 10000, (draw // 7) % 10000)
    if country.code == "SG":
        return "%s %04d %04d" % (country.dial_prefix, 6000 + draw % 3999, (draw // 5) % 10000)
    return "%s %d %04d %04d" % (country.dial_prefix, 1 + draw % 8,
                                (draw // 3) % 10000, (draw // 11) % 10000)


def format_address_lines(style: str, street_no: int, street: str, locality: str,
                         subdivision: str, postal: str) -> tuple:
    """Address line composition differs by region, not just by content.

    NA puts the number first and the state before the ZIP, EU puts the number
    after the street and the postcode before the town, APAC leads with a
    building level and puts the state between town and postcode.
    """
    if style == "us":
        return ("%d %s" % (street_no, street), "%s, %s %s" % (locality, subdivision, postal))
    if style == "eu":
        return ("%s %d" % (street, street_no), "%s %s" % (postal, locality))
    return ("Level %d, %d %s" % (1 + street_no % 30, street_no, street),
            "%s %s %s" % (locality, subdivision, postal))
