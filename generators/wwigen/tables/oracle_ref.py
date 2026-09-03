"""Oracle reference data: WWI_REF.

Reference data is fully refreshed every week, which is why none of these
tables carry effective dates - except the fiscal calendar, which has to,
because the three regions do not share one. The code-translation table is the
only place where the NA, EU and APAC vocabularies are mapped onto each other,
and it is deliberately incomplete.
"""

from __future__ import annotations

import datetime

from .. import regions, rng, schema, text
from ..schema import (TableSpec, date_col, dec_col, flag_col, int_col,
                      str_col)

CURRENCY_COLUMNS = (
    str_col("CURRENCY_CD", 3, nullable=False),
    str_col("CURRENCY_NM", 40),
    int_col("MINOR_UNIT_QTY", note="0 for JPY - the amount columns are still scaled 2"),
    str_col("SYMBOL_TX", 4),
    flag_col("ACTIVE_FLG"),
    str_col("ROUNDING_RULE_CD", 8, note="HALF_UP everywhere except APAC, which truncates"),
)

_CURRENCY_NAMES = {
    "USD": ("US Dollar", "$"), "CAD": ("Canadian Dollar", "C$"),
    "MXN": ("Mexican Peso", "Mex$"), "GBP": ("Pound Sterling", "GBP"),
    "EUR": ("Euro", "EUR"), "AUD": ("Australian Dollar", "A$"),
    "NZD": ("New Zealand Dollar", "NZ$"), "SGD": ("Singapore Dollar", "S$"),
    "JPY": ("Japanese Yen", "JPY"),
}

_APAC_CURRENCIES = ("AUD", "NZD", "SGD", "JPY")


def produce_currency_code(cfg, ctx):
    for code in sorted(_CURRENCY_NAMES):
        name, symbol = _CURRENCY_NAMES[code]
        yield (code, name, 0 if code == "JPY" else 2, symbol, "Y",
               "TRUNC" if code in _APAC_CURRENCIES else "HALF_UP")


FX_RATE_COLUMNS = (
    str_col("FROM_CURRENCY_CD", 3, nullable=False),
    str_col("TO_CURRENCY_CD", 3, nullable=False),
    date_col("RATE_DT", nullable=False),
    dec_col("RATE_AMT", 18, 8),
    str_col("RATE_TYPE_CD", 8, note="SPOT / MONTHAVG - APAC restates on the monthly average"),
    str_col("SOURCE_CD", 10),
    flag_col("OVERRIDE_FLG", note="set when treasury pasted a rate in by hand"),
)


def produce_fx_rate_daily(cfg, ctx):
    """A rate for every currency for every calendar day, including weekends."""
    days = min(cfg.count("fx_rate_days"), cfg.history_days + 1)
    start = cfg.history_end - datetime.timedelta(days=days - 1)
    for offset in range(days):
        when = start + datetime.timedelta(days=offset)
        for currency in ctx.currencies:
            if currency == "USD":
                continue
            rate = ctx.rate_to_usd(currency, when)
            yield ("USD", currency, when, rate, "SPOT", "TREASURY",
                   "Y" if rng.chance(cfg.seed, 0.004, "fx-override", currency, offset) else "N")
            if currency in _APAC_CURRENCIES and when.day == 1:
                month_average = sum(
                    ctx.rate_to_usd(currency, when + datetime.timedelta(days=step))
                    for step in (0, 7, 14, 21)) / 4.0
                yield ("USD", currency, when, round(month_average, 8), "MONTHAVG",
                       "TREASURY", "N")


COUNTRY_COLUMNS = (
    str_col("COUNTRY_CD", 2, nullable=False),
    str_col("COUNTRY_NM", 60),
    str_col("REGION_CD", 4),
    str_col("CURRENCY_CD", 3),
    str_col("DIAL_PREFIX_TX", 6),
    str_col("POSTAL_FORMAT_CD", 12, note="the shape the postal code is stored in, not validated"),
    str_col("ADDRESS_FORMAT_CD", 6),
    dec_col("STD_TAX_RATE", 7, 4),
    str_col("TAX_REGIME_CD", 12),
    int_col("FISCAL_START_MONTH_NO"),
    flag_col("EU_MEMBER_FLG"),
)

_EU_MEMBERS = ("DE", "FR", "NL", "IE")


def produce_country_ref(cfg, ctx):
    for region in regions.REGIONS:
        for country in regions.countries(region):
            yield (country.code, country.name, region, country.currency,
                   country.dial_prefix, country.postal_style, country.address_style,
                   country.tax_rate, country.tax_label,
                   regions.FISCAL_START_MONTH[region],
                   "Y" if country.code in _EU_MEMBERS else "N")


REGION_COLUMNS = (
    str_col("REGION_CD", 4, nullable=False),
    str_col("REGION_NM", 40),
    str_col("REPORTING_CURRENCY_CD", 3),
    str_col("FX_CONVENTION_CD", 20),
    int_col("FISCAL_START_MONTH_NO"),
    str_col("CONSENT_BASIS_CD", 12),
    int_col("RETENTION_MONTHS"),
    str_col("CLASS_SCHEME_CD", 12),
)

_REGION_NAMES = {"NA": "North America", "EU": "Europe", "APAC": "Asia Pacific"}
_CLASS_SCHEMES = {"NA": "NA-ABC", "EU": "EU-KLASSE", "APAC": "APAC-TIER"}


def produce_region_ref(cfg, ctx):
    for region in regions.REGIONS:
        consent = regions.CONSENT_MODEL[region]
        yield (region, _REGION_NAMES[region], regions.REPORTING_CURRENCY[region],
               regions.FX_CONVENTION[region], regions.FISCAL_START_MONTH[region],
               consent["basis"], consent["retention_months"], _CLASS_SCHEMES[region])


CALENDAR_COLUMNS = (
    str_col("REGION_CD", 4, nullable=False),
    date_col("CAL_DT", nullable=False),
    int_col("FISCAL_YEAR_NO"),
    int_col("FISCAL_PERIOD_NO"),
    str_col("FISCAL_PERIOD_CD", 12),
    int_col("FISCAL_QUARTER_NO"),
    int_col("FISCAL_WEEK_NO"),
    flag_col("PERIOD_END_FLG"),
    flag_col("QUARTER_END_FLG"),
    flag_col("BUSINESS_DAY_FLG"),
)


def produce_calendar_fiscal(cfg, ctx):
    """One row per region per day. Three calendars, three period numbers."""
    for region in regions.REGIONS:
        start_month = regions.FISCAL_START_MONTH[region]
        for offset in range(cfg.history_days + 1):
            when = cfg.history_start + datetime.timedelta(days=offset)
            period = (when.month - start_month) % 12 + 1
            fiscal_year = when.year if when.month >= start_month else when.year - 1
            if start_month != 1:
                fiscal_year += 1
            next_day = when + datetime.timedelta(days=1)
            yield (region, when, fiscal_year, period,
                   regions.fiscal_period(region, when),
                   (period - 1) // 3 + 1,
                   (offset // 7) % 53 + 1,
                   "Y" if next_day.month != when.month else "N",
                   "Y" if next_day.month != when.month and period % 3 == 0 else "N",
                   "Y" if when.weekday() < 5 else "N")


UOM_COLUMNS = (
    str_col("UOM_CD", 4, nullable=False),
    str_col("UOM_NM", 30),
    str_col("UOM_CLASS_CD", 10),
    dec_col("BASE_FACTOR", 12, 4),
    flag_col("ACTIVE_FLG"),
)

_UOMS = (("EA", "Each", "COUNT", 1.0), ("BOX", "Box", "COUNT", 12.0),
         ("CTN", "Carton", "COUNT", 144.0), ("PLT", "Pallet", "COUNT", 1440.0),
         ("KG", "Kilogram", "MASS", 1.0), ("G", "Gram", "MASS", 0.001),
         ("L", "Litre", "VOLUME", 1.0), ("ML", "Millilitre", "VOLUME", 0.001))


def produce_uom_ref(cfg, ctx):
    for code, name, uom_class, factor in _UOMS:
        yield (code, name, uom_class, factor, "Y")


STATUS_CODE_COLUMNS = (
    str_col("CODE_SET_CD", 20, nullable=False),
    str_col("REGION_CD", 4, nullable=False),
    str_col("CODE_VALUE", 8, nullable=False),
    str_col("CODE_DESC", 60),
    int_col("SORT_ORDER_NO"),
    flag_col("ACTIVE_FLG"),
)

_CODE_SETS = (
    ("CUST_STATUS", regions.CUSTOMER_STATUS_CODES),
    ("CUST_CLASS", regions.CUSTOMER_CLASS_CODES),
    ("ORDER_STATUS", regions.ORDER_STATUS_CODES),
    ("RETURN_REASON", regions.RETURN_REASON_CODES),
    ("PAYMENT_METHOD", regions.PAYMENT_METHOD_CODES),
)


def produce_status_code_ref(cfg, ctx):
    for set_name, mapping in _CODE_SETS:
        for region in regions.REGIONS:
            for index, value in enumerate(mapping[region]):
                yield (set_name, region, value,
                       "%s %s (%s)" % (set_name.replace("_", " ").title(), value, region),
                       index + 1, "Y")


CODE_TRANSLATION_COLUMNS = (
    str_col("CODE_SET_CD", 20, nullable=False),
    str_col("FROM_REGION_CD", 4, nullable=False),
    str_col("FROM_CODE", 8, nullable=False),
    str_col("TO_REGION_CD", 4, nullable=False),
    str_col("TO_CODE", 8),
    str_col("CANONICAL_CD", 12, note="the value the warehouse conforms to"),
    flag_col("APPROVED_FLG"),
    str_col("NOTE_TX", 80),
)

_CANONICAL = {
    "CUST_STATUS": ("ACTIVE", "INACTIVE", "SUSPENDED", "CLOSED"),
    "CUST_CLASS": ("TIER1", "TIER2", "TIER3", "SPECIAL"),
    "ORDER_STATUS": ("OPEN", "PICKED", "SHIPPED", "INVOICED", "CANCELLED"),
    "RETURN_REASON": ("DAMAGED", "WRONG_ITEM", "LATE", "QUANTITY", "OTHER"),
    "PAYMENT_METHOD": ("ACH", "CHEQUE", "CARD", "WIRE"),
}


def produce_code_translation(cfg, ctx):
    """Maps every regional code onto NA, badly.

    Roughly one mapping in twenty was never approved and one in thirty is
    simply absent, which is what the DQ packages are meant to surface.
    """
    seed = cfg.seed
    for set_name, mapping in _CODE_SETS:
        canonical = _CANONICAL[set_name]
        for region in ("EU", "APAC"):
            for index, value in enumerate(mapping[region]):
                if rng.chance(seed, 0.033, "xlat-missing", set_name, region, index):
                    continue
                target = mapping["NA"][index] if index < len(mapping["NA"]) else None
                approved = not rng.chance(seed, 0.05, "xlat-appr", set_name, region, index)
                yield (set_name, region, value, "NA", target,
                       canonical[index] if index < len(canonical) else None,
                       "Y" if approved else "N",
                       "" if approved else "raised by the 2019 harmonisation review")


SOURCE_SYSTEM_COLUMNS = (
    str_col("SRC_SYSTEM_CD", 8, nullable=False),
    str_col("SRC_SYSTEM_NM", 60),
    str_col("PLATFORM_TX", 40),
    str_col("REGION_CD", 4),
    str_col("OWNER_TEAM_TX", 40),
    str_col("EXTRACT_METHOD_CD", 16),
    flag_col("ACTIVE_FLG"),
)


def produce_source_system_ref(cfg, ctx):
    yield ("ERPNA", "Oracle ERP - North America", "Oracle 11g", "NA",
           "Finance Systems", "SQLLOADER", "Y")
    yield ("ERPEU", "Oracle ERP - Europe", "Oracle 11g", "EU",
           "Finance Systems", "SQLLOADER", "Y")
    yield ("ERPAP", "Oracle ERP - Asia Pacific", "Oracle 11g", "APAC",
           "Finance Systems", "SQLLOADER", "Y")
    yield ("WWIOLTP", "WideWorldImporters order management", "SQL Server", "NA",
           "Commercial IT", "BULKINSERT", "Y")
    yield ("PARTNER", "Partner sales file feed", "SFTP drop", "NA",
           "Commercial IT", "FILE", "Y")
    yield ("CARRIER", "Carrier scan feed", "SFTP drop", "NA", "Logistics", "FILE", "Y")
    yield ("SUPPCAT", "Supplier catalogue feed", "SFTP drop", "EU", "Procurement", "FILE", "Y")
    yield ("FXOVR", "Treasury FX override sheet", "Shared drive", "EU", "Treasury", "FILE", "Y")


PAYMENT_METHOD_REF_COLUMNS = (
    str_col("PAYMENT_METHOD_CD", 8, nullable=False),
    str_col("REGION_CD", 4, nullable=False),
    str_col("METHOD_NM", 40),
    int_col("CLEARING_DAYS"),
    flag_col("REQUIRES_BANK_FLG"),
    dec_col("FEE_AMT", 9, 2),
    str_col("FEE_CURRENCY_CD", 3),
)

_METHOD_NAMES = {
    "ACH": "Automated clearing house", "CHK": "Cheque", "CC": "Credit card",
    "WIRE": "Wire transfer", "SEPA": "SEPA credit transfer", "BACS": "BACS",
    "CARD": "Payment card", "SWIFT": "SWIFT transfer", "BECS": "Direct entry",
    "CHQ": "Cheque", "TT": "Telegraphic transfer",
}


def produce_payment_method_ref(cfg, ctx):
    seed = cfg.seed
    for region in regions.REGIONS:
        for code in regions.PAYMENT_METHOD_CODES[region]:
            yield (code, region, _METHOD_NAMES.get(code, code),
                   rng.pick(seed, (0, 1, 2, 3, 5), "clear", region, code),
                   "Y" if code in ("ACH", "WIRE", "SEPA", "BACS", "BECS", "TT", "SWIFT") else "N",
                   round(rng.stable_hash(seed, "fee", region, code) % 2500 / 100.0, 2),
                   regions.REPORTING_CURRENCY[region])


CITY_REF_COLUMNS = (
    str_col("CITY_ID", 10, nullable=False),
    str_col("CITY_NM", 60),
    str_col("STATE_PROV_CD", 20),
    str_col("COUNTRY_CD", 2),
    str_col("REGION_CD", 4),
    int_col("POPULATION_QTY"),
    dec_col("LATITUDE", 9, 6),
    dec_col("LONGITUDE", 9, 6),
)


def produce_city_ref(cfg, ctx):
    seed = cfg.seed
    index = 0
    for region in regions.REGIONS:
        for locality in text.LOCALITIES[region]:
            for subdivision in text.SUBDIVISIONS[region]:
                index += 1
                country = regions.countries(region)[index % len(regions.countries(region))]
                yield ("CTY%06d" % index, locality, subdivision, country.code, region,
                       5000 + rng.stable_hash(seed, "pop", index) % 4000000,
                       round(-60 + rng.stable_hash(seed, "lat", index) % 12000 / 100.0, 6),
                       round(-180 + rng.stable_hash(seed, "lon", index) % 36000 / 100.0, 6))


def _spec(name, columns, produce, row_count_key, target, description, tags=()):
    return TableSpec(
        key="oracle.WWI_REF.%s" % name,
        system=schema.ORACLE,
        schema="WWI_REF",
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group="oracle_reference",
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("CURRENCY_CODE", CURRENCY_COLUMNS, produce_currency_code, "", "raw.OracleCurrency",
          "Currency reference including the APAC truncation rounding rule.", ("regional",)),
    _spec("FX_RATE_DAILY", FX_RATE_COLUMNS, produce_fx_rate_daily, "fx_rate_days",
          "raw.OracleFxRate",
          "Daily spot rates for every calendar day plus APAC monthly averages.",
          ("regional", "fx")),
    _spec("COUNTRY_REF", COUNTRY_COLUMNS, produce_country_ref, "", "raw.OracleGeography",
          "Country reference with postal shape, tax regime and fiscal start month.",
          ("regional",)),
    _spec("REGION_REF", REGION_COLUMNS, produce_region_ref, "", "raw.OracleGeography",
          "The three regional operating models in one table.", ("regional",)),
    _spec("CALENDAR_FISCAL", CALENDAR_COLUMNS, produce_calendar_fiscal, "", "raw.OracleGeography",
          "Three fiscal calendars, one row per region per day.", ("regional",)),
    _spec("UOM_REF", UOM_COLUMNS, produce_uom_ref, "", "raw.OracleProductMaster",
          "Units of measure with base conversion factors.", ()),
    _spec("STATUS_CODE_REF", STATUS_CODE_COLUMNS, produce_status_code_ref, "",
          "raw.OracleCustomerMaster",
          "Every regional code set, unharmonised, as the ERP holds it.", ("regional",)),
    _spec("CODE_TRANSLATION", CODE_TRANSLATION_COLUMNS, produce_code_translation, "",
          "raw.OracleCustomerMaster",
          "Incomplete regional code mapping - the source of unknown-code rejects.",
          ("regional", "dataquality")),
    _spec("SOURCE_SYSTEM_REF", SOURCE_SYSTEM_COLUMNS, produce_source_system_ref, "",
          "raw.OracleCustomerMaster",
          "The systems that feed the estate and how each is extracted.", ()),
    _spec("PAYMENT_METHOD_REF", PAYMENT_METHOD_REF_COLUMNS, produce_payment_method_ref, "",
          "raw.OraclePaymentTerms",
          "Payment methods per region with clearing days and fees.", ("regional",)),
    _spec("CITY_REF", CITY_REF_COLUMNS, produce_city_ref, "", "raw.OracleGeography",
          "City reference used by address standardisation.", ()),
)