"""The file landing zone.

Six feeds arrive as flat files from outside the estate. They are the dirtiest
part of the data: nobody validates them at source, the partner systems each
have their own idea of a date format, and the APAC partner sends a tab
separated file with a Latin-1 codepage regardless of what the interface
specification says.

Each feed carries a configurable proportion of rows that the ingestion
packages are expected to reject rather than load, produced by rewriting an
otherwise valid row. Every rejected row is also written to the quarantine feed
with the reject code the DQ framework classifies it under, so the two are
reconcilable.
"""

from __future__ import annotations

import datetime

from .. import defects, entities, regions, rng, schema, skew, text
from ..schema import TableSpec, dec_col, int_col, str_col


def _feed(name, columns, produce, row_count_key, target, description,
          delimiter="|", extension="dat", encoding="utf-8", header=False, tags=()):
    return TableSpec(
        key="file.landing.%s" % name,
        system=schema.FILE_FEED,
        schema="landing",
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        delimiter=delimiter,
        encoding=encoding,
        header=header,
        extension=extension,
        target_object=target,
        group="file_landing",
        description=description,
        allows_defects=True,
        tags=tags,
    )


def _defective(cfg, spec, fields, kind: str, ordinal: int):
    """Return (fields, reject_code) - reject_code is None for a clean row."""
    seed = cfg.seed
    if not rng.chance(seed, cfg.defect("file_defect_rate"), "defect", kind, ordinal):
        return fields, None
    defect_kind = defects.choose_defect(seed, kind, ordinal)
    return defects.apply_defect(seed, defect_kind, fields, spec, ordinal)


# ---------------------------------------------------------------------------
# partner_sales - three regional variants of the same logical feed
# ---------------------------------------------------------------------------

PARTNER_SALES_COLUMNS = (
    str_col("PARTNER_CD", 12, nullable=False),
    str_col("PARTNER_CUST_CD", 16, note="the partner's own customer code, not ours"),
    str_col("SALE_DATE", 10, note="format differs per region: ISO, DD/MM/YYYY, YYYYMMDD"),
    str_col("ORDER_REF", 20),
    str_col("ITEM_CD", 20, note="partner catalogue code, mapped through ref.CODE_TRANSLATION"),
    int_col("QTY"),
    dec_col("UNIT_PRICE", 13, 2),
    dec_col("LINE_AMT", 15, 2),
    str_col("CURRENCY", 3),
    str_col("TAX_CD", 12),
    dec_col("TAX_AMT", 15, 2),
    str_col("REGION_CD", 4),
    str_col("STORE_CD", 10),
    str_col("SALES_CHANNEL", 10),
)

_PARTNERS = {
    "NA": ("PTR-NA-0001", "PTR-NA-0002", "PTR-NA-0003", "PTR-NA-0004"),
    "EU": ("PTR-EU-0001", "PTR-EU-0002", "PTR-EU-0003"),
    "APAC": ("PTR-AP-0001", "PTR-AP-0002"),
}


def _partner_date(region: str, when: datetime.date) -> str:
    """Every partner region formats the date differently. None is corrected."""
    if region == "NA":
        return when.strftime("%m/%d/%Y")
    if region == "EU":
        return when.strftime("%d/%m/%Y")
    return when.strftime("%Y%m%d")


def _partner_rows(cfg, ctx, region: str, spec):
    seed = cfg.seed
    total = max(int(cfg.count("partner_sales_rows") * cfg.region_mix[region]), 1)
    customers = cfg.count("customers")
    products = cfg.count("products")
    for ordinal in range(total):
        customer_ordinal = skew.pareto_ordinal(seed, customers, "pf-cust", region, ordinal)
        cust = entities.customer(cfg, customer_ordinal)
        product_ordinal = skew.pareto_ordinal(seed, products, "pf-prod", region, ordinal)
        prod = entities.product(cfg, product_ordinal)
        when = ctx.dates.date_for(seed, "pf-when", region, ordinal)
        price, _cost = prod.price_on(when)
        quantity = skew.long_tail_quantity(seed, "pf-qty", region, ordinal)
        amount = round(price * quantity, 2)
        country = cust.country if cust.region == region else regions.countries(region)[0]
        tax_code, _rate, tax_amount = regions.tax_treatment(region, country, amount, False)
        fields = [
            rng.pick(seed, _PARTNERS[region], "pf-partner", ordinal),
            "%s%08d" % (region[:1], customer_ordinal),
            _partner_date(region, when),
            "PO%s%08d" % (region[:2], rng.stable_hash(seed, "pf-ref", region, ordinal) % 10 ** 8),
            "%s/%s" % (rng.pick(seed, text.CATEGORY_CODES, "pf-cat", ordinal), prod.erp_code),
            quantity, price, amount,
            regions.REPORTING_CURRENCY[region] if region != "NA" else cust.country.currency,
            tax_code, tax_amount, region,
            "STR%s%04d" % (region[:2], rng.stable_hash(seed, "pf-store", region, ordinal) % 10000),
            rng.pick(seed, ("RETAIL", "ONLINE", "WHOLESALE", "MARKET"), "pf-chan", ordinal),
        ]
        yield _defective(cfg, spec, fields, "partner-" + region, ordinal)


def _partner_producer(region: str):
    def produce(cfg, ctx):
        spec = SPEC_BY_REGION[region]
        for fields, _reject in _partner_rows(cfg, ctx, region, spec):
            yield fields
    produce.__name__ = "produce_partner_sales_%s" % region.lower()
    produce.__doc__ = "Partner sales rows for %s, with regional date and tax shapes." % region
    return produce


# ---------------------------------------------------------------------------
# carrier_scan
# ---------------------------------------------------------------------------

CARRIER_SCAN_COLUMNS = (
    str_col("CARRIER_CD", 10, nullable=False),
    str_col("TRACKING_NO", 30),
    str_col("SCAN_TS", 19, note="local time at the scan location, no offset recorded"),
    str_col("SCAN_CODE", 8),
    str_col("LOCATION_CD", 12),
    str_col("POSTAL_CD", 12),
    str_col("COUNTRY_CD", 2),
    str_col("SHIPMENT_REF", 20),
    int_col("PIECE_COUNT"),
    dec_col("WEIGHT_KG", 10, 3),
    str_col("EXCEPTION_CD", 10),
    str_col("SIGNED_BY", 60),
)

_SCAN_CODES = ("PU", "DP", "AR", "OD", "DL", "EX", "RT")
_EXCEPTION_CODES = ("NOACCESS", "REFUSED", "DAMAGED", "ADDRESS", "WEATHER")


def produce_carrier_scan(cfg, ctx):
    """Carrier scans, several per shipment, with no timezone on the timestamp."""
    seed = cfg.seed
    spec = CARRIER_SCAN_SPEC
    shipments = max(cfg.count("shipments"), 1)
    for ordinal in range(cfg.count("carrier_scan_rows")):
        shipment_ordinal = ordinal % shipments
        when = ctx.dates.datetime_for(seed, "cs-when", ordinal)
        scan_code = rng.pick(seed, _SCAN_CODES, "cs-code", ordinal)
        exception = scan_code == "EX"
        fields = [
            rng.pick(seed, ("MERIDIAN", "NORTHWAY", "EUROLINK", "PACRIM", "SWIFTFRT"),
                     "cs-carrier", ordinal),
            "%s%012d" % (rng.pick(seed, ("1Z", "TT", "AU", "EU"), "cs-pre", shipment_ordinal),
                         rng.stable_hash(seed, "trk", shipment_ordinal) % 10 ** 12),
            when.strftime("%Y-%m-%d %H:%M:%S"),
            scan_code,
            "LOC%05d" % (rng.stable_hash(seed, "cs-loc", ordinal) % 10 ** 5),
            regions.postal_code(rng.pick(seed, ("US", "GB", "DE", "AU"), "cs-style", ordinal),
                                rng.stable_hash(seed, "cs-postal", ordinal)),
            rng.pick(seed, ("US", "CA", "GB", "DE", "FR", "AU", "NZ", "SG", "JP"),
                     "cs-country", ordinal),
            "SHP%09d" % shipment_ordinal,
            1 + rng.stable_hash(seed, "cs-pieces", ordinal) % 8,
            round(rng.stable_hash(seed, "cs-weight", ordinal) % 400000 / 1000.0, 3),
            rng.pick(seed, _EXCEPTION_CODES, "cs-exc", ordinal) if exception else "",
            ("%s %s" % text.person_name(seed, ordinal)) if scan_code == "DL" else "",
        ]
        yield _defective(cfg, spec, fields, "carrier", ordinal)[0]


# ---------------------------------------------------------------------------
# supplier_catalog
# ---------------------------------------------------------------------------

SUPPLIER_CATALOG_COLUMNS = (
    str_col("SUPPLIER_CD", 14, nullable=False),
    str_col("SUPPLIER_ITEM_CD", 24, nullable=False),
    str_col("OUR_ITEM_CD", 14, note="often blank on newly listed lines"),
    str_col("DESCRIPTION", 120),
    str_col("UOM", 6),
    int_col("PACK_QTY"),
    dec_col("LIST_PRICE", 13, 4),
    str_col("CURRENCY", 3),
    str_col("PRICE_VALID_FROM", 10),
    str_col("PRICE_VALID_TO", 10),
    int_col("LEAD_TIME_DAYS"),
    int_col("MOQ"),
    str_col("COUNTRY_OF_ORIGIN", 2),
    str_col("TARIFF_CD", 12),
    str_col("HAZMAT_FLAG", 1),
)


def produce_supplier_catalog(cfg, ctx):
    """Pipe-separated supplier price list, refreshed quarterly by each supplier."""
    seed = cfg.seed
    spec = SUPPLIER_CATALOG_SPEC
    products = max(cfg.count("products"), 1)
    for ordinal in range(cfg.count("supplier_catalog_rows")):
        product_ordinal = ordinal % products
        prod = entities.product(cfg, product_ordinal)
        supp = entities.supplier(cfg, prod.supplier_ordinal)
        valid_from = ctx.dates.date_for(seed, "sc-from", ordinal)
        _price, cost = prod.price_on(valid_from)
        unlisted = rng.chance(seed, 0.07, "sc-unlisted", ordinal)
        fields = [
            supp.erp_code,
            "%s-%06d" % (supp.erp_code[-4:], rng.stable_hash(seed, "sc-item", ordinal) % 10 ** 6),
            "" if unlisted else prod.erp_code,
            prod.name.upper(),
            prod.uom, prod.pack_size, round(cost, 4), supp.country.currency,
            valid_from.isoformat(),
            (valid_from + datetime.timedelta(days=90)).isoformat(),
            prod.lead_time_days,
            prod.pack_size * (1 + rng.stable_hash(seed, "sc-moq", ordinal) % 6),
            supp.country.code,
            "%04d.%02d.%02d" % (rng.stable_hash(seed, "sc-t1", ordinal) % 10000,
                                rng.stable_hash(seed, "sc-t2", ordinal) % 100,
                                rng.stable_hash(seed, "sc-t3", ordinal) % 100),
            "Y" if rng.chance(seed, 0.04, "sc-haz", ordinal) else "N",
        ]
        yield _defective(cfg, spec, fields, "suppcat", ordinal)[0]


# ---------------------------------------------------------------------------
# fx_override
# ---------------------------------------------------------------------------

FX_OVERRIDE_COLUMNS = (
    str_col("EFFECTIVE_DATE", 10, nullable=False),
    str_col("FROM_CCY", 3, nullable=False),
    str_col("TO_CCY", 3, nullable=False),
    dec_col("RATE", 18, 8),
    str_col("RATE_TYPE", 10),
    str_col("REASON_TX", 80),
    str_col("APPROVED_BY", 30),
    str_col("SOURCE_SHEET", 40, note="the spreadsheet tab the row was pasted from"),
)

_FX_REASONS = ("Month-end restatement", "Treasury hedge rate", "Corrected feed error",
               "Intercompany settlement", "Audit adjustment")


def produce_fx_override(cfg, ctx):
    """Treasury's manual rate overrides, pasted out of a spreadsheet."""
    seed = cfg.seed
    spec = FX_OVERRIDE_SPEC
    currencies = tuple(c for c in ctx.currencies if c != "USD")
    for ordinal in range(cfg.count("fx_override_rows")):
        when = ctx.dates.date_for(seed, "fx-ov-when", ordinal)
        currency = rng.pick(seed, currencies, "fx-ov-ccy", ordinal)
        target = rng.pick(seed, ("USD", "EUR", "AUD"), "fx-ov-target", ordinal)
        fields = [
            when.isoformat(), currency, target,
            round(ctx.cross_rate(currency, target, when) * 1.002, 8),
            rng.pick(seed, ("SPOT", "MONTHAVG", "HEDGE"), "fx-ov-type", ordinal),
            rng.pick(seed, _FX_REASONS, "fx-ov-reason", ordinal),
            rng.pick(seed, ("TREASURY", "GROUPFIN", "CONTROLLER"), "fx-ov-by", ordinal),
            "FX_%s_%s.xls#Sheet%d" % (when.strftime("%Y%m"), target,
                                      1 + rng.stable_hash(seed, "fx-ov-tab", ordinal) % 4),
        ]
        yield _defective(cfg, spec, fields, "fxover", ordinal)[0]


# ---------------------------------------------------------------------------
# quarantine - the reject side of every feed, in one file
# ---------------------------------------------------------------------------

QUARANTINE_COLUMNS = (
    str_col("FEED_CD", 20, nullable=False),
    int_col("SOURCE_ROW_NO", nullable=False),
    str_col("REJECT_CD", 30, nullable=False),
    str_col("RAW_ROW_TX", 400, note="the row exactly as it arrived, delimiters replaced"),
    str_col("DETECTED_TS", 19),
)


def produce_quarantine(cfg, ctx):
    """Every row the feeds expect to be rejected, with its classification.

    This is the reconciliation target: the count per feed and reject code here
    must equal what the ingestion packages quarantine when the same files are
    loaded.
    """
    seed = cfg.seed
    detected = cfg.snapshot_date.strftime("%Y-%m-%d 03:15:00")
    for region in regions.REGIONS:
        spec = SPEC_BY_REGION[region]
        row_number = 0
        for fields, reject in _partner_rows(cfg, ctx, region, spec):
            row_number += 1
            if reject is None:
                continue
            yield ("PARTNER_%s" % region, row_number, reject,
                   _raw_row(fields, spec.delimiter), detected)
    for feed_code, spec, producer_kind, count_key in (
            ("CARRIER_SCAN", CARRIER_SCAN_SPEC, "carrier", "carrier_scan_rows"),
            ("SUPPLIER_CAT", SUPPLIER_CATALOG_SPEC, "suppcat", "supplier_catalog_rows"),
            ("FX_OVERRIDE", FX_OVERRIDE_SPEC, "fxover", "fx_override_rows")):
        for ordinal in range(cfg.count(count_key)):
            if not rng.chance(seed, cfg.defect("file_defect_rate"), "defect",
                              producer_kind, ordinal):
                continue
            kind = defects.choose_defect(seed, producer_kind, ordinal)
            yield (feed_code, ordinal + 1, defects.reject_code(kind),
                   "row %d withheld by the feed generator" % (ordinal + 1), detected)


def _raw_row(fields, delimiter: str) -> str:
    parts = []
    for value in fields:
        if value is None:
            parts.append("")
        elif isinstance(value, bytes):
            parts.append(value.decode("latin-1", "replace"))
        else:
            parts.append(str(value))
    return delimiter.join(parts).replace(delimiter, " ")[:400]


# ---------------------------------------------------------------------------
# Specifications
# ---------------------------------------------------------------------------

PARTNER_SALES_NA_SPEC = _feed(
    "partner_sales_na", PARTNER_SALES_COLUMNS, _partner_producer("NA"), "",
    "raw.FilePartnerSales",
    "North American partner sales, comma separated with a US date format.",
    delimiter=",", extension="csv", header=True, tags=("regional", "dataquality"))

PARTNER_SALES_EU_SPEC = _feed(
    "partner_sales_eu", PARTNER_SALES_COLUMNS, _partner_producer("EU"), "",
    "raw.FilePartnerSales",
    "European partner sales, comma separated with a day-first date format.",
    delimiter=",", extension="csv", header=True, tags=("regional", "dataquality"))

PARTNER_SALES_APAC_SPEC = _feed(
    "partner_sales_apac", PARTNER_SALES_COLUMNS, _partner_producer("APAC"), "",
    "raw.FilePartnerSales",
    "APAC partner sales, tab separated and Latin-1 encoded despite the spec.",
    delimiter="\t", extension="txt", encoding="latin-1", header=False,
    tags=("regional", "dataquality"))

SPEC_BY_REGION = {
    "NA": PARTNER_SALES_NA_SPEC,
    "EU": PARTNER_SALES_EU_SPEC,
    "APAC": PARTNER_SALES_APAC_SPEC,
}

CARRIER_SCAN_SPEC = _feed(
    "carrier_scan", CARRIER_SCAN_COLUMNS, produce_carrier_scan, "carrier_scan_rows",
    "raw.FileCarrierScan",
    "Carrier scan events with local timestamps and no offset.",
    delimiter=",", extension="csv", header=True, tags=("dataquality", "reconciliation"))

SUPPLIER_CATALOG_SPEC = _feed(
    "supplier_catalog", SUPPLIER_CATALOG_COLUMNS, produce_supplier_catalog,
    "supplier_catalog_rows", "raw.FileSupplierCatalog",
    "Quarterly supplier price lists, pipe separated.",
    delimiter="|", extension="psv", header=True, tags=("dataquality",))

FX_OVERRIDE_SPEC = _feed(
    "fx_override", FX_OVERRIDE_COLUMNS, produce_fx_override, "fx_override_rows",
    "raw.FileFxOverride",
    "Treasury rate overrides pasted from a spreadsheet.",
    delimiter=",", extension="csv", header=True, tags=("fx", "dataquality"))

QUARANTINE_SPEC = _feed(
    "quarantine_rejects", QUARANTINE_COLUMNS, produce_quarantine, "",
    "err.RejectedFileRow",
    "The expected reject set for every feed, for reconciliation against the load.",
    delimiter="|", extension="dat", header=True, tags=("dataquality",))

SPECS = (
    PARTNER_SALES_NA_SPEC,
    PARTNER_SALES_EU_SPEC,
    PARTNER_SALES_APAC_SPEC,
    CARRIER_SCAN_SPEC,
    SUPPLIER_CATALOG_SPEC,
    FX_OVERRIDE_SPEC,
    QUARANTINE_SPEC,
)