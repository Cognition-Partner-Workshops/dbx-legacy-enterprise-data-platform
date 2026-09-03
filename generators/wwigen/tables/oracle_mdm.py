"""Oracle ERP master data: WWI_MDM.

The ERP is the book of record for customers, suppliers and products. Its rows
carry the twenty-year-old column vocabulary (``_CD``, ``_FLG``, ``_DT``,
``_NO``) and the audit trailer every table in the schema grew in 2007 when the
first data-warehouse project needed change detection.

Three things in here exist specifically to give the downstream ETL work:

* ``CUST_MASTER`` emits near-duplicate and exact-duplicate rows for the
  match/merge logic - the same trading entity keyed twice, once with a
  spelling variant of the name and a moved address.
* the address, classification and price history tables are the *source* of
  SCD Type 2 history: they carry effective dates, and the warehouse has to
  reconstruct the versions from them.
* ``PARTY_XREF`` is the crosswalk to the SQL Server OLTP identifiers, with a
  controlled proportion of missing, retired, stale and duplicated mappings.
"""

from __future__ import annotations

import datetime

from .. import entities, keys, regions, rng, schema, text
from ..schema import (TableSpec, date_col, dec_col, flag_col, int_col, str_col,
                      ts_col)

AUDIT_COLUMNS = (
    str_col("SRC_SYSTEM_CD", 8, nullable=False, note="ERPNA / ERPEU / ERPAP"),
    str_col("CREATED_BY", 30),
    date_col("CREATED_DT"),
    str_col("LAST_UPD_BY", 30),
    ts_col("LAST_UPD_TS"),
    int_col("ROW_VERSION_NO"),
)

_OPERATORS = ("BATCH", "CONVRT", "JSCHMIDT", "MOTOOLE", "KTANAKA", "DFERRIS",
              "INTERFACE", "EDIUSER", "MDMSTEW")


def _audit(cfg, kind: str, ordinal: int, created: datetime.date,
           region: str, updated: datetime.date = None):
    seed = cfg.seed
    updated = updated or created
    return (
        regions.SOURCE_SYSTEM_CODE[region],
        rng.pick(seed, _OPERATORS, "created-by", kind, ordinal),
        created,
        rng.pick(seed, _OPERATORS, "upd-by", kind, ordinal),
        datetime.datetime(updated.year, updated.month, updated.day,
                          rng.stable_hash(seed, "upd-h", kind, ordinal) % 24,
                          rng.stable_hash(seed, "upd-m", kind, ordinal) % 60,
                          rng.stable_hash(seed, "upd-s", kind, ordinal) % 60),
        1 + rng.stable_hash(seed, "ver", kind, ordinal) % 9,
    )


# ---------------------------------------------------------------------------
# CUST_MASTER
# ---------------------------------------------------------------------------

CUST_MASTER_COLUMNS = (
    str_col("CUST_CODE", 12, nullable=False, note="CUS-nnnnnnn"),
    str_col("LEGACY_ACCT_NO", 10, note="pre-1998 account number, still quoted by customers"),
    str_col("CUST_NAME", 100, nullable=False),
    str_col("TRADING_NAME", 100),
    str_col("CUST_CLASS_CD", 4, note="regional vocabulary: A/B/C, K1/K2/K3, T1/T2/T3"),
    str_col("STATUS_CD", 4),
    str_col("REGION_CD", 4),
    str_col("COUNTRY_CD", 2),
    str_col("CURRENCY_CD", 3),
    str_col("TAX_REG_NO", 20),
    flag_col("TAX_REG_FLG"),
    dec_col("CREDIT_LIMIT_AMT", 15, 2),
    str_col("PAYMENT_TERMS_CD", 8),
    int_col("SALES_REP_ID"),
    str_col("BUYING_GROUP_CD", 10),
    str_col("CONSENT_CD", 10),
    flag_col("MARKETING_FLG"),
    int_col("RETENTION_MONTHS"),
    date_col("OPEN_DT"),
    date_col("LAST_ORDER_DT"),
    flag_col("MERGE_CAND_FLG", note="set by the 2011 dedup project, never cleaned up"),
) + AUDIT_COLUMNS


def _customer_row(cfg, cust, code: str, name: str, address, class_code: str,
                  merge_candidate: str, ordinal: int):
    seed = cfg.seed
    last_order = cust.opened_date + datetime.timedelta(
        days=rng.stable_hash(seed, "lastord", ordinal) % max(
            (cfg.history_end - cust.opened_date).days, 1))
    return (
        code,
        "%06d" % (700000 + cust.ordinal) if cust.opened_date <= cfg.history_start else None,
        name,
        cust.trading_name if cust.trading_name != name else None,
        class_code,
        cust.status_code,
        cust.region,
        address.country_code,
        cust.country.currency,
        cust.tax_registration,
        "Y" if cust.tax_registered else "N",
        cust.credit_limit,
        "NET%02d" % cust.payment_terms_days,
        1000 + cust.salesperson_on(cfg.history_end),
        cust.buying_group or None,
        cust.consent_code,
        cust.marketing_flag,
        cust.retention_months,
        cust.opened_date,
        last_order,
        merge_candidate,
    ) + _audit(cfg, "cust", ordinal, cust.opened_date, cust.region, last_order)


def produce_cust_master(cfg, ctx):
    population = cfg.count("customers")
    seed = cfg.seed
    for ordinal in range(population):
        cust = entities.customer(cfg, ordinal)
        address = cust.addresses[-1]
        class_code = cust.class_on(cfg.history_end)
        yield _customer_row(cfg, cust, cust.erp_code, cust.name, address,
                            class_code, "N", ordinal)

        # A near-duplicate: the same company re-keyed by a different branch,
        # with a name variant, a different address version and its own code.
        if cust.duplicate_of >= 0:
            source = entities.customer(cfg, cust.duplicate_of)
            variant_name = text.name_variant(seed, source.name, ordinal)
            yield _customer_row(
                cfg, source, "CUS-9%06d" % (100000 + ordinal), variant_name,
                source.addresses[0], source.class_on(cfg.history_end), "Y", ordinal)

        # An exact duplicate: the same row loaded twice by a failed interface
        # restart in 2016. Byte-identical apart from the row version.
        if rng.chance(seed, cfg.defect("exact_duplicate_rate"), "cust-exact-dup", ordinal):
            yield _customer_row(cfg, cust, cust.erp_code, cust.name, address,
                                class_code, "Y", ordinal)


# ---------------------------------------------------------------------------
# CUST_ADDRESS  (SCD source)
# ---------------------------------------------------------------------------

CUST_ADDRESS_COLUMNS = (
    str_col("CUST_CODE", 12, nullable=False),
    int_col("ADDR_SEQ_NO", nullable=False),
    str_col("ADDR_TYPE_CD", 4, note="BILL / SHIP / STMT"),
    str_col("ADDR_LINE_1", 80),
    str_col("ADDR_LINE_2", 80),
    str_col("CITY_NM", 60),
    str_col("STATE_PROV_CD", 20),
    str_col("POSTAL_CD", 12),
    str_col("COUNTRY_CD", 2),
    date_col("VALID_FROM_DT"),
    date_col("VALID_TO_DT", note="null on the current version"),
    flag_col("PRIMARY_FLG"),
    str_col("GEOCODE_QLTY_CD", 4, note="EXCT / ROOF / CENT / NONE - populated by a 2013 project"),
    str_col("STD_STATUS_CD", 4, note="address standardisation outcome, blank in EU"),
) + AUDIT_COLUMNS

_ADDRESS_TYPES = ("BILL", "SHIP", "STMT")


def produce_cust_address(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        history = cust.addresses
        for index, address in enumerate(history):
            valid_to = None
            if index + 1 < len(history):
                valid_to = history[index + 1].valid_from - datetime.timedelta(days=1)
            addr_type = _ADDRESS_TYPES[0] if index == 0 else rng.pick(
                seed, _ADDRESS_TYPES, "addr-type", ordinal, index)
            # Only NA ever ran address standardisation; the EU rollout was
            # cancelled and APAC was never in scope.
            std_status = ""
            if cust.region == "NA":
                std_status = rng.weighted_pick(seed, ("STD", "PART", "FAIL"),
                                               (0.81, 0.13, 0.06), "std", ordinal, index)
            elif cust.region == "APAC":
                std_status = "NSCP"
            yield (
                cust.erp_code, index + 1, addr_type,
                address.line1, address.line2, address.locality, address.subdivision,
                address.postal_code, address.country_code,
                address.valid_from, valid_to,
                "Y" if index == len(history) - 1 else "N",
                rng.weighted_pick(seed, ("EXCT", "ROOF", "CENT", "NONE"),
                                  (0.44, 0.3, 0.18, 0.08), "geo", ordinal, index),
                std_status,
            ) + _audit(cfg, "addr", ordinal * 8 + index, address.valid_from, cust.region)


# ---------------------------------------------------------------------------
# CUST_CONTACT
# ---------------------------------------------------------------------------

CUST_CONTACT_COLUMNS = (
    str_col("CUST_CODE", 12, nullable=False),
    int_col("CONTACT_SEQ_NO", nullable=False),
    str_col("FIRST_NM", 40),
    str_col("LAST_NM", 40),
    str_col("EMAIL_ADDR", 120),
    str_col("PHONE_NO", 30, note="stored in the country's own format, never normalised"),
    str_col("ROLE_CD", 8),
    str_col("PREF_LANG_CD", 5),
    str_col("CONSENT_CD", 10),
    date_col("CONSENT_DT"),
    date_col("OPT_OUT_DT"),
    flag_col("ACTIVE_FLG"),
) + AUDIT_COLUMNS

_ROLES = ("AP", "BUYER", "OPS", "EXEC", "STORE")
_LANGS = {"NA": ("en-US", "en-CA", "fr-CA", "es-MX"),
          "EU": ("en-GB", "de-DE", "fr-FR", "nl-NL"),
          "APAC": ("en-AU", "en-NZ", "en-SG", "ja-JP")}


def produce_cust_contact(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        contacts = 1 + rng.stable_hash(seed, "contact-n", ordinal) % (4 if cust.is_dominant else 2)
        for index in range(contacts):
            first, last = text.person_name(seed, ordinal * 7 + index)
            draw = rng.stable_hash(seed, "phone", ordinal, index)
            consent_date = cust.opened_date + datetime.timedelta(
                days=rng.stable_hash(seed, "consent-dt", ordinal, index) % 900)
            opted_out = None
            if cust.region == "EU" and rng.chance(seed, 0.18, "optout", ordinal, index):
                opted_out = consent_date + datetime.timedelta(
                    days=30 + rng.stable_hash(seed, "optout-dt", ordinal, index) % 800)
            yield (
                cust.erp_code, index + 1, first, last,
                text.email_for(first, last, cust.name, ordinal * 7 + index),
                regions.phone_number(cust.country, draw),
                rng.pick(seed, _ROLES, "role", ordinal, index),
                rng.pick(seed, _LANGS[cust.region], "lang", ordinal, index),
                cust.consent_code,
                consent_date if consent_date <= cfg.history_end else cfg.history_end,
                opted_out,
                "N" if opted_out else "Y",
            ) + _audit(cfg, "contact", ordinal * 7 + index, cust.opened_date, cust.region)


# ---------------------------------------------------------------------------
# CUST_CLASSIFICATION  (SCD source)
# ---------------------------------------------------------------------------

CUST_CLASSIFICATION_COLUMNS = (
    str_col("CUST_CODE", 12, nullable=False),
    int_col("CLASS_SEQ_NO", nullable=False),
    str_col("CLASS_SCHEME_CD", 10, note="one scheme per region, never harmonised"),
    str_col("CLASS_CD", 4),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
    str_col("REASON_CD", 8),
    str_col("APPROVED_BY", 30),
) + AUDIT_COLUMNS

_CLASS_SCHEMES = {"NA": "NA-ABC", "EU": "EU-KLASSE", "APAC": "APAC-TIER"}
_CLASS_REASONS = ("ANNUAL", "VOLUME", "CREDIT", "MERGER", "MANUAL")


def produce_cust_classification(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        history = cust.class_history
        for index, (valid_from, class_code) in enumerate(history):
            valid_to = None
            if index + 1 < len(history):
                valid_to = history[index + 1][0] - datetime.timedelta(days=1)
            yield (
                cust.erp_code, index + 1, _CLASS_SCHEMES[cust.region], class_code,
                valid_from, valid_to,
                rng.pick(seed, _CLASS_REASONS, "class-reason", ordinal, index),
                rng.pick(seed, _OPERATORS, "class-appr", ordinal, index),
            ) + _audit(cfg, "class", ordinal * 5 + index, valid_from, cust.region)


# ---------------------------------------------------------------------------
# CUST_CREDIT_PROFILE
# ---------------------------------------------------------------------------

CUST_CREDIT_COLUMNS = (
    str_col("CUST_CODE", 12, nullable=False),
    dec_col("CREDIT_LIMIT_AMT", 15, 2),
    str_col("CREDIT_CURRENCY_CD", 3),
    int_col("RISK_SCORE"),
    str_col("RISK_BAND_CD", 4),
    int_col("DSO_DAYS", note="days sales outstanding at the last review"),
    date_col("LAST_REVIEW_DT"),
    date_col("NEXT_REVIEW_DT"),
    flag_col("ON_HOLD_FLG"),
    str_col("HOLD_REASON_CD", 8),
    str_col("COLLECTOR_ID", 10),
    dec_col("EXPOSURE_AMT", 15, 2, note="uncleared balance, refreshed nightly"),
) + AUDIT_COLUMNS


def produce_cust_credit_profile(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        score = 300 + rng.stable_hash(seed, "risk", ordinal) % 600
        band = "A" if score > 780 else "B" if score > 640 else "C" if score > 480 else "D"
        review = cfg.history_end - datetime.timedelta(
            days=rng.stable_hash(seed, "review", ordinal) % 400)
        on_hold = band == "D" and rng.chance(seed, 0.35, "hold", ordinal)
        yield (
            cust.erp_code, cust.credit_limit, cust.country.currency, score, band,
            18 + rng.stable_hash(seed, "dso", ordinal) % 70,
            review, review + datetime.timedelta(days=365),
            "Y" if on_hold else "N",
            rng.pick(seed, ("PASTDUE", "DISPUTE", "REVIEW"), "hold-reason", ordinal) if on_hold else None,
            "COL%03d" % (rng.stable_hash(seed, "collector", ordinal) % 40),
            round(cust.credit_limit * (rng.stable_hash(seed, "expo", ordinal) % 90) / 100.0, 2),
        ) + _audit(cfg, "credit", ordinal, cust.opened_date, cust.region, review)


# ---------------------------------------------------------------------------
# SUPP_MASTER / SUPP_ADDRESS / SUPP_BANK_ACCOUNT
# ---------------------------------------------------------------------------

SUPP_MASTER_COLUMNS = (
    str_col("SUPP_CODE", 12, nullable=False, note="SUPnnnnnn"),
    str_col("SUPP_NAME", 100, nullable=False),
    str_col("STATUS_CD", 8),
    str_col("REGION_CD", 4),
    str_col("COUNTRY_CD", 2),
    str_col("CURRENCY_CD", 3),
    str_col("TAX_REG_NO", 20),
    str_col("PAYMENT_TERMS_CD", 8),
    str_col("PAYMENT_METHOD_CD", 8),
    str_col("CERTIFICATION_CD", 10),
    str_col("SCORECARD_BAND_CD", 2),
    int_col("LEAD_TIME_DAYS"),
    flag_col("APPROVED_FLG"),
    flag_col("SINGLE_SOURCE_FLG"),
    date_col("ONBOARD_DT"),
    date_col("LAST_AUDIT_DT"),
) + AUDIT_COLUMNS


def produce_supp_master(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        audit_date = supp.onboarded_date + datetime.timedelta(
            days=rng.stable_hash(seed, "supp-audit", ordinal) % 1200)
        yield (
            supp.erp_code, supp.name, supp.status_code, supp.region,
            supp.country.code, supp.country.currency, supp.tax_registration,
            "NET%02d" % supp.payment_terms_days, supp.payment_method,
            supp.certification_code, supp.scorecard_band,
            rng.pick(seed, (3, 5, 7, 14, 21, 30, 60), "supp-lead", ordinal),
            "Y" if supp.status_code == "ACTIVE" else "N",
            "Y" if supp.is_dominant else "N",
            supp.onboarded_date,
            min(audit_date, cfg.history_end),
        ) + _audit(cfg, "supp", ordinal, supp.onboarded_date, supp.region)


SUPP_ADDRESS_COLUMNS = (
    str_col("SUPP_CODE", 12, nullable=False),
    int_col("ADDR_SEQ_NO", nullable=False),
    str_col("ADDR_TYPE_CD", 4, note="REMIT / ORDER / SHIP"),
    str_col("ADDR_LINE_1", 80),
    str_col("ADDR_LINE_2", 80),
    str_col("CITY_NM", 60),
    str_col("STATE_PROV_CD", 20),
    str_col("POSTAL_CD", 12),
    str_col("COUNTRY_CD", 2),
    date_col("VALID_FROM_DT"),
    flag_col("PRIMARY_FLG"),
) + AUDIT_COLUMNS

_SUPP_ADDRESS_TYPES = ("REMT", "ORDR", "SHIP")


def produce_supp_address(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        for index, address in enumerate(supp.addresses):
            yield (
                supp.erp_code, index + 1,
                _SUPP_ADDRESS_TYPES[index % len(_SUPP_ADDRESS_TYPES)],
                address.line1, address.line2, address.locality, address.subdivision,
                address.postal_code, address.country_code, address.valid_from,
                "Y" if index == len(supp.addresses) - 1 else "N",
            ) + _audit(cfg, "supp-addr", ordinal * 4 + index, address.valid_from, supp.region)


SUPP_BANK_COLUMNS = (
    str_col("SUPP_CODE", 12, nullable=False),
    int_col("BANK_SEQ_NO", nullable=False),
    str_col("BANK_NAME", 60),
    str_col("ACCOUNT_MASK", 12, note="only the last four digits survived the 2015 masking project"),
    str_col("ROUTING_REF", 20, note="ABA in NA, IBAN prefix in EU, BSB in APAC"),
    str_col("SWIFT_BIC", 11),
    str_col("CURRENCY_CD", 3),
    flag_col("PRIMARY_FLG"),
    flag_col("VERIFIED_FLG"),
    date_col("VERIFIED_DT"),
) + AUDIT_COLUMNS

_BANK_NAMES = ("First Meridian Bank", "Northgate Trust", "Banque Verlaine",
               "Hanseatic Sparkasse", "Pacific Union Bank", "Kansai Commercial",
               "Southern Cross Bank", "Thistlewood Bank")


def produce_supp_bank_account(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        accounts = 1 + rng.stable_hash(seed, "bank-n", ordinal) % 2
        for index in range(accounts):
            draw = rng.stable_hash(seed, "routing", ordinal, index)
            if supp.region == "NA":
                routing = "%09d" % (draw % 10 ** 9)
            elif supp.region == "EU":
                routing = "%s%02d%014d" % (supp.country.code, draw % 100, draw % 10 ** 14)
            else:
                routing = "%03d-%03d" % (draw % 1000, (draw // 7) % 1000)
            verified = supp.onboarded_date + datetime.timedelta(days=draw % 700)
            yield (
                supp.erp_code, index + 1,
                rng.pick(seed, _BANK_NAMES, "bank-nm", ordinal, index),
                supp.bank_account_masked, routing,
                "%s%s%02dXXX" % (supp.country.code, "BANK"[:2], draw % 100),
                supp.country.currency,
                "Y" if index == 0 else "N",
                "Y" if verified <= cfg.history_end else "N",
                min(verified, cfg.history_end),
            ) + _audit(cfg, "supp-bank", ordinal * 3 + index, supp.onboarded_date, supp.region)


# ---------------------------------------------------------------------------
# PRODUCT_MASTER / PRODUCT_CATEGORY / PRODUCT_HIERARCHY
# ---------------------------------------------------------------------------

PRODUCT_MASTER_COLUMNS = (
    str_col("ITEM_CODE", 14, nullable=False, note="ITM-nnnnnn-A"),
    str_col("ITEM_DESC", 120, nullable=False),
    str_col("CATEGORY_CD", 8),
    str_col("UOM_CD", 4),
    int_col("PACK_SIZE_QTY"),
    dec_col("LIST_PRICE_AMT", 13, 2),
    dec_col("STD_COST_AMT", 13, 4, note="four decimals; the OLTP copy only keeps two"),
    str_col("PRICE_CURRENCY_CD", 3),
    str_col("PRIMARY_SUPP_CD", 12),
    int_col("LEAD_TIME_DAYS"),
    flag_col("CHILLER_FLG"),
    flag_col("HAZMAT_FLG"),
    flag_col("DISCONTINUED_FLG"),
    date_col("DISCONTINUED_DT"),
    dec_col("NET_WEIGHT_KG", 11, 3),
    str_col("COUNTRY_OF_ORIGIN_CD", 2),
    str_col("TARIFF_CD", 12),
) + AUDIT_COLUMNS


def produce_product_master(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("products")):
        prod = entities.product(cfg, ordinal)
        supp = entities.supplier(cfg, prod.supplier_ordinal)
        price, cost = prod.price_on(cfg.history_end)
        yield (
            prod.erp_code, prod.name, prod.category_on(cfg.history_end), prod.uom,
            prod.pack_size, price, round(cost, 4), supp.country.currency,
            supp.erp_code, prod.lead_time_days,
            "Y" if prod.is_chiller else "N",
            "Y" if rng.chance(seed, 0.03, "hazmat", ordinal) else "N",
            "Y" if prod.discontinued_on else "N",
            prod.discontinued_on,
            round(0.05 + (rng.stable_hash(seed, "weight", ordinal) % 30000) / 1000.0, 3),
            supp.country.code,
            "%04d.%02d.%02d" % (rng.stable_hash(seed, "tariff", ordinal) % 10000,
                                rng.stable_hash(seed, "tariff2", ordinal) % 100,
                                rng.stable_hash(seed, "tariff3", ordinal) % 100),
        ) + _audit(cfg, "prod", ordinal, cfg.history_start, supp.region)


PRODUCT_CATEGORY_COLUMNS = (
    str_col("CATEGORY_CD", 8, nullable=False),
    str_col("CATEGORY_NM", 60),
    str_col("PARENT_CATEGORY_CD", 8),
    int_col("LEVEL_NO"),
    str_col("MERCH_GROUP_CD", 8),
    flag_col("ACTIVE_FLG"),
    date_col("EFF_FROM_DT"),
) + AUDIT_COLUMNS

_MERCH_GROUPS = ("AMBIENT", "CHILLED", "CONSUM", "PACKAGE", "SEASON")


def produce_product_category(cfg, ctx):
    seed = cfg.seed
    for index, code in enumerate(text.CATEGORY_CODES):
        yield (
            code, "%s category" % code.replace("-", " ").title(),
            None, 1, rng.pick(seed, _MERCH_GROUPS, "merch", index), "Y",
            cfg.history_start,
        ) + _audit(cfg, "cat", index, cfg.history_start, "NA")
        for sub in range(1, 4):
            child = "%s%d" % (code[:6], sub)
            yield (
                child, "%s subgroup %d" % (code.replace("-", " ").title(), sub),
                code, 2, rng.pick(seed, _MERCH_GROUPS, "merch-sub", index, sub),
                "Y" if sub < 3 else "N", cfg.history_start,
            ) + _audit(cfg, "cat", index * 10 + sub, cfg.history_start, "NA")


PRODUCT_HIERARCHY_COLUMNS = (
    str_col("ITEM_CODE", 14, nullable=False),
    str_col("HIER_TYPE_CD", 8, nullable=False, note="MERCH / PLAN / REPORT - three parallel trees"),
    str_col("LEVEL_1_CD", 8),
    str_col("LEVEL_2_CD", 8),
    str_col("LEVEL_3_CD", 8),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
) + AUDIT_COLUMNS

_HIER_TYPES = ("MERCH", "PLAN", "REPORT")


def produce_product_hierarchy(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("products")):
        prod = entities.product(cfg, ordinal)
        history = prod.category_history
        for index, (valid_from, category) in enumerate(history):
            valid_to = None
            if index + 1 < len(history):
                valid_to = history[index + 1][0] - datetime.timedelta(days=1)
            for hier in _HIER_TYPES:
                yield (
                    prod.erp_code, hier, category[:6] + "0",
                    "%s%d" % (category[:6], 1 + rng.stable_hash(seed, "hier2", ordinal, hier) % 3),
                    category, valid_from, valid_to,
                ) + _audit(cfg, "hier", ordinal * 9 + index, valid_from, "NA")


# ---------------------------------------------------------------------------
# PARTY_XREF - the crosswalk the reconciliation packages live off
# ---------------------------------------------------------------------------

PARTY_XREF_COLUMNS = (
    str_col("XREF_ID", 24, nullable=False),
    str_col("PARTY_TYPE_CD", 8, nullable=False, note="CUST / SUPP / ITEM"),
    str_col("ERP_CODE", 14, nullable=False),
    str_col("TARGET_SYSTEM_CD", 8, nullable=False, note="WWIOLTP"),
    str_col("TARGET_KEY", 20, note="null where the mapping was never created"),
    str_col("MATCH_STATUS_CD", 16, note="CLEAN / MISSING_XREF / RETIRED_TARGET / STALE_CODE / DUPLICATE_XREF"),
    str_col("MATCH_METHOD_CD", 8, note="AUTO / MANUAL / LEGACY"),
    dec_col("MATCH_CONFIDENCE", 5, 2),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
    flag_col("ACTIVE_FLG"),
) + AUDIT_COLUMNS


def _xref_rows(cfg, party_type: str, ordinal: int, erp_code: str, target_key,
               state: str, region: str):
    seed = cfg.seed
    xref_id = "XR%s%08d" % (party_type[:2], ordinal)
    method = rng.weighted_pick(seed, ("AUTO", "MANUAL", "LEGACY"), (0.72, 0.19, 0.09),
                               "xref-method", party_type, ordinal)
    confidence = round(0.72 + (rng.stable_hash(seed, "xref-conf", party_type, ordinal) % 28) / 100.0, 2)
    eff_from = cfg.history_start
    if state == keys.CROSSWALK_MISSING:
        rows = [(xref_id, party_type, erp_code, "WWIOLTP", None, state, method,
                 confidence, eff_from, None, "Y")]
    elif state == keys.CROSSWALK_RETIRED:
        rows = [(xref_id, party_type, erp_code, "WWIOLTP", keys.retired_variant(str(target_key)),
                 state, method, confidence, eff_from, cfg.history_end, "N")]
    elif state == keys.CROSSWALK_STALE:
        rows = [(xref_id, party_type, keys.stale_variant(erp_code), "WWIOLTP", str(target_key),
                 state, "LEGACY", confidence, eff_from, None, "Y")]
    elif state == keys.CROSSWALK_DUPLICATE:
        rows = [(xref_id, party_type, erp_code, "WWIOLTP", str(target_key), state, method,
                 confidence, eff_from, None, "Y"),
                (xref_id + "D", party_type, erp_code, "WWIOLTP", str(target_key), state,
                 "MANUAL", confidence, eff_from, None, "Y")]
    else:
        rows = [(xref_id, party_type, erp_code, "WWIOLTP", str(target_key), state, method,
                 confidence, eff_from, None, "Y")]
    for index, row in enumerate(rows):
        yield row + _audit(cfg, "xref", ordinal * 3 + index, eff_from, region)


def produce_party_xref(cfg, ctx):
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        for row in _xref_rows(cfg, "CUST", ordinal, cust.erp_code, cust.oltp_id,
                              cust.crosswalk, cust.region):
            yield row
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        for row in _xref_rows(cfg, "SUPP", ordinal, supp.erp_code, supp.oltp_id,
                              supp.crosswalk, supp.region):
            yield row
    for ordinal in range(cfg.count("products")):
        prod = entities.product(cfg, ordinal)
        for row in _xref_rows(cfg, "ITEM", ordinal, prod.erp_code, prod.oltp_id,
                              prod.crosswalk, "NA"):
            yield row


# ---------------------------------------------------------------------------
# MDM_MERGE_HISTORY
# ---------------------------------------------------------------------------

MERGE_HISTORY_COLUMNS = (
    int_col("MERGE_ID", nullable=False),
    str_col("PARTY_TYPE_CD", 8),
    str_col("SURVIVOR_CODE", 14),
    str_col("MERGED_CODE", 14),
    str_col("MERGE_REASON_CD", 12),
    dec_col("MATCH_SCORE", 5, 2),
    str_col("MERGED_BY", 30),
    date_col("MERGED_DT"),
    flag_col("REVERSED_FLG", note="a handful of merges were undone by hand"),
) + AUDIT_COLUMNS


def produce_mdm_merge_history(cfg, ctx):
    seed = cfg.seed
    merge_id = 0
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        if cust.duplicate_of < 0:
            continue
        merge_id += 1
        survivor = entities.customer(cfg, cust.duplicate_of)
        merged_on = cfg.history_start + datetime.timedelta(
            days=rng.stable_hash(seed, "merge-dt", ordinal) % max(cfg.history_days, 1))
        yield (
            merge_id, "CUST", survivor.erp_code, cust.erp_code,
            rng.pick(seed, ("NAME_ADDR", "TAXID", "MANUAL", "GROUP"), "merge-reason", ordinal),
            round(0.80 + (rng.stable_hash(seed, "merge-score", ordinal) % 20) / 100.0, 2),
            rng.pick(seed, _OPERATORS, "merge-by", ordinal), merged_on,
            "Y" if rng.chance(seed, 0.04, "merge-rev", ordinal) else "N",
        ) + _audit(cfg, "merge", ordinal, merged_on, cust.region)


# ---------------------------------------------------------------------------


def _spec(name, columns, produce, row_count_key, target, description, tags=()):
    return TableSpec(
        key="oracle.WWI_MDM.%s" % name,
        system=schema.ORACLE,
        schema="WWI_MDM",
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group="oracle_master",
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("CUST_MASTER", CUST_MASTER_COLUMNS, produce_cust_master, "customers",
          "raw.OracleCustomerMaster",
          "Customer book of record, including near-duplicate and exact-duplicate rows.",
          ("dedup", "regional")),
    _spec("CUST_ADDRESS", CUST_ADDRESS_COLUMNS, produce_cust_address, "customers",
          "raw.OracleCustomerAddress",
          "Address versions with effective dates; the SCD Type 2 source for customer geography.",
          ("scd2", "regional")),
    _spec("CUST_CONTACT", CUST_CONTACT_COLUMNS, produce_cust_contact, "customers",
          "raw.OracleCustomerMaster",
          "Contacts with region-specific consent, language and phone formats.",
          ("consent", "regional")),
    _spec("CUST_CLASSIFICATION", CUST_CLASSIFICATION_COLUMNS, produce_cust_classification,
          "customers", "raw.OracleCustomerMaster",
          "Commercial classification history under three unharmonised regional schemes.",
          ("scd2",)),
    _spec("CUST_CREDIT_PROFILE", CUST_CREDIT_COLUMNS, produce_cust_credit_profile, "customers",
          "raw.OracleCustomerMaster",
          "Credit limit, risk band, DSO and hold state as at the last review.",
          ()),
    _spec("SUPP_MASTER", SUPP_MASTER_COLUMNS, produce_supp_master, "suppliers",
          "raw.OracleSupplierMaster",
          "Supplier book of record with scorecard band and certification.",
          ("regional",)),
    _spec("SUPP_ADDRESS", SUPP_ADDRESS_COLUMNS, produce_supp_address, "suppliers",
          "raw.OracleSupplierMaster",
          "Remit-to, order and ship-from addresses per supplier.",
          ("regional",)),
    _spec("SUPP_BANK_ACCOUNT", SUPP_BANK_COLUMNS, produce_supp_bank_account, "suppliers",
          "raw.OracleSupplierMaster",
          "Masked bank details with regionally different routing references.",
          ("regional",)),
    _spec("PRODUCT_MASTER", PRODUCT_MASTER_COLUMNS, produce_product_master, "products",
          "raw.OracleProductMaster",
          "Item master priced as at the snapshot, cost carried to four decimals.",
          ()),
    _spec("PRODUCT_CATEGORY", PRODUCT_CATEGORY_COLUMNS, produce_product_category, "",
          "raw.OracleProductMaster",
          "Two-level category reference, including inactive subgroups.",
          ()),
    _spec("PRODUCT_HIERARCHY", PRODUCT_HIERARCHY_COLUMNS, produce_product_hierarchy, "products",
          "raw.OracleProductMaster",
          "Three parallel product trees with category reassignment over time.",
          ("scd2",)),
    _spec("PARTY_XREF", PARTY_XREF_COLUMNS, produce_party_xref, "customers",
          "raw.OracleCustomerMaster",
          "ERP-to-OLTP crosswalk with a controlled proportion of broken mappings.",
          ("crosswalk", "reconciliation")),
    _spec("MDM_MERGE_HISTORY", MERGE_HISTORY_COLUMNS, produce_mdm_merge_history, "",
          "raw.OracleCustomerMaster",
          "Surviving-record decisions from the 2011 dedup project, some reversed.",
          ("dedup",)),
)
