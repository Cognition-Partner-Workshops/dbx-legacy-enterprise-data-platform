"""Oracle finance and accounts payable: WWI_FIN.

Everything here is denominated twice: once in the transaction currency and
once in the region's reporting currency, converted under that region's own FX
convention. NA converts at the transaction-date spot rate, the EU triangulates
through EUR, and APAC restates at the monthly average - so the same invoice
loaded through three regional feeds produces three different base amounts, and
the reconciliation package has to know which convention applied.

Payments are deliberately not one-to-one with invoices. A payment run settles
several invoices, part-payments exist, and a small number of applications
point at an invoice number that was never extracted.
"""

from __future__ import annotations

import datetime

from .. import documents, entities, keys, regions, rng, schema, timeline
from ..schema import (TableSpec, date_col, dec_col, flag_col, int_col,
                      str_col, ts_col)

AUDIT_COLUMNS = (
    str_col("SRC_SYSTEM_CD", 8, nullable=False),
    str_col("CREATED_BY", 30),
    ts_col("CREATED_TS"),
    str_col("PERIOD_CD", 12, note="the posting period under the region's fiscal calendar"),
)

_CLERKS = ("APCLERK1", "APCLERK2", "GLPOST", "BATCH", "TREASURY", "SHAREDSVC")


def _audit(cfg, ctx, kind: str, ordinal: int, when: datetime.date, region: str):
    seed = cfg.seed
    return (
        regions.SOURCE_SYSTEM_CODE[region],
        rng.pick(seed, _CLERKS, "fin-by", kind, ordinal),
        datetime.datetime(when.year, when.month, when.day,
                          rng.stable_hash(seed, "fin-h", kind, ordinal) % 24,
                          rng.stable_hash(seed, "fin-m", kind, ordinal) % 60,
                          rng.stable_hash(seed, "fin-s", kind, ordinal) % 60),
        ctx.fiscal_period(region, when),
    )


# ---------------------------------------------------------------------------
# AP_INVOICE_HDR / AP_INVOICE_LINE / AP_INVOICE_HOLD
# ---------------------------------------------------------------------------

AP_HDR_COLUMNS = (
    str_col("INVOICE_NO", 20, nullable=False),
    str_col("SUPP_CODE", 12, nullable=False),
    str_col("PO_NO", 14),
    str_col("REGION_CD", 4),
    str_col("CURRENCY_CD", 3),
    date_col("INVOICE_DT"),
    date_col("RECEIVED_DT", note="often days after the invoice date; late arrivals live here"),
    date_col("DUE_DT"),
    date_col("GL_DT"),
    dec_col("GROSS_AMT", 15, 2),
    dec_col("NET_AMT", 15, 2),
    dec_col("TAX_AMT", 15, 2),
    dec_col("WITHHOLDING_AMT", 15, 2),
    dec_col("BASE_AMT", 15, 2, note="restated under the region's own FX convention"),
    str_col("BASE_CURRENCY_CD", 3),
    dec_col("FX_RATE", 18, 8),
    str_col("FX_TYPE_CD", 16),
    str_col("STATUS_CD", 10),
    str_col("HOLD_CD", 10),
    str_col("PAYMENT_TERMS_CD", 8),
    flag_col("MATCHED_FLG", note="three-way match outcome; N is what the DQ package chases"),
    str_col("SUPPLIER_INVOICE_REF", 30, note="the supplier's own number, free format"),
) + AUDIT_COLUMNS


def _ap_amounts(cfg, ctx, doc):
    """Net, tax and gross for an AP invoice, from its own lines."""
    supp = entities.supplier(cfg, doc.supplier_ordinal)
    net = 0.0
    for line in _ap_lines(cfg, doc):
        net += line[4]
    net = round(net, 2)
    tax_code, tax_rate, tax_amount = regions.tax_treatment(doc.region, supp.country, net, False)
    return net, tax_code, tax_rate, tax_amount, round(net + tax_amount, 2)


def _ap_lines(cfg, doc):
    """Deterministic line detail for one AP invoice."""
    seed = cfg.seed
    products = cfg.count("products")
    lines = []
    for line_number in range(1, doc.line_count + 1):
        product_ordinal = rng.stable_hash(seed, "ap-line-prod", doc.ordinal, line_number) % max(products, 1)
        prod = entities.product(cfg, product_ordinal)
        _, unit_cost = prod.price_on(doc.invoiced_on)
        quantity = 1 + rng.stable_hash(seed, "ap-qty", doc.ordinal, line_number) % 240
        amount = round(unit_cost * quantity, 2)
        lines.append((line_number, product_ordinal, quantity, round(unit_cost, 4), amount))
    return lines


def produce_ap_invoice_hdr(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("ap_invoices")):
        doc = documents.ap_invoice(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        net, _tax_code, _rate, tax_amount, gross = _ap_amounts(cfg, ctx, doc)
        withholding = 0.0
        if doc.region == "APAC" and rng.chance(seed, 0.11, "ap-wht", ordinal):
            withholding = round(net * 0.05, 2)
        base = ctx.reporting_amount(doc.region, doc.currency, gross, doc.invoiced_on)
        gl_date = doc.received_on
        # Invoices received after the period closed are posted in the next one.
        if gl_date.day > 25 and rng.chance(seed, 0.3, "ap-gl-slip", ordinal):
            gl_date = timeline.add_months(gl_date.replace(day=1), 1)
        yield (
            doc.invoice_number, supp.erp_code,
            documents.purchase_order(cfg, ctx, doc.po_ordinal).po_number,
            doc.region, doc.currency, doc.invoiced_on, doc.received_on, doc.due_on,
            min(gl_date, cfg.history_end),
            gross, net, tax_amount, withholding, base,
            regions.REPORTING_CURRENCY[doc.region],
            ctx.cross_rate(doc.currency, regions.REPORTING_CURRENCY[doc.region], doc.invoiced_on),
            regions.FX_CONVENTION[doc.region],
            doc.status_code, doc.hold_code or None,
            "NET%02d" % supp.payment_terms_days,
            "N" if doc.hold_code else "Y",
            "%s/%06d" % (supp.erp_code[-4:], rng.stable_hash(seed, "supp-ref", ordinal) % 10 ** 6),
        ) + _audit(cfg, ctx, "ap", ordinal, doc.invoiced_on, doc.region)


AP_LINE_COLUMNS = (
    str_col("INVOICE_NO", 20, nullable=False),
    int_col("INVOICE_LINE_NO", nullable=False),
    str_col("PO_NO", 14),
    int_col("PO_LINE_NO"),
    str_col("ITEM_CODE", 14),
    str_col("LINE_TYPE_CD", 8, note="ITEM / FREIGHT / MISC / TAX"),
    int_col("QTY"),
    dec_col("UNIT_PRICE_AMT", 13, 4),
    dec_col("LINE_AMT", 15, 2),
    dec_col("LINE_TAX_AMT", 15, 2),
    str_col("TAX_CODE", 16),
    str_col("COST_CENTER_CD", 10),
    str_col("GL_ACCOUNT_CD", 20),
    str_col("VARIANCE_CD", 8, note="PRICE / QTY, set where the match failed"),
) + AUDIT_COLUMNS


def produce_ap_invoice_line(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("ap_invoices")):
        doc = documents.ap_invoice(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        po_doc = documents.purchase_order(cfg, ctx, doc.po_ordinal)
        for (line_number, product_ordinal, quantity, unit_cost, amount) in _ap_lines(cfg, doc):
            prod = entities.product(cfg, product_ordinal)
            tax_code, _rate, tax_amount = regions.tax_treatment(
                doc.region, supp.country, amount, False)
            line_type = rng.weighted_pick(seed, ("ITEM", "FREIGHT", "MISC"),
                                          (0.88, 0.08, 0.04), "ap-ltype", ordinal, line_number)
            variance = None
            if rng.chance(seed, 0.06, "ap-var", ordinal, line_number):
                variance = rng.pick(seed, ("PRICE", "QTY"), "ap-var-kind", ordinal, line_number)
            yield (
                doc.invoice_number, line_number, po_doc.po_number, line_number,
                prod.erp_code if line_type == "ITEM" else None, line_type,
                quantity, unit_cost, amount, tax_amount, tax_code,
                "CC%04d" % (rng.stable_hash(seed, "ap-cc", ordinal, line_number) % 400),
                "%03d-%04d-%05d" % (rng.stable_hash(seed, "ap-gl1", ordinal) % 1000,
                                    rng.stable_hash(seed, "ap-gl2", ordinal) % 10000,
                                    50000 + rng.stable_hash(seed, "ap-gl3", product_ordinal) % 9999),
                variance,
            ) + _audit(cfg, ctx, "ap-line", ordinal * 7 + line_number, doc.invoiced_on, doc.region)


AP_HOLD_COLUMNS = (
    str_col("INVOICE_NO", 20, nullable=False),
    int_col("HOLD_SEQ_NO", nullable=False),
    str_col("HOLD_CD", 10),
    str_col("HOLD_REASON_TX", 80),
    date_col("PLACED_DT"),
    date_col("RELEASED_DT"),
    str_col("PLACED_BY", 30),
    str_col("RELEASED_BY", 30),
    flag_col("MANUAL_FLG"),
) + AUDIT_COLUMNS

_HOLD_TEXT = {
    "PRICE": "Unit price above purchase order tolerance",
    "QTY": "Quantity billed exceeds quantity received",
    "TAX": "Tax code not valid for the supplier country",
    "APPROVAL": "Awaiting cost centre owner approval",
}


def produce_ap_invoice_hold(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("ap_invoices")):
        doc = documents.ap_invoice(cfg, ctx, ordinal)
        if not doc.hold_code:
            continue
        placed = doc.received_on
        released = None
        if rng.chance(seed, 0.62, "hold-rel", ordinal):
            released = timeline.business_days_after(
                placed, 1 + rng.stable_hash(seed, "hold-rel-lag", ordinal) % 45)
            released = min(released, cfg.history_end)
        yield (
            doc.invoice_number, 1, doc.hold_code, _HOLD_TEXT.get(doc.hold_code, "Held"),
            placed, released,
            rng.pick(seed, _CLERKS, "hold-by", ordinal),
            rng.pick(seed, _CLERKS, "hold-relby", ordinal) if released else None,
            "Y" if doc.hold_code == "APPROVAL" else "N",
        ) + _audit(cfg, ctx, "hold", ordinal, placed, doc.region)


# ---------------------------------------------------------------------------
# AP_PAYMENT / AP_PAYMENT_APPLY
# ---------------------------------------------------------------------------

PAYMENT_COLUMNS = (
    str_col("PAYMENT_NO", 16, nullable=False),
    str_col("SUPP_CODE", 12),
    str_col("PAYMENT_METHOD_CD", 8),
    str_col("BANK_ACCOUNT_MASK", 12),
    str_col("REGION_CD", 4),
    str_col("CURRENCY_CD", 3),
    date_col("PAYMENT_DT"),
    date_col("VALUE_DT", note="cleared value date, later than the payment date in APAC"),
    dec_col("PAYMENT_AMT", 15, 2),
    dec_col("BASE_AMT", 15, 2),
    str_col("BASE_CURRENCY_CD", 3),
    dec_col("FX_RATE", 18, 8),
    dec_col("DISCOUNT_TAKEN_AMT", 15, 2),
    str_col("PAYMENT_RUN_ID", 14),
    str_col("STATUS_CD", 10),
    flag_col("VOID_FLG"),
    date_col("VOID_DT"),
    str_col("REMITTANCE_REF", 30),
) + AUDIT_COLUMNS


def _payment_supplier(cfg, ordinal: int) -> int:
    from .. import skew
    return skew.pareto_ordinal(cfg.seed, cfg.count("suppliers"), "pay-sup", ordinal)


def produce_ap_payment(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("payments")):
        supplier_ordinal = _payment_supplier(cfg, ordinal)
        supp = entities.supplier(cfg, supplier_ordinal)
        paid_on = ctx.dates.date_for(seed, "pay-when", ordinal)
        # Payment runs are weekly in NA and EU, fortnightly in APAC, so the
        # payment date always lands on the run day, not on the invoice due date.
        run_day = 2 if supp.region != "APAC" else 3
        paid_on = paid_on + datetime.timedelta(days=(run_day - paid_on.weekday()) % 7)
        if paid_on > cfg.history_end:
            paid_on = cfg.history_end
        value_date = paid_on + datetime.timedelta(days=0 if supp.region == "NA" else
                                                  1 if supp.region == "EU" else 3)
        amount = round(100 + rng.stable_hash(seed, "pay-amt", ordinal) % 90000000 / 100.0, 2)
        void = rng.chance(seed, 0.006, "pay-void", ordinal)
        yield (
            keys.erp_payment_number(ordinal), supp.erp_code, supp.payment_method,
            supp.bank_account_masked, supp.region, supp.country.currency,
            paid_on, value_date, amount,
            ctx.reporting_amount(supp.region, supp.country.currency, amount, paid_on),
            regions.REPORTING_CURRENCY[supp.region],
            ctx.cross_rate(supp.country.currency, regions.REPORTING_CURRENCY[supp.region], paid_on),
            round(amount * (rng.stable_hash(seed, "pay-disc", ordinal) % 20) / 1000.0, 2),
            "RUN%s%06d" % (supp.region[:2], rng.stable_hash(seed, "pay-run", ordinal) % 10 ** 6),
            "VOID" if void else rng.weighted_pick(seed, ("CLEARED", "ISSUED", "RECONCILED"),
                                                  (0.74, 0.09, 0.17), "pay-status", ordinal),
            "Y" if void else "N",
            (paid_on + datetime.timedelta(days=4)) if void else None,
            "REM%012d" % (rng.stable_hash(seed, "pay-rem", ordinal) % 10 ** 12),
        ) + _audit(cfg, ctx, "pay", ordinal, paid_on, supp.region)


PAYMENT_APPLY_COLUMNS = (
    str_col("PAYMENT_NO", 16, nullable=False),
    int_col("APPLY_SEQ_NO", nullable=False),
    str_col("INVOICE_NO", 20, note="a small proportion point at invoices that were never extracted"),
    dec_col("APPLIED_AMT", 15, 2),
    dec_col("DISCOUNT_AMT", 15, 2),
    dec_col("WRITEOFF_AMT", 15, 2),
    date_col("APPLIED_DT"),
    flag_col("PARTIAL_FLG"),
    str_col("APPLY_STATUS_CD", 10),
) + AUDIT_COLUMNS


def produce_ap_payment_apply(cfg, ctx):
    """A payment settles one to several invoices; some references are orphans."""
    seed = cfg.seed
    invoices = cfg.count("ap_invoices")
    for ordinal in range(cfg.count("payments")):
        supplier_ordinal = _payment_supplier(cfg, ordinal)
        supp = entities.supplier(cfg, supplier_ordinal)
        paid_on = ctx.dates.date_for(seed, "pay-when", ordinal)
        applications = 1 + rng.stable_hash(seed, "apply-n", ordinal) % 4
        for sequence in range(1, applications + 1):
            orphan = rng.chance(seed, cfg.defect("orphan_reference_rate"),
                                "apply-orphan", ordinal, sequence)
            if orphan:
                invoice_no = keys.erp_ap_invoice_number(
                    invoices + rng.stable_hash(seed, "orphan", ordinal, sequence) % 90000,
                    supplier_ordinal)
            else:
                invoice_ordinal = rng.stable_hash(seed, "apply-inv", ordinal, sequence) % max(invoices, 1)
                invoice_no = documents.ap_invoice(cfg, ctx, invoice_ordinal).invoice_number
            partial = rng.chance(seed, 0.19, "apply-part", ordinal, sequence)
            amount = round(50 + rng.stable_hash(seed, "apply-amt", ordinal, sequence) % 4000000 / 100.0, 2)
            yield (
                keys.erp_payment_number(ordinal), sequence, invoice_no,
                round(amount * (0.4 if partial else 1.0), 2),
                round(amount * (rng.stable_hash(seed, "apply-disc", ordinal, sequence) % 15) / 1000.0, 2),
                round(rng.stable_hash(seed, "apply-wo", ordinal, sequence) % 300 / 100.0, 2)
                if rng.chance(seed, 0.02, "apply-wo-flag", ordinal, sequence) else 0.0,
                paid_on, "Y" if partial else "N",
                "UNMATCHED" if orphan else "APPLIED",
            ) + _audit(cfg, ctx, "apply", ordinal * 5 + sequence, paid_on, supp.region)


AGING_COLUMNS = (
    date_col("SNAPSHOT_DT", nullable=False),
    str_col("SUPP_CODE", 12, nullable=False),
    str_col("REGION_CD", 4),
    str_col("CURRENCY_CD", 3),
    dec_col("CURRENT_AMT", 15, 2),
    dec_col("BUCKET_1_30_AMT", 15, 2),
    dec_col("BUCKET_31_60_AMT", 15, 2),
    dec_col("BUCKET_61_90_AMT", 15, 2),
    dec_col("BUCKET_OVER_90_AMT", 15, 2),
    dec_col("TOTAL_AMT", 15, 2),
    int_col("OPEN_INVOICE_QTY"),
    str_col("AGING_BASIS_CD", 10, note="DUE in NA and EU, INVOICE in APAC"),
) + AUDIT_COLUMNS


def produce_ap_aging_snapshot(cfg, ctx):
    """Month-end aging per supplier. The APAC ledger ages on invoice date."""
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        when = cfg.history_start.replace(day=1)
        sequence = 0
        while when <= cfg.history_end:
            sequence += 1
            if when >= supp.onboarded_date:
                buckets = [round(rng.stable_hash(seed, "age", ordinal, sequence, index) % 900000 / 100.0, 2)
                           for index in range(5)]
                yield (
                    when, supp.erp_code, supp.region, supp.country.currency,
                    buckets[0], buckets[1], buckets[2], buckets[3], buckets[4],
                    round(sum(buckets), 2),
                    rng.stable_hash(seed, "age-cnt", ordinal, sequence) % 60,
                    "INVOICE" if supp.region == "APAC" else "DUE",
                ) + _audit(cfg, ctx, "aging", ordinal * 90 + sequence, when, supp.region)
            when = timeline.add_months(when, 1)


# ---------------------------------------------------------------------------
# General ledger
# ---------------------------------------------------------------------------

GL_ACCOUNT_COLUMNS = (
    str_col("GL_ACCOUNT_CD", 20, nullable=False),
    str_col("ACCOUNT_NM", 60),
    str_col("ACCOUNT_TYPE_CD", 8, note="ASSET / LIAB / EQTY / REV / EXP"),
    str_col("PARENT_ACCOUNT_CD", 20),
    flag_col("POSTING_ALLOWED_FLG"),
    flag_col("INTERCOMPANY_FLG"),
    str_col("REGION_CD", 4, note="three regional charts of accounts, never merged"),
    flag_col("ACTIVE_FLG"),
)

_ACCOUNT_TYPES = ("ASSET", "LIAB", "EQTY", "REV", "EXP")
_ACCOUNT_NAMES = ("Trade receivables", "Trade payables", "Inventory", "Freight in",
                  "Freight out", "Product revenue", "Cost of sales", "Rebate accrual",
                  "FX gain or loss", "Rounding difference", "Accrued liabilities",
                  "Bank clearing", "Intercompany balance", "Duty and tariffs")


def produce_gl_account(cfg, ctx):
    seed = cfg.seed
    for region in regions.REGIONS:
        for index, name in enumerate(_ACCOUNT_NAMES):
            code = "%s-%05d" % (region[:2], 10000 + index * 10)
            yield (code, name, _ACCOUNT_TYPES[index % len(_ACCOUNT_TYPES)],
                   "%s-%05d" % (region[:2], 10000),
                   "Y" if index else "N",
                   "Y" if "Intercompany" in name else "N",
                   region,
                   "N" if rng.chance(seed, 0.05, "gl-inactive", region, index) else "Y")


GL_JOURNAL_HDR_COLUMNS = (
    str_col("JOURNAL_NO", 18, nullable=False),
    str_col("JOURNAL_SOURCE_CD", 10, note="AP / AR / INV / MANUAL / INTERFACE"),
    str_col("JOURNAL_CATEGORY_CD", 12),
    str_col("REGION_CD", 4),
    str_col("PERIOD_CD", 12),
    date_col("JOURNAL_DT"),
    date_col("POSTED_DT"),
    str_col("STATUS_CD", 10),
    dec_col("CONTROL_TOTAL_AMT", 17, 2),
    str_col("CURRENCY_CD", 3),
    flag_col("REVERSAL_FLG"),
    str_col("REVERSES_JOURNAL_NO", 18),
    str_col("PREPARED_BY", 30),
    str_col("APPROVED_BY", 30),
) + AUDIT_COLUMNS

_JOURNAL_SOURCES = ("AP", "AR", "INV", "MANUAL", "INTERFACE")
_JOURNAL_CATEGORIES = ("PURCHASE", "PAYMENT", "ACCRUAL", "REVALUE", "ADJUST", "CLOSE")


def produce_gl_journal_hdr(cfg, ctx):
    seed = cfg.seed
    headers = max(cfg.count("gl_journal_lines") // 6, 1)
    for ordinal in range(headers):
        region = regions.REGIONS[rng.stable_hash(seed, "gl-region", ordinal) % 3]
        journal_date = ctx.dates.date_for(seed, "gl-when", ordinal)
        posted = timeline.business_days_after(
            journal_date, rng.weighted_pick(seed, (0, 1, 2, 6), (0.5, 0.28, 0.15, 0.07),
                                            "gl-post-lag", ordinal))
        reversal = rng.chance(seed, 0.07, "gl-rev", ordinal)
        yield (
            keys.erp_journal_number(ordinal, journal_date.year, journal_date.month),
            rng.weighted_pick(seed, _JOURNAL_SOURCES, (0.34, 0.21, 0.18, 0.12, 0.15),
                              "gl-src", ordinal),
            rng.pick(seed, _JOURNAL_CATEGORIES, "gl-cat", ordinal),
            region, ctx.fiscal_period(region, journal_date), journal_date,
            min(posted, cfg.history_end),
            rng.weighted_pick(seed, ("POSTED", "UNPOSTED", "ERROR"), (0.93, 0.05, 0.02),
                              "gl-status", ordinal),
            round(rng.stable_hash(seed, "gl-ctrl", ordinal) % 900000000 / 100.0, 2),
            regions.REPORTING_CURRENCY[region],
            "Y" if reversal else "N",
            keys.erp_journal_number(max(ordinal - 1, 0), journal_date.year, journal_date.month)
            if reversal else None,
            rng.pick(seed, _CLERKS, "gl-prep", ordinal),
            rng.pick(seed, _CLERKS, "gl-appr", ordinal),
        ) + _audit(cfg, ctx, "gl-hdr", ordinal, journal_date, region)


GL_JOURNAL_LINE_COLUMNS = (
    str_col("JOURNAL_NO", 18, nullable=False),
    int_col("JOURNAL_LINE_NO", nullable=False),
    str_col("GL_ACCOUNT_CD", 20),
    str_col("COST_CENTER_CD", 10),
    str_col("REGION_CD", 4),
    dec_col("DEBIT_AMT", 17, 2),
    dec_col("CREDIT_AMT", 17, 2),
    str_col("CURRENCY_CD", 3),
    dec_col("BASE_DEBIT_AMT", 17, 2),
    dec_col("BASE_CREDIT_AMT", 17, 2),
    str_col("LINE_DESC_TX", 100),
    str_col("REFERENCE_NO", 20, note="the AP invoice, payment or shipment that drove the posting"),
    date_col("EFFECTIVE_DT"),
) + AUDIT_COLUMNS


def produce_gl_journal_line(cfg, ctx):
    """Balanced pairs of lines. Every journal debits and credits the same amount."""
    seed = cfg.seed
    total = cfg.count("gl_journal_lines")
    headers = max(total // 6, 1)
    emitted = 0
    for header_ordinal in range(headers):
        region = regions.REGIONS[rng.stable_hash(seed, "gl-region", header_ordinal) % 3]
        journal_date = ctx.dates.date_for(seed, "gl-when", header_ordinal)
        journal_no = keys.erp_journal_number(header_ordinal, journal_date.year, journal_date.month)
        pairs = 1 + rng.stable_hash(seed, "gl-pairs", header_ordinal) % 3
        for pair in range(pairs):
            if emitted >= total:
                return
            amount = round(10 + rng.stable_hash(seed, "gl-amt", header_ordinal, pair) % 9000000 / 100.0, 2)
            currency = regions.REPORTING_CURRENCY[region]
            for side in (0, 1):
                line_number = pair * 2 + side + 1
                account_index = rng.stable_hash(seed, "gl-acct", header_ordinal, pair, side) % len(_ACCOUNT_NAMES)
                yield (
                    journal_no, line_number,
                    "%s-%05d" % (region[:2], 10000 + account_index * 10),
                    "CC%04d" % (rng.stable_hash(seed, "gl-cc", header_ordinal, pair) % 400),
                    region,
                    amount if side == 0 else 0.0,
                    0.0 if side == 0 else amount,
                    currency,
                    amount if side == 0 else 0.0,
                    0.0 if side == 0 else amount,
                    "%s posting for %s" % (_ACCOUNT_NAMES[account_index], journal_no),
                    keys.erp_ap_invoice_number(
                        rng.stable_hash(seed, "gl-ref", header_ordinal, pair) % max(cfg.count("ap_invoices"), 1),
                        rng.stable_hash(seed, "gl-ref-sup", header_ordinal, pair) % max(cfg.count("suppliers"), 1)),
                    journal_date,
                ) + _audit(cfg, ctx, "gl-line", header_ordinal * 17 + line_number, journal_date, region)
                emitted += 1


COST_CENTER_COLUMNS = (
    str_col("COST_CENTER_CD", 10, nullable=False),
    str_col("COST_CENTER_NM", 60),
    str_col("REGION_CD", 4),
    str_col("SITE_CD", 8),
    str_col("MANAGER_ID", 10),
    str_col("PARENT_CC_CD", 10),
    flag_col("ACTIVE_FLG"),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
)

_CC_FUNCTIONS = ("Sales", "Warehouse", "Transport", "Finance", "Procurement",
                 "Customer service", "IT", "Facilities")


def produce_cost_center(cfg, ctx):
    seed = cfg.seed
    for index in range(400):
        region = regions.REGIONS[index % 3]
        closed = rng.chance(seed, 0.08, "cc-closed", index)
        yield (
            "CC%04d" % index,
            "%s %s %02d" % (region, rng.pick(seed, _CC_FUNCTIONS, "cc-fn", index), index % 40),
            region,
            rng.pick(seed, regions.SITES[region], "cc-site", index),
            "EMP%05d" % (rng.stable_hash(seed, "cc-mgr", index) % max(cfg.count("employees"), 1)),
            "CC%04d" % (index % 20),
            "N" if closed else "Y",
            cfg.history_start,
            cfg.history_end if closed else None,
        )


PAYMENT_TERMS_COLUMNS = (
    str_col("PAYMENT_TERMS_CD", 8, nullable=False),
    str_col("TERMS_NM", 40),
    int_col("NET_DAYS"),
    int_col("DISCOUNT_DAYS"),
    dec_col("DISCOUNT_PCT", 6, 3),
    str_col("DUE_BASIS_CD", 10, note="INVOICE / EOM - the EU ledger uses end of month"),
    flag_col("ACTIVE_FLG"),
)


def produce_payment_terms(cfg, ctx):
    for days in (0, 7, 14, 21, 30, 45, 60, 90, 120):
        for basis in ("INVOICE", "EOM"):
            code = "NET%02d" % days if basis == "INVOICE" else "EOM%02d" % days
            yield (code, "Net %d days from %s" % (days, basis.lower()), days,
                   10 if days >= 30 else 0, 2.0 if days >= 30 else 0.0, basis, "Y")


TAX_RATE_COLUMNS = (
    str_col("TAX_CODE", 16, nullable=False),
    str_col("TAX_REGIME_CD", 12),
    str_col("COUNTRY_CD", 2),
    str_col("JURISDICTION_CD", 12),
    dec_col("RATE_PCT", 7, 4),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
    flag_col("RECOVERABLE_FLG"),
    flag_col("REVERSE_CHARGE_FLG"),
    str_col("ROUNDING_RULE_CD", 8),
)


def produce_tax_rate(cfg, ctx):
    """Tax codes per country, plus the EU reverse-charge and APAC truncation cases."""
    for region in regions.REGIONS:
        for country in regions.countries(region):
            base_code = "%s-%s" % (country.tax_label, country.code)
            rounding = "TRUNC" if region == "APAC" else "HALF_UP"
            yield (base_code, country.tax_label, country.code,
                   "%s-STD" % country.code, country.tax_rate,
                   cfg.history_start, None, "Y", "N", rounding)
            if region == "EU":
                yield ("VAT-RC-%s" % country.code, "VAT", country.code,
                       "%s-RC" % country.code, 0.0, cfg.history_start, None, "Y", "Y", rounding)
                yield ("VAT-%s-STD" % country.code, "VAT", country.code,
                       "%s-STD" % country.code, country.tax_rate,
                       cfg.history_start, None, "Y", "N", rounding)
                yield ("VAT-%s-RED" % country.code, "VAT", country.code,
                       "%s-RED" % country.code, round(country.tax_rate / 2, 4),
                       cfg.history_start, None, "Y", "N", rounding)
            if region == "APAC":
                yield ("%s%s" % (country.tax_label, country.code), country.tax_label,
                       country.code, "%s-GST" % country.code, country.tax_rate,
                       cfg.history_start, None, "Y", "N", "TRUNC")
            if region == "NA":
                for suffix, uplift in (("CTY", 0.01), ("CNTY", 0.005)):
                    yield ("%s-%s-%s" % (country.tax_label, country.code, suffix),
                           country.tax_label, country.code,
                           "%s-%s" % (country.code, suffix),
                           round(country.tax_rate + uplift, 4),
                           cfg.history_start, None, "Y", "N", "HALF_UP")


def _spec(name, columns, produce, row_count_key, target, description, tags=()):
    return TableSpec(
        key="oracle.WWI_FIN.%s" % name,
        system=schema.ORACLE,
        schema="WWI_FIN",
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group="oracle_finance",
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("AP_INVOICE_HDR", AP_HDR_COLUMNS, produce_ap_invoice_hdr, "ap_invoices",
          "raw.OracleApInvoiceHdr",
          "AP invoice headers restated under three different FX conventions.",
          ("fx", "regional", "reconciliation")),
    _spec("AP_INVOICE_LINE", AP_LINE_COLUMNS, produce_ap_invoice_line, "ap_invoice_lines",
          "raw.OracleApInvoiceLine",
          "AP invoice lines with match variances and GL coding.", ("reconciliation",)),
    _spec("AP_INVOICE_HOLD", AP_HOLD_COLUMNS, produce_ap_invoice_hold, "",
          "raw.OracleApInvoiceHdr",
          "Holds placed on invoices that failed the three-way match.", ("dataquality",)),
    _spec("AP_PAYMENT", PAYMENT_COLUMNS, produce_ap_payment, "payments",
          "raw.OracleApPayment",
          "Payment runs on the regional run day, with voids and discounts taken.",
          ("fx", "regional")),
    _spec("AP_PAYMENT_APPLY", PAYMENT_APPLY_COLUMNS, produce_ap_payment_apply, "payments",
          "raw.OracleApPayment",
          "Payment-to-invoice applications including orphaned invoice references.",
          ("dataquality", "reconciliation")),
    _spec("AP_AGING_SNAPSHOT", AGING_COLUMNS, produce_ap_aging_snapshot, "",
          "raw.OracleApInvoiceHdr",
          "Month-end aging per supplier; APAC ages on invoice date, not due date.",
          ("regional",)),
    _spec("GL_ACCOUNT", GL_ACCOUNT_COLUMNS, produce_gl_account, "", "raw.OracleGlJournalLine",
          "Three regional charts of accounts that were never merged.", ("regional",)),
    _spec("GL_JOURNAL_HDR", GL_JOURNAL_HDR_COLUMNS, produce_gl_journal_hdr, "",
          "raw.OracleGlJournalLine",
          "Journal headers with control totals, reversals and posting lag.", ()),
    _spec("GL_JOURNAL_LINE", GL_JOURNAL_LINE_COLUMNS, produce_gl_journal_line,
          "gl_journal_lines", "raw.OracleGlJournalLine",
          "Balanced journal lines referencing the AP documents that drove them.", ()),
    _spec("COST_CENTER", COST_CENTER_COLUMNS, produce_cost_center, "", "raw.OracleCostCenter",
          "Cost centres per region and site, including closed ones.", ()),
    _spec("PAYMENT_TERMS", PAYMENT_TERMS_COLUMNS, produce_payment_terms, "",
          "raw.OraclePaymentTerms",
          "Payment terms on both invoice-date and end-of-month bases.", ("regional",)),
    _spec("TAX_RATE", TAX_RATE_COLUMNS, produce_tax_rate, "", "raw.OracleTaxRate",
          "Tax codes per country including EU reverse charge and NA local uplifts.",
          ("regional",)),
)
