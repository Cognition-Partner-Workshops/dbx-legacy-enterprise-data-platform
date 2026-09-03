"""Oracle procurement: WWI_PROC.

Requisition to receipt. The header/line split is the ERP's, including its
habit of holding the amount on the header *and* on the lines and letting them
drift when a change order was applied to only one of them - the AP matching
package exists to find those.

Receipts are deliberately not one-to-one with purchase orders: some lines are
received short, some in two deliveries, and a small number arrive against a
purchase order that was cancelled in the meantime.
"""

from __future__ import annotations

import datetime

from .. import documents, entities, keys, rng, schema, timeline
from ..schema import (TableSpec, date_col, dec_col, flag_col, int_col,
                      str_col, ts_col)

AUDIT_COLUMNS = (
    str_col("SRC_SYSTEM_CD", 8, nullable=False),
    str_col("CREATED_BY", 30),
    ts_col("CREATED_TS"),
    ts_col("LAST_UPD_TS"),
)

_BUYERS = ("KTANAKA", "MOTOOLE", "PBAUER", "LGARZA", "SNAIDU", "BATCH")


def _audit(cfg, kind: str, ordinal: int, when: datetime.date, region: str):
    from .. import regions as region_module
    seed = cfg.seed
    created = datetime.datetime(when.year, when.month, when.day,
                                7 + rng.stable_hash(seed, "h", kind, ordinal) % 11,
                                rng.stable_hash(seed, "m", kind, ordinal) % 60,
                                rng.stable_hash(seed, "s", kind, ordinal) % 60)
    updated = created + datetime.timedelta(
        seconds=rng.stable_hash(seed, "upd", kind, ordinal) % 900000)
    return (region_module.SOURCE_SYSTEM_CODE[region],
            rng.pick(seed, _BUYERS, "by", kind, ordinal), created, updated)


# ---------------------------------------------------------------------------
# REQUISITION_HDR / REQUISITION_LINE
# ---------------------------------------------------------------------------

REQ_HDR_COLUMNS = (
    str_col("REQ_NO", 14, nullable=False),
    str_col("REQUESTER_ID", 10),
    str_col("COST_CENTER_CD", 10),
    str_col("REGION_CD", 4),
    date_col("REQ_DT"),
    date_col("NEED_BY_DT"),
    str_col("STATUS_CD", 8, note="DRAFT / SUBMIT / APPR / REJ / CONV"),
    str_col("APPROVER_ID", 10),
    date_col("APPROVED_DT"),
    dec_col("EST_TOTAL_AMT", 15, 2),
    str_col("CURRENCY_CD", 3),
    str_col("PO_NO", 14, note="populated when the requisition converted to a PO"),
    str_col("JUSTIFICATION_TX", 120),
) + AUDIT_COLUMNS

_REQ_STATUS = ("DRAFT", "SUBMIT", "APPR", "REJ", "CONV")
_JUSTIFICATIONS = ("Replenishment", "Stock-out cover", "New line launch",
                   "Seasonal build", "Promotion support", "Quality replacement")


def produce_requisition_hdr(cfg, ctx):
    """One requisition per purchase order, plus the ones that never converted."""
    seed = cfg.seed
    total = cfg.count("purchase_orders")
    for ordinal in range(total):
        doc = documents.purchase_order(cfg, ctx, ordinal)
        converted = not rng.chance(seed, 0.12, "req-noconv", ordinal)
        req_date = doc.raised_on - datetime.timedelta(
            days=1 + rng.stable_hash(seed, "req-lead", ordinal) % 12)
        status = "CONV" if converted else rng.weighted_pick(
            seed, _REQ_STATUS[:4], (0.18, 0.34, 0.28, 0.20), "req-status", ordinal)
        yield (
            "REQ%08d" % ordinal,
            "EMP%05d" % doc.buyer_ordinal,
            "CC%04d" % (rng.stable_hash(seed, "req-cc", ordinal) % 400),
            doc.region, req_date,
            timeline.business_days_after(req_date, 3 + rng.stable_hash(seed, "req-need", ordinal) % 20),
            status,
            "EMP%05d" % (rng.stable_hash(seed, "req-appr", ordinal) % max(cfg.count("employees"), 1)),
            doc.approved_on if status in ("APPR", "CONV") else None,
            round(500 + rng.stable_hash(seed, "req-amt", ordinal) % 400000 / 10.0, 2),
            doc.currency,
            doc.po_number if converted else None,
            rng.pick(seed, _JUSTIFICATIONS, "req-just", ordinal),
        ) + _audit(cfg, "req", ordinal, req_date, doc.region)


REQ_LINE_COLUMNS = (
    str_col("REQ_NO", 14, nullable=False),
    int_col("REQ_LINE_NO", nullable=False),
    str_col("ITEM_CODE", 14),
    str_col("ITEM_DESC_TX", 120, note="free text on lines raised against a non-catalogue item"),
    int_col("REQ_QTY"),
    str_col("UOM_CD", 4),
    dec_col("EST_UNIT_PRICE", 13, 4),
    dec_col("EST_LINE_AMT", 15, 2),
    str_col("SUGGESTED_SUPP_CD", 12),
    date_col("NEED_BY_DT"),
) + AUDIT_COLUMNS


def produce_requisition_line(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("purchase_orders")):
        doc = documents.purchase_order(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        for (line_number, product_ordinal, quantity, unit_cost,
             line_amount, _received, promised) in documents.purchase_order_lines(cfg, doc):
            prod = entities.product(cfg, product_ordinal)
            # Non-catalogue lines carry a description and no item code.
            non_catalogue = rng.chance(seed, 0.04, "req-nc", ordinal, line_number)
            yield (
                "REQ%08d" % ordinal, line_number,
                None if non_catalogue else prod.erp_code,
                prod.name if non_catalogue else None,
                quantity, prod.uom, unit_cost, line_amount,
                supp.erp_code, promised,
            ) + _audit(cfg, "req-line", ordinal * 7 + line_number, doc.raised_on, doc.region)


# ---------------------------------------------------------------------------
# PURCHASE_ORDER_HDR / PURCHASE_ORDER_LINE / PO_CHANGE_ORDER
# ---------------------------------------------------------------------------

PO_HDR_COLUMNS = (
    str_col("PO_NO", 14, nullable=False),
    str_col("SUPP_CODE", 12, nullable=False),
    str_col("BUYER_ID", 10),
    str_col("REGION_CD", 4),
    str_col("CURRENCY_CD", 3),
    date_col("PO_DT"),
    date_col("APPROVED_DT"),
    str_col("STATUS_CD", 8),
    str_col("INCOTERM_CD", 4),
    str_col("SHIP_TO_SITE_CD", 8),
    dec_col("PO_TOTAL_AMT", 15, 2, note="header total; change orders do not always update it"),
    dec_col("PO_TAX_AMT", 15, 2),
    dec_col("PO_TOTAL_BASE_AMT", 15, 2, note="restated into the regional reporting currency"),
    str_col("BASE_CURRENCY_CD", 3),
    str_col("PAYMENT_TERMS_CD", 8),
    flag_col("BLANKET_FLG"),
    str_col("CONTRACT_NO", 14),
) + AUDIT_COLUMNS


def _po_totals(cfg, doc):
    net = 0.0
    for (_line, _prod, _qty, _cost, line_amount, _recv, _prom) in \
            documents.purchase_order_lines(cfg, doc):
        net += line_amount
    return round(net, 2)


def produce_purchase_order_hdr(cfg, ctx):
    seed = cfg.seed
    from .. import regions as region_module
    for ordinal in range(cfg.count("purchase_orders")):
        doc = documents.purchase_order(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        net = _po_totals(cfg, doc)
        # The header total drifts on the small number of orders that were
        # changed after approval without the total being recalculated.
        stated = net
        if rng.chance(seed, 0.015, "po-drift", ordinal):
            stated = round(net * (1.0 + (rng.stable_hash(seed, "po-drift-amt", ordinal) % 11 - 5) / 100.0), 2)
        tax = round(net * supp.country.tax_rate, 2)
        yield (
            doc.po_number, supp.erp_code, "EMP%05d" % doc.buyer_ordinal, doc.region,
            doc.currency, doc.raised_on, doc.approved_on, doc.status_code, doc.incoterm,
            rng.pick(seed, region_module.SITES[doc.region], "po-site", ordinal),
            stated, tax,
            ctx.reporting_amount(doc.region, doc.currency, stated, doc.raised_on),
            region_module.REPORTING_CURRENCY[doc.region],
            "NET%02d" % supp.payment_terms_days,
            "Y" if rng.chance(seed, 0.08, "po-blanket", ordinal) else "N",
            "VC%07d" % (doc.supplier_ordinal) if rng.chance(seed, 0.3, "po-contract", ordinal) else None,
        ) + _audit(cfg, "po", ordinal, doc.raised_on, doc.region)


PO_LINE_COLUMNS = (
    str_col("PO_NO", 14, nullable=False),
    int_col("PO_LINE_NO", nullable=False),
    str_col("ITEM_CODE", 14),
    int_col("ORDER_QTY"),
    int_col("RECEIVED_QTY"),
    int_col("CANCELLED_QTY"),
    str_col("UOM_CD", 4),
    dec_col("UNIT_PRICE_AMT", 13, 4),
    dec_col("LINE_NET_AMT", 15, 2),
    dec_col("LINE_TAX_AMT", 15, 2),
    str_col("TAX_CODE", 16),
    date_col("PROMISED_DT"),
    date_col("LAST_RECEIPT_DT"),
    str_col("LINE_STATUS_CD", 8),
    str_col("CHARGE_ACCOUNT_CD", 20, note="GL account string, concatenated segments"),
) + AUDIT_COLUMNS


def produce_purchase_order_line(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("purchase_orders")):
        doc = documents.purchase_order(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        for (line_number, product_ordinal, quantity, unit_cost, line_amount,
             received, promised) in documents.purchase_order_lines(cfg, doc):
            prod = entities.product(cfg, product_ordinal)
            cancelled = quantity - received if doc.status_code == "CANC" else 0
            status = "CLOSED" if received >= quantity else "OPEN"
            yield (
                doc.po_number, line_number, prod.erp_code, quantity, received, cancelled,
                prod.uom, unit_cost, line_amount,
                round(line_amount * supp.country.tax_rate, 2),
                "%s-%s" % (supp.country.tax_label, supp.country.code),
                promised,
                promised if received else None,
                status,
                "%03d-%04d-%05d-%03d" % (rng.stable_hash(seed, "acct1", ordinal) % 1000,
                                         rng.stable_hash(seed, "acct2", ordinal) % 10000,
                                         50000 + rng.stable_hash(seed, "acct3", product_ordinal) % 9999,
                                         rng.stable_hash(seed, "acct4", ordinal) % 1000),
            ) + _audit(cfg, "po-line", ordinal * 11 + line_number, doc.raised_on, doc.region)


PO_CHANGE_COLUMNS = (
    str_col("PO_NO", 14, nullable=False),
    int_col("CHANGE_SEQ_NO", nullable=False),
    int_col("PO_LINE_NO"),
    str_col("CHANGE_TYPE_CD", 10, note="QTY / PRICE / DATE / CANCEL"),
    str_col("OLD_VALUE_TX", 40),
    str_col("NEW_VALUE_TX", 40),
    str_col("REASON_CD", 10),
    str_col("CHANGED_BY", 30),
    date_col("CHANGED_DT"),
    flag_col("SUPPLIER_ACK_FLG"),
) + AUDIT_COLUMNS

_CHANGE_TYPES = ("QTY", "PRICE", "DATE", "CANCEL")
_CHANGE_REASONS = ("SUPPREQ", "DEMAND", "PRICEUP", "SHORTSHIP", "ERROR")


def produce_po_change_order(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("purchase_orders")):
        if not rng.chance(seed, 0.14, "po-chg", ordinal):
            continue
        doc = documents.purchase_order(cfg, ctx, ordinal)
        changes = 1 + rng.stable_hash(seed, "po-chg-n", ordinal) % 3
        for sequence in range(1, changes + 1):
            change_type = rng.pick(seed, _CHANGE_TYPES, "po-chg-type", ordinal, sequence)
            old_value = "%d" % (10 + rng.stable_hash(seed, "chg-old", ordinal, sequence) % 500)
            new_value = "%d" % (10 + rng.stable_hash(seed, "chg-new", ordinal, sequence) % 500)
            changed_on = timeline.business_days_after(
                doc.approved_on, 1 + rng.stable_hash(seed, "chg-when", ordinal, sequence) % 25)
            yield (
                doc.po_number, sequence,
                1 + rng.stable_hash(seed, "chg-line", ordinal, sequence) % max(doc.line_count, 1),
                change_type, old_value, new_value,
                rng.pick(seed, _CHANGE_REASONS, "chg-reason", ordinal, sequence),
                rng.pick(seed, _BUYERS, "chg-by", ordinal, sequence),
                min(changed_on, cfg.history_end),
                "Y" if rng.chance(seed, 0.71, "chg-ack", ordinal, sequence) else "N",
            ) + _audit(cfg, "po-chg", ordinal * 5 + sequence, doc.raised_on, doc.region)


# ---------------------------------------------------------------------------
# PO_RECEIPT_HDR / PO_RECEIPT_LINE / GOODS_RETURN
# ---------------------------------------------------------------------------

RECEIPT_HDR_COLUMNS = (
    str_col("RECEIPT_NO", 14, nullable=False),
    str_col("PO_NO", 14),
    str_col("SUPP_CODE", 12),
    str_col("SITE_CD", 8),
    date_col("RECEIPT_DT"),
    ts_col("RECEIVED_TS", note="scanner time; occasionally earlier than the receipt date"),
    str_col("PACKING_SLIP_NO", 20),
    str_col("CARRIER_CD", 8),
    str_col("RECEIVER_ID", 10),
    str_col("STATUS_CD", 8),
    flag_col("INSPECTION_REQ_FLG"),
) + AUDIT_COLUMNS

_CARRIERS = ("MERIDIAN", "NORTHWAY", "EUROLINK", "PACRIM", "SWIFTFRT", "LOCALDEL")


def produce_po_receipt_hdr(cfg, ctx):
    seed = cfg.seed
    from .. import regions as region_module
    receipts = cfg.count("receipts")
    for ordinal in range(receipts):
        po_ordinal = ordinal % max(cfg.count("purchase_orders"), 1)
        doc = documents.purchase_order(cfg, ctx, po_ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        receipt_date = timeline.business_days_after(
            doc.approved_on, 1 + rng.stable_hash(seed, "recv-lag", ordinal) % 40)
        if receipt_date > cfg.history_end:
            receipt_date = cfg.history_end
        scanned = datetime.datetime(receipt_date.year, receipt_date.month, receipt_date.day,
                                    6 + rng.stable_hash(seed, "recv-h", ordinal) % 14,
                                    rng.stable_hash(seed, "recv-m", ordinal) % 60, 0)
        scanned += datetime.timedelta(
            seconds=timeline.out_of_order_shift(seed, cfg.defect("out_of_order_rate"),
                                                "recv-ooo", ordinal))
        yield (
            keys.erp_receipt_number(ordinal), doc.po_number, supp.erp_code,
            rng.pick(seed, region_module.SITES[doc.region], "recv-site", ordinal),
            receipt_date, scanned,
            "PS%09d" % (rng.stable_hash(seed, "slip", ordinal) % 10 ** 9),
            rng.pick(seed, _CARRIERS, "recv-carrier", ordinal),
            "EMP%05d" % (rng.stable_hash(seed, "receiver", ordinal) % max(cfg.count("employees"), 1)),
            rng.weighted_pick(seed, ("RECEIVED", "PARTIAL", "INSPECT"), (0.82, 0.13, 0.05),
                              "recv-status", ordinal),
            "Y" if rng.chance(seed, 0.09, "recv-insp", ordinal) else "N",
        ) + _audit(cfg, "recv", ordinal, receipt_date, doc.region)


RECEIPT_LINE_COLUMNS = (
    str_col("RECEIPT_NO", 14, nullable=False),
    int_col("RECEIPT_LINE_NO", nullable=False),
    str_col("PO_NO", 14),
    int_col("PO_LINE_NO"),
    str_col("ITEM_CODE", 14),
    int_col("RECEIVED_QTY"),
    int_col("ACCEPTED_QTY"),
    int_col("REJECTED_QTY"),
    str_col("REJECT_REASON_CD", 10),
    str_col("UOM_CD", 4),
    dec_col("UNIT_COST_AMT", 13, 4),
    str_col("LOT_NO", 20),
    date_col("EXPIRY_DT", note="only populated for chilled lines"),
    str_col("BIN_CD", 10),
) + AUDIT_COLUMNS

_REJECT_REASONS = ("DAMAGE", "SPEC", "SHORT", "LATE", "LABEL")


def produce_po_receipt_line(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("receipts")):
        po_ordinal = ordinal % max(cfg.count("purchase_orders"), 1)
        doc = documents.purchase_order(cfg, ctx, po_ordinal)
        receipt_no = keys.erp_receipt_number(ordinal)
        for (line_number, product_ordinal, quantity, unit_cost, _amount,
             received, _promised) in documents.purchase_order_lines(cfg, doc):
            if received <= 0:
                continue
            prod = entities.product(cfg, product_ordinal)
            rejected = 0
            reason = None
            if rng.chance(seed, 0.05, "recv-rej", ordinal, line_number):
                rejected = 1 + rng.stable_hash(seed, "rej-qty", ordinal, line_number) % max(received, 1)
                reason = rng.pick(seed, _REJECT_REASONS, "rej-reason", ordinal, line_number)
            receipt_date = timeline.business_days_after(doc.approved_on, prod.lead_time_days)
            yield (
                receipt_no, line_number, doc.po_number, line_number, prod.erp_code,
                received, received - rejected, rejected, reason, prod.uom, unit_cost,
                "LOT%s%06d" % (prod.erp_code[4:8], rng.stable_hash(seed, "lot", ordinal, line_number) % 10 ** 6),
                (receipt_date + datetime.timedelta(days=21 + rng.stable_hash(seed, "exp", ordinal, line_number) % 200))
                if prod.is_chiller else None,
                "%s-%02d-%02d" % (rng.pick(seed, ("A", "B", "C", "D"), "bin-a", ordinal, line_number),
                                  rng.stable_hash(seed, "bin-r", ordinal, line_number) % 40,
                                  rng.stable_hash(seed, "bin-l", ordinal, line_number) % 8),
            ) + _audit(cfg, "recv-line", ordinal * 13 + line_number, doc.raised_on, doc.region)


GOODS_RETURN_COLUMNS = (
    str_col("RETURN_NO", 14, nullable=False),
    str_col("PO_NO", 14),
    str_col("SUPP_CODE", 12),
    str_col("ITEM_CODE", 14),
    int_col("RETURN_QTY"),
    str_col("REASON_CD", 10),
    date_col("RETURN_DT"),
    str_col("RMA_NO", 20),
    str_col("STATUS_CD", 8),
    dec_col("CREDIT_EXPECTED_AMT", 15, 2),
    str_col("CURRENCY_CD", 3),
    flag_col("CREDIT_RECEIVED_FLG"),
) + AUDIT_COLUMNS


def produce_goods_return_hdr(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("purchase_orders")):
        if not rng.chance(seed, 0.035, "gr", ordinal):
            continue
        doc = documents.purchase_order(cfg, ctx, ordinal)
        supp = entities.supplier(cfg, doc.supplier_ordinal)
        product_ordinal = skew_product(cfg, ordinal)
        prod = entities.product(cfg, product_ordinal)
        _, unit_cost = prod.price_on(doc.raised_on)
        quantity = 1 + rng.stable_hash(seed, "gr-qty", ordinal) % 40
        return_date = timeline.business_days_after(
            doc.approved_on, 5 + rng.stable_hash(seed, "gr-when", ordinal) % 60)
        yield (
            "GR%08d" % ordinal, doc.po_number, supp.erp_code, prod.erp_code, quantity,
            rng.pick(seed, _REJECT_REASONS, "gr-reason", ordinal),
            min(return_date, cfg.history_end),
            "RMA%010d" % (rng.stable_hash(seed, "rma", ordinal) % 10 ** 10),
            rng.weighted_pick(seed, ("OPEN", "SHIPPED", "CREDITED"), (0.12, 0.23, 0.65),
                              "gr-status", ordinal),
            round(unit_cost * quantity, 2), doc.currency,
            "Y" if rng.chance(seed, 0.65, "gr-credit", ordinal) else "N",
        ) + _audit(cfg, "gr", ordinal, doc.raised_on, doc.region)


def skew_product(cfg, ordinal: int) -> int:
    from .. import skew
    return skew.pareto_ordinal(cfg.seed, cfg.count("products"), "gr-prod", ordinal)


# ---------------------------------------------------------------------------
# VENDOR_CONTRACT / SUPPLIER_SCORECARD
# ---------------------------------------------------------------------------

CONTRACT_COLUMNS = (
    str_col("CONTRACT_NO", 14, nullable=False),
    str_col("SUPP_CODE", 12),
    str_col("CONTRACT_TYPE_CD", 10, note="RATE / VOLUME / SLA"),
    date_col("EFF_FROM_DT"),
    date_col("EFF_TO_DT"),
    dec_col("COMMIT_AMT", 15, 2),
    str_col("CURRENCY_CD", 3),
    dec_col("REBATE_PCT", 6, 3),
    str_col("PAYMENT_TERMS_CD", 8),
    str_col("OWNER_ID", 10),
    flag_col("AUTO_RENEW_FLG"),
    str_col("STATUS_CD", 8),
) + AUDIT_COLUMNS


def produce_vendor_contract(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        if not rng.chance(seed, 0.45, "vc", ordinal) and not supp.is_dominant:
            continue
        start = supp.onboarded_date + datetime.timedelta(
            days=rng.stable_hash(seed, "vc-start", ordinal) % 400)
        end = start + datetime.timedelta(days=365 * (1 + rng.stable_hash(seed, "vc-len", ordinal) % 3))
        yield (
            "VC%07d" % ordinal, supp.erp_code,
            rng.pick(seed, ("RATE", "VOLUME", "SLA"), "vc-type", ordinal),
            start, end,
            round(10000 + rng.stable_hash(seed, "vc-amt", ordinal) % 5000000 / 10.0, 2),
            supp.country.currency,
            round(rng.stable_hash(seed, "vc-rebate", ordinal) % 750 / 100.0, 3),
            "NET%02d" % supp.payment_terms_days,
            "EMP%05d" % (rng.stable_hash(seed, "vc-owner", ordinal) % max(cfg.count("employees"), 1)),
            "Y" if rng.chance(seed, 0.4, "vc-renew", ordinal) else "N",
            "ACTIVE" if end >= cfg.history_end else "EXPIRED",
        ) + _audit(cfg, "vc", ordinal, start, supp.region)


SCORECARD_COLUMNS = (
    str_col("SUPP_CODE", 12, nullable=False),
    str_col("PERIOD_CD", 12, nullable=False, note="the supplier's own regional fiscal period"),
    dec_col("OTIF_PCT", 6, 2),
    dec_col("DEFECT_PPM", 10, 2),
    dec_col("PRICE_VARIANCE_PCT", 7, 3),
    int_col("RECEIPTS_QTY"),
    int_col("REJECTS_QTY"),
    str_col("BAND_CD", 2),
    str_col("REVIEWER_ID", 10),
    date_col("REVIEWED_DT"),
) + AUDIT_COLUMNS


def produce_supplier_scorecard(cfg, ctx):
    """Quarterly scorecard per supplier, in that supplier's own fiscal calendar."""
    seed = cfg.seed
    for ordinal in range(cfg.count("suppliers")):
        supp = entities.supplier(cfg, ordinal)
        when = supp.onboarded_date
        sequence = 0
        while when < cfg.history_end:
            sequence += 1
            yield (
                supp.erp_code, ctx.fiscal_period(supp.region, when),
                round(70 + rng.stable_hash(seed, "otif", ordinal, sequence) % 3000 / 100.0, 2),
                round(rng.stable_hash(seed, "ppm", ordinal, sequence) % 800000 / 100.0, 2),
                round((rng.stable_hash(seed, "pv", ordinal, sequence) % 900 - 400) / 100.0, 3),
                10 + rng.stable_hash(seed, "sc-recv", ordinal, sequence) % 900,
                rng.stable_hash(seed, "sc-rej", ordinal, sequence) % 40,
                supp.scorecard_band,
                "EMP%05d" % (rng.stable_hash(seed, "sc-rev", ordinal) % max(cfg.count("employees"), 1)),
                when,
            ) + _audit(cfg, "sc", ordinal * 40 + sequence, when, supp.region)
            when = timeline.add_months(when, 3)


def _spec(name, columns, produce, row_count_key, target, description, tags=()):
    return TableSpec(
        key="oracle.WWI_PROC.%s" % name,
        system=schema.ORACLE,
        schema="WWI_PROC",
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group="oracle_procurement",
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("REQUISITION_HDR", REQ_HDR_COLUMNS, produce_requisition_hdr, "purchase_orders",
          "raw.OraclePurchaseOrderHdr",
          "Requisitions, including the ones that never became a purchase order.", ()),
    _spec("REQUISITION_LINE", REQ_LINE_COLUMNS, produce_requisition_line,
          "purchase_order_lines", "raw.OraclePurchaseOrderLine",
          "Requisition lines with free-text non-catalogue entries.", ("dataquality",)),
    _spec("PURCHASE_ORDER_HDR", PO_HDR_COLUMNS, produce_purchase_order_hdr, "purchase_orders",
          "raw.OraclePurchaseOrderHdr",
          "Purchase order headers whose stated total occasionally disagrees with the lines.",
          ("reconciliation", "fx")),
    _spec("PURCHASE_ORDER_LINE", PO_LINE_COLUMNS, produce_purchase_order_line,
          "purchase_order_lines", "raw.OraclePurchaseOrderLine",
          "Purchase order lines with received and cancelled quantities.", ()),
    _spec("PO_CHANGE_ORDER", PO_CHANGE_COLUMNS, produce_po_change_order, "",
          "raw.OraclePurchaseOrderHdr",
          "Post-approval amendments, the reason header and line totals drift apart.",
          ("reconciliation",)),
    _spec("PO_RECEIPT_HDR", RECEIPT_HDR_COLUMNS, produce_po_receipt_hdr, "receipts",
          "raw.OracleReceiptLine",
          "Goods receipts with scanner timestamps that are not always monotonic.",
          ("outoforder",)),
    _spec("PO_RECEIPT_LINE", RECEIPT_LINE_COLUMNS, produce_po_receipt_line, "receipt_lines",
          "raw.OracleReceiptLine",
          "Receipt lines with accept/reject split, lot numbers and chilled expiry dates.", ()),
    _spec("GOODS_RETURN_HDR", GOODS_RETURN_COLUMNS, produce_goods_return_hdr, "",
          "raw.OracleReceiptLine",
          "Returns to supplier and the credit expected against them.", ()),
    _spec("VENDOR_CONTRACT", CONTRACT_COLUMNS, produce_vendor_contract, "suppliers",
          "raw.OracleVendorContract",
          "Rate, volume and SLA contracts with rebate percentages.", ()),
    _spec("SUPPLIER_SCORECARD", SCORECARD_COLUMNS, produce_supplier_scorecard, "suppliers",
          "raw.OracleVendorContract",
          "Quarterly supplier performance in each supplier's own fiscal calendar.",
          ("regional",)),
)
