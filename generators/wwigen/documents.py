"""Transaction documents, derived from the seed and an ordinal.

Nothing in the estate is held in memory as a population. A sales order is a
pure function of ``(seed, ordinal)``, so ``Sales.OrderLines`` can be generated
on its own, months after ``Sales.Orders``, and still agree with it line for
line. That is what makes each table independently runnable and resumable.

The same idea carries the cross-system coherence: an order's customer is
chosen once here, and the Oracle master, the SQL Server OLTP copy and the
partner file feed all render *that* customer under their own key format.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

from . import entities, keys, regions, rng, skew, timeline


@dataclass(frozen=True)
class OrderLine:
    line_number: int
    product_ordinal: int
    quantity: int
    unit_price: float
    unit_cost: float
    discount_pct: float
    net_amount: float
    tax_code: str
    tax_rate: float
    tax_amount: float
    picked_quantity: int
    backorder_quantity: int


@dataclass(frozen=True)
class Order:
    ordinal: int
    order_id: int
    order_reference: str
    customer_ordinal: int
    salesperson_ordinal: int
    region: str
    currency: str
    ordered_on: datetime.date
    ordered_at: datetime.datetime
    recorded_at: datetime.datetime
    status_code: str
    channel_code: str
    is_backorder: bool
    is_correction: bool
    delivery_promise: datetime.date
    line_count: int


@dataclass(frozen=True)
class PurchaseOrder:
    ordinal: int
    po_number: str
    supplier_ordinal: int
    buyer_ordinal: int
    region: str
    currency: str
    raised_on: datetime.date
    approved_on: datetime.date
    status_code: str
    incoterm: str
    line_count: int


@dataclass(frozen=True)
class ApInvoice:
    ordinal: int
    invoice_number: str
    supplier_ordinal: int
    po_ordinal: int
    region: str
    currency: str
    invoiced_on: datetime.date
    received_on: datetime.date
    due_on: datetime.date
    hold_code: str
    status_code: str
    line_count: int


# ---------------------------------------------------------------------------
# sales orders
# ---------------------------------------------------------------------------

CHANNELS = ("WEB", "EDI", "PHONE", "FIELD", "RETAIL")
_CHANNEL_WEIGHTS = (0.31, 0.24, 0.18, 0.19, 0.08)


def order(cfg, ctx, ordinal: int) -> Order:
    seed = cfg.seed
    customers = cfg.count("customers")
    customer_ordinal = skew.pareto_ordinal(seed, customers, "order-cust", ordinal)
    cust = entities.customer(cfg, customer_ordinal)
    ordered_at = ctx.dates.datetime_for(seed, "order-when", ordinal)
    ordered_on = ordered_at.date()

    # The row does not necessarily reach the warehouse system on the day the
    # order was taken: EDI batches land late and the phone desk keys up
    # yesterday's paperwork in the morning.
    late = timeline.lateness_days(seed, cfg.defect("late_arrival_rate"), "order-late", ordinal)
    recorded_at = ordered_at + datetime.timedelta(
        days=late,
        seconds=timeline.out_of_order_shift(seed, cfg.defect("out_of_order_rate"),
                                            "order-ooo", ordinal))

    status_codes = regions.ORDER_STATUS_CODES[cust.region]
    age_days = (cfg.history_end - ordered_on).days
    if age_days > 45:
        status = rng.weighted_pick(seed, status_codes, (0.01, 0.01, 0.06, 0.88, 0.04),
                                   "order-status", ordinal)
    else:
        status = rng.weighted_pick(seed, status_codes, (0.34, 0.21, 0.22, 0.19, 0.04),
                                   "order-status", ordinal)

    return Order(
        ordinal=ordinal,
        order_id=keys.oltp_order_id(ordinal),
        order_reference="SO%s%08d" % (regions.SOURCE_SYSTEM_CODE[cust.region][-2:], ordinal),
        customer_ordinal=customer_ordinal,
        salesperson_ordinal=entities.salesperson_ordinal(
            cfg, cust.salesperson_on(ordered_on)),
        region=cust.region,
        currency=cust.country.currency,
        ordered_on=ordered_on,
        ordered_at=ordered_at,
        recorded_at=recorded_at,
        status_code=status,
        channel_code=rng.weighted_pick(seed, CHANNELS, _CHANNEL_WEIGHTS, "order-chan", ordinal),
        is_backorder=rng.chance(seed, 0.06, "order-bo", ordinal),
        is_correction=rng.chance(seed, cfg.defect("correction_rate"), "order-corr", ordinal),
        delivery_promise=timeline.business_days_after(
            ordered_on, rng.pick(seed, (1, 2, 2, 3, 5, 10), "promise", ordinal)),
        line_count=skew.line_count(seed, cfg.ratio("avg_order_lines"), "order-lines", ordinal),
    )


def order_lines(cfg, ctx, doc: Order):
    """Yield the lines of one order, priced as at the order date."""
    seed = cfg.seed
    products = cfg.count("products")
    cust = entities.customer(cfg, doc.customer_ordinal)
    for line_number in range(1, doc.line_count + 1):
        product_ordinal = skew.pareto_ordinal(seed, products, "line-prod", doc.ordinal, line_number)
        prod = entities.product(cfg, product_ordinal)
        unit_price, unit_cost = prod.price_on(doc.ordered_on)
        quantity = skew.long_tail_quantity(seed, "line-qty", doc.ordinal, line_number)
        discount = _discount_for(cfg, cust, doc, line_number)
        net = round(unit_price * quantity * (1.0 - discount), 2)
        tax_code, tax_rate, tax_amount = regions.tax_treatment(
            doc.region, cust.country, net, cust.tax_registered)
        picked = quantity
        backorder = 0
        if doc.is_backorder and rng.chance(seed, 0.4, "line-bo", doc.ordinal, line_number):
            backorder = 1 + rng.stable_hash(seed, "bo-qty", doc.ordinal, line_number) % quantity
            picked = quantity - backorder
        yield OrderLine(
            line_number=line_number,
            product_ordinal=product_ordinal,
            quantity=quantity,
            unit_price=unit_price,
            unit_cost=unit_cost,
            discount_pct=discount,
            net_amount=net,
            tax_code=tax_code,
            tax_rate=tax_rate,
            tax_amount=tax_amount,
            picked_quantity=picked,
            backorder_quantity=backorder,
        )


def _discount_for(cfg, cust, doc: Order, line_number: int) -> float:
    """Discounting is a regional habit, not a global policy.

    NA sales reps discount off list at their own discretion, the EU operates a
    contracted matrix by customer class, and APAC prices net with the discount
    only ever applied at the invoice footer.
    """
    seed = cfg.seed
    if cust.region == "NA":
        if cust.is_dominant:
            return 0.18
        return rng.weighted_pick(seed, (0.0, 0.025, 0.05, 0.10, 0.15),
                                 (0.52, 0.18, 0.15, 0.10, 0.05),
                                 "disc-na", doc.ordinal, line_number)
    if cust.region == "EU":
        matrix = {"K1": 0.12, "K2": 0.07, "K3": 0.03, "KX": 0.0}
        return matrix.get(cust.class_on(doc.ordered_on), 0.0)
    return 0.0


def invoice_ordinal_for_order(cfg, ordinal: int) -> int:
    """Most orders invoice; the tail of the sequence is still open."""
    return ordinal


def order_is_invoiced(cfg, doc: Order) -> bool:
    return doc.ordinal < cfg.count("invoices")


# ---------------------------------------------------------------------------
# procurement
# ---------------------------------------------------------------------------

INCOTERMS = ("EXW", "FCA", "FOB", "CIF", "DAP", "DDP")


def purchase_order(cfg, ctx, ordinal: int) -> PurchaseOrder:
    seed = cfg.seed
    supplier_ordinal = skew.pareto_ordinal(seed, cfg.count("suppliers"), "po-sup", ordinal)
    supp = entities.supplier(cfg, supplier_ordinal)
    raised_on = ctx.dates.date_for(seed, "po-when", ordinal)
    approval_lag = rng.weighted_pick(seed, (0, 1, 2, 4, 9), (0.31, 0.29, 0.21, 0.13, 0.06),
                                     "po-appr", ordinal)
    approved_on = timeline.business_days_after(raised_on, approval_lag)
    return PurchaseOrder(
        ordinal=ordinal,
        po_number=keys.erp_po_number(ordinal, raised_on.year),
        supplier_ordinal=supplier_ordinal,
        buyer_ordinal=rng.stable_hash(seed, "po-buyer", ordinal) % max(cfg.count("employees"), 1),
        region=supp.region,
        currency=supp.country.currency,
        raised_on=raised_on,
        approved_on=approved_on,
        status_code=rng.weighted_pick(seed, ("OPEN", "APPR", "PARTRCV", "CLOSED", "CANC"),
                                      (0.05, 0.08, 0.11, 0.72, 0.04), "po-status", ordinal),
        incoterm=rng.pick(seed, INCOTERMS, "po-inco", ordinal),
        line_count=skew.line_count(seed, cfg.ratio("avg_po_lines"), "po-lines", ordinal),
    )


def purchase_order_lines(cfg, doc: PurchaseOrder):
    seed = cfg.seed
    products = cfg.count("products")
    for line_number in range(1, doc.line_count + 1):
        product_ordinal = skew.pareto_ordinal(seed, products, "po-line-prod",
                                              doc.ordinal, line_number)
        prod = entities.product(cfg, product_ordinal)
        _, unit_cost = prod.price_on(doc.raised_on)
        quantity = skew.long_tail_quantity(seed, "po-qty", doc.ordinal, line_number) * prod.pack_size
        received = quantity
        if rng.chance(seed, 0.09, "po-short", doc.ordinal, line_number):
            received = quantity - 1 - rng.stable_hash(seed, "po-shortqty",
                                                      doc.ordinal, line_number) % quantity
        promised = timeline.business_days_after(doc.approved_on, prod.lead_time_days)
        yield (line_number, product_ordinal, quantity, round(unit_cost, 2),
               round(unit_cost * quantity, 2), received, promised)


def ap_invoice(cfg, ctx, ordinal: int) -> ApInvoice:
    seed = cfg.seed
    purchase_orders = cfg.count("purchase_orders")
    po_ordinal = rng.stable_hash(seed, "ap-po", ordinal) % max(purchase_orders, 1)
    po_doc = purchase_order(cfg, ctx, po_ordinal)
    supp = entities.supplier(cfg, po_doc.supplier_ordinal)
    invoiced_on = timeline.business_days_after(
        po_doc.approved_on, 2 + rng.stable_hash(seed, "ap-lag", ordinal) % 30)
    if invoiced_on > cfg.history_end:
        invoiced_on = cfg.history_end
    received_on = timeline.business_days_after(
        invoiced_on, rng.weighted_pick(seed, (0, 1, 3, 8, 21), (0.42, 0.24, 0.18, 0.11, 0.05),
                                       "ap-recv", ordinal))
    hold = rng.weighted_pick(seed, ("", "", "", "PRICE", "QTY", "TAX", "APPROVAL"),
                             (0.4, 0.3, 0.14, 0.05, 0.05, 0.03, 0.03), "ap-hold", ordinal)
    return ApInvoice(
        ordinal=ordinal,
        invoice_number=keys.erp_ap_invoice_number(ordinal, po_doc.supplier_ordinal),
        supplier_ordinal=po_doc.supplier_ordinal,
        po_ordinal=po_ordinal,
        region=supp.region,
        currency=supp.country.currency,
        invoiced_on=invoiced_on,
        received_on=received_on,
        due_on=invoiced_on + datetime.timedelta(days=supp.payment_terms_days),
        hold_code=hold,
        status_code="HELD" if hold else rng.weighted_pick(
            seed, ("PAID", "APPROVED", "ENTERED"), (0.78, 0.14, 0.08), "ap-status", ordinal),
        line_count=skew.line_count(seed, cfg.ratio("avg_ap_invoice_lines"), "ap-lines", ordinal),
    )
