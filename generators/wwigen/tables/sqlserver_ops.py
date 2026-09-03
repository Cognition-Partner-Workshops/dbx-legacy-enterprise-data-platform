"""SQL Server OLTP - warehouse, shipping, returns, loyalty, web and interfaces.

Inventory movement is the highest-volume table in the estate and the one the
warehouse loads incrementally, so its transaction timestamps matter: the
handheld terminals buffer offline and flush later, which is where the
out-of-order and late-arriving rows come from.

The Integration tables are the estate's own bookkeeping: an outbound queue
that other systems poll, a register of files received in the landing zone, and
the change-tracking watermarks the extracts read.
"""

from __future__ import annotations

import datetime

from .. import (documents, entities, keys, regions, rng, schema, skew, text,
                timeline)
from ..schema import (TableSpec, date_col, dec_col, flag_col, int_col,
                      str_col, ts_col)

EDIT_COLUMNS = (
    int_col("LastEditedBy"),
    ts_col("LastEditedWhen"),
)


def _edited(cfg, kind: str, ordinal: int, when: datetime.date):
    seed = cfg.seed
    return (
        1 + rng.stable_hash(seed, "ops-by", kind, ordinal) % max(cfg.count("employees"), 1),
        datetime.datetime(when.year, when.month, when.day,
                          rng.stable_hash(seed, "ops-h", kind, ordinal) % 24,
                          rng.stable_hash(seed, "ops-m", kind, ordinal) % 60,
                          rng.stable_hash(seed, "ops-s", kind, ordinal) % 60),
    )


# ---------------------------------------------------------------------------
# Warehouse.StockItems / StockItemTransactions
# ---------------------------------------------------------------------------

STOCK_ITEM_COLUMNS = (
    int_col("StockItemID", nullable=False),
    str_col("StockItemName", 100, nullable=False),
    int_col("SupplierID"),
    int_col("ColorID"),
    int_col("UnitPackageID"),
    int_col("OuterPackageID"),
    str_col("Brand", 50),
    str_col("Size", 20),
    int_col("LeadTimeDays"),
    int_col("QuantityPerOuter"),
    flag_col("IsChillerStock"),
    str_col("Barcode", 20),
    dec_col("TaxRate", 18, 3),
    dec_col("UnitPrice", 18, 2),
    dec_col("RecommendedRetailPrice", 18, 2),
    dec_col("TypicalWeightPerUnit", 18, 3),
    str_col("MarketingComments", 200),
    str_col("InternalComments", 200),
    str_col("ErpItemCode", 14, note="the ERP item code, stale for items recategorised since"),
    date_col("DiscontinuedDate"),
) + EDIT_COLUMNS


def produce_stock_items(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("products")):
        prod = entities.product(cfg, ordinal)
        supp = entities.supplier(cfg, prod.supplier_ordinal)
        price, _cost = prod.price_on(cfg.history_end)
        erp_code = prod.erp_code
        if prod.crosswalk == keys.CROSSWALK_STALE:
            erp_code = keys.stale_variant(erp_code)
        elif prod.crosswalk == keys.CROSSWALK_MISSING:
            erp_code = None
        yield (
            prod.oltp_id, prod.name, supp.oltp_id,
            1 + rng.stable_hash(seed, "colour", ordinal) % 8,
            1 + rng.stable_hash(seed, "unitpkg", ordinal) % 7,
            1 + rng.stable_hash(seed, "outerpkg", ordinal) % 7,
            rng.pick(seed, text.COMPANY_HEADS, "brand", ordinal),
            rng.pick(seed, text.PRODUCT_SIZES, "size", ordinal),
            prod.lead_time_days, prod.pack_size,
            "Y" if prod.is_chiller else "N",
            "%013d" % (rng.stable_hash(seed, "barcode", ordinal) % 10 ** 13),
            round(supp.country.tax_rate * 100, 3), price,
            round(price * 1.35, 2),
            round(0.05 + rng.stable_hash(seed, "weight", ordinal) % 40000 / 1000.0, 3),
            None, None, erp_code, prod.discontinued_on,
        ) + _edited(cfg, "item", ordinal, cfg.history_start)


STOCK_TXN_COLUMNS = (
    int_col("StockItemTransactionID", nullable=False),
    int_col("StockItemID"),
    int_col("TransactionTypeID"),
    int_col("CustomerID"),
    int_col("InvoiceID"),
    int_col("SupplierID"),
    int_col("PurchaseOrderID"),
    ts_col("TransactionOccurredWhen", note="handheld terminal clock; buffered offline"),
    ts_col("TransactionRecordedWhen"),
    dec_col("Quantity", 18, 3),
    str_col("MovementTypeCode", 8, note="RCPT / ISSUE / ADJ / XFER / SCRAP"),
    str_col("WarehouseSiteCode", 8),
    str_col("BinCode", 10),
    str_col("LotNumber", 20),
    str_col("ReasonCode", 10),
) + EDIT_COLUMNS

_MOVEMENT_TYPES = ("RCPT", "ISSUE", "ADJ", "XFER", "SCRAP")
_MOVEMENT_WEIGHTS = (0.21, 0.58, 0.09, 0.09, 0.03)
_MOVEMENT_REASONS = ("SALE", "REPLEN", "COUNT", "DAMAGE", "EXPIRY", "RELOC")


def produce_stock_item_transactions(cfg, ctx):
    """The estate's largest table: one row per inventory movement."""
    seed = cfg.seed
    products = cfg.count("products")
    customers = cfg.count("customers")
    for ordinal in range(cfg.count("stock_movements")):
        product_ordinal = skew.pareto_ordinal(seed, products, "mv-prod", ordinal)
        prod = entities.product(cfg, product_ordinal)
        occurred = ctx.dates.datetime_for(seed, "mv-when", ordinal)
        # Terminals buffer while out of coverage, so the recorded time can be
        # hours or days after the movement, and occasionally before it.
        recorded = occurred + datetime.timedelta(
            minutes=timeline.lateness_days(seed, cfg.defect("late_arrival_rate"),
                                           "mv-late", ordinal) * 1440
            + rng.stable_hash(seed, "mv-lag", ordinal) % 240)
        recorded += datetime.timedelta(
            seconds=timeline.out_of_order_shift(seed, cfg.defect("out_of_order_rate"),
                                                "mv-ooo", ordinal))
        movement = rng.weighted_pick(seed, _MOVEMENT_TYPES, _MOVEMENT_WEIGHTS,
                                     "mv-type", ordinal)
        quantity = skew.long_tail_quantity(seed, "mv-qty", ordinal)
        if movement in ("ISSUE", "SCRAP"):
            quantity = -quantity
        region = regions.REGIONS[rng.stable_hash(seed, "mv-region", ordinal) % 3]
        is_sale = movement == "ISSUE"
        yield (
            ordinal + 1, prod.oltp_id,
            1 + _MOVEMENT_TYPES.index(movement),
            keys.oltp_customer_id(skew.pareto_ordinal(seed, customers, "mv-cust", ordinal))
            if is_sale else None,
            keys.oltp_invoice_id(1 + rng.stable_hash(seed, "mv-inv", ordinal) % max(cfg.count("invoices"), 1))
            if is_sale else None,
            entities.supplier(cfg, prod.supplier_ordinal).oltp_id if movement == "RCPT" else None,
            (1 + rng.stable_hash(seed, "mv-po", ordinal) % max(cfg.count("purchase_orders"), 1))
            if movement == "RCPT" else None,
            occurred, recorded, float(quantity), movement,
            rng.pick(seed, regions.SITES[region], "mv-site", ordinal),
            "%s-%02d-%02d" % (rng.pick(seed, ("A", "B", "C", "D"), "mv-bin", ordinal),
                              rng.stable_hash(seed, "mv-row", ordinal) % 40,
                              rng.stable_hash(seed, "mv-lvl", ordinal) % 8),
            "LOT%09d" % (rng.stable_hash(seed, "mv-lot", ordinal) % 10 ** 9),
            rng.pick(seed, _MOVEMENT_REASONS, "mv-reason", ordinal),
        ) + _edited(cfg, "mv", ordinal, occurred.date())


BIN_COLUMNS = (
    int_col("BinID", nullable=False),
    str_col("BinCode", 10),
    str_col("WarehouseSiteCode", 8),
    str_col("ZoneCode", 6),
    str_col("BinTypeCode", 8),
    int_col("CapacityUnits"),
    flag_col("IsChiller"),
    flag_col("IsActive"),
) + EDIT_COLUMNS


def produce_bins(cfg, ctx):
    seed = cfg.seed
    index = 0
    for region in regions.REGIONS:
        for site in regions.SITES[region]:
            for slot in range(24):
                index += 1
                yield (
                    index,
                    "%s-%02d-%02d" % ("ABCD"[slot % 4], slot, index % 8),
                    site, "Z%d" % (slot % 6),
                    rng.pick(seed, ("PALLET", "SHELF", "FLOOR", "PICKFACE"), "bin-type", index),
                    100 * (1 + rng.stable_hash(seed, "bin-cap", index) % 40),
                    "Y" if slot % 7 == 0 else "N", "Y",
                ) + _edited(cfg, "bin", index, cfg.history_start)


# ---------------------------------------------------------------------------
# Shipping
# ---------------------------------------------------------------------------

SHIPMENT_HDR_COLUMNS = (
    int_col("ShipmentID", nullable=False),
    int_col("OrderID"),
    int_col("CustomerID"),
    str_col("CarrierCode", 8),
    str_col("ServiceLevelCode", 8),
    str_col("TrackingNumber", 30),
    date_col("ShippedDate"),
    ts_col("ShippedWhen"),
    date_col("PromisedDeliveryDate"),
    ts_col("DeliveredWhen"),
    str_col("ShipmentStatusCode", 10),
    str_col("OriginSiteCode", 8),
    str_col("DestinationPostalCode", 10),
    str_col("DestinationCountryCode", 2),
    dec_col("FreightChargeAmount", 18, 2),
    str_col("CurrencyCode", 3),
    dec_col("TotalWeightKg", 18, 3),
    int_col("PackageCount"),
    str_col("IncotermCode", 4, note="only populated on cross-border shipments"),
) + EDIT_COLUMNS

_CARRIERS = ("MERIDIAN", "NORTHWAY", "EUROLINK", "PACRIM", "SWIFTFRT", "LOCALDEL")
_SERVICE_LEVELS = ("STD", "EXP", "OVN", "ECON", "CHILL")


def produce_shipment_headers(cfg, ctx):
    seed = cfg.seed
    orders = cfg.count("orders")
    for ordinal in range(cfg.count("shipments")):
        order_ordinal = ordinal % max(orders, 1)
        doc = documents.order(cfg, ctx, order_ordinal)
        cust = entities.customer(cfg, doc.customer_ordinal)
        address = cust.address_on(doc.ordered_on)
        shipped = timeline.business_days_after(
            doc.ordered_on, 1 + rng.stable_hash(seed, "ship-lag", ordinal) % 5)
        shipped = min(shipped, cfg.history_end)
        shipped_at = datetime.datetime(shipped.year, shipped.month, shipped.day,
                                       6 + rng.stable_hash(seed, "ship-h", ordinal) % 12,
                                       rng.stable_hash(seed, "ship-m", ordinal) % 60, 0)
        delivered = None
        status = "INTRANSIT"
        if not rng.chance(seed, 0.06, "ship-open", ordinal):
            delivered = shipped_at + datetime.timedelta(
                hours=8 + rng.stable_hash(seed, "ship-transit", ordinal) % 180)
            status = "DELIVERED"
        cross_border = rng.chance(seed, 0.18, "ship-xb", ordinal)
        yield (
            keys.oltp_shipment_id(ordinal), doc.order_id, cust.oltp_id,
            rng.pick(seed, _CARRIERS, "ship-carrier", ordinal),
            rng.pick(seed, _SERVICE_LEVELS, "ship-svc", ordinal),
            "%s%012d" % (rng.pick(seed, ("1Z", "TT", "AU", "EU"), "trk-pre", ordinal),
                         rng.stable_hash(seed, "trk", ordinal) % 10 ** 12),
            shipped, shipped_at, doc.delivery_promise, delivered, status,
            rng.pick(seed, regions.SITES[doc.region], "ship-origin", ordinal),
            address.postal_code, address.country_code,
            round(rng.stable_hash(seed, "freight", ordinal) % 45000 / 100.0, 2),
            doc.currency,
            round(rng.stable_hash(seed, "ship-wt", ordinal) % 900000 / 1000.0, 3),
            1 + rng.stable_hash(seed, "ship-pkgs", ordinal) % 12,
            rng.pick(seed, documents.INCOTERMS, "ship-inco", ordinal) if cross_border else None,
        ) + _edited(cfg, "ship", ordinal, shipped)


SHIPMENT_LINE_COLUMNS = (
    int_col("ShipmentLineID", nullable=False),
    int_col("ShipmentID"),
    int_col("OrderLineID"),
    int_col("StockItemID"),
    int_col("ShippedQuantity"),
    int_col("ShortShippedQuantity"),
    str_col("PackageTypeCode", 8),
    dec_col("LineWeightKg", 18, 3),
    str_col("LotNumber", 20),
    str_col("SerialNumber", 30, note="only for the small number of serialised lines"),
) + EDIT_COLUMNS


def produce_shipment_lines(cfg, ctx):
    seed = cfg.seed
    orders = cfg.count("orders")
    line_id = 0
    for ordinal in range(cfg.count("shipments")):
        order_ordinal = ordinal % max(orders, 1)
        doc = documents.order(cfg, ctx, order_ordinal)
        for line in documents.order_lines(cfg, ctx, doc):
            line_id += 1
            prod = entities.product(cfg, line.product_ordinal)
            short = line.backorder_quantity
            serialised = rng.chance(seed, 0.03, "ship-serial", ordinal, line.line_number)
            yield (
                line_id, keys.oltp_shipment_id(ordinal), line_id, prod.oltp_id,
                max(line.quantity - short, 0), short,
                rng.pick(seed, ("CTN", "PLT", "BOX", "ENV"), "ship-pkg", ordinal, line.line_number),
                round(rng.stable_hash(seed, "ship-lwt", ordinal, line.line_number) % 60000 / 1000.0, 3),
                "LOT%09d" % (rng.stable_hash(seed, "ship-lot", ordinal, line.line_number) % 10 ** 9),
                "SN%014d" % (rng.stable_hash(seed, "ship-sn", ordinal, line.line_number) % 10 ** 14)
                if serialised else None,
            ) + _edited(cfg, "shipl", line_id, doc.ordered_on)


SHIPMENT_EVENT_COLUMNS = (
    int_col("ShipmentEventID", nullable=False),
    int_col("ShipmentID"),
    str_col("EventCode", 10),
    ts_col("EventWhen"),
    str_col("LocationCode", 12),
    str_col("CarrierCode", 8),
    str_col("EventDescription", 100),
    flag_col("IsException"),
) + EDIT_COLUMNS

_EVENTS = (("PICKUP", "Collected from origin site"),
           ("DEPART", "Departed sort facility"),
           ("ARRIVE", "Arrived at destination facility"),
           ("OUTDEL", "Out for delivery"),
           ("DELIV", "Delivered"),
           ("EXCEPT", "Delivery exception - no access"))


def produce_shipment_events(cfg, ctx):
    seed = cfg.seed
    event_id = 0
    for ordinal in range(cfg.count("shipments")):
        base = ctx.dates.datetime_for(seed, "evt-base", ordinal)
        count = 3 + rng.stable_hash(seed, "evt-n", ordinal) % 4
        for step in range(count):
            event_id += 1
            code, description = _EVENTS[min(step, len(_EVENTS) - 1)]
            yield (
                event_id, keys.oltp_shipment_id(ordinal), code,
                base + datetime.timedelta(hours=step * 7 + rng.stable_hash(seed, "evt-h", ordinal, step) % 6),
                "LOC%05d" % (rng.stable_hash(seed, "evt-loc", ordinal, step) % 10 ** 5),
                rng.pick(seed, _CARRIERS, "evt-carrier", ordinal),
                description, "Y" if code == "EXCEPT" else "N",
            ) + _edited(cfg, "evt", event_id, base.date())


# ---------------------------------------------------------------------------
# Returns
# ---------------------------------------------------------------------------

RETURN_LINE_COLUMNS = (
    int_col("ReturnLineID", nullable=False),
    int_col("ReturnAuthorizationID"),
    int_col("OrderID"),
    int_col("StockItemID"),
    int_col("CustomerID"),
    int_col("ReturnedQuantity"),
    str_col("ReturnReasonCode", 8, note="regional vocabularies, never harmonised"),
    date_col("RequestedDate"),
    date_col("ReceivedDate"),
    str_col("ConditionCode", 8),
    str_col("DispositionCode", 10),
    dec_col("RefundAmount", 18, 2),
    str_col("CurrencyCode", 3),
    flag_col("IsRestocked"),
) + EDIT_COLUMNS


def produce_return_lines(cfg, ctx):
    seed = cfg.seed
    orders = cfg.count("orders")
    for ordinal in range(cfg.count("returns")):
        order_ordinal = rng.stable_hash(seed, "ret-order", ordinal) % max(orders, 1)
        doc = documents.order(cfg, ctx, order_ordinal)
        cust = entities.customer(cfg, doc.customer_ordinal)
        product_ordinal = skew.pareto_ordinal(seed, cfg.count("products"), "ret-prod", ordinal)
        prod = entities.product(cfg, product_ordinal)
        requested = timeline.business_days_after(
            doc.ordered_on, 5 + rng.stable_hash(seed, "ret-lag", ordinal) % 60)
        received = timeline.business_days_after(
            requested, 2 + rng.stable_hash(seed, "ret-recv", ordinal) % 20)
        price, _cost = prod.price_on(doc.ordered_on)
        quantity = 1 + rng.stable_hash(seed, "ret-qty", ordinal) % 12
        yield (
            ordinal + 1, 800000 + ordinal, doc.order_id, prod.oltp_id, cust.oltp_id,
            quantity,
            rng.pick(seed, regions.RETURN_REASON_CODES[cust.region], "ret-reason", ordinal),
            min(requested, cfg.history_end), min(received, cfg.history_end),
            rng.weighted_pick(seed, ("NEW", "OPENED", "DAMAGED"), (0.42, 0.38, 0.20),
                              "ret-cond", ordinal),
            rng.weighted_pick(seed, ("RESTOCK", "SCRAP", "SUPPLIER", "REWORK"),
                              (0.55, 0.21, 0.14, 0.10), "ret-disp", ordinal),
            round(price * quantity, 2), doc.currency,
            "Y" if rng.chance(seed, 0.55, "ret-restock", ordinal) else "N",
        ) + _edited(cfg, "ret", ordinal, requested)


CREDIT_NOTE_LINE_COLUMNS = (
    int_col("CreditNoteLineID", nullable=False),
    int_col("CreditNoteID"),
    int_col("ReturnLineID"),
    int_col("InvoiceID"),
    int_col("StockItemID"),
    int_col("Quantity"),
    dec_col("UnitPrice", 18, 2),
    dec_col("TaxRate", 18, 3),
    dec_col("TaxAmount", 18, 2),
    dec_col("LineAmount", 18, 2),
    str_col("CurrencyCode", 3),
    date_col("CreditNoteDate"),
    str_col("ReasonCode", 8),
) + EDIT_COLUMNS


def produce_credit_note_lines(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("credit_notes")):
        return_ordinal = ordinal % max(cfg.count("returns"), 1)
        order_ordinal = rng.stable_hash(seed, "ret-order", return_ordinal) % max(cfg.count("orders"), 1)
        doc = documents.order(cfg, ctx, order_ordinal)
        cust = entities.customer(cfg, doc.customer_ordinal)
        product_ordinal = skew.pareto_ordinal(seed, cfg.count("products"), "ret-prod", return_ordinal)
        prod = entities.product(cfg, product_ordinal)
        price, _cost = prod.price_on(doc.ordered_on)
        quantity = 1 + rng.stable_hash(seed, "cn-qty", ordinal) % 10
        amount = round(price * quantity, 2)
        _code, rate, tax = regions.tax_treatment(cust.region, cust.country, amount,
                                                 cust.tax_registered)
        when = timeline.business_days_after(
            doc.ordered_on, 20 + rng.stable_hash(seed, "cn-lag", ordinal) % 70)
        yield (
            ordinal + 1, 900000 + ordinal, return_ordinal + 1,
            keys.oltp_invoice_id(1 + ordinal % max(cfg.count("invoices"), 1)),
            prod.oltp_id, quantity, price, round(rate * 100, 3), tax,
            round(amount + tax, 2), doc.currency, min(when, cfg.history_end),
            rng.pick(seed, regions.RETURN_REASON_CODES[cust.region], "cn-reason", ordinal),
        ) + _edited(cfg, "cn", ordinal, when)


# ---------------------------------------------------------------------------
# Loyalty and e-commerce
# ---------------------------------------------------------------------------

LOYALTY_LEDGER_COLUMNS = (
    int_col("LoyaltyLedgerID", nullable=False),
    int_col("LoyaltyMemberID"),
    int_col("CustomerID"),
    int_col("OrderID"),
    str_col("EntryTypeCode", 8, note="EARN / REDEEM / EXPIRE / ADJUST"),
    int_col("PointsDelta"),
    int_col("PointsBalance"),
    date_col("EntryDate"),
    date_col("ExpiryDate", note="EU points expire on a rolling 24 months, APAC on 12"),
    str_col("ProgramCode", 10),
    str_col("TierCode", 6),
    str_col("ReasonCode", 10),
) + EDIT_COLUMNS

_LOYALTY_TIERS = {"NA": ("BRONZE", "SILVER", "GOLD"),
                  "EU": ("BASIS", "PLUS", "PREM"),
                  "APAC": ("T1", "T2", "T3")}
_POINT_EXPIRY_MONTHS = {"NA": 36, "EU": 24, "APAC": 12}


def produce_loyalty_points_ledger(cfg, ctx):
    seed = cfg.seed
    customers = cfg.count("customers")
    balances = {}
    for ordinal in range(cfg.count("loyalty_ledger")):
        customer_ordinal = skew.pareto_ordinal(seed, customers, "loy-cust", ordinal)
        cust = entities.customer(cfg, customer_ordinal)
        when = ctx.dates.date_for(seed, "loy-when", ordinal)
        entry_type = rng.weighted_pick(seed, ("EARN", "REDEEM", "EXPIRE", "ADJUST"),
                                       (0.68, 0.21, 0.08, 0.03), "loy-type", ordinal)
        points = 1 + rng.stable_hash(seed, "loy-pts", ordinal) % 4000
        if entry_type != "EARN":
            points = -points
        balance = balances.get(customer_ordinal, 0) + points
        balances[customer_ordinal] = balance
        yield (
            ordinal + 1, 500000 + customer_ordinal, cust.oltp_id,
            keys.oltp_order_id(rng.stable_hash(seed, "loy-order", ordinal) % max(cfg.count("orders"), 1))
            if entry_type in ("EARN", "REDEEM") else None,
            entry_type, points, balance, when,
            timeline.add_months(when, _POINT_EXPIRY_MONTHS[cust.region]),
            "LOY-%s" % cust.region,
            rng.pick(seed, _LOYALTY_TIERS[cust.region], "loy-tier", customer_ordinal),
            rng.pick(seed, ("PURCHASE", "PROMO", "GOODWILL", "LAPSE"), "loy-reason", ordinal),
        ) + _edited(cfg, "loy", ordinal, when)


WEB_SESSION_COLUMNS = (
    str_col("WebSessionID", 36, nullable=False, note="GUID-shaped, derived from the seed"),
    int_col("CustomerID", note="null for anonymous sessions - most of them"),
    ts_col("SessionStartedWhen"),
    ts_col("SessionEndedWhen"),
    int_col("PageViewCount"),
    str_col("EntryPageURL", 200),
    str_col("ExitPageURL", 200),
    str_col("ReferrerDomain", 100),
    str_col("DeviceTypeCode", 10),
    str_col("BrowserFamily", 30),
    str_col("CountryCode", 2),
    str_col("RegionCode", 4),
    flag_col("ConvertedToOrder"),
    int_col("OrderID"),
    str_col("ConsentStateCode", 12, note="EU sessions carry an explicit consent decision"),
    str_col("CampaignCode", 20),
) + EDIT_COLUMNS

_DEVICES = ("DESKTOP", "MOBILE", "TABLET", "KIOSK")
_BROWSERS = ("Chromium", "Firefox", "Safari", "Edge", "LegacyIE")
_REFERRERS = ("search.example", "partner.example", "mail.example", "direct",
              "social.example")


def produce_web_sessions(cfg, ctx):
    seed = cfg.seed
    customers = cfg.count("customers")
    for ordinal in range(cfg.count("web_sessions")):
        started = ctx.dates.datetime_for(seed, "web-when", ordinal)
        duration = 30 + rng.stable_hash(seed, "web-dur", ordinal) % 5400
        identified = rng.chance(seed, 0.28, "web-known", ordinal)
        customer_ordinal = skew.pareto_ordinal(seed, customers, "web-cust", ordinal)
        cust = entities.customer(cfg, customer_ordinal)
        converted = rng.chance(seed, 0.041, "web-conv", ordinal)
        consent = regions.CONSENT_MODEL[cust.region]["consent_code_set"]
        digest = rng.stable_hash(seed, "web-guid", ordinal)
        guid = "%08x-%04x-%04x-%04x-%012x" % (
            digest % 2 ** 32, (digest >> 32) % 2 ** 16, (digest >> 48) % 2 ** 16,
            (digest >> 64) % 2 ** 16, (digest >> 80) % 2 ** 48)
        yield (
            guid, cust.oltp_id if identified else None,
            started, started + datetime.timedelta(seconds=duration),
            1 + rng.stable_hash(seed, "web-views", ordinal) % 40,
            "/catalogue/%s" % rng.pick(seed, text.CATEGORY_CODES, "web-entry", ordinal).lower(),
            "/checkout" if converted else "/catalogue",
            rng.pick(seed, _REFERRERS, "web-ref", ordinal),
            rng.pick(seed, _DEVICES, "web-dev", ordinal),
            rng.pick(seed, _BROWSERS, "web-br", ordinal),
            cust.country.code, cust.region,
            "Y" if converted else "N",
            keys.oltp_order_id(rng.stable_hash(seed, "web-order", ordinal) % max(cfg.count("orders"), 1))
            if converted else None,
            rng.pick(seed, consent, "web-consent", ordinal),
            "CMP%s%05d" % (cust.region[:2], rng.stable_hash(seed, "web-camp", ordinal) % 10 ** 5),
        ) + _edited(cfg, "web", ordinal, started.date())


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

OUTBOUND_QUEUE_COLUMNS = (
    int_col("QueueEntryID", nullable=False),
    str_col("InterfaceCode", 20),
    str_col("EntityTypeCode", 20),
    str_col("EntityKey", 40),
    str_col("OperationCode", 8, note="INS / UPD / DEL"),
    ts_col("EnqueuedWhen"),
    ts_col("DequeuedWhen"),
    int_col("AttemptCount"),
    str_col("StatusCode", 10),
    str_col("LastErrorText", 200),
    str_col("PayloadFormatCode", 8),
    int_col("PayloadBytes"),
) + EDIT_COLUMNS

_INTERFACES = ("IF_ERP_CUSTOMER", "IF_ERP_ORDER", "IF_WMS_MOVEMENT",
               "IF_CARRIER_MANIFEST", "IF_FIN_INVOICE")


def produce_outbound_interface_queue(cfg, ctx):
    seed = cfg.seed
    total = max(cfg.count("orders") // 20, 100)
    for ordinal in range(total):
        enqueued = ctx.dates.datetime_for(seed, "q-when", ordinal)
        failed = rng.chance(seed, 0.06, "q-fail", ordinal)
        attempts = 1 + (rng.stable_hash(seed, "q-att", ordinal) % 5 if failed else 0)
        yield (
            ordinal + 1, rng.pick(seed, _INTERFACES, "q-if", ordinal),
            rng.pick(seed, ("CUSTOMER", "ORDER", "MOVEMENT", "INVOICE"), "q-ent", ordinal),
            "%d" % keys.oltp_order_id(rng.stable_hash(seed, "q-key", ordinal) % max(cfg.count("orders"), 1)),
            rng.weighted_pick(seed, ("INS", "UPD", "DEL"), (0.6, 0.37, 0.03), "q-op", ordinal),
            enqueued,
            None if failed else enqueued + datetime.timedelta(
                seconds=5 + rng.stable_hash(seed, "q-deq", ordinal) % 3600),
            attempts,
            "ERROR" if failed else "SENT",
            "Downstream endpoint returned a non-success status" if failed else None,
            rng.pick(seed, ("XML", "CSV", "JSON", "FLAT"), "q-fmt", ordinal),
            200 + rng.stable_hash(seed, "q-bytes", ordinal) % 40000,
        ) + _edited(cfg, "queue", ordinal, enqueued.date())


INBOUND_REGISTER_COLUMNS = (
    int_col("FileRegisterID", nullable=False),
    str_col("FeedCode", 20),
    str_col("FileName", 120),
    str_col("LandingPath", 200),
    ts_col("ReceivedWhen"),
    ts_col("ProcessedWhen"),
    int_col("FileBytes"),
    int_col("RowCount"),
    int_col("RejectedRowCount"),
    str_col("StatusCode", 12),
    str_col("Checksum", 64, note="hex digest recorded when the file landed"),
    flag_col("IsReprocessed"),
) + EDIT_COLUMNS

_FEEDS = (("PARTNER_NA", "partner_sales_na_%s.csv"),
          ("PARTNER_EU", "partner_sales_eu_%s.csv"),
          ("PARTNER_APAC", "partner_sales_apac_%s.txt"),
          ("CARRIER_SCAN", "carrier_scan_%s.csv"),
          ("SUPPLIER_CAT", "supplier_catalog_%s.psv"),
          ("FX_OVERRIDE", "fx_override_%s.csv"))


def produce_inbound_file_register(cfg, ctx):
    """One row per file the landing zone received, matching the feed generators."""
    seed = cfg.seed
    entry = 0
    for index, (feed_code, pattern) in enumerate(_FEEDS):
        when = cfg.history_start
        while when <= cfg.history_end:
            entry += 1
            received = datetime.datetime(when.year, when.month, when.day,
                                         2 + index, rng.stable_hash(seed, "f-m", entry) % 60, 0)
            rejected = rng.stable_hash(seed, "f-rej", entry) % 40
            yield (
                entry, feed_code, pattern % when.strftime("%Y%m%d"),
                "/landing/%s/%s" % (feed_code.lower(), when.strftime("%Y/%m")),
                received,
                received + datetime.timedelta(minutes=4 + rng.stable_hash(seed, "f-proc", entry) % 90),
                10000 + rng.stable_hash(seed, "f-bytes", entry) % 9000000,
                100 + rng.stable_hash(seed, "f-rows", entry) % 40000,
                rejected,
                "REJECTS" if rejected > 20 else "LOADED",
                "%064x" % (rng.stable_hash(seed, "f-sum", entry) % 16 ** 64),
                "Y" if rng.chance(seed, 0.02, "f-repro", entry) else "N",
            ) + _edited(cfg, "reg", entry, when)
            when = when + datetime.timedelta(days=7)


WATERMARK_COLUMNS = (
    str_col("SourceSystemCode", 8, nullable=False),
    str_col("SourceObjectName", 60, nullable=False),
    ts_col("LastExtractedWatermark"),
    ts_col("LastRunStartedWhen"),
    ts_col("LastRunCompletedWhen"),
    int_col("LastRowCount"),
    str_col("WatermarkColumnName", 40),
    str_col("LoadTypeCode", 12, note="FULL / INCREMENTAL / CDC"),
    flag_col("IsEnabled"),
) + EDIT_COLUMNS

_WATERMARK_OBJECTS = (
    ("ERPNA", "WWI_MDM.CUST_MASTER", "LAST_UPD_TS", "INCREMENTAL"),
    ("ERPEU", "WWI_MDM.CUST_MASTER", "LAST_UPD_TS", "INCREMENTAL"),
    ("ERPAP", "WWI_MDM.CUST_MASTER", "LAST_UPD_TS", "FULL"),
    ("ERPNA", "WWI_PROC.PURCHASE_ORDER_HDR", "LAST_UPD_TS", "INCREMENTAL"),
    ("ERPNA", "WWI_FIN.AP_INVOICE_HDR", "CREATED_TS", "INCREMENTAL"),
    ("ERPEU", "WWI_FIN.AP_PAYMENT", "CREATED_TS", "INCREMENTAL"),
    ("ERPAP", "WWI_REF.FX_RATE_DAILY", "RATE_DT", "FULL"),
    ("WWIOLTP", "Sales.Orders", "LastEditedWhen", "CDC"),
    ("WWIOLTP", "Sales.OrderLines", "LastEditedWhen", "CDC"),
    ("WWIOLTP", "Warehouse.StockItemTransactions", "TransactionRecordedWhen", "INCREMENTAL"),
    ("WWIOLTP", "Shipping.ShipmentHeaders", "LastEditedWhen", "INCREMENTAL"),
    ("WWIOLTP", "Ecommerce.WebSessions", "SessionStartedWhen", "INCREMENTAL"),
)


def produce_change_tracking_watermark(cfg, ctx):
    seed = cfg.seed
    snapshot = cfg.snapshot_date
    for index, (system, obj, column, load_type) in enumerate(_WATERMARK_OBJECTS):
        started = datetime.datetime(snapshot.year, snapshot.month, snapshot.day,
                                    1 + index % 5, rng.stable_hash(seed, "wm-m", index) % 60, 0)
        yield (
            system, obj,
            datetime.datetime(cfg.history_end.year, cfg.history_end.month,
                              cfg.history_end.day, 23, 59, 59),
            started,
            started + datetime.timedelta(minutes=2 + rng.stable_hash(seed, "wm-dur", index) % 120),
            rng.stable_hash(seed, "wm-rows", index) % 900000,
            column, load_type, "Y",
        ) + _edited(cfg, "wm", index, snapshot)


def _spec(schema_name, name, columns, produce, row_count_key, target, description,
          group, tags=()):
    return TableSpec(
        key="sqlserver.%s.%s" % (schema_name, name),
        system=schema.SQLSERVER,
        schema=schema_name,
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group=group,
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("Warehouse", "StockItems", STOCK_ITEM_COLUMNS, produce_stock_items, "products",
          "raw.SqlStockItem",
          "OLTP stock items carrying the ERP item code, sometimes a stale one.",
          "sqlserver_warehouse", ("crosswalk",)),
    _spec("Warehouse", "StockItemTransactions", STOCK_TXN_COLUMNS,
          produce_stock_item_transactions, "stock_movements", "raw.SqlStockMovement",
          "Inventory movements with buffered handheld timestamps.",
          "sqlserver_warehouse", ("skew", "outoforder", "latearriving")),
    _spec("Warehouse", "Bins", BIN_COLUMNS, produce_bins, "", "raw.SqlStockMovement",
          "Bin locations per warehouse site and zone.", "sqlserver_warehouse"),
    _spec("Shipping", "ShipmentHeaders", SHIPMENT_HDR_COLUMNS, produce_shipment_headers,
          "shipments", "raw.SqlShipment",
          "Shipments with carrier, service level and promised versus actual delivery.",
          "sqlserver_shipping"),
    _spec("Shipping", "ShipmentLines", SHIPMENT_LINE_COLUMNS, produce_shipment_lines,
          "", "raw.SqlShipmentLine",
          "Shipment lines including short-shipped quantities.", "sqlserver_shipping"),
    _spec("Shipping", "ShipmentEvents", SHIPMENT_EVENT_COLUMNS, produce_shipment_events,
          "", "raw.SqlShipment",
          "Carrier scan events, the OLTP counterpart of the carrier file feed.",
          "sqlserver_shipping", ("reconciliation",)),
    _spec("Returns", "ReturnLines", RETURN_LINE_COLUMNS, produce_return_lines, "returns",
          "raw.SqlReturnLine",
          "Returns with regional reason vocabularies and disposition outcomes.",
          "sqlserver_returns", ("regional",)),
    _spec("Returns", "CreditNoteLines", CREDIT_NOTE_LINE_COLUMNS, produce_credit_note_lines,
          "credit_notes", "raw.SqlCreditNote",
          "Credit notes raised against returns, taxed under the regional rule.",
          "sqlserver_returns", ("regional",)),
    _spec("Loyalty", "LoyaltyPointsLedger", LOYALTY_LEDGER_COLUMNS,
          produce_loyalty_points_ledger, "loyalty_ledger", "raw.SqlLoyaltyLedger",
          "Points ledger with per-region expiry horizons.",
          "sqlserver_loyalty", ("regional",)),
    _spec("Ecommerce", "WebSessions", WEB_SESSION_COLUMNS, produce_web_sessions,
          "web_sessions", "raw.SqlWebSession",
          "Web sessions, mostly anonymous, carrying the regional consent decision.",
          "sqlserver_ecommerce", ("regional", "skew")),
    _spec("Integration", "OutboundInterfaceQueue", OUTBOUND_QUEUE_COLUMNS,
          produce_outbound_interface_queue, "", "raw.SqlOrder",
          "Outbound interface queue including failed, retried entries.",
          "sqlserver_integration", ("dataquality",)),
    _spec("Integration", "InboundFileRegister", INBOUND_REGISTER_COLUMNS,
          produce_inbound_file_register, "", "raw.FilePartnerSales",
          "Register of every file the landing zone received, with reject counts.",
          "sqlserver_integration", ("dataquality",)),
    _spec("Integration", "ChangeTrackingWatermark", WATERMARK_COLUMNS,
          produce_change_tracking_watermark, "", "raw.SqlOrder",
          "Extract watermarks per source object and load type.",
          "sqlserver_integration"),
)
