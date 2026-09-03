"""SQL Server OLTP - the order-to-cash side.

These are the WideWorldImporters order management tables as the estate holds
them, keyed by integer identity values rather than by the ERP's character
codes. The link back to Oracle is the crosswalk in WWI_MDM.PARTY_XREF, and it
is deliberately imperfect: some customers here have no ERP counterpart at all
because they were created in the web channel and never pushed to the master.

Money on this side is always in the customer's transaction currency with no
base-currency column - the warehouse has to convert, which is why the FX
reconciliation exists.
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
        1 + rng.stable_hash(seed, "edit-by", kind, ordinal) % max(cfg.count("employees"), 1),
        datetime.datetime(when.year, when.month, when.day,
                          rng.stable_hash(seed, "edit-h", kind, ordinal) % 24,
                          rng.stable_hash(seed, "edit-m", kind, ordinal) % 60,
                          rng.stable_hash(seed, "edit-s", kind, ordinal) % 60),
    )


# ---------------------------------------------------------------------------
# Sales.Customers
# ---------------------------------------------------------------------------

CUSTOMER_COLUMNS = (
    int_col("CustomerID", nullable=False),
    str_col("CustomerName", 100, nullable=False),
    int_col("BillToCustomerID"),
    int_col("CustomerCategoryID"),
    int_col("BuyingGroupID"),
    int_col("PrimaryContactPersonID"),
    int_col("DeliveryMethodID"),
    int_col("DeliveryCityID"),
    int_col("PostalCityID"),
    dec_col("CreditLimit", 18, 2),
    date_col("AccountOpenedDate"),
    dec_col("StandardDiscountPercentage", 18, 3),
    flag_col("IsStatementSent"),
    flag_col("IsOnCreditHold"),
    int_col("PaymentDays"),
    str_col("PhoneNumber", 20),
    str_col("FaxNumber", 20, note="still populated for accounts opened before 2006"),
    str_col("DeliveryRun", 5),
    str_col("RunPosition", 5),
    str_col("WebsiteURL", 256),
    str_col("DeliveryAddressLine1", 60),
    str_col("DeliveryAddressLine2", 60),
    str_col("DeliveryPostalCode", 10),
    str_col("ErpCustomerCode", 12, note="null where the account was never mastered in the ERP"),
) + EDIT_COLUMNS


def produce_customers(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("customers")):
        cust = entities.customer(cfg, ordinal)
        address = cust.addresses[-1]
        # The crosswalk gap: web-created accounts that never reached the ERP.
        erp_code = None if cust.crosswalk == keys.CROSSWALK_MISSING else cust.erp_code
        yield (
            cust.oltp_id, cust.name,
            keys.oltp_customer_id(cust.duplicate_of if cust.duplicate_of >= 0 else ordinal),
            1 + rng.stable_hash(seed, "cat", ordinal) % 8,
            (1 + rng.stable_hash(seed, "bg", ordinal) % 3) if cust.buying_group else None,
            3000 + ordinal,
            1 + rng.stable_hash(seed, "delivmeth", ordinal) % 6,
            1 + rng.stable_hash(seed, "delivcity", ordinal) % 900,
            1 + rng.stable_hash(seed, "postcity", ordinal) % 900,
            cust.credit_limit, cust.opened_date,
            round(rng.stable_hash(seed, "disc", ordinal) % 1200 / 100.0, 3),
            "Y" if rng.chance(seed, 0.62, "stmt", ordinal) else "N",
            "Y" if cust.status_code in ("SUSP", "GESP", "HOLD") else "N",
            cust.payment_terms_days,
            regions.phone_number(cust.country, rng.stable_hash(seed, "phone", ordinal)),
            regions.phone_number(cust.country, rng.stable_hash(seed, "fax", ordinal))
            if cust.opened_date.year < 2006 else None,
            "%s%d" % (rng.pick(seed, ("A", "B", "C", "D", "E"), "run", ordinal),
                      rng.stable_hash(seed, "run-no", ordinal) % 9),
            "%02d" % (rng.stable_hash(seed, "runpos", ordinal) % 60),
            "http://www.%s.example" % cust.name.split()[0].lower().strip(".,&"),
            address.line1, address.line2, address.postal_code, erp_code,
        ) + _edited(cfg, "cust", ordinal, cust.opened_date)


# ---------------------------------------------------------------------------
# Sales.Orders / Sales.OrderLines
# ---------------------------------------------------------------------------

ORDER_COLUMNS = (
    int_col("OrderID", nullable=False),
    int_col("CustomerID", nullable=False),
    int_col("SalespersonPersonID"),
    int_col("PickedByPersonID"),
    int_col("ContactPersonID"),
    int_col("BackorderOrderID"),
    date_col("OrderDate"),
    date_col("ExpectedDeliveryDate"),
    str_col("CustomerPurchaseOrderNumber", 20),
    flag_col("IsUndersupplyBackordered"),
    str_col("Comments", 200),
    str_col("DeliveryInstructions", 200),
    str_col("InternalComments", 200),
    ts_col("OrderedWhen", note="captured by the channel; web orders arrive out of order"),
    ts_col("RecordedWhen", note="when the OLTP actually saw the row"),
    ts_col("PickingCompletedWhen"),
    str_col("OrderStatusCode", 8),
    str_col("ChannelCode", 8),
    str_col("CurrencyCode", 3),
    flag_col("IsCorrection", note="restatement of an order previously sent downstream"),
) + EDIT_COLUMNS


def produce_orders(cfg, ctx):
    seed = cfg.seed
    for ordinal in range(cfg.count("orders")):
        doc = documents.order(cfg, ctx, ordinal)
        cust = entities.customer(cfg, doc.customer_ordinal)
        picked = None
        if doc.status_code not in ("OPEN", "OFFEN", "NEW"):
            picked = doc.ordered_at + datetime.timedelta(
                hours=2 + rng.stable_hash(seed, "pick", ordinal) % 90)
        yield (
            doc.order_id, cust.oltp_id, 1 + doc.salesperson_ordinal,
            (1 + rng.stable_hash(seed, "picker", ordinal) % max(cfg.count("employees"), 1))
            if picked else None,
            3000 + doc.customer_ordinal,
            keys.oltp_order_id(ordinal - 1) if doc.is_backorder and ordinal else None,
            doc.ordered_on, doc.delivery_promise, doc.order_reference,
            "Y" if doc.is_backorder else "N",
            "Correction of an earlier transmission" if doc.is_correction else None,
            rng.pick(seed, ("Leave at dock", "Call on arrival", "Rear entrance only",
                            "No deliveries before 9am", "Signature required"),
                     "deliv-instr", ordinal),
            None,
            doc.ordered_at, doc.recorded_at, picked,
            doc.status_code, doc.channel_code, doc.currency,
            "Y" if doc.is_correction else "N",
        ) + _edited(cfg, "order", ordinal, doc.ordered_on)


ORDER_LINE_COLUMNS = (
    int_col("OrderLineID", nullable=False),
    int_col("OrderID", nullable=False),
    int_col("StockItemID"),
    str_col("Description", 100, note="denormalised at order time; drifts from the item master"),
    int_col("PackageTypeID"),
    int_col("Quantity"),
    dec_col("UnitPrice", 18, 2),
    dec_col("UnitCost", 18, 4),
    dec_col("DiscountPercentage", 18, 3),
    dec_col("TaxRate", 18, 3),
    str_col("TaxCode", 16),
    dec_col("LineNetAmount", 18, 2),
    dec_col("LineTaxAmount", 18, 2),
    int_col("PickedQuantity"),
    int_col("BackorderQuantity"),
    ts_col("PickingCompletedWhen"),
) + EDIT_COLUMNS


def produce_order_lines(cfg, ctx):
    seed = cfg.seed
    line_id = 0
    for ordinal in range(cfg.count("orders")):
        doc = documents.order(cfg, ctx, ordinal)
        for line in documents.order_lines(cfg, ctx, doc):
            line_id += 1
            prod = entities.product(cfg, line.product_ordinal)
            yield (
                line_id, doc.order_id, prod.oltp_id,
                # The description was copied at order time, so old orders keep
                # the old product name after a rename.
                prod.name,
                1 + rng.stable_hash(seed, "pkg", line.product_ordinal) % 7,
                line.quantity, line.unit_price, line.unit_cost, line.discount_pct,
                line.tax_rate, line.tax_code, line.net_amount, line.tax_amount,
                line.picked_quantity, line.backorder_quantity,
                doc.ordered_at + datetime.timedelta(
                    hours=3 + rng.stable_hash(seed, "line-pick", ordinal, line.line_number) % 70)
                if line.picked_quantity else None,
            ) + _edited(cfg, "ol", line_id, doc.ordered_on)


# ---------------------------------------------------------------------------
# Sales.Invoices / Sales.InvoiceLines
# ---------------------------------------------------------------------------

INVOICE_COLUMNS = (
    int_col("InvoiceID", nullable=False),
    int_col("CustomerID"),
    int_col("BillToCustomerID"),
    int_col("OrderID"),
    int_col("DeliveryMethodID"),
    int_col("ContactPersonID"),
    int_col("AccountsPersonID"),
    int_col("SalespersonPersonID"),
    int_col("PackedByPersonID"),
    date_col("InvoiceDate"),
    str_col("CustomerPurchaseOrderNumber", 20),
    flag_col("IsCreditNote"),
    str_col("CreditNoteReason", 200),
    dec_col("TotalDryItems", 18, 0),
    dec_col("TotalChillerItems", 18, 0),
    str_col("DeliveryRun", 5),
    ts_col("ConfirmedDeliveryWhen"),
    str_col("ConfirmedReceivedBy", 60),
    dec_col("InvoiceNetAmount", 18, 2),
    dec_col("InvoiceTaxAmount", 18, 2),
    str_col("CurrencyCode", 3),
) + EDIT_COLUMNS


def produce_invoices(cfg, ctx):
    seed = cfg.seed
    invoice_id = 0
    for ordinal in range(cfg.count("orders")):
        doc = documents.order(cfg, ctx, ordinal)
        if not documents.order_is_invoiced(cfg, doc):
            continue
        invoice_id += 1
        cust = entities.customer(cfg, doc.customer_ordinal)
        net = 0.0
        tax = 0.0
        dry = 0
        chilled = 0
        for line in documents.order_lines(cfg, ctx, doc):
            net += line.net_amount
            tax += line.tax_amount
            if entities.product(cfg, line.product_ordinal).is_chiller:
                chilled += line.quantity
            else:
                dry += line.quantity
        invoiced_on = timeline.business_days_after(
            doc.ordered_on, 1 + rng.stable_hash(seed, "inv-lag", ordinal) % 6)
        credit_note = rng.chance(seed, 0.014, "inv-cn", ordinal)
        yield (
            keys.oltp_invoice_id(invoice_id), cust.oltp_id, cust.oltp_id, doc.order_id,
            1 + rng.stable_hash(seed, "inv-dm", ordinal) % 6,
            3000 + doc.customer_ordinal,
            1 + rng.stable_hash(seed, "inv-acct", ordinal) % max(cfg.count("employees"), 1),
            1 + doc.salesperson_ordinal,
            1 + rng.stable_hash(seed, "inv-pack", ordinal) % max(cfg.count("employees"), 1),
            min(invoiced_on, cfg.history_end), doc.order_reference,
            "Y" if credit_note else "N",
            "Goods returned - see credit note" if credit_note else None,
            dry, chilled,
            "%s%d" % (rng.pick(seed, ("A", "B", "C"), "inv-run", ordinal),
                      rng.stable_hash(seed, "inv-runno", ordinal) % 9),
            datetime.datetime(invoiced_on.year, invoiced_on.month, invoiced_on.day,
                              8 + rng.stable_hash(seed, "inv-conf", ordinal) % 10, 0, 0),
            "%s %s" % text.person_name(seed, doc.customer_ordinal),
            round(net, 2), round(tax, 2), doc.currency,
        ) + _edited(cfg, "inv", ordinal, invoiced_on)


INVOICE_LINE_COLUMNS = (
    int_col("InvoiceLineID", nullable=False),
    int_col("InvoiceID"),
    int_col("StockItemID"),
    str_col("Description", 100),
    int_col("PackageTypeID"),
    int_col("Quantity"),
    dec_col("UnitPrice", 18, 2),
    dec_col("TaxRate", 18, 3),
    dec_col("TaxAmount", 18, 2),
    dec_col("LineProfit", 18, 2),
    dec_col("ExtendedPrice", 18, 2),
) + EDIT_COLUMNS


def produce_invoice_lines(cfg, ctx):
    invoice_id = 0
    line_id = 0
    for ordinal in range(cfg.count("orders")):
        doc = documents.order(cfg, ctx, ordinal)
        if not documents.order_is_invoiced(cfg, doc):
            continue
        invoice_id += 1
        for line in documents.order_lines(cfg, ctx, doc):
            line_id += 1
            prod = entities.product(cfg, line.product_ordinal)
            yield (
                line_id, keys.oltp_invoice_id(invoice_id), prod.oltp_id, prod.name,
                1 + line.product_ordinal % 7, line.quantity, line.unit_price,
                line.tax_rate, line.tax_amount,
                round(line.net_amount - line.unit_cost * line.quantity, 2),
                round(line.net_amount + line.tax_amount, 2),
            ) + _edited(cfg, "invl", line_id, doc.ordered_on)


# ---------------------------------------------------------------------------
# Sales.CustomerTransactions
# ---------------------------------------------------------------------------

CUSTOMER_TXN_COLUMNS = (
    int_col("CustomerTransactionID", nullable=False),
    int_col("CustomerID"),
    int_col("TransactionTypeID"),
    int_col("InvoiceID"),
    int_col("PaymentMethodID"),
    date_col("TransactionDate"),
    dec_col("AmountExcludingTax", 18, 2),
    dec_col("TaxAmount", 18, 2),
    dec_col("TransactionAmount", 18, 2),
    dec_col("OutstandingBalance", 18, 2),
    date_col("FinalizationDate"),
    flag_col("IsFinalized"),
    str_col("CurrencyCode", 3),
) + EDIT_COLUMNS


def produce_customer_transactions(cfg, ctx):
    """Receipts against invoices. Part-payments leave an outstanding balance."""
    seed = cfg.seed
    total = cfg.count("invoices")
    for ordinal in range(total):
        customer_ordinal = skew.pareto_ordinal(seed, cfg.count("customers"), "ctxn-cust", ordinal)
        cust = entities.customer(cfg, customer_ordinal)
        when = ctx.dates.date_for(seed, "ctxn-when", ordinal)
        net = round(50 + rng.stable_hash(seed, "ctxn-amt", ordinal) % 5000000 / 100.0, 2)
        _code, _rate, tax = regions.tax_treatment(cust.region, cust.country, net,
                                                  cust.tax_registered)
        outstanding = 0.0
        finalized = True
        if rng.chance(seed, 0.14, "ctxn-part", ordinal):
            outstanding = round(net * 0.35, 2)
            finalized = False
        yield (
            ordinal + 1, cust.oltp_id,
            1 + rng.stable_hash(seed, "ctxn-type", ordinal) % 4,
            keys.oltp_invoice_id(1 + ordinal % max(total, 1)),
            1 + rng.stable_hash(seed, "ctxn-pm", ordinal) % 4,
            when, net, tax, round(net + tax, 2), outstanding,
            when + datetime.timedelta(days=cust.payment_terms_days) if finalized else None,
            "Y" if finalized else "N", cust.country.currency,
        ) + _edited(cfg, "ctxn", ordinal, when)


# ---------------------------------------------------------------------------
# Sales.Promotions / PromotionRedemptions / SalesTerritories / SalesQuotas
# ---------------------------------------------------------------------------

PROMOTION_COLUMNS = (
    int_col("PromotionID", nullable=False),
    str_col("PromotionCode", 16),
    str_col("PromotionName", 80),
    str_col("RegionCode", 4),
    date_col("ValidFrom"),
    date_col("ValidTo"),
    str_col("DiscountTypeCode", 8, note="PCT / AMT / BOGO"),
    dec_col("DiscountValue", 18, 3),
    dec_col("MinimumOrderValue", 18, 2),
    str_col("CurrencyCode", 3),
    flag_col("IsActive"),
    flag_col("RequiresCoupon"),
) + EDIT_COLUMNS

_PROMO_NAMES = ("Winter clearance", "Chiller launch", "Volume rebate",
                "Loyalty double points", "Freight free week", "End of range")


def produce_promotions(cfg, ctx):
    seed = cfg.seed
    for index in range(240):
        region = regions.REGIONS[index % 3]
        start = cfg.history_start + datetime.timedelta(
            days=rng.stable_hash(seed, "promo-start", index) % max(cfg.history_days, 1))
        yield (
            index + 1, "PR%s%04d" % (region[:2], index),
            "%s %s %d" % (region, rng.pick(seed, _PROMO_NAMES, "promo-nm", index), index),
            region, start, start + datetime.timedelta(days=14 + index % 60),
            rng.pick(seed, ("PCT", "AMT", "BOGO"), "promo-type", index),
            round(rng.stable_hash(seed, "promo-val", index) % 2500 / 100.0, 3),
            round(rng.stable_hash(seed, "promo-min", index) % 200000 / 100.0, 2),
            regions.REPORTING_CURRENCY[region],
            "Y" if start + datetime.timedelta(days=14 + index % 60) >= cfg.history_end else "N",
            "Y" if rng.chance(seed, 0.35, "promo-coupon", index) else "N",
        ) + _edited(cfg, "promo", index, start)


REDEMPTION_COLUMNS = (
    int_col("RedemptionID", nullable=False),
    int_col("PromotionID"),
    int_col("OrderID"),
    int_col("CustomerID"),
    date_col("RedeemedDate"),
    dec_col("DiscountAmount", 18, 2),
    str_col("CouponCode", 20),
    str_col("CurrencyCode", 3),
) + EDIT_COLUMNS


def produce_promotion_redemptions(cfg, ctx):
    seed = cfg.seed
    redemption = 0
    for ordinal in range(cfg.count("orders")):
        if not rng.chance(seed, 0.09, "redeem", ordinal):
            continue
        redemption += 1
        doc = documents.order(cfg, ctx, ordinal)
        cust = entities.customer(cfg, doc.customer_ordinal)
        yield (
            redemption, 1 + rng.stable_hash(seed, "redeem-promo", ordinal) % 240,
            doc.order_id, cust.oltp_id, doc.ordered_on,
            round(rng.stable_hash(seed, "redeem-amt", ordinal) % 40000 / 100.0, 2),
            "CPN%08d" % (rng.stable_hash(seed, "coupon", ordinal) % 10 ** 8),
            doc.currency,
        ) + _edited(cfg, "redeem", redemption, doc.ordered_on)


TERRITORY_COLUMNS = (
    int_col("SalesTerritoryID", nullable=False),
    str_col("TerritoryCode", 10),
    str_col("TerritoryName", 60),
    str_col("RegionCode", 4),
    str_col("CountryCode", 2),
    int_col("ManagerPersonID"),
    date_col("ValidFrom"),
    date_col("ValidTo"),
    flag_col("IsActive"),
) + EDIT_COLUMNS


def produce_sales_territories(cfg, ctx):
    seed = cfg.seed
    index = 0
    for region in regions.REGIONS:
        for country in regions.countries(region):
            for slot in range(4):
                index += 1
                yield (
                    index, "%s-%s-%02d" % (region[:2], country.code, slot),
                    "%s %s territory %d" % (country.name, region, slot + 1),
                    region, country.code,
                    1 + rng.stable_hash(seed, "terr-mgr", index) % max(cfg.count("employees"), 1),
                    cfg.history_start, None, "Y",
                ) + _edited(cfg, "terr", index, cfg.history_start)


QUOTA_COLUMNS = (
    int_col("SalesQuotaID", nullable=False),
    int_col("SalespersonPersonID"),
    int_col("SalesTerritoryID"),
    str_col("PeriodCode", 12, note="the salesperson's regional fiscal period"),
    dec_col("QuotaAmount", 18, 2),
    dec_col("AchievedAmount", 18, 2),
    str_col("CurrencyCode", 3),
    flag_col("IsFinal"),
) + EDIT_COLUMNS


def produce_sales_quotas(cfg, ctx):
    seed = cfg.seed
    quota = 0
    for ordinal in range(cfg.count("employees")):
        employee = entities.employee(cfg, ordinal)
        if not employee.is_salesperson:
            continue
        when = cfg.history_start.replace(day=1)
        while when <= cfg.history_end:
            quota += 1
            target = round(50000 + rng.stable_hash(seed, "quota", ordinal, quota) % 40000000 / 100.0, 2)
            yield (
                quota, 1 + ordinal,
                1 + rng.stable_hash(seed, "quota-terr", ordinal) % 40,
                ctx.fiscal_period(employee.region, when), target,
                round(target * (rng.stable_hash(seed, "quota-ach", ordinal, quota) % 180) / 100.0, 2),
                regions.REPORTING_CURRENCY[employee.region],
                "Y" if when < cfg.history_end.replace(day=1) else "N",
            ) + _edited(cfg, "quota", quota, when)
            when = timeline.add_months(when, 3)


def _spec(schema_name, name, columns, produce, row_count_key, target, description, tags=()):
    return TableSpec(
        key="sqlserver.%s.%s" % (schema_name, name),
        system=schema.SQLSERVER,
        schema=schema_name,
        name=name,
        columns=columns,
        produce=produce,
        row_count_key=row_count_key,
        target_object=target,
        group="sqlserver_sales",
        description=description,
        tags=tags,
    )


SPECS = (
    _spec("Sales", "Customers", CUSTOMER_COLUMNS, produce_customers, "customers",
          "raw.SqlOrder",
          "OLTP customer accounts, some with no ERP counterpart at all.",
          ("crosswalk", "dataquality")),
    _spec("Sales", "Orders", ORDER_COLUMNS, produce_orders, "orders", "raw.SqlOrder",
          "Sales orders with channel capture time and OLTP recording time.",
          ("outoforder", "skew")),
    _spec("Sales", "OrderLines", ORDER_LINE_COLUMNS, produce_order_lines, "order_lines",
          "raw.SqlOrderLine",
          "Order lines carrying the product description as it was at order time.",
          ("skew",)),
    _spec("Sales", "Invoices", INVOICE_COLUMNS, produce_invoices, "invoices", "raw.SqlInvoice",
          "Invoices raised from picked orders, including credit notes.", ()),
    _spec("Sales", "InvoiceLines", INVOICE_LINE_COLUMNS, produce_invoice_lines, "invoice_lines",
          "raw.SqlInvoiceLine",
          "Invoice lines with line profit computed against the cost of the day.", ()),
    _spec("Sales", "CustomerTransactions", CUSTOMER_TXN_COLUMNS, produce_customer_transactions,
          "invoices", "raw.SqlInvoice",
          "Cash receipts with part-payments left outstanding.", ("regional",)),
    _spec("Sales", "Promotions", PROMOTION_COLUMNS, produce_promotions, "", "raw.SqlOrder",
          "Regional promotions with different discount mechanics per region.",
          ("regional",)),
    _spec("Sales", "PromotionRedemptions", REDEMPTION_COLUMNS, produce_promotion_redemptions,
          "", "raw.SqlOrderLine",
          "Promotion redemptions tied to the orders that used them.", ()),
    _spec("Sales", "SalesTerritories", TERRITORY_COLUMNS, produce_sales_territories, "",
          "raw.SqlOrder", "Sales territories per country and region.", ("regional",)),
    _spec("Sales", "SalesQuotas", QUOTA_COLUMNS, produce_sales_quotas, "", "raw.SqlOrder",
          "Quarterly quotas in each salesperson's own fiscal calendar.", ("regional",)),
)
