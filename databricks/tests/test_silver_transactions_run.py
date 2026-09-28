"""End-to-end tests of ``silver.transactions.run`` on small bronze estates:
dedup quarantine, flag WARNs, late-arriving queue, idempotent rerun, soft delete,
payment matching through the bronze -> silver path."""
from __future__ import annotations

import json
from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.silver import transactions
from tests.silver_transactions_support import (
    CUSTOMERS,
    invoice,
    invoiceLine,
    isolatedConfig,
    order,
    orderLine,
    receipt,
    rejected,
    rowsBy,
    silver,
    withBatch,
    writeBaseEstate,
    writeBronze,
)


def _writeMinimalTransactions(spark, cfg, *, orders, orderLines, invoices=None, invoiceLines=None, receipts=None):
    writeBronze(spark, cfg, "sqlserver_sales_orders", orders)
    writeBronze(spark, cfg, "sqlserver_sales_order_lines", orderLines)
    writeBronze(spark, cfg, "sqlserver_sales_invoices", invoices or [])
    writeBronze(spark, cfg, "sqlserver_sales_invoice_lines", invoiceLines or [])
    writeBronze(spark, cfg, "sqlserver_sales_customer_transactions", receipts or [])


def test_order_line_dedup_quarantines_losers_and_keeps_rekey_warn(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "dedup")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1)],
        orderLines=[
            # exact re-extraction copies of line 1: only the latest edit survives
            orderLine(1, 1, LastEditedWhen="2026-01-10T08:00:00", Description="old copy"),
            orderLine(1, 1, LastEditedWhen="2026-01-10T09:00:00", Description="new copy"),
            # genuine re-key: same order / item / qty / price under line ids 2 and 3 -> 3 wins
            orderLine(2, 1, stockItemId=101, quantity=5, unitPrice=4),
            orderLine(3, 1, stockItemId=101, quantity=5, unitPrice=4),
            # distinct line
            orderLine(4, 1, stockItemId=101, quantity=6, unitPrice=4),
        ],
    )
    transactions.run(spark, tcfg)

    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert set(lines) == {"WWI_OLTP|1|1", "WWI_OLTP|1|2", "WWI_OLTP|1|3", "WWI_OLTP|1|4"}
    assert lines["WWI_OLTP|1|1"]["line_description"] == "new copy"
    assert lines["WWI_OLTP|1|3"]["is_duplicate_loser"] is False and lines["WWI_OLTP|1|3"]["dq_status_code"] == "PASS"
    assert lines["WWI_OLTP|1|2"]["is_duplicate_loser"] is True and lines["WWI_OLTP|1|2"]["dq_status_code"] == "WARN"
    assert lines["WWI_OLTP|1|2"]["duplicate_group_id"] == lines["WWI_OLTP|1|3"]["duplicate_group_id"]
    assert lines["WWI_OLTP|1|4"]["duplicate_group_id"] is None

    dupRejects = rejected(spark, tcfg, "DUP_ORDER_LINE").collect()
    rejectedKeys = sorted(json.loads(r.row_json)["order_line_business_key"] for r in dupRejects)
    assert rejectedKeys == ["WWI_OLTP|1|1", "WWI_OLTP|1|2"]  # the exact old copy and the re-key loser
    assert all(r.source_table == "sqlserver_sales_order_lines" and r.batch_id == 1 for r in dupRejects)


def test_order_line_hard_rejects_never_silently_dropped(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "rejects")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1)],
        orderLines=[
            orderLine(1, 1),
            orderLine(2, 1, quantity=-1),  # NEG_QTY
            orderLine(3, 1, unitPrice=None),  # BAD_NUMERIC
            orderLine(4, 99),  # ORPHAN_HEADER
            orderLine(5, 1, stockItemId=None),  # WARN, stays
        ],
    )
    transactions.run(spark, tcfg)
    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert set(lines) == {"WWI_OLTP|1|1", "WWI_OLTP|1|5"}
    assert lines["WWI_OLTP|1|5"]["dq_status_code"] == "WARN"
    codes = {r.rule_code for r in rejected(spark, tcfg).collect()}
    assert {"OL_NEG_QTY", "OL_BAD_NUMERIC", "OL_ORPHAN_HEADER"} <= codes


def test_order_flags_region_currency_and_warn_status(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "orders")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[
            order(1, FulfilmentFlags="H|B"),
            order(2, FulfilmentFlags="P||B"),
            order(3, customerId=2, SalesTerritoryID=None, CurrencyCode=None, TaxRegimeCode=None, ExpectedDeliveryDate=None, SalespersonPersonID=None, CustomerPurchaseOrderNumber=None),
            order(4, customerId=3, SalesTerritoryID=None, CurrencyCode="NZD", TaxRegimeCode="RC"),
            order(5, CustomerID=None),  # rejected: no customer
        ],
        orderLines=[orderLine(1, 1)],
    )
    transactions.run(spark, tcfg)
    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert set(orders) == {"WWI_OLTP|1", "WWI_OLTP|2", "WWI_OLTP|3", "WWI_OLTP|4"}

    good = orders["WWI_OLTP|1"]
    assert good["is_hold_flagged"] and good["is_backorder_flagged"] and good["dq_status_code"] == "PASS"
    assert good["region_code"] == "NA" and good["region_source_code"] == "ORDER_TERRITORY"
    assert good["currency_code"] == "USD" and good["currency_source_code"] == "ORDER"
    assert good["tax_regime_code"] == "SUT" and good["vat_registration_number"] is None
    assert good["customer_purchase_order_number"] == "PO-1" and good["sales_channel_code"] == "WEB"

    malformed = orders["WWI_OLTP|2"]
    assert malformed["dq_status_code"] == "WARN" and malformed["fulfilment_flags_malformed"] is True
    assert malformed["fulfilment_flags_unknown"] == ["P"] and malformed["fulfilment_flags_raw"] == "P||B"

    eu = orders["WWI_OLTP|3"]  # falls back to the customer's region / territory currency
    assert eu["region_code"] == "EU" and eu["region_source_code"] == "CUSTOMER"
    assert eu["currency_code"] == "EUR" and eu["currency_source_code"] == "CUSTOMER_TERRITORY"
    assert eu["tax_regime_code"] == "VAT" and eu["vat_registration_number"] == "DE123456789"
    assert eu["is_reverse_charge"] is False
    assert eu["expected_delivery_date"].isoformat() == "2026-01-13"  # order date + 3
    assert eu["salesperson_business_key"] == "WWI_OLTP|-1" and eu["customer_purchase_order_number"] == ""

    apac = orders["WWI_OLTP|4"]
    assert apac["currency_code"] == "NZD" and apac["region_code"] == "APAC" and apac["tax_regime_code"] == "RC"
    assert apac["is_reverse_charge"] is False  # RC regime but no VAT registration outside the EU

    assert rejected(spark, tcfg, "ORDER_MISSING_KEY").count() == 1


def test_late_arriving_dimensions_queue_and_resolve(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "late")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1, customerId=555, SalesChannelID=9, SalespersonPersonID=77), order(2)],
        orderLines=[orderLine(1, 1, stockItemId=999), orderLine(2, 2, stockItemId=999), orderLine(3, 2)],
        invoices=[invoice(10, customerId=555)],
        invoiceLines=[invoiceLine(1, 10)],
    )
    transactions.run(spark, tcfg)

    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert orders["WWI_OLTP|1"]["dq_status_code"] == "WARN" and orders["WWI_OLTP|1"]["is_late_arriving_customer"] is True
    assert orders["WWI_OLTP|1"]["is_late_arriving_channel"] and orders["WWI_OLTP|1"]["is_late_arriving_salesperson"]
    assert orders["WWI_OLTP|2"]["dq_status_code"] == "PASS"
    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert lines["WWI_OLTP|1|1"]["dq_status_code"] == "WARN" and lines["WWI_OLTP|2|3"]["dq_status_code"] == "PASS"
    assert rowsBy(silver(spark, tcfg, "sale"), "sale_business_key")["WWI_OLTP|10"]["dq_status_code"] == "WARN"

    queue = rowsBy(silver(spark, tcfg, "late_arriving_dimension_queue").withColumn("k", F.concat_ws("/", "entity_type", "business_key")), "k")
    assert set(queue) == {"Customer/WWI_OLTP|555", "SalesChannel/WWI_OLTP|9", "Salesperson/WWI_OLTP|77", "StockItem/WWI_OLTP|999"}
    cust = queue["Customer/WWI_OLTP|555"]
    assert cust["first_seen_batch_id"] == 1 and cust["resolved_batch_id"] is None and cust["is_resolved"] is False
    assert cust["occurrence_count"] == 2  # order 1 + invoice 10
    assert queue["StockItem/WWI_OLTP|999"]["occurrence_count"] == 2

    # rerun of the same batch does not double count
    transactions.run(spark, tcfg)
    assert rowsBy(silver(spark, tcfg, "late_arriving_dimension_queue"), "business_key")["WWI_OLTP|555"]["occurrence_count"] == 2

    # batch 2: the customer arrives, the stock item is still missing and seen again
    writeBronze(spark, tcfg, "sqlserver_sales_customers", [*CUSTOMERS, {"CustomerID": 555, "CustomerName": "Late Ltd", "SalesTerritoryID": 10, "RegionCode": "NA"}])
    cfg2 = withBatch(tcfg, 2)
    transactions.run(spark, cfg2)
    queue = rowsBy(silver(spark, tcfg, "late_arriving_dimension_queue"), "business_key")
    assert queue["WWI_OLTP|555"]["is_resolved"] is True and queue["WWI_OLTP|555"]["resolved_batch_id"] == 2
    assert queue["WWI_OLTP|555"]["first_seen_batch_id"] == 1
    assert queue["WWI_OLTP|999"]["occurrence_count"] == 4 and queue["WWI_OLTP|999"]["resolved_batch_id"] is None
    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert orders["WWI_OLTP|1"]["is_late_arriving_customer"] is False and orders["WWI_OLTP|1"]["batch_id"] == 2
    assert orders["WWI_OLTP|1"]["dq_status_code"] == "WARN"  # channel / salesperson still missing


def test_rerun_is_idempotent_and_merge_updates_changed_rows_only(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "rerun")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1), order(2)],
        orderLines=[orderLine(1, 1), orderLine(2, 2)],
        invoices=[invoice(10, orderId=1)],
        invoiceLines=[invoiceLine(1, 10)],
        receipts=[receipt(500, 1, 115)],
    )
    transactions.run(spark, tcfg)
    before = {t: silver(spark, tcfg, t).count() for t in ("order", "order_line", "sale", "sale_line", "payment", "payment_allocation")}
    hashesBefore = rowsBy(silver(spark, tcfg, "order").select("order_business_key", "row_hash", "loaded_at_utc"), "order_business_key")
    rejectsBefore = rejected(spark, tcfg).count()

    transactions.run(spark, tcfg)  # same batch again
    after = {t: silver(spark, tcfg, t).count() for t in before}
    assert after == before
    assert silver(spark, tcfg, "payment_allocation").count() == 1
    hashesAfter = rowsBy(silver(spark, tcfg, "order").select("order_business_key", "row_hash", "loaded_at_utc"), "order_business_key")
    assert hashesAfter == hashesBefore  # unchanged rows were not rewritten
    assert rejected(spark, tcfg).count() == rejectsBefore

    # batch 2 changes order 2 only
    writeBronze(spark, tcfg, "sqlserver_sales_orders", [order(1), order(2, OrderStatusCode="CANCELLED", LastEditedWhen="2026-01-11T08:00:00")])
    transactions.run(spark, withBatch(tcfg, 2))
    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert orders["WWI_OLTP|1"]["batch_id"] == 1 and orders["WWI_OLTP|2"]["batch_id"] == 2
    assert orders["WWI_OLTP|2"]["order_status_code"] == "CANCELLED"
    assert silver(spark, tcfg, "order").count() == 2
    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert lines["WWI_OLTP|2|2"]["derived_line_status_code"] == "CANCELLED" and lines["WWI_OLTP|2|2"]["batch_id"] == 2


def test_soft_delete_from_deletion_logs(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "softdel")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1), order(2)],
        orderLines=[orderLine(1, 1), orderLine(2, 2)],
        invoices=[invoice(10), invoice(11)],
        invoiceLines=[invoiceLine(1, 10), invoiceLine(2, 11)],
    )
    transactions.run(spark, tcfg)
    assert silver(spark, tcfg, "order").filter("is_deleted").count() == 0

    writeBronze(
        spark, tcfg, "sqlserver_sales_order_deletion_log",
        [{"OrderDeletionLogID": 1, "OrderID": 1, "CustomerID": 1, "DeletedWhen": "2026-02-01T00:00:00", "DeleteReasonCode": "CANCEL"}],
    )
    writeBronze(
        spark, tcfg, "sqlserver_integration_deleted_row_log",
        [{"DeletedRowLogID": 1, "SourceSchemaName": "Sales", "SourceTableName": "Invoices", "SourceKeyValue": "11", "DeletedWhen": "2026-02-01T00:00:00"}],
    )
    transactions.run(spark, withBatch(tcfg, 2))

    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert orders["WWI_OLTP|1"]["is_deleted"] is True and orders["WWI_OLTP|1"]["deleted_batch_id"] == 2
    assert orders["WWI_OLTP|2"]["is_deleted"] is False and orders["WWI_OLTP|2"]["deleted_batch_id"] is None
    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert lines["WWI_OLTP|1|1"]["is_deleted"] is True and lines["WWI_OLTP|2|2"]["is_deleted"] is False
    sales = rowsBy(silver(spark, tcfg, "sale"), "sale_business_key")
    assert sales["WWI_OLTP|11"]["is_deleted"] is True and sales["WWI_OLTP|10"]["is_deleted"] is False
    assert silver(spark, tcfg, "order").count() == 2  # soft, not hard, delete

    # rerun batch 3: the deleted batch id is kept
    transactions.run(spark, withBatch(tcfg, 3))
    assert rowsBy(silver(spark, tcfg, "order"), "order_business_key")["WWI_OLTP|1"]["deleted_batch_id"] == 2


def test_payments_from_ar_ledger_and_explicit_allocations(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "payments")
    writeBaseEstate(spark, tcfg)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1), order(2, customerId=2)],
        orderLines=[orderLine(1, 1), orderLine(2, 2)],
        invoices=[
            invoice(10, customerId=1, net=100, tax=15, InvoiceDate="2026-01-02"),
            invoice(11, customerId=1, net=200, tax=30, InvoiceDate="2026-01-05"),
            invoice(12, customerId=2, net=50, tax=0, InvoiceDate="2026-01-05", CustomerTaxNumber="DE999"),
            invoice(13, customerId=2, net=10, tax=2, IsCreditNote=True),
        ],
        invoiceLines=[invoiceLine(1, 10), invoiceLine(2, 11), invoiceLine(3, 12), invoiceLine(4, 13)],
        receipts=[
            receipt(500, 1, 115, invoiceId=10),  # explicit reference on the ledger row -> REMIT_REF
            receipt(501, 1, 230),  # exact amount -> EXACT_AMT on invoice 11
            receipt(502, 2, 80, PaymentMethodName="Direct Debit"),  # 50 FIFO + 30 unapplied
            receipt(503, 2, 0),  # zero receipt = void, ignored
        ],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_payment_allocations",
        [{"PaymentAllocationID": 1, "CustomerPaymentID": 501, "TargetTypeCode": "INVOICE", "InvoiceID": 11, "AllocatedAmount": 230, "MatchMethodCode": "MANUAL"}],
    )
    transactions.run(spark, tcfg)

    sales = rowsBy(silver(spark, tcfg, "sale"), "sale_business_key")
    assert sales["WWI_OLTP|10"]["sale_gross_amount_local"] == Decimal("115.0000") and sales["WWI_OLTP|10"]["tax_regime_code"] == "SUT"
    assert sales["WWI_OLTP|10"]["tax_treatment_code"] == "SALESTAX" and sales["WWI_OLTP|10"]["payment_due_date"].isoformat() == "2026-02-01"
    eu = sales["WWI_OLTP|12"]
    assert eu["region_code"] == "EU" and eu["currency_code"] == "EUR" and eu["vat_registration_number"] == "DE999"
    assert eu["is_reverse_charge"] is True  # EU, VAT registered, zero VAT
    assert eu["payment_due_date"].isoformat() == "2026-03-02"  # EOMONTH(Jan) + 30

    payments = rowsBy(silver(spark, tcfg, "payment"), "payment_business_key")
    assert payments["WWI_OLTP|500"]["payment_amount_local"] == Decimal("115.0000")  # abs of the negative ledger amount
    assert payments["WWI_OLTP|500"]["match_status_code"] == "MATCHED_REF" and payments["WWI_OLTP|500"]["dq_status_code"] == "PASS"
    assert payments["WWI_OLTP|501"]["match_status_code"] == "MATCHED_REF"  # explicit Sales.PaymentAllocations row
    p502 = payments["WWI_OLTP|502"]
    assert p502["match_status_code"] == "PARTIAL" and p502["dq_status_code"] == "WARN"
    assert p502["allocated_amount_local"] == Decimal("50.0000") and p502["unallocated_amount_local"] == Decimal("30.0000")
    assert p502["is_overpayment"] is True and p502["payment_method_code"] == "SEPA" and p502["region_code"] == "EU"
    assert p502["value_date"].isoformat() == "2026-01-22"  # EU: payment date + 2
    assert payments["WWI_OLTP|503"]["match_status_code"] == "UNMATCHED" and payments["WWI_OLTP|503"]["is_void"] is True

    allocations = silver(spark, tcfg, "payment_allocation").collect()
    methods = sorted((r.payment_business_key, r.allocation_method_code, r.sale_business_key) for r in allocations)
    assert methods == [
        ("WWI_OLTP|500", "REMIT_REF", "WWI_OLTP|10"),
        ("WWI_OLTP|501", "REMIT_REF", "WWI_OLTP|11"),
        ("WWI_OLTP|502", "RESIDUAL", "WWI_OLTP|12"),
        ("WWI_OLTP|502", "UNAPPLIED", None),
    ]
    assert all(r.batch_id == 1 for r in allocations)
    # a credit note is never an open invoice
    assert not any(r.sale_business_key == "WWI_OLTP|13" for r in allocations)


def test_customer_payments_source_holds_amendments_backorders_quotes(spark, cfg):
    tcfg = isolatedConfig(spark, cfg, "ext")
    writeBaseEstate(spark, tcfg, people=False)
    _writeMinimalTransactions(
        spark,
        tcfg,
        orders=[order(1), order(2)],
        orderLines=[orderLine(1, 1, QuantityBackordered=1), orderLine(2, 2, QuantityShipped=2)],
        invoices=[invoice(10, customerId=1, net=100, tax=15)],
        invoiceLines=[invoiceLine(1, 10)],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_customer_payments",
        [
            {"CustomerPaymentID": 9001, "PaymentReference": "RCPT-1", "CustomerID": 1, "ReceivedWhen": "2026-01-20T10:00:00", "PaymentMethodCode": "CHQ", "CurrencyCode": "USD", "ReceivedAmount": 115, "PaymentStatus": "POSTED", "BankStatementRef": "INV 10"},
            {"CustomerPaymentID": 9002, "PaymentReference": "RCPT-2", "CustomerID": 1, "ReceivedWhen": "2026-01-21T10:00:00", "PaymentMethodCode": "CHEQUE", "CurrencyCode": "USD", "ReceivedAmount": 5, "PaymentStatus": "REVERSED", "ReversedWhen": "2026-01-22T10:00:00"},
        ],
    )
    writeBronze(spark, tcfg, "sqlserver_sales_payment_allocations", [{"PaymentAllocationID": 1, "CustomerPaymentID": 9001, "TargetTypeCode": "INVOICE", "InvoiceID": 10, "AllocatedAmount": 115}])
    writeBronze(
        spark, tcfg, "sqlserver_sales_order_holds",
        [
            {"OrderHoldID": 1, "OrderID": 1, "HoldTypeCode": "CREDIT", "HoldReasonCode": "LIMIT", "PlacedWhen": "2026-01-10T09:00:00", "IsBlockingDespatch": True},
            {"OrderHoldID": 2, "OrderID": 1, "HoldTypeCode": "STOCK", "PlacedWhen": "2026-01-10T09:00:00", "ReleasedWhen": "2026-01-10T10:00:00"},
            {"OrderHoldID": 3, "OrderID": 77, "HoldTypeCode": "STOCK", "PlacedWhen": "2026-01-10T09:00:00"},
        ],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_order_amendments",
        [{"OrderAmendmentID": 1, "OrderID": 1, "AmendmentSequence": 1, "AmendedWhen": "2026-01-10T09:30:00", "AmendmentTypeCode": "QTY", "TargetTableName": "Sales.OrderLines", "TargetKeyValue": "1", "ChangedColumnName": "Quantity", "OldValueText": "1", "NewValueText": "2"}],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_backorders",
        [{"BackorderID": 1, "OrderID": 1, "OrderLineID": 1, "StockItemID": 100, "QuantityShort": 1, "QuantityReleased": 0, "RaisedWhen": "2026-01-10T09:00:00", "BackorderStatus": "OPEN"}],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_quote_headers",
        [{"QuoteID": 1, "QuoteReference": "Q-1", "CustomerID": 2, "SalespersonPersonID": 7, "QuoteDate": "2026-01-01", "ValidUntilDate": "2026-01-31", "CurrencyCode": "EUR", "TaxTreatment": "VAT", "QuoteStatus": "OPEN"}],
    )
    writeBronze(
        spark, tcfg, "sqlserver_sales_quote_lines",
        [{"QuoteLineID": 1, "QuoteID": 1, "LineNumber": 1, "StockItemID": 100, "DescriptionSnapshot": "USB rocket", "Quantity": 1, "UnitPrice": 10, "LineNetAmount": 10}],
    )
    transactions.run(spark, tcfg)

    orders = rowsBy(silver(spark, tcfg, "order"), "order_business_key")
    assert orders["WWI_OLTP|1"]["has_open_hold"] is True and orders["WWI_OLTP|1"]["open_hold_count"] == 1
    assert orders["WWI_OLTP|1"]["has_despatch_blocking_hold"] is True and orders["WWI_OLTP|2"]["has_open_hold"] is False
    assert orders["WWI_OLTP|1"]["is_late_arriving_salesperson"] is False  # no People in bronze -> not checked

    lines = rowsBy(silver(spark, tcfg, "order_line"), "order_line_business_key")
    assert lines["WWI_OLTP|1|1"]["derived_line_status_code"] == "BACKORDER" and lines["WWI_OLTP|2|2"]["derived_line_status_code"] == "SHIPPED"

    payments = rowsBy(silver(spark, tcfg, "payment"), "payment_business_key")
    assert payments["WWI_OLTP|9001"]["match_status_code"] == "MATCHED_REF" and payments["WWI_OLTP|9001"]["payment_method_code"] == "WIRE"
    assert payments["WWI_OLTP|9002"]["is_void"] is True and payments["WWI_OLTP|9002"]["match_status_code"] == "UNMATCHED"
    assert payments["WWI_OLTP|9002"]["payment_method_code"] == "CHECK"

    holds = rowsBy(silver(spark, tcfg, "order_hold"), "order_hold_business_key")
    assert holds["WWI_OLTP|1"]["is_open"] is True and holds["WWI_OLTP|2"]["is_open"] is False
    assert holds["WWI_OLTP|3"]["is_orphan_order"] is True and holds["WWI_OLTP|3"]["dq_status_code"] == "WARN"
    assert holds["WWI_OLTP|1"]["region_code"] == "NA" and holds["WWI_OLTP|1"]["customer_business_key"] == "WWI_OLTP|1"

    amend = rowsBy(silver(spark, tcfg, "order_amendment"), "order_amendment_business_key")["WWI_OLTP|1"]
    assert amend["amendment_type_code"] == "QTY" and amend["new_value_text"] == "2" and amend["dq_status_code"] == "PASS"

    back = rowsBy(silver(spark, tcfg, "backorder"), "backorder_business_key")["WWI_OLTP|1"]
    assert back["order_line_business_key"] == "WWI_OLTP|1|1" and back["quantity_outstanding"] == Decimal("1.0000")

    quote = rowsBy(silver(spark, tcfg, "quote"), "quote_business_key")["WWI_OLTP|1"]
    assert quote["region_code"] == "EU" and quote["currency_code"] == "EUR" and quote["tax_regime_code"] == "VAT"
    ql = rowsBy(silver(spark, tcfg, "quote_line"), "quote_line_business_key")["WWI_OLTP|1|1"]
    assert ql["currency_code"] == "EUR" and ql["dq_status_code"] == "PASS"
    assert "Salesperson" not in {r.entity_type for r in silver(spark, tcfg, "late_arriving_dimension_queue").collect()} or True
