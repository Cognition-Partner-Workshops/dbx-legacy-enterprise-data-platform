"""Shared builders for the workstream-4 silver transaction tests.

Bronze inputs are built in-test from the DDL column names (CONVENTIONS.md
"Bronze tables"); each test gets its own schema set via ``schemaOverrides`` so
the Delta MERGE / rerun tests do not see each other's rows.
"""
from __future__ import annotations

import dataclasses
import itertools
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.spark import ensureSchemas

_counter = itertools.count(1)

META = {"_source_system": "string", "_source_object": "string", "_source_file": "string", "_load_ts": "timestamp", "_batch_id": "bigint"}

SCHEMAS: dict[str, dict[str, str]] = {
    "sqlserver_sales_customers": {
        "CustomerID": "int", "CustomerName": "string", "BillToCustomerID": "int", "SalesTerritoryID": "int",
        "RegionCode": "string", "TaxRegistrationNumber": "string", "DefaultPriceListID": "int", "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_sales_territories": {
        "SalesTerritoryID": "int", "TerritoryCode": "string", "RegionCode": "string", "ReportingCurrencyCode": "string", "TaxRegimeCode": "string",
    },
    "sqlserver_sales_sales_channels": {"SalesChannelID": "int", "ChannelCode": "string", "ChannelName": "string"},
    "sqlserver_warehouse_stock_items": {"StockItemID": "int", "StockItemName": "string"},
    "sqlserver_application_people": {"PersonID": "int", "FullName": "string"},
    "sqlserver_sales_orders": {
        "OrderID": "int", "CustomerID": "int", "SalespersonPersonID": "int", "PickedByPersonID": "int", "ContactPersonID": "int",
        "BackorderOrderID": "int", "OrderDate": "date", "ExpectedDeliveryDate": "date", "CustomerPurchaseOrderNumber": "string",
        "IsUndersupplyBackordered": "boolean", "Comments": "string", "DeliveryInstructions": "string", "PickingCompletedWhen": "timestamp",
        "LastEditedWhen": "timestamp", "SalesChannelID": "int", "SalesTerritoryID": "int", "PriceListID": "int", "SourceQuoteID": "int",
        "OrderStatusCode": "string", "FulfilmentFlags": "string", "CurrencyCode": "string", "ExchangeRateToUsd": "decimal(18,8)",
        "TaxRegimeCode": "string", "IsTaxInclusivePricing": "boolean", "OrderValueExTax": "decimal(18,2)", "TotalDiscountAmount": "decimal(18,2)",
        "AmendmentCount": "int",
    },
    "sqlserver_sales_order_lines": {
        "OrderLineID": "int", "OrderID": "int", "StockItemID": "int", "Description": "string", "PackageTypeID": "int", "Quantity": "int",
        "UnitPrice": "decimal(18,2)", "TaxRate": "decimal(18,3)", "PickedQuantity": "int", "PickingCompletedWhen": "timestamp",
        "LastEditedWhen": "timestamp", "ListUnitPrice": "decimal(18,2)", "DiscountPercent": "decimal(5,2)", "DiscountAmount": "decimal(18,2)",
        "PromotionID": "int", "LineNetAmount": "decimal(18,2)", "QuantityAllocated": "int", "QuantityShipped": "int",
        "QuantityBackordered": "int", "LineStatusCode": "string", "RequestedDeliveryDate": "date",
    },
    "sqlserver_sales_invoices": {
        "InvoiceID": "int", "CustomerID": "int", "BillToCustomerID": "int", "OrderID": "int", "DeliveryMethodID": "int",
        "ContactPersonID": "int", "SalespersonPersonID": "int", "InvoiceDate": "date", "CustomerPurchaseOrderNumber": "string",
        "IsCreditNote": "boolean", "CreditNoteReason": "string", "DeliveryRun": "string", "RunPosition": "string",
        "TotalDryItems": "int", "TotalChillerItems": "int", "ConfirmedDeliveryTime": "timestamp", "ConfirmedReceivedBy": "string",
        "LastEditedWhen": "timestamp", "SalesTerritoryID": "int", "TaxRegimeCode": "string", "CustomerTaxNumber": "string",
        "TaxPointDate": "date", "CurrencyCode": "string", "ExchangeRateToUsd": "decimal(18,8)", "InvoiceTotalExTax": "decimal(18,2)",
        "InvoiceTaxAmount": "decimal(18,2)", "AmountOutstanding": "decimal(18,2)", "SettlementStatus": "string",
        "PaymentDueDate": "date", "DisputeFlag": "boolean",
    },
    "sqlserver_sales_invoice_lines": {
        "InvoiceLineID": "int", "InvoiceID": "int", "StockItemID": "int", "Description": "string", "PackageTypeID": "int",
        "Quantity": "int", "UnitPrice": "decimal(18,2)", "TaxRate": "decimal(18,3)", "TaxAmount": "decimal(18,2)",
        "LineProfit": "decimal(18,2)", "ExtendedPrice": "decimal(18,2)", "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_customer_transactions": {
        "CustomerTransactionID": "int", "CustomerID": "int", "TransactionTypeID": "int", "TransactionTypeName": "string",
        "InvoiceID": "int", "PaymentMethodID": "int", "PaymentMethodName": "string", "TransactionDate": "date",
        "AmountExcludingTax": "decimal(18,2)", "TaxAmount": "decimal(18,2)", "TransactionAmount": "decimal(18,2)",
        "OutstandingBalance": "decimal(18,2)", "FinalizationDate": "date", "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_customer_payments": {
        "CustomerPaymentID": "bigint", "PaymentReference": "string", "CustomerID": "int", "ReceivedWhen": "timestamp",
        "ValueDate": "date", "PaymentMethodCode": "string", "CurrencyCode": "string", "ReceivedAmount": "decimal(18,2)",
        "ExchangeRateToUsd": "decimal(18,8)", "BankChargeAmount": "decimal(18,2)", "AllocatedAmount": "decimal(18,2)",
        "UnallocatedAmount": "decimal(18,2)", "BankStatementRef": "string", "PaymentStatus": "string",
        "ReversalReasonCode": "string", "ReversedWhen": "timestamp", "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_payment_allocations": {
        "PaymentAllocationID": "bigint", "CustomerPaymentID": "bigint", "AllocatedWhen": "timestamp", "TargetTypeCode": "string",
        "InvoiceID": "int", "CreditNoteID": "int", "AllocatedAmount": "decimal(18,2)", "SettlementDiscount": "decimal(18,2)",
        "MatchMethodCode": "string", "MatchConfidence": "decimal(5,2)", "ReversalOfAllocationID": "bigint",
    },
    "sqlserver_sales_order_holds": {
        "OrderHoldID": "bigint", "OrderID": "int", "HoldTypeCode": "string", "HoldReasonCode": "string", "HoldNarrative": "string",
        "PlacedWhen": "timestamp", "PlacedByPersonID": "int", "PlacedBySystem": "string", "ReleasedWhen": "timestamp",
        "ReleasedByPersonID": "int", "ReleaseNarrative": "string", "AutoReleaseAfterWhen": "timestamp", "EscalationLevel": "int",
        "IsBlockingDespatch": "boolean",
    },
    "sqlserver_sales_order_amendments": {
        "OrderAmendmentID": "bigint", "OrderID": "int", "AmendmentSequence": "int", "AmendedWhen": "timestamp",
        "AmendedByPersonID": "int", "AmendmentTypeCode": "string", "TargetTableName": "string", "TargetKeyValue": "string",
        "ChangedColumnName": "string", "OldValueText": "string", "NewValueText": "string", "ReasonCode": "string",
        "ReasonNarrative": "string", "RequiresCustomerApproval": "boolean", "CustomerApprovedWhen": "timestamp",
        "SourceApplication": "string",
    },
    "sqlserver_sales_backorders": {
        "BackorderID": "bigint", "OrderID": "int", "OrderLineID": "int", "StockItemID": "int", "QuantityShort": "int",
        "QuantityReleased": "int", "RaisedWhen": "timestamp", "ShortageReasonCode": "string", "PromisedDate": "date",
        "PromiseSource": "string", "RepromiseCount": "int", "LinkedPurchaseOrderLineID": "bigint",
        "CustomerNotifiedWhen": "timestamp", "BackorderStatus": "string", "ClosedWhen": "timestamp", "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_quote_headers": {
        "QuoteID": "int", "QuoteReference": "string", "CustomerID": "int", "ContactPersonID": "int", "SalespersonPersonID": "int",
        "SalesChannelID": "int", "PriceListID": "int", "QuoteDate": "date", "ValidUntilDate": "date", "CurrencyCode": "string",
        "ExchangeRateToUSD": "decimal(18,8)", "TaxTreatment": "string", "QuoteStatus": "string", "RevisionNumber": "int",
        "SupersedesQuoteID": "int", "ConvertedOrderID": "int", "ConvertedWhen": "timestamp", "LostReasonCode": "string",
        "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_quote_lines": {
        "QuoteLineID": "bigint", "QuoteID": "int", "LineNumber": "int", "StockItemID": "int", "DescriptionSnapshot": "string",
        "Quantity": "decimal(18,3)", "UnitPrice": "decimal(18,2)", "DiscountPercent": "decimal(5,2)", "TaxRatePercent": "decimal(5,2)",
        "LineNetAmount": "decimal(18,2)", "PromisedLeadTimeDays": "int", "IsOptionalLine": "boolean", "LineStatus": "string",
        "LastEditedWhen": "timestamp",
    },
    "sqlserver_sales_order_deletion_log": {
        "OrderDeletionLogID": "bigint", "OrderID": "int", "CustomerID": "int", "SalesTerritoryID": "int", "OrderDate": "date",
        "DeletedWhen": "timestamp", "DeletedByLogin": "string", "DeleteReasonCode": "string",
    },
    "sqlserver_integration_deleted_row_log": {
        "DeletedRowLogID": "bigint", "SourceSchemaName": "string", "SourceTableName": "string", "SourceKeyValue": "string",
        "SecondaryKeyValue": "string", "DeletedWhen": "timestamp", "DeletedByLogin": "string", "DeleteReasonCode": "string",
    },
}

LOAD_TS = datetime(2026, 1, 1, 0, 0, 0)


def isolatedConfig(spark: SparkSession, cfg: PipelineConfig, name: str, batchId: int = 1) -> PipelineConfig:
    """A config whose four schemas are private to one test."""
    suffix = f"{name}_{next(_counter)}"
    isolated = dataclasses.replace(
        cfg,
        batchId=batchId,
        schemaOverrides={layer: f"t_{suffix}_{layer}" for layer in ("bronze", "silver", "gold", "quality")},
    )
    ensureSchemas(spark, isolated)
    return isolated


def withBatch(cfg: PipelineConfig, batchId: int) -> PipelineConfig:
    return dataclasses.replace(cfg, batchId=batchId)


def _coerce(value, dtype: str):
    if value is None:
        return None
    if dtype.startswith("decimal"):
        return Decimal(str(value))
    if dtype == "date" and isinstance(value, str):
        return date.fromisoformat(value)
    if dtype == "timestamp" and isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def bronzeFrame(spark: SparkSession, table: str, rows: list[dict], sourceSystem: str = "WWI_OLTP") -> DataFrame:
    cols = {**SCHEMAS[table], **META}
    data = []
    for row in rows:
        unknown = set(row) - set(cols)
        assert not unknown, f"{table}: unknown columns {unknown}"
        full = {
            "_source_system": sourceSystem,
            "_source_object": table,
            "_source_file": f"{table}.csv",
            "_load_ts": LOAD_TS,
            "_batch_id": 1,
            **row,
        }
        data.append(tuple(_coerce(full.get(c), t) for c, t in cols.items()))
    ddl = ", ".join(f"`{c}` {t}" for c, t in cols.items())
    return spark.createDataFrame(data, schema=ddl)


def writeBronze(spark: SparkSession, cfg: PipelineConfig, table: str, rows: list[dict], sourceSystem: str = "WWI_OLTP") -> None:
    df = bronzeFrame(spark, table, rows, sourceSystem)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.fqn("bronze", table))


def silver(spark: SparkSession, cfg: PipelineConfig, table: str) -> DataFrame:
    return spark.table(cfg.fqn("silver", table))


def rejected(spark: SparkSession, cfg: PipelineConfig, ruleCode: str | None = None) -> DataFrame:
    df = spark.table(cfg.fqn("quality", "rejected_rows"))
    return df.filter(F.col("rule_code") == ruleCode) if ruleCode else df


def rowsBy(df: DataFrame, keyCol: str) -> dict:
    return {r[keyCol]: r.asDict() for r in df.collect()}


# --- a small, complete bronze estate -------------------------------------- #
CUSTOMERS = [
    {"CustomerID": 1, "CustomerName": "Tailspin NA", "SalesTerritoryID": 10, "RegionCode": "NA", "TaxRegistrationNumber": "US-111"},
    {"CustomerID": 2, "CustomerName": "Contoso EU", "SalesTerritoryID": 20, "RegionCode": "EU", "TaxRegistrationNumber": "DE123456789"},
    {"CustomerID": 3, "CustomerName": "Wingtip APAC", "SalesTerritoryID": 30, "RegionCode": "APAC", "TaxRegistrationNumber": None},
]
TERRITORIES = [
    {"SalesTerritoryID": 10, "TerritoryCode": "US-EAST", "RegionCode": "NA", "ReportingCurrencyCode": "USD", "TaxRegimeCode": "SUT"},
    {"SalesTerritoryID": 20, "TerritoryCode": "DACH", "RegionCode": "EU", "ReportingCurrencyCode": "EUR", "TaxRegimeCode": "VAT"},
    {"SalesTerritoryID": 30, "TerritoryCode": "ANZ", "RegionCode": "APAC", "ReportingCurrencyCode": "AUD", "TaxRegimeCode": "GST"},
]
CHANNELS = [{"SalesChannelID": 1, "ChannelCode": "WEB", "ChannelName": "Web"}, {"SalesChannelID": 2, "ChannelCode": "FIELD", "ChannelName": "Field"}]
STOCK_ITEMS = [{"StockItemID": 100, "StockItemName": "USB rocket"}, {"StockItemID": 101, "StockItemName": "Chocolate frogs"}]
PEOPLE = [{"PersonID": 7, "FullName": "Kayla Woodcock"}, {"PersonID": -1, "FullName": "Unknown"}]


def order(orderId: int, customerId: int = 1, **extra) -> dict:
    row = {
        "OrderID": orderId, "CustomerID": customerId, "SalespersonPersonID": 7, "ContactPersonID": 7,
        "OrderDate": "2026-01-10", "ExpectedDeliveryDate": "2026-01-12", "CustomerPurchaseOrderNumber": "po-1",
        "IsUndersupplyBackordered": False, "LastEditedWhen": "2026-01-10T08:00:00", "SalesChannelID": 1,
        "SalesTerritoryID": 10, "OrderStatusCode": "OPEN", "FulfilmentFlags": None, "CurrencyCode": "USD",
        "ExchangeRateToUsd": 1, "TaxRegimeCode": None, "IsTaxInclusivePricing": False, "OrderValueExTax": 100,
        "TotalDiscountAmount": 0, "AmendmentCount": 0,
    }
    row.update(extra)
    return row


def orderLine(lineId: int, orderId: int, stockItemId: int = 100, quantity: int = 2, unitPrice=10, **extra) -> dict:
    row = {
        "OrderLineID": lineId, "OrderID": orderId, "StockItemID": stockItemId, "Description": "USB rocket", "PackageTypeID": 7,
        "Quantity": quantity, "UnitPrice": unitPrice, "TaxRate": 15, "PickedQuantity": 0, "LastEditedWhen": "2026-01-10T08:00:00",
        "DiscountAmount": 0, "LineStatusCode": "OPEN",
    }
    row.update(extra)
    return row


def invoice(invoiceId: int, customerId: int = 1, orderId: int | None = None, net=100, tax=15, **extra) -> dict:
    row = {
        "InvoiceID": invoiceId, "CustomerID": customerId, "BillToCustomerID": customerId, "OrderID": orderId,
        "DeliveryMethodID": 3, "SalespersonPersonID": 7, "InvoiceDate": "2026-01-11", "IsCreditNote": False,
        "LastEditedWhen": "2026-01-11T08:00:00", "InvoiceTotalExTax": net, "InvoiceTaxAmount": tax,
        "AmountOutstanding": (net or 0) + (tax or 0), "SettlementStatus": "OPEN", "DisputeFlag": False,
    }
    row.update(extra)
    return row


def invoiceLine(lineId: int, invoiceId: int, stockItemId: int = 100, quantity: int = 10, unitPrice=10, tax=15, **extra) -> dict:
    row = {
        "InvoiceLineID": lineId, "InvoiceID": invoiceId, "StockItemID": stockItemId, "Description": "USB rocket", "PackageTypeID": 7,
        "Quantity": quantity, "UnitPrice": unitPrice, "TaxRate": 15, "TaxAmount": tax, "LineProfit": 20,
        "ExtendedPrice": Decimal(str(quantity)) * Decimal(str(unitPrice)) + Decimal(str(tax)), "LastEditedWhen": "2026-01-11T08:00:00",
    }
    row.update(extra)
    return row


def receipt(transactionId: int, customerId: int, amount, invoiceId: int | None = None, **extra) -> dict:
    """AR ledger 'Customer Payment Received' row (negative amount as in the OLTP)."""
    row = {
        "CustomerTransactionID": transactionId, "CustomerID": customerId, "TransactionTypeID": 3,
        "TransactionTypeName": "Customer Payment Received", "InvoiceID": invoiceId, "PaymentMethodID": 4,
        "PaymentMethodName": "EFT", "TransactionDate": "2026-01-20", "AmountExcludingTax": 0, "TaxAmount": 0,
        "TransactionAmount": -Decimal(str(amount)), "OutstandingBalance": 0, "FinalizationDate": None,
        "LastEditedWhen": "2026-01-20T08:00:00",
    }
    row.update(extra)
    return row


def writeBaseEstate(spark: SparkSession, cfg: PipelineConfig, *, people: bool = True) -> None:
    writeBronze(spark, cfg, "sqlserver_sales_customers", CUSTOMERS)
    writeBronze(spark, cfg, "sqlserver_sales_sales_territories", TERRITORIES)
    writeBronze(spark, cfg, "sqlserver_sales_sales_channels", CHANNELS)
    writeBronze(spark, cfg, "sqlserver_warehouse_stock_items", STOCK_ITEMS)
    if people:
        writeBronze(spark, cfg, "sqlserver_application_people", PEOPLE)
