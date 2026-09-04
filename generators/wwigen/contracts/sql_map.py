"""How each SQL Server producer binds to the raw landing table it lands in.

Only twelve of the OLTP extracts have a landing table: the staging estate
lands what the ETL actually reads before it runs, not the whole OLTP
database. The rest are declared in :data:`UNLANDED` - they are still
generated as source content, they simply have no pre-ETL landing target, and
inventing one would be a schema change rather than a generator fix.

Every rule below renames producer columns onto the canonical vocabulary; a
producer column with no canonical home is left out of the extract, and a
canonical column with no producer value is left out too unless the table
requires it, in which case :mod:`wwigen.conform` supplies the technical
value (BatchId, LoadedAtUtc, SourceSystemCode).
"""

from __future__ import annotations

from ..conform import Rule

# spec key -> landing table
TARGETS = {
    "sqlserver.Sales.Orders": "raw.SqlOrder",
    "sqlserver.Sales.OrderLines": "raw.SqlOrderLine",
    "sqlserver.Sales.Invoices": "raw.SqlInvoice",
    "sqlserver.Sales.InvoiceLines": "raw.SqlInvoiceLine",
    "sqlserver.Warehouse.StockItems": "raw.SqlStockItem",
    "sqlserver.Warehouse.StockItemTransactions": "raw.SqlStockMovement",
    "sqlserver.Shipping.ShipmentHeaders": "raw.SqlShipment",
    "sqlserver.Shipping.ShipmentLines": "raw.SqlShipmentLine",
    "sqlserver.Returns.ReturnLines": "raw.SqlReturnLine",
    "sqlserver.Returns.CreditNoteLines": "raw.SqlCreditNote",
    "sqlserver.Loyalty.LoyaltyPointsLedger": "raw.SqlLoyaltyLedger",
    "sqlserver.Ecommerce.WebSessions": "raw.SqlWebSession",
}

# Generated OLTP content the staging estate does not land before ETL. Listed
# explicitly so a new extract cannot quietly acquire someone else's table.
UNLANDED = (
    "sqlserver.Sales.Customers",
    "sqlserver.Sales.CustomerTransactions",
    "sqlserver.Sales.Promotions",
    "sqlserver.Sales.PromotionRedemptions",
    "sqlserver.Sales.SalesTerritories",
    "sqlserver.Sales.SalesQuotas",
    "sqlserver.Warehouse.Bins",
    "sqlserver.Shipping.ShipmentEvents",
    "sqlserver.Integration.OutboundInterfaceQueue",
    "sqlserver.Integration.InboundFileRegister",
    "sqlserver.Integration.ChangeTrackingWatermark",
)

RULES = {
    "sqlserver.Sales.Orders": Rule(
        rename={"ChannelCode": "SalesChannelCode"},
    ),
    "sqlserver.Sales.OrderLines": Rule(
        rename={"DiscountPercentage": "LineDiscountPercent"},
    ),
    "sqlserver.Sales.Invoices": Rule(
        rename={"ConfirmedDeliveryWhen": "ConfirmedDeliveryTime"},
    ),
    "sqlserver.Sales.InvoiceLines": Rule(),
    "sqlserver.Warehouse.StockItems": Rule(
        rename={"ErpItemCode": "ErpProductCode"},
    ),
    "sqlserver.Warehouse.StockItemTransactions": Rule(
        rename={"WarehouseSiteCode": "WarehouseCode",
                "MovementTypeCode": "TransactionTypeName",
                "ReasonCode": "MovementReasonCode"},
    ),
    "sqlserver.Shipping.ShipmentHeaders": Rule(
        rename={"PromisedDeliveryDate": "PromisedDeliveryWhen",
                "OriginSiteCode": "ShipFromWarehouseCode",
                "DestinationPostalCode": "ShipToPostalCode",
                "DestinationCountryCode": "ShipToCountryCode",
                "CurrencyCode": "FreightCurrencyCode"},
    ),
    "sqlserver.Shipping.ShipmentLines": Rule(
        rename={"LineWeightKg": "WeightKg", "SerialNumber": "SerialNumbers"},
    ),
    "sqlserver.Returns.ReturnLines": Rule(
        rename={"ReceivedDate": "ReturnedWhen",
                "ConditionCode": "InspectionResultCode"},
    ),
    # The landing table is a credit-note header; the extract carries one
    # credited line per note, which is the grain the source system emits.
    "sqlserver.Returns.CreditNoteLines": Rule(
        rename={"InvoiceID": "OriginalInvoiceID", "LineAmount": "NetAmount",
                "ReasonCode": "CreditReasonCode"},
    ),
    "sqlserver.Loyalty.LoyaltyPointsLedger": Rule(
        rename={"PointsBalance": "PointsBalanceAfter", "EntryDate": "EntryWhen"},
    ),
    "sqlserver.Ecommerce.WebSessions": Rule(
        rename={"EntryPageURL": "LandingPageUrl", "ReferrerDomain": "ReferrerUrl",
                "DeviceTypeCode": "DeviceCategory",
                "ConvertedToOrder": "OrderPlacedFlag",
                "ConsentStateCode": "ConsentCategories"},
    ),
}
