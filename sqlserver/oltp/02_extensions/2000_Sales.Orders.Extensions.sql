/*
    Sales.Orders - additive column extensions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2000 - after 01_tables
    Depends on    : Sales.Orders (Microsoft sample), Sales.SalesChannels,
                    Sales.SalesTerritories, Sales.PriceLists
    Called by     : Sales.usp_ApplyPromotionToOrder, Sales.usp_CalculateOrderDiscounts,
                    Shipping.usp_CreateShipmentFromOrder, Sales.vw_OrderLineExtract

    Twenty years of columns bolted onto the order header. The sample's
    Sales.Orders is not modified in wwi-ssdt; everything here is added with
    ALTER so the base project still deploys unchanged.

    Notes on the shape of what accreted:
      * OrderStatusCode is a second status column living beside the sample's
        IsUndersupplyBackordered / PickingCompletedWhen columns. Neither is
        authoritative on its own and the extract view has to combine them.
      * FulfilmentFlags is a pipe-delimited list of one-character flags
        ('H' hold, 'B' backorder, 'S' split, 'X' export) written by the order
        entry screen because adding a column each time needed a change board.
      * OrderValueExTax is a maintained denormalisation, refreshed by
        Sales.usp_CalculateOrderDiscounts and by the line trigger.
*/
IF COL_LENGTH(N'Sales.Orders', N'SalesChannelID') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [SalesChannelID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'SalesTerritoryID') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [SalesTerritoryID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'PriceListID') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [PriceListID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'SourceQuoteID') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [SourceQuoteID] INT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'OrderStatusCode') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [OrderStatusCode] NVARCHAR (12) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'FulfilmentFlags') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [FulfilmentFlags] NVARCHAR (40) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'CurrencyCode') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [CurrencyCode] NCHAR (3) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'ExchangeRateToUsd') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [ExchangeRateToUsd] DECIMAL (18, 8) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'TaxRegimeCode') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [TaxRegimeCode] NVARCHAR (12) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'IsTaxInclusivePricing') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [IsTaxInclusivePricing] BIT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'OrderValueExTax') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [OrderValueExTax] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'TotalDiscountAmount') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [TotalDiscountAmount] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'AmendmentCount') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [AmendmentCount] SMALLINT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'CreditHoldAppliedWhen') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [CreditHoldAppliedWhen] DATETIME2 (7) NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'WebCartID') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [WebCartID] BIGINT NULL;
GO

IF COL_LENGTH(N'Sales.Orders', N'ExtractedRowVersion') IS NULL
    ALTER TABLE [Sales].[Orders] ADD [ExtractedRowVersion] BINARY (8) NULL;
GO

IF OBJECT_ID(N'Sales.CK_Sales_Orders_OrderStatusCode', N'C') IS NULL
    ALTER TABLE [Sales].[Orders]
        ADD CONSTRAINT [CK_Sales_Orders_OrderStatusCode]
        CHECK ([OrderStatusCode] IS NULL OR [OrderStatusCode] IN (N'ENTERED', N'CONFIRMED', N'HOLD', N'ALLOCATED', N'PICKING', N'PARTSHIP', N'SHIPPED', N'INVOICED', N'CANCELLED'));
GO

IF OBJECT_ID(N'Sales.FK_Sales_Orders_SalesChannels', N'F') IS NULL
    ALTER TABLE [Sales].[Orders]
        ADD CONSTRAINT [FK_Sales_Orders_SalesChannels]
        FOREIGN KEY ([SalesChannelID]) REFERENCES [Sales].[SalesChannels] ([SalesChannelID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_Orders_SalesTerritories', N'F') IS NULL
    ALTER TABLE [Sales].[Orders]
        ADD CONSTRAINT [FK_Sales_Orders_SalesTerritories]
        FOREIGN KEY ([SalesTerritoryID]) REFERENCES [Sales].[SalesTerritories] ([SalesTerritoryID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_Orders_PriceLists', N'F') IS NULL
    ALTER TABLE [Sales].[Orders]
        ADD CONSTRAINT [FK_Sales_Orders_PriceLists]
        FOREIGN KEY ([PriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_Orders_QuoteHeaders', N'F') IS NULL
    ALTER TABLE [Sales].[Orders]
        ADD CONSTRAINT [FK_Sales_Orders_QuoteHeaders]
        FOREIGN KEY ([SourceQuoteID]) REFERENCES [Sales].[QuoteHeaders] ([QuoteID]);
GO
