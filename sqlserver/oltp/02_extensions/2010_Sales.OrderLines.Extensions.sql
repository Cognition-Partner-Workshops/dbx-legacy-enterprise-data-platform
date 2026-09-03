/*
    Sales.OrderLines - additive column extensions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2010 - after 2000
    Depends on    : Sales.OrderLines (Microsoft sample), Sales.PriceListLines,
                    Sales.Promotions
    Called by     : Sales.usp_CalculateOrderDiscounts, Sales.vw_OrderLineExtract

    Line-level pricing and allocation columns. The line net amount is
    maintained by trigger rather than computed, because the discount
    resolution it depends on is not deterministic: the same line re-priced
    tomorrow can come out differently, and finance requires the figure that
    was agreed with the customer to stay put.
*/
IF COL_LENGTH(N'Sales.OrderLines', N'PriceListLineID') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [PriceListLineID] BIGINT NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'ListUnitPrice') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [ListUnitPrice] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'DiscountPercent') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [DiscountPercent] DECIMAL (5, 2) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'DiscountAmount') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [DiscountAmount] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'PromotionID') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [PromotionID] INT NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'LineNetAmount') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [LineNetAmount] DECIMAL (18, 2) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'QuantityAllocated') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [QuantityAllocated] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'QuantityShipped') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [QuantityShipped] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'QuantityBackordered') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [QuantityBackordered] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'LineStatusCode') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [LineStatusCode] NVARCHAR (12) NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'RequestedDeliveryDate') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [RequestedDeliveryDate] DATE NULL;
GO

IF COL_LENGTH(N'Sales.OrderLines', N'SourceLineReference') IS NULL
    ALTER TABLE [Sales].[OrderLines] ADD [SourceLineReference] NVARCHAR (40) NULL;
GO

IF OBJECT_ID(N'Sales.CK_Sales_OrderLines_LineStatusCode', N'C') IS NULL
    ALTER TABLE [Sales].[OrderLines]
        ADD CONSTRAINT [CK_Sales_OrderLines_LineStatusCode]
        CHECK ([LineStatusCode] IS NULL OR [LineStatusCode] IN (N'OPEN', N'ALLOCATED', N'PICKED', N'SHIPPED', N'BACKORDER', N'CANCELLED'));
GO

IF OBJECT_ID(N'Sales.CK_Sales_OrderLines_DiscountPercent', N'C') IS NULL
    ALTER TABLE [Sales].[OrderLines]
        ADD CONSTRAINT [CK_Sales_OrderLines_DiscountPercent]
        CHECK ([DiscountPercent] IS NULL OR ([DiscountPercent] >= 0 AND [DiscountPercent] <= 100));
GO

IF OBJECT_ID(N'Sales.FK_Sales_OrderLines_PriceListLines', N'F') IS NULL
    ALTER TABLE [Sales].[OrderLines]
        ADD CONSTRAINT [FK_Sales_OrderLines_PriceListLines]
        FOREIGN KEY ([PriceListLineID]) REFERENCES [Sales].[PriceListLines] ([PriceListLineID]);
GO

IF OBJECT_ID(N'Sales.FK_Sales_OrderLines_Promotions', N'F') IS NULL
    ALTER TABLE [Sales].[OrderLines]
        ADD CONSTRAINT [FK_Sales_OrderLines_Promotions]
        FOREIGN KEY ([PromotionID]) REFERENCES [Sales].[Promotions] ([PromotionID]);
GO
