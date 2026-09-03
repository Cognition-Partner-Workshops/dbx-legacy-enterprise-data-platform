/*
    Additive indexes on Sales.Orders and Sales.OrderLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 03_indexes / 3000 - after 02_extensions
    Depends on    : Sales.Orders, Sales.OrderLines and their 02_extensions columns
    Called by     : Sales.vw_OrderLineExtract, allocation and despatch queries

    Covering and filtered indexes added over the years for the extract and for
    the order book screens. The extract index is keyed on LastEditedWhen
    because the incremental packages chase that column; it is the widest index
    on the table and the reason overnight order maintenance is slow.
*/
IF INDEXPROPERTY(OBJECT_ID(N'Sales.Orders'), N'IX_Sales_Orders_ChangeExtract', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Orders_ChangeExtract]
        ON [Sales].[Orders] ([LastEditedWhen] ASC)
        INCLUDE ([CustomerID], [SalespersonPersonID], [OrderDate], [OrderStatusCode], [OrderValueExTax]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.Orders'), N'IX_Sales_Orders_OpenByChannel', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Orders_OpenByChannel]
        ON [Sales].[Orders] ([SalesChannelID] ASC, [OrderDate] ASC)
        INCLUDE ([CustomerID], [ExpectedDeliveryDate], [OrderValueExTax])
        WHERE [OrderStatusCode] IN (N'ENTERED', N'CONFIRMED', N'HOLD', N'ALLOCATED', N'PICKING', N'PARTSHIP');
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.Orders'), N'IX_Sales_Orders_CreditHeld', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_Orders_CreditHeld]
        ON [Sales].[Orders] ([CreditHoldAppliedWhen] ASC)
        INCLUDE ([CustomerID], [OrderValueExTax])
        WHERE [CreditHoldAppliedWhen] IS NOT NULL;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.OrderLines'), N'IX_Sales_OrderLines_Backordered', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_OrderLines_Backordered]
        ON [Sales].[OrderLines] ([StockItemID] ASC)
        INCLUDE ([OrderID], [QuantityBackordered], [RequestedDeliveryDate])
        WHERE [LineStatusCode] = N'BACKORDER';
GO

IF INDEXPROPERTY(OBJECT_ID(N'Sales.OrderLines'), N'IX_Sales_OrderLines_PromotionApplied', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Sales_OrderLines_PromotionApplied]
        ON [Sales].[OrderLines] ([PromotionID] ASC)
        INCLUDE ([OrderID], [StockItemID], [DiscountAmount], [LineNetAmount])
        WHERE [PromotionID] IS NOT NULL;
GO
