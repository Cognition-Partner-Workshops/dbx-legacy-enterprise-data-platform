/*
    Sales.Backorders

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2130 - after 2120
    Depends on    : Sales.Orders, Sales.OrderLines, Warehouse.StockItems,
                    Purchasing.PurchaseOrderLines
    Called by     : backorder release run, customer service screens

    Backorder register. The sample flags an order as undersupplied on the
    header; this holds the actual shortfall per line and the promise made to
    the customer. PromisedDate is re-promised in place, so the original
    promise is lost unless the amendment log caught it - it usually did not,
    because the backorder screen writes here directly.
*/
CREATE TABLE [Sales].[Backorders] (
    [BackorderID]           BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderID]               INT             NOT NULL,
    [OrderLineID]           INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [QuantityShort]         DECIMAL (18, 3) NOT NULL,
    [QuantityReleased]      DECIMAL (18, 3) CONSTRAINT [DF_Sales_Backorders_QuantityReleased] DEFAULT (0) NOT NULL,
    [RaisedWhen]            DATETIME2 (7)   CONSTRAINT [DF_Sales_Backorders_RaisedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [ShortageReasonCode]    NVARCHAR (10)   NULL,
    [PromisedDate]          DATE            NULL,
    [PromiseSource]         NVARCHAR (12)   NULL,
    [RepromiseCount]        SMALLINT        CONSTRAINT [DF_Sales_Backorders_RepromiseCount] DEFAULT (0) NOT NULL,
    [LinkedPurchaseOrderLineID] INT         NULL,
    [CustomerNotifiedWhen]  DATETIME2 (7)   NULL,
    [BackorderStatus]       NVARCHAR (12)   CONSTRAINT [DF_Sales_Backorders_BackorderStatus] DEFAULT (N'OPEN') NOT NULL,
    [ClosedWhen]            DATETIME2 (7)   NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_Backorders_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_Backorders] PRIMARY KEY CLUSTERED ([BackorderID] ASC),
    CONSTRAINT [CK_Sales_Backorders_Quantity] CHECK ([QuantityShort] > 0),
    CONSTRAINT [CK_Sales_Backorders_Status] CHECK ([BackorderStatus] IN (N'OPEN', N'PARTRELEASED', N'RELEASED', N'CANCELLED', N'SUBSTITUTED')),
    CONSTRAINT [CK_Sales_Backorders_PromiseSource] CHECK ([PromiseSource] IS NULL OR [PromiseSource] IN (N'PURCHASING', N'TRANSFER', N'MANUAL', N'SYSTEM')),
    CONSTRAINT [FK_Sales_Backorders_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_Backorders_OrderLines] FOREIGN KEY ([OrderLineID]) REFERENCES [Sales].[OrderLines] ([OrderLineID]),
    CONSTRAINT [FK_Sales_Backorders_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_Backorders_PurchaseOrderLines] FOREIGN KEY ([LinkedPurchaseOrderLineID]) REFERENCES [Purchasing].[PurchaseOrderLines] ([PurchaseOrderLineID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_Backorders_Open_Item]
    ON [Sales].[Backorders] ([StockItemID] ASC, [PromisedDate] ASC)
    INCLUDE ([OrderID], [QuantityShort], [QuantityReleased])
    WHERE [BackorderStatus] IN (N'OPEN', N'PARTRELEASED');
GO
