/*
    Sales.PromotionLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1060 - after Sales.Promotions
    Depends on    : Sales.Promotions, Warehouse.StockItems, Warehouse.StockGroups
    Called by     : Sales.usp_ApplyPromotionToOrder

    A promotion applies either to a stock item, a stock group, or (when both
    keys are null) to the whole order. The three cases are evaluated in that
    order of precedence by Sales.usp_ApplyPromotionToOrder; there is no
    precedence column, the procedure hard-codes it.
*/
CREATE TABLE [Sales].[PromotionLines] (
    [PromotionLineID]       INT             IDENTITY (1, 1) NOT NULL,
    [PromotionID]           INT             NOT NULL,
    [StockItemID]           INT             NULL,
    [StockGroupID]          INT             NULL,
    [MinimumQuantity]       DECIMAL (18, 3) NULL,
    [MinimumOrderValue]     DECIMAL (18, 2) NULL,
    [DiscountPercent]       DECIMAL (5, 2)  NULL,
    [DiscountAmount]        DECIMAL (18, 2) NULL,
    [FreeQuantity]          DECIMAL (18, 3) NULL,
    [FreeStockItemID]       INT             NULL,
    [PointsMultiplier]      DECIMAL (5, 2)  NULL,
    [LineSequence]          SMALLINT        CONSTRAINT [DF_Sales_PromotionLines_LineSequence] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_PromotionLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_PromotionLines] PRIMARY KEY CLUSTERED ([PromotionLineID] ASC),
    CONSTRAINT [UQ_Sales_PromotionLines_Sequence] UNIQUE ([PromotionID], [LineSequence]),
    CONSTRAINT [CK_Sales_PromotionLines_Target] CHECK ([StockItemID] IS NULL OR [StockGroupID] IS NULL),
    CONSTRAINT [CK_Sales_PromotionLines_Benefit] CHECK ([DiscountPercent] IS NOT NULL OR [DiscountAmount] IS NOT NULL OR [FreeQuantity] IS NOT NULL OR [PointsMultiplier] IS NOT NULL),
    CONSTRAINT [CK_Sales_PromotionLines_DiscountPercent] CHECK ([DiscountPercent] IS NULL OR [DiscountPercent] BETWEEN 0 AND 100),
    CONSTRAINT [FK_Sales_PromotionLines_Promotions] FOREIGN KEY ([PromotionID]) REFERENCES [Sales].[Promotions] ([PromotionID]),
    CONSTRAINT [FK_Sales_PromotionLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_PromotionLines_StockGroups] FOREIGN KEY ([StockGroupID]) REFERENCES [Warehouse].[StockGroups] ([StockGroupID]),
    CONSTRAINT [FK_Sales_PromotionLines_FreeStockItems] FOREIGN KEY ([FreeStockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_PromotionLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PromotionLines_StockItem]
    ON [Sales].[PromotionLines] ([StockItemID] ASC)
    INCLUDE ([PromotionID], [DiscountPercent], [DiscountAmount])
    WHERE [StockItemID] IS NOT NULL;
GO
