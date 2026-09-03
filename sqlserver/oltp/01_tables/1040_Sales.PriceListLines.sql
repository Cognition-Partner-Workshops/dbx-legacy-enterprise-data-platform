/*
    Sales.PriceListLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1040 - after Sales.PriceLists
    Depends on    : Sales.PriceLists, Warehouse.StockItems, Application.People
    Called by     : Sales.usp_CalculateOrderDiscounts, Sales.usp_ConvertQuoteToOrder

    One row per item per quantity break. The margin floor is advisory in the
    order entry application (it warns) but blocking in the quote conversion
    procedure, which is a long-standing complaint from the sales desk.
*/
CREATE TABLE [Sales].[PriceListLines] (
    [PriceListLineID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [PriceListID]           INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [MinimumQuantity]       DECIMAL (18, 3) CONSTRAINT [DF_Sales_PriceListLines_MinimumQuantity] DEFAULT (1) NOT NULL,
    [UnitPrice]             DECIMAL (18, 2) NOT NULL,
    [StandardCostAtLoad]    DECIMAL (18, 2) NULL,
    [MarginFloorPercent]    DECIMAL (5, 2)  NULL,
    [MaximumDiscountPercent] DECIMAL (5, 2) CONSTRAINT [DF_Sales_PriceListLines_MaximumDiscountPercent] DEFAULT (0) NOT NULL,
    [IsPromotionalPrice]    BIT             CONSTRAINT [DF_Sales_PriceListLines_IsPromotionalPrice] DEFAULT (0) NOT NULL,
    [SourceSystemReference] NVARCHAR (40)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_PriceListLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_PriceListLines] PRIMARY KEY CLUSTERED ([PriceListLineID] ASC),
    CONSTRAINT [UQ_Sales_PriceListLines_Break] UNIQUE ([PriceListID], [StockItemID], [MinimumQuantity]),
    CONSTRAINT [CK_Sales_PriceListLines_UnitPrice] CHECK ([UnitPrice] >= 0),
    CONSTRAINT [CK_Sales_PriceListLines_MinimumQuantity] CHECK ([MinimumQuantity] > 0),
    CONSTRAINT [CK_Sales_PriceListLines_MaximumDiscount] CHECK ([MaximumDiscountPercent] BETWEEN 0 AND 95),
    CONSTRAINT [FK_Sales_PriceListLines_PriceLists] FOREIGN KEY ([PriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]),
    CONSTRAINT [FK_Sales_PriceListLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_PriceListLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PriceListLines_StockItem]
    ON [Sales].[PriceListLines] ([StockItemID] ASC, [PriceListID] ASC)
    INCLUDE ([MinimumQuantity], [UnitPrice], [MaximumDiscountPercent]);
GO
