/*
    Sales.OrderDiscounts

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1080
    Depends on    : Sales.Orders, Sales.OrderLines, Sales.Promotions, Application.People
    Called by     : Sales.usp_CalculateOrderDiscounts, Sales.ufn_LineNetAmount,
                    Sales.vw_OrderLineExtract

    Every discount applied to an order or an order line, whatever its origin.
    A null OrderLineID means the discount is at header level and is spread
    across the lines pro rata by Sales.usp_CalculateOrderDiscounts (the spread
    is recomputed from scratch each time, not incrementally).

    ApprovalStatus only matters for DiscountSource = 'MANUAL'; for every other
    source it is left at 'NOTREQUIRED' by convention rather than by constraint.
*/
CREATE TABLE [Sales].[OrderDiscounts] (
    [OrderDiscountID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderID]               INT             NOT NULL,
    [OrderLineID]           INT             NULL,
    [DiscountSource]        NVARCHAR (12)   NOT NULL,
    [PromotionID]           INT             NULL,
    [PriceListID]           INT             NULL,
    [DiscountPercent]       DECIMAL (5, 2)  NULL,
    [DiscountAmount]        DECIMAL (18, 2) NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [ReasonCode]            NVARCHAR (10)   NULL,
    [ApprovalStatus]        NVARCHAR (12)   CONSTRAINT [DF_Sales_OrderDiscounts_ApprovalStatus] DEFAULT (N'NOTREQUIRED') NOT NULL,
    [ApprovedByPersonID]    INT             NULL,
    [AppliedSequence]       SMALLINT        CONSTRAINT [DF_Sales_OrderDiscounts_AppliedSequence] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_OrderDiscounts_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_OrderDiscounts] PRIMARY KEY CLUSTERED ([OrderDiscountID] ASC),
    CONSTRAINT [CK_Sales_OrderDiscounts_Source] CHECK ([DiscountSource] IN (N'PROMO', N'PRICELIST', N'CONTRACT', N'MANUAL', N'LOYALTY', N'SETTLEMENT')),
    CONSTRAINT [CK_Sales_OrderDiscounts_PromoKey] CHECK ([DiscountSource] <> N'PROMO' OR [PromotionID] IS NOT NULL),
    CONSTRAINT [CK_Sales_OrderDiscounts_Approval] CHECK ([ApprovalStatus] IN (N'NOTREQUIRED', N'PENDING', N'APPROVED', N'REJECTED')),
    CONSTRAINT [CK_Sales_OrderDiscounts_Amount] CHECK ([DiscountAmount] >= 0),
    CONSTRAINT [FK_Sales_OrderDiscounts_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_OrderDiscounts_OrderLines] FOREIGN KEY ([OrderLineID]) REFERENCES [Sales].[OrderLines] ([OrderLineID]),
    CONSTRAINT [FK_Sales_OrderDiscounts_Promotions] FOREIGN KEY ([PromotionID]) REFERENCES [Sales].[Promotions] ([PromotionID]),
    CONSTRAINT [FK_Sales_OrderDiscounts_PriceLists] FOREIGN KEY ([PriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]),
    CONSTRAINT [FK_Sales_OrderDiscounts_ApprovedBy] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_OrderDiscounts_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderDiscounts_Order]
    ON [Sales].[OrderDiscounts] ([OrderID] ASC, [OrderLineID] ASC)
    INCLUDE ([DiscountSource], [DiscountAmount], [DiscountPercent]);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderDiscounts_PendingApproval]
    ON [Sales].[OrderDiscounts] ([LastEditedWhen] ASC)
    INCLUDE ([OrderID], [DiscountAmount], [LastEditedBy])
    WHERE [ApprovalStatus] = N'PENDING';
GO
