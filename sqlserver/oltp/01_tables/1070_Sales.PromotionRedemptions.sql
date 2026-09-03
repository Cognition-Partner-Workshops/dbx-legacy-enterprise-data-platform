/*
    Sales.PromotionRedemptions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1070 - after Sales.Promotions
    Depends on    : Sales.Promotions, Sales.Orders, Sales.Customers
    Called by     : Sales.usp_ApplyPromotionToOrder, Sales.vw_PromotionEffectiveness

    One row per promotion actually applied to an order. Rows are never deleted
    when an order is cancelled; the RedemptionStatus is moved to 'REVERSED' and
    the reversal is what the effectiveness view nets off. Deletions that do
    happen (data fixes) are captured in Integration.DeletionLog.
*/
CREATE TABLE [Sales].[PromotionRedemptions] (
    [PromotionRedemptionID] BIGINT          IDENTITY (1, 1) NOT NULL,
    [PromotionID]           INT             NOT NULL,
    [OrderID]               INT             NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [CouponCode]            NVARCHAR (30)   NULL,
    [RedeemedWhen]          DATETIME2 (7)   CONSTRAINT [DF_Sales_PromotionRedemptions_RedeemedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [DiscountValue]         DECIMAL (18, 2) NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [FundedBySupplierValue] DECIMAL (18, 2) CONSTRAINT [DF_Sales_PromotionRedemptions_FundedBySupplierValue] DEFAULT (0) NOT NULL,
    [RedemptionStatus]      NVARCHAR (12)   CONSTRAINT [DF_Sales_PromotionRedemptions_RedemptionStatus] DEFAULT (N'APPLIED') NOT NULL,
    [ReversalReason]        NVARCHAR (100)  NULL,
    [AppliedByPersonID]     INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_PromotionRedemptions_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_PromotionRedemptions] PRIMARY KEY CLUSTERED ([PromotionRedemptionID] ASC),
    CONSTRAINT [CK_Sales_PromotionRedemptions_Status] CHECK ([RedemptionStatus] IN (N'APPLIED', N'REVERSED', N'DISPUTED')),
    CONSTRAINT [CK_Sales_PromotionRedemptions_Reversal] CHECK ([RedemptionStatus] <> N'REVERSED' OR [ReversalReason] IS NOT NULL),
    CONSTRAINT [FK_Sales_PromotionRedemptions_Promotions] FOREIGN KEY ([PromotionID]) REFERENCES [Sales].[Promotions] ([PromotionID]),
    CONSTRAINT [FK_Sales_PromotionRedemptions_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_PromotionRedemptions_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_PromotionRedemptions_Application_People] FOREIGN KEY ([AppliedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PromotionRedemptions_Promotion_When]
    ON [Sales].[PromotionRedemptions] ([PromotionID] ASC, [RedeemedWhen] ASC)
    INCLUDE ([DiscountValue], [RedemptionStatus], [CustomerID]);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_PromotionRedemptions_Order]
    ON [Sales].[PromotionRedemptions] ([OrderID] ASC);
GO
