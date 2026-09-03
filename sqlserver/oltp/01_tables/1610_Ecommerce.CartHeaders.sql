/*
    Ecommerce.CartHeaders

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1610 - after Ecommerce.WebSessions
    Depends on    : Ecommerce.WebSessions, Sales.Customers, Sales.Orders,
                    Sales.Promotions
    Called by     : Ecommerce.CartLines, Ecommerce.vw_WebConversionFunnel

    Basket header. Abandonment is not an event: a cart is considered abandoned
    when it is still ACTIVE and LastActivityWhen is older than the site's
    window, and the nightly job stamps AbandonedWhen retrospectively. Carts
    abandoned before that job was written in 2014 have a null AbandonedWhen and
    an ACTIVE status forever, and the funnel view has to allow for them.
*/
CREATE TABLE [Ecommerce].[CartHeaders] (
    [CartID]                BIGINT          IDENTITY (1, 1) NOT NULL,
    [CartGuid]              UNIQUEIDENTIFIER NOT NULL,
    [WebSessionID]          BIGINT          NULL,
    [CustomerID]            INT             NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [IsTaxInclusivePricing] BIT             CONSTRAINT [DF_Ecommerce_CartHeaders_IsTaxInclusivePricing] DEFAULT (0) NOT NULL,
    [CreatedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_CartHeaders_CreatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [LastActivityWhen]      DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_CartHeaders_LastActivityWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [AbandonedWhen]         DATETIME2 (7)   NULL,
    [RecoveryEmailSentWhen] DATETIME2 (7)   NULL,
    [ConvertedOrderID]      INT             NULL,
    [ConvertedWhen]         DATETIME2 (7)   NULL,
    [LineCount]             SMALLINT        CONSTRAINT [DF_Ecommerce_CartHeaders_LineCount] DEFAULT (0) NOT NULL,
    [MerchandiseSubtotal]   DECIMAL (18, 2) CONSTRAINT [DF_Ecommerce_CartHeaders_MerchandiseSubtotal] DEFAULT (0) NOT NULL,
    [EstimatedTaxAmount]    DECIMAL (18, 2) NULL,
    [EstimatedFreightAmount] DECIMAL (18, 2) NULL,
    [CouponCode]            NVARCHAR (24)   NULL,
    [PromotionID]           INT             NULL,
    [CartStatus]            NVARCHAR (12)   CONSTRAINT [DF_Ecommerce_CartHeaders_CartStatus] DEFAULT (N'ACTIVE') NOT NULL,
    [CheckoutStepReached]   NVARCHAR (16)   NULL,
    CONSTRAINT [PK_Ecommerce_CartHeaders] PRIMARY KEY CLUSTERED ([CartID] ASC),
    CONSTRAINT [UQ_Ecommerce_CartHeaders_Guid] UNIQUE ([CartGuid]),
    CONSTRAINT [CK_Ecommerce_CartHeaders_Status] CHECK ([CartStatus] IN (N'ACTIVE', N'ABANDONED', N'CONVERTED', N'MERGED', N'EXPIRED')),
    CONSTRAINT [CK_Ecommerce_CartHeaders_Converted] CHECK ([CartStatus] <> N'CONVERTED' OR [ConvertedOrderID] IS NOT NULL),
    CONSTRAINT [CK_Ecommerce_CartHeaders_Step] CHECK ([CheckoutStepReached] IS NULL OR [CheckoutStepReached] IN (N'BASKET', N'DELIVERY', N'PAYMENT', N'REVIEW', N'PLACED')),
    CONSTRAINT [FK_Ecommerce_CartHeaders_Sessions] FOREIGN KEY ([WebSessionID]) REFERENCES [Ecommerce].[WebSessions] ([WebSessionID]),
    CONSTRAINT [FK_Ecommerce_CartHeaders_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Ecommerce_CartHeaders_Orders] FOREIGN KEY ([ConvertedOrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Ecommerce_CartHeaders_Promotions] FOREIGN KEY ([PromotionID]) REFERENCES [Sales].[Promotions] ([PromotionID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_CartHeaders_Abandoned]
    ON [Ecommerce].[CartHeaders] ([LastActivityWhen] ASC)
    INCLUDE ([CustomerID], [MerchandiseSubtotal], [RecoveryEmailSentWhen])
    WHERE [CartStatus] = N'ACTIVE';
GO
