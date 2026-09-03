/*
    Ecommerce.ProductReviews

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1630 - after Ecommerce.WebSessions
    Depends on    : Warehouse.StockItems, Sales.Customers, Application.People
    Called by     : merchandising extracts

    Customer reviews with a hand-rolled moderation workflow. ModerationStatus
    and PublishedWhen disagree on a few thousand historical rows migrated from
    the previous webshop, where publication was implied by the absence of a
    rejection.
*/
CREATE TABLE [Ecommerce].[ProductReviews] (
    [ProductReviewID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [CustomerID]            INT             NULL,
    [WebSessionID]          BIGINT          NULL,
    [SubmittedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_ProductReviews_SubmittedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RatingValue]           TINYINT         NOT NULL,
    [ReviewTitle]           NVARCHAR (120)  NULL,
    [ReviewBody]            NVARCHAR (MAX)  NULL,
    [DisplayName]           NVARCHAR (60)   NULL,
    [IsVerifiedPurchase]    BIT             CONSTRAINT [DF_Ecommerce_ProductReviews_IsVerifiedPurchase] DEFAULT (0) NOT NULL,
    [ModerationStatus]      NVARCHAR (12)   CONSTRAINT [DF_Ecommerce_ProductReviews_ModerationStatus] DEFAULT (N'PENDING') NOT NULL,
    [ModeratedByPersonID]   INT             NULL,
    [ModeratedWhen]         DATETIME2 (7)   NULL,
    [RejectionReasonCode]   NVARCHAR (10)   NULL,
    [PublishedWhen]         DATETIME2 (7)   NULL,
    [HelpfulVoteCount]      INT             CONSTRAINT [DF_Ecommerce_ProductReviews_HelpfulVoteCount] DEFAULT (0) NOT NULL,
    [ReportedCount]         INT             CONSTRAINT [DF_Ecommerce_ProductReviews_ReportedCount] DEFAULT (0) NOT NULL,
    [LanguageCode]          NCHAR (2)       NULL,
    CONSTRAINT [PK_Ecommerce_ProductReviews] PRIMARY KEY CLUSTERED ([ProductReviewID] ASC),
    CONSTRAINT [CK_Ecommerce_ProductReviews_Rating] CHECK ([RatingValue] BETWEEN 1 AND 5),
    CONSTRAINT [CK_Ecommerce_ProductReviews_Status] CHECK ([ModerationStatus] IN (N'PENDING', N'APPROVED', N'REJECTED', N'WITHDRAWN')),
    CONSTRAINT [FK_Ecommerce_ProductReviews_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Ecommerce_ProductReviews_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Ecommerce_ProductReviews_Sessions] FOREIGN KEY ([WebSessionID]) REFERENCES [Ecommerce].[WebSessions] ([WebSessionID]),
    CONSTRAINT [FK_Ecommerce_ProductReviews_Application_People] FOREIGN KEY ([ModeratedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_ProductReviews_Item_Published]
    ON [Ecommerce].[ProductReviews] ([StockItemID] ASC, [PublishedWhen] DESC)
    INCLUDE ([RatingValue], [IsVerifiedPurchase])
    WHERE [ModerationStatus] = N'APPROVED';
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_ProductReviews_Queue]
    ON [Ecommerce].[ProductReviews] ([SubmittedWhen] ASC)
    WHERE [ModerationStatus] = N'PENDING';
GO
