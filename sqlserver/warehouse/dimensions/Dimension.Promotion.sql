/*
    Object        : [Dimension].[Promotion]  (SCD Type 2 - a promotion is amended mid-flight)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Sales Channel.sql
    Depends on    : Sequences.PromotionKey, Dimension.Sales Channel, Dimension.Product Category
    Called by     : Integration.usp_MigrateStagedPromotionData

    Promotions are extended, discounted further and re-scoped while they are
    running, and the effectiveness reporting must attribute a sale to the terms in
    force on the day. Hence Type 2, and hence the same-day handling: a promotion
    can be amended twice on a launch day.

    Mechanics are stored as a code plus parameters rather than a rule engine, and
    the parameters mean different things per mechanic, which is exactly the kind
    of overloading the estate is full of:

        PCTOFF   [Parameter 1] = percentage off
        AMTOFF   [Parameter 1] = amount off, [Parameter 2] = minimum spend
        BXGY     [Parameter 1] = buy quantity, [Parameter 2] = free quantity
        BUNDLE   [Parameter 1] = bundle price, [Parameter 3] = bundle SKU count
        THRESH   [Parameter 1] = spend threshold, [Parameter 2] = reward amount

    Regional rules: EU price-display law requires the prior lowest price for 30
    days to be shown, so [EU Prior Price Reference Date] is captured; NA runs
    manufacturer-funded coupons with a funding split; APAC promotions are often
    marketplace-funded and carry the marketplace's own campaign identifier.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Promotion', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Promotion]
    (
        [Promotion Key]                 INT             CONSTRAINT [DF_Dimension_Promotion_Key] DEFAULT (NEXT VALUE FOR [Sequences].[PromotionKey]) NOT NULL,
        [WWI Promotion ID]              INT             NULL,
        [Promotion Code]                NVARCHAR(20)    NOT NULL,
        [Promotion Name]                NVARCHAR(120)   NOT NULL,
        [Campaign Code]                 NVARCHAR(20)    NULL,
        [Campaign Name]                 NVARCHAR(120)   NULL,
        [Promotion Type Code]           NVARCHAR(10)    NULL,   -- PRICE / COUPON / BUNDLE / LOYALTY / TRADE
        [Mechanic Code]                 NVARCHAR(10)    NULL,   -- PCTOFF / AMTOFF / BXGY / BUNDLE / THRESH
        [Parameter 1]                   DECIMAL(18, 4)  NULL,
        [Parameter 2]                   DECIMAL(18, 4)  NULL,
        [Parameter 3]                   DECIMAL(18, 4)  NULL,
        [Parameter Currency Code]       NVARCHAR(3)     NULL,

        [Region Code]                   NVARCHAR(10)    NULL,
        [Sales Channel Key]             INT             NULL,
        [Channel Scope Code]            NVARCHAR(10)    NULL,   -- ALL / ONE / LIST
        [Product Scope Code]            NVARCHAR(10)    NULL,   -- ALL / CATEGORY / SKULIST
        [Product Category Key]          INT             NULL,
        [Customer Scope Code]           NVARCHAR(10)    NULL,   -- ALL / SEGMENT / GROUP / LIST
        [Customer Segment Code]         NVARCHAR(15)    NULL,

        [Start Date]                    DATE            NULL,
        [End Date]                      DATE            NULL,
        [Redemption Limit]              INT             NULL,
        [Redemption Limit Per Customer] INT             NULL,
        [Is Stackable]                  BIT             NULL,
        [Priority Order]                SMALLINT        NULL,
        [Funding Source Code]           NVARCHAR(10)    NULL,   -- INTERNAL / VENDOR / MARKETPLACE / SHARED
        [Vendor Funding Percentage]     DECIMAL(9, 4)   NULL,
        [Budget Amount]                 DECIMAL(18, 2)  NULL,
        [Accrual GL Account Code]       NVARCHAR(20)    NULL,

        [EU Prior Price Reference Date] DATE            NULL,
        [EU Price Display Compliant]    BIT             NULL,
        [NA Coupon Series Code]         NVARCHAR(20)    NULL,
        [APAC Marketplace Campaign Id]  NVARCHAR(40)    NULL,

        [Amendment Reason Code]         NVARCHAR(20)    NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Effective From]                DATETIME2(7)    NOT NULL,
        [Effective To]                  DATETIME2(7)    NOT NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NOT NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Promotion] PRIMARY KEY CLUSTERED ([Promotion Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Promotion_Current]
        ON [Dimension].[Promotion] ([Promotion Code] ASC, [Is Current Row] ASC)
        INCLUDE ([Promotion Key], [Start Date], [End Date], [Row Hash Type 2]);
END;
GO
