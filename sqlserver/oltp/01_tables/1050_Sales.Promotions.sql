/*
    Sales.Promotions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1050
    Depends on    : Application.People
    Called by     : Sales.PromotionLines, Sales.PromotionRedemptions,
                    Sales.usp_ApplyPromotionToOrder, Sales.vw_PromotionEffectiveness

    Campaign header. Marketing owns the codes; finance owns the funding split.

    Legacy pattern (delimited list): EligibleCustomerCategoryList holds a
    pipe-delimited list of Sales.CustomerCategories.CustomerCategoryName values
    rather than a child table. It was added in a 2008 hotfix "temporarily" and
    every consumer since has parsed it with STRING_SPLIT or a LIKE on
    '%|' + name + '|%'. It is not referentially enforced and it is known to
    contain names of categories that have since been renamed.
*/
CREATE TABLE [Sales].[Promotions] (
    [PromotionID]                   INT             CONSTRAINT [DF_Sales_Promotions_PromotionID] DEFAULT (NEXT VALUE FOR [Sequences].[PromotionID]) NOT NULL,
    [PromotionCode]                 NVARCHAR (20)   NOT NULL,
    [PromotionName]                 NVARCHAR (100)  NOT NULL,
    [RegionCode]                    NCHAR (4)       NOT NULL,
    [PromotionType]                 NVARCHAR (16)   NOT NULL,
    [CampaignReference]             NVARCHAR (30)   NULL,
    [StartDate]                     DATE            NOT NULL,
    [EndDate]                       DATE            NOT NULL,
    [BudgetAmount]                  DECIMAL (18, 2) NULL,
    [BudgetCurrencyCode]            NCHAR (3)       NULL,
    [SupplierFundedPercent]         DECIMAL (5, 2)  CONSTRAINT [DF_Sales_Promotions_SupplierFundedPercent] DEFAULT (0) NOT NULL,
    [EligibleCustomerCategoryList]  NVARCHAR (400)  NULL,
    [MaximumRedemptionsPerCustomer] INT             NULL,
    [RequiresCouponCode]            BIT             CONSTRAINT [DF_Sales_Promotions_RequiresCouponCode] DEFAULT (0) NOT NULL,
    [IsStackable]                   BIT             CONSTRAINT [DF_Sales_Promotions_IsStackable] DEFAULT (0) NOT NULL,
    [PromotionStatus]               NVARCHAR (12)   NOT NULL,
    [ApprovedByPersonID]            INT             NULL,
    [RedemptionCount]               INT             CONSTRAINT [DF_Sales_Promotions_RedemptionCount] DEFAULT (0) NOT NULL,
    [RedeemedValue]                 DECIMAL (18, 2) CONSTRAINT [DF_Sales_Promotions_RedeemedValue] DEFAULT (0) NOT NULL,
    [LastEditedBy]                  INT             NOT NULL,
    [LastEditedWhen]                DATETIME2 (7)   CONSTRAINT [DF_Sales_Promotions_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_Promotions] PRIMARY KEY CLUSTERED ([PromotionID] ASC),
    CONSTRAINT [UQ_Sales_Promotions_Code] UNIQUE ([PromotionCode], [RegionCode]),
    CONSTRAINT [CK_Sales_Promotions_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Sales_Promotions_Type] CHECK ([PromotionType] IN (N'PERCENT', N'AMOUNTOFF', N'BOGO', N'FREIGHTFREE', N'BUNDLE', N'POINTSBOOST')),
    CONSTRAINT [CK_Sales_Promotions_Dates] CHECK ([EndDate] >= [StartDate]),
    CONSTRAINT [CK_Sales_Promotions_Status] CHECK ([PromotionStatus] IN (N'DRAFT', N'APPROVED', N'LIVE', N'PAUSED', N'EXPIRED', N'CANCELLED')),
    CONSTRAINT [CK_Sales_Promotions_SupplierFunded] CHECK ([SupplierFundedPercent] BETWEEN 0 AND 100),
    CONSTRAINT [FK_Sales_Promotions_ApprovedBy] FOREIGN KEY ([ApprovedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_Promotions_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_Promotions_Live]
    ON [Sales].[Promotions] ([RegionCode] ASC, [StartDate] ASC, [EndDate] ASC)
    INCLUDE ([PromotionCode], [PromotionType], [IsStackable])
    WHERE [PromotionStatus] IN (N'APPROVED', N'LIVE');
GO
