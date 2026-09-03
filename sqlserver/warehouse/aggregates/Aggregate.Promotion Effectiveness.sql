/*
    Aggregate.Promotion Effectiveness

    Object        : [Aggregate].[Promotion Effectiveness] - promotion x product
                    category x region summary of take-up, incremental revenue
                    and cannibalisation.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema,
                    [Fact].[Promotion Eligibility], [Fact].[Sales Margin].
    Called by     : Integration.usp_RefreshAggregatePromotionEffectiveness,
                    which builds the eligibility denominator first.

    Baseline revenue is the four weeks before the promotion window, taken from
    [Aggregate].[Daily Sales Summary] rather than recomputed. Incremental
    revenue is therefore only as good as the baseline, and the marketing team
    has argued about it since 2012 - the columns exist so the argument at least
    happens over the same numbers.
*/
CREATE TABLE [Aggregate].[Promotion Effectiveness] (
    [Promotion Effectiveness Key]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [Promotion Key]                 INT             NOT NULL,
    [Product Category Key]          INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Promotion Code]                NVARCHAR (20)   NOT NULL,
    [Promotion Start Date]          DATE            NULL,
    [Promotion End Date]            DATE            NULL,
    [Baseline Start Date]           DATE            NULL,
    [Baseline End Date]             DATE            NULL,
    [Eligible Customer Count]       INT             NULL,
    [Participating Customer Count]  INT             NULL,
    [Take Up Rate Percent]          DECIMAL (9, 4)  NULL,
    [Eligible Not Purchased Count]  INT             NULL,
    [New Customer Count]            INT             NULL,
    [Reactivated Customer Count]    INT             NULL,
    [Promoted Units Sold]           DECIMAL (18, 4) NULL,
    [Baseline Revenue Reporting]    DECIMAL (18, 2) NULL,
    [Promotion Revenue Reporting]   DECIMAL (18, 2) NULL,
    [Incremental Revenue Reporting] DECIMAL (18, 2) NULL,
    [Discount Cost Reporting]       DECIMAL (18, 2) NULL,
    [Promotion Margin Reporting]    DECIMAL (18, 2) NULL,
    [Baseline Margin Reporting]     DECIMAL (18, 2) NULL,
    [Incremental Margin Reporting]  DECIMAL (18, 2) NULL,
    [Cannibalised Revenue]          DECIMAL (18, 2) NULL,
    [Return Rate Percent]           DECIMAL (9, 4)  NULL,
    [Loyalty Points Issued]         DECIMAL (18, 2) NULL,
    [Roi Percent]                   DECIMAL (9, 4)  NULL,
    [Payback Achieved Flag]         BIT             NULL,
    [Consent Restricted Flag]       BIT             NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Promotion_Effectiveness] PRIMARY KEY NONCLUSTERED ([Promotion Effectiveness Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Promotion_Effectiveness_Grain]
    ON [Aggregate].[Promotion Effectiveness] ([Promotion Key] ASC, [Product Category Key] ASC, [Region Code] ASC, [Sales Channel Key] ASC);
GO
