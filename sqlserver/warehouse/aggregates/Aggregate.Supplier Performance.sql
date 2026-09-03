/*
    Aggregate.Supplier Performance

    Object        : [Aggregate].[Supplier Performance] - supplier x month
                    scorecard: spend, delivery reliability, quality and payment
                    behaviour.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[Purchase],
                    [Fact].[Purchase Receipt], [Fact].[Supplier Payment].
    Called by     : Integration.usp_RefreshAggregateSupplierPerformance.

    Spend is reported two ways because procurement and finance never agreed:
    [Committed Spend Reporting] is PO value in the month the PO was raised,
    [Recognised Spend Reporting] is invoice value in the month it was posted.
    Both are stored so the two departments can each be right.

    Landed cost only includes duty for APAC (FOB terms) and EU imports from
    outside the customs union; NA purchases are DDP so duty is inside unit
    price and the duty column is zero there. Supplier league tables that ignore
    that comparison are wrong, which is why [Landed Cost Basis Code] exists.
*/
CREATE TABLE [Aggregate].[Supplier Performance] (
    [Supplier Performance Key]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Supplier Key]                  INT             NOT NULL,
    [Product Category Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Vendor Contract Key]           INT             NULL,
    [Purchase Order Count]          INT             NULL,
    [Purchase Line Count]           INT             NULL,
    [Receipt Count]                 INT             NULL,
    [Committed Spend Reporting]     DECIMAL (18, 2) NULL,
    [Recognised Spend Reporting]    DECIMAL (18, 2) NULL,
    [Year To Date Spend Reporting]  DECIMAL (18, 2) NULL,
    [Contract Covered Spend]        DECIMAL (18, 2) NULL,
    [Maverick Spend Reporting]      DECIMAL (18, 2) NULL,
    [Freight In Reporting]          DECIMAL (18, 2) NULL,
    [Customs Duty Reporting]        DECIMAL (18, 2) NULL,
    [Landed Cost Reporting]         DECIMAL (18, 2) NULL,
    [Landed Cost Basis Code]        NVARCHAR (6)    NULL,
    [On Time Receipt Count]         INT             NULL,
    [On Time Percent]               DECIMAL (9, 4)  NULL,
    [In Full Percent]               DECIMAL (9, 4)  NULL,
    [On Time In Full Percent]       DECIMAL (9, 4)  NULL,
    [Average Days Late]             DECIMAL (9, 2)  NULL,
    [Average Lead Time Days]        DECIMAL (9, 2)  NULL,
    [Lead Time Variability Days]    DECIMAL (9, 2)  NULL,
    [Rejected Quantity]             DECIMAL (18, 4) NULL,
    [Quality Reject Rate Percent]   DECIMAL (9, 4)  NULL,
    [Match Exception Count]         INT             NULL,
    [Price Variance Reporting]      DECIMAL (18, 2) NULL,
    [Discount Captured Reporting]   DECIMAL (18, 2) NULL,
    [Discount Lost Reporting]       DECIMAL (18, 2) NULL,
    [Average Days Beyond Terms]     DECIMAL (9, 2)  NULL,
    [Scorecard Rating Code]         NVARCHAR (4)    NULL,
    [Rank In Category By Spend]     INT             NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Supplier_Performance] PRIMARY KEY NONCLUSTERED ([Supplier Performance Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Supplier_Performance_Grain]
    ON [Aggregate].[Supplier Performance] ([Calendar Month] ASC, [Supplier Key] ASC, [Product Category Key] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Supplier_Performance_Scorecard]
    ON [Aggregate].[Supplier Performance] ([Scorecard Rating Code] ASC, [Calendar Month] ASC)
    INCLUDE ([Recognised Spend Reporting], [On Time In Full Percent], [Quality Reject Rate Percent]);
GO
