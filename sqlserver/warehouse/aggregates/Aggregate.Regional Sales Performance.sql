/*
    Aggregate.Regional Sales Performance

    Object        : [Aggregate].[Regional Sales Performance] - region x
                    territory x fiscal period summary used by the executive
                    pack, including the currency translation the group applies
                    on top of the transaction-level FX.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[Sale],
                    [Fact].[Daily Sales Snapshot].
    Called by     : Integration.usp_RefreshAggregateRegionalSales.

    Two currency columns are deliberately different numbers: the daily-rate
    total is the sum of the transaction-level conversions, the monthly-average
    total is what group finance publishes. The difference is stored as
    [Translation Difference] rather than hidden, because the executive pack has
    to foot to the published figure while the sales reports foot to the daily
    one.

    Tax treatment is summarised per region and cannot be added across them:
    [Sales Tax Collected] is NA only, [Vat Output Amount] and
    [Vat Reverse Charge Amount] are EU only, [Gst Collected] and
    [Gst Free Sales] are APAC only. Every one of these columns is NULL for the
    other two regions on purpose.
*/
CREATE TABLE [Aggregate].[Regional Sales Performance] (
    [Regional Sales Perf Key]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Sales Territory Key]           INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Fiscal Calendar Code]          NVARCHAR (10)   NULL,
    [Local Currency Code]           NCHAR (3)       NULL,
    [Order Count]                   INT             NULL,
    [Invoice Count]                 INT             NULL,
    [Active Customer Count]         INT             NULL,
    [Active Salesperson Count]      INT             NULL,
    [Net Sales Local]               DECIMAL (18, 2) NULL,
    [Net Sales Daily Rate]          DECIMAL (18, 2) NULL,
    [Net Sales Monthly Average Rate] DECIMAL (18, 2) NULL,
    [Translation Difference]        DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Sales Tax Collected]           DECIMAL (18, 2) NULL,
    [Vat Output Amount]             DECIMAL (18, 2) NULL,
    [Vat Reverse Charge Amount]     DECIMAL (18, 2) NULL,
    [Gst Collected]                 DECIMAL (18, 2) NULL,
    [Gst Free Sales]                DECIMAL (18, 2) NULL,
    [Budget Net Sales Reporting]    DECIMAL (18, 2) NULL,
    [Budget Variance Reporting]     DECIMAL (18, 2) NULL,
    [Budget Attainment Percent]     DECIMAL (9, 4)  NULL,
    [Prior Year Net Sales]          DECIMAL (18, 2) NULL,
    [Year Over Year Percent]        DECIMAL (9, 4)  NULL,
    [Year To Date Net Sales]        DECIMAL (18, 2) NULL,
    [Rank In Region By Sales]       INT             NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Regional_Sales_Performance] PRIMARY KEY NONCLUSTERED ([Regional Sales Perf Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Regional_Sales_Performance_Grain]
    ON [Aggregate].[Regional Sales Performance] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Region Code] ASC, [Sales Territory Key] ASC, [Sales Channel Key] ASC);
GO
