/*
    Monthly aggregate tables

    Objects       : [Aggregate].[Monthly Sales Summary]
                    [Aggregate].[Monthly Margin Analysis]
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[Sale],
                    [Fact].[Sales Margin].
    Called by     : refreshed by Integration.usp_RefreshAggregateMonthlySales
                    and Integration.usp_RefreshAggregateMarginAnalysis.

    Both tables are keyed on fiscal period, not calendar month, and the fiscal
    period depends on the region's calendar (NA 4-4-5, EU April-March, APAC
    July-June). [Calendar Month] is carried as well so the group consolidation
    pack can still line the regions up, and the two rarely agree.
*/
CREATE TABLE [Aggregate].[Monthly Sales Summary] (
    [Monthly Sales Summary Key]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Sales Territory Key]           INT             NULL,
    [Customer Segment Key]          INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Fiscal Calendar Code]          NVARCHAR (10)   NULL,
    [Order Count]                   INT             NULL,
    [Invoice Count]                 INT             NULL,
    [Return Count]                  INT             NULL,
    [Quantity Sold Base UOM]        DECIMAL (18, 4) NULL,
    [Gross Revenue]                 DECIMAL (18, 2) NULL,
    [Discount Given]                DECIMAL (18, 2) NULL,
    [Net Revenue]                   DECIMAL (18, 2) NULL,
    [Net Revenue Reporting]         DECIMAL (18, 2) NULL,
    [Credit Notes Reporting]        DECIMAL (18, 2) NULL,
    [Returns Reporting]             DECIMAL (18, 2) NULL,
    [Net Revenue After Credits]     DECIMAL (18, 2) NULL,
    [Cost Of Sales Reporting]       DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Average Order Value]           DECIMAL (18, 2) NULL,
    [Prior Period Net Revenue]      DECIMAL (18, 2) NULL,
    [Prior Year Net Revenue]        DECIMAL (18, 2) NULL,
    [Rolling 3 Period Net Revenue]  DECIMAL (18, 2) NULL,
    [Period Over Period Percent]    DECIMAL (9, 4)  NULL,
    [Year Over Year Percent]        DECIMAL (9, 4)  NULL,
    [Period Closed Flag]            BIT             CONSTRAINT [DF_Aggregate_Monthly_Sales_Summary_Period_Closed_Flag] DEFAULT (0) NOT NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Monthly_Sales_Summary] PRIMARY KEY NONCLUSTERED ([Monthly Sales Summary Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Monthly_Sales_Summary_Grain]
    ON [Aggregate].[Monthly Sales Summary] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Customer Key] ASC, [Sales Channel Key] ASC);
GO

/*
    Margin analysis is held at product category rather than customer, and adds
    the price/volume/mix bridge that the commercial team asks for every quarter.
    The bridge components are stored because reproducing them at query time
    needs the prior period on the same row.
*/
CREATE TABLE [Aggregate].[Monthly Margin Analysis] (
    [Monthly Margin Analysis Key]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [Fiscal Year]                   SMALLINT        NOT NULL,
    [Fiscal Period]                 TINYINT         NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Product Category Key]          INT             NOT NULL,
    [Sales Territory Key]           INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Cost Basis Code]               NVARCHAR (6)    NULL,
    [Quantity Sold Base UOM]        DECIMAL (18, 4) NULL,
    [Net Revenue Reporting]         DECIMAL (18, 2) NULL,
    [Cost Of Sales Reporting]       DECIMAL (18, 2) NULL,
    [Standard Cost Reporting]       DECIMAL (18, 2) NULL,
    [Purchase Price Variance]       DECIMAL (18, 2) NULL,
    [Freight Cost Reporting]        DECIMAL (18, 2) NULL,
    [Rebate Accrual Reporting]      DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Standard Margin Reporting]     DECIMAL (18, 2) NULL,
    [Contribution Margin Reporting] DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Standard Margin Percent]       DECIMAL (9, 4)  NULL,
    [Price Effect Amount]           DECIMAL (18, 2) NULL,
    [Volume Effect Amount]          DECIMAL (18, 2) NULL,
    [Mix Effect Amount]             DECIMAL (18, 2) NULL,
    [Cost Effect Amount]            DECIMAL (18, 2) NULL,
    [Negative Margin Line Count]    INT             NULL,
    [Prior Period Margin Percent]   DECIMAL (9, 4)  NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Monthly_Margin_Analysis] PRIMARY KEY NONCLUSTERED ([Monthly Margin Analysis Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Monthly_Margin_Analysis_Grain]
    ON [Aggregate].[Monthly Margin Analysis] ([Fiscal Year] ASC, [Fiscal Period] ASC, [Product Category Key] ASC, [Region Code] ASC);
GO
