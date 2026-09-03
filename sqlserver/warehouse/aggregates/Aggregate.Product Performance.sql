/*
    Aggregate.Product Performance

    Object        : [Aggregate].[Product Performance] - product x month
                    performance summary combining sales, returns, inventory and
                    supply.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema, [Fact].[Sales Margin],
                    [Fact].[Daily Inventory Snapshot], [Fact].[Return].
    Called by     : Integration.usp_RefreshAggregateProductPerformance, which
                    deletes and reinserts the current and previous month only.

    The ABC and XYZ classifications are recomputed on every refresh and stored,
    so a product's class can change month to month; merchandising reports on
    the transitions, which is only possible because history is kept here.
*/
CREATE TABLE [Aggregate].[Product Performance] (
    [Product Performance Key]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [Calendar Month]                DATE            NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Product Category Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Primary Supplier Key]          INT             NULL,
    [Units Sold Base UOM]           DECIMAL (18, 4) NULL,
    [Net Revenue Reporting]         DECIMAL (18, 2) NULL,
    [Gross Margin Reporting]        DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Discount Depth Percent]        DECIMAL (9, 4)  NULL,
    [Units Returned]                DECIMAL (18, 4) NULL,
    [Return Rate Percent]           DECIMAL (9, 4)  NULL,
    [Average Selling Price]         DECIMAL (18, 4) NULL,
    [Average Unit Cost]             DECIMAL (18, 4) NULL,
    [Average Stock Value Reporting] DECIMAL (18, 2) NULL,
    [Inventory Turns]               DECIMAL (9, 4)  NULL,
    [Days Inventory Outstanding]    DECIMAL (9, 2)  NULL,
    [Stockout Days]                 INT             NULL,
    [Lost Sales Estimate Reporting] DECIMAL (18, 2) NULL,
    [Sell Through Percent]          DECIMAL (9, 4)  NULL,
    [Distinct Customer Count]       INT             NULL,
    [Abc Class]                     NCHAR (1)       NULL,
    [Xyz Class]                     NCHAR (1)       NULL,
    [Prior Month Abc Class]         NCHAR (1)       NULL,
    [Rank In Category By Revenue]   INT             NULL,
    [Rank In Category By Margin]    INT             NULL,
    [New Product Flag]              BIT             NULL,
    [Discontinued Flag]             BIT             NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Aggregate_Product_Performance] PRIMARY KEY NONCLUSTERED ([Product Performance Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Product_Performance_Grain]
    ON [Aggregate].[Product Performance] ([Calendar Month] ASC, [Stock Item Key] ASC, [Region Code] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Product_Performance_Category_Rank]
    ON [Aggregate].[Product Performance] ([Product Category Key] ASC, [Calendar Month] ASC, [Rank In Category By Revenue] ASC)
    INCLUDE ([Net Revenue Reporting], [Gross Margin Reporting], [Return Rate Percent]);
GO
