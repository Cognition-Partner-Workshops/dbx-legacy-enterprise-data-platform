/*
    Daily aggregate tables

    Objects       : [Aggregate].[Daily Sales Summary]
                    [Aggregate].[Daily Inventory Health]
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.00 Schema and the contributing facts.
    Called by     : refreshed by Integration.usp_RefreshAggregateDailySales and
                    Integration.usp_RefreshAggregateInventoryHealth, both of
                    which rebuild a rolling window rather than the whole table.

    Deployed in one file because the nightly refresh writes both in the same
    job step and a partial deployment leaves the morning reports broken.
*/
CREATE TABLE [Aggregate].[Daily Sales Summary] (
    [Daily Sales Summary Key]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [Sales Date]                    DATE            NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Product Category Key]          INT             NULL,
    [Sales Territory Key]           INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Invoice Count]                 INT             NULL,
    [Line Count]                    INT             NULL,
    [Distinct Customer Count]       INT             NULL,
    [Quantity Sold Base UOM]        DECIMAL (18, 4) NULL,
    [Gross Sales Amount]            DECIMAL (18, 2) NULL,
    [Line Discount Amount]          DECIMAL (18, 2) NULL,
    [Promotion Discount Amount]     DECIMAL (18, 2) NULL,
    [Net Sales Amount]              DECIMAL (18, 2) NULL,
    [Tax Amount]                    DECIMAL (18, 2) NULL,
    [Freight Amount]                DECIMAL (18, 2) NULL,
    [Cost Of Sales Amount]          DECIMAL (18, 2) NULL,
    [Gross Margin Amount]           DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Returns Amount]                DECIMAL (18, 2) NULL,
    [Net Sales Amount Reporting]    DECIMAL (18, 2) NULL,
    [Prior Year Net Sales]          DECIMAL (18, 2) NULL,
    [Prior Year Variance Percent]   DECIMAL (9, 4)  NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    [Source Row Count]              BIGINT          NULL,
    CONSTRAINT [PK_Aggregate_Daily_Sales_Summary] PRIMARY KEY NONCLUSTERED ([Daily Sales Summary Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Daily_Sales_Summary_Grain]
    ON [Aggregate].[Daily Sales Summary] ([Sales Date] ASC, [Stock Item Key] ASC, [Sales Territory Key] ASC, [Sales Channel Key] ASC);
GO

CREATE NONCLUSTERED INDEX [IX_Aggregate_Daily_Sales_Summary_Region]
    ON [Aggregate].[Daily Sales Summary] ([Region Code] ASC, [Sales Date] ASC)
    INCLUDE ([Net Sales Amount Reporting], [Gross Margin Amount]);
GO

/*
    Inventory health is not a sales-shaped table: it counts exception states
    across sites rather than summing money, and it is the only aggregate the
    supply chain team is allowed to refresh on demand during the day.
*/
CREATE TABLE [Aggregate].[Daily Inventory Health] (
    [Daily Inventory Health Key]    BIGINT          IDENTITY (1, 1) NOT NULL,
    [Snapshot Date]                 DATE            NOT NULL,
    [Warehouse Site Key]            INT             NOT NULL,
    [Product Category Key]          INT             NOT NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Sku Count]                     INT             NULL,
    [Sku Stocked Count]             INT             NULL,
    [Stockout Sku Count]            INT             NULL,
    [Below Reorder Sku Count]       INT             NULL,
    [Excess Sku Count]              INT             NULL,
    [Slow Moving Sku Count]         INT             NULL,
    [Quarantined Sku Count]         INT             NULL,
    [Total Quantity On Hand]        DECIMAL (18, 4) NULL,
    [Total Stock Value Reporting]   DECIMAL (18, 2) NULL,
    [Excess Stock Value Reporting]  DECIMAL (18, 2) NULL,
    [Obsolescence Provision Amount] DECIMAL (18, 2) NULL,
    [Average Days Of Cover]         DECIMAL (9, 2)  NULL,
    [Service Level Percent]         DECIMAL (9, 4)  NULL,
    [Inventory Turns Annualised]    DECIMAL (9, 4)  NULL,
    [Days Inventory Outstanding]    DECIMAL (9, 2)  NULL,
    [Stockout Rate Percent]         DECIMAL (9, 4)  NULL,
    [Refresh Batch Id]              BIGINT          NULL,
    [Refreshed Datetime]            DATETIME2 (3)   NULL,
    [Intraday Refresh Count]        INT             NULL,
    CONSTRAINT [PK_Aggregate_Daily_Inventory_Health] PRIMARY KEY NONCLUSTERED ([Daily Inventory Health Key] ASC)
);
GO

CREATE UNIQUE CLUSTERED INDEX [CX_Aggregate_Daily_Inventory_Health_Grain]
    ON [Aggregate].[Daily Inventory Health] ([Snapshot Date] ASC, [Warehouse Site Key] ASC, [Product Category Key] ASC);
GO
