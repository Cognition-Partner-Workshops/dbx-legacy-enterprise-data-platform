/*
    Daily periodic snapshot facts, deployed together because the nightly
    snapshot job builds all three in one pass over the same source facts:
      [Fact].[Daily Inventory Snapshot]
      [Fact].[Daily Sales Snapshot]
      [Fact].[Daily Backlog]
*/

/*
    Fact.Daily Inventory Snapshot

    Object        : [Fact].[Daily Inventory Snapshot] - periodic snapshot fact,
                    one row per stock item per warehouse site per day.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Stock Item, Dimension.Warehouse Site (WP05)
                    and after [Fact].[Movement].
    Called by     : loaded by Integration.usp_LoadFactDailyInventorySnapshot,
                    which rebuilds one day at a time with a delete-by-window
                    incremental (the snapshot is re-derivable, so the load
                    deletes the target day and re-inserts rather than merging).
    Grain         : stock item x warehouse site x snapshot date.

    This is the fact that grows fastest in the estate and it is the reason the
    date partition scheme exists. [Fact].[Stock Holding] remains the current
    position only; history lives here.
*/
CREATE TABLE [Fact].[Daily Inventory Snapshot] (
    [Daily Inventory Snapshot Key]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [Snapshot Date Key]             DATE            NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Warehouse Site Key]            INT             NOT NULL,
    [Product Category Key]          INT             NULL,
    [Supplier Key]                  INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Opening Quantity]              DECIMAL (18, 4) NULL,
    [Quantity On Hand]              DECIMAL (18, 4) NOT NULL,
    [Quantity Allocated]            DECIMAL (18, 4) NULL,
    [Quantity Available]            DECIMAL (18, 4) NULL,
    [Quantity On Order]             DECIMAL (18, 4) NULL,
    [Quantity In Transit]           DECIMAL (18, 4) NULL,
    [Quantity Quarantined]          DECIMAL (18, 4) NULL,
    [Quantity Received Today]       DECIMAL (18, 4) NULL,
    [Quantity Issued Today]         DECIMAL (18, 4) NULL,
    [Quantity Adjusted Today]       DECIMAL (18, 4) NULL,
    [Unit Cost At Snapshot]         DECIMAL (18, 4) NULL,
    [Costing Method Code]           NVARCHAR (6)    NULL,
    [Stock Value At Cost]           DECIMAL (18, 2) NULL,
    [Stock Value Reporting]         DECIMAL (18, 2) NULL,
    [Reorder Level]                 DECIMAL (18, 4) NULL,
    [Target Stock Level]            DECIMAL (18, 4) NULL,
    [Below Reorder Level Flag]      BIT             NULL,
    [Stockout Flag]                 BIT             NULL,
    [Excess Stock Flag]             BIT             NULL,
    [Days Of Cover]                 DECIMAL (9, 2)  NULL,
    [Rolling 30 Day Issue Qty]      DECIMAL (18, 4) NULL,
    [Days Since Last Movement]      INT             NULL,
    [Slow Moving Flag]              BIT             NULL,
    [Obsolescence Provision Amount] DECIMAL (18, 2) NULL,
    [Shelf Life Days Remaining]     INT             NULL,
    [Stock Age Bucket Code]         NVARCHAR (10)   NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Daily_Inventory_Snapshot] PRIMARY KEY NONCLUSTERED ([Daily Inventory Snapshot Key] ASC, [Snapshot Date Key] ASC) ON [PS_Date] ([Snapshot Date Key]),
    CONSTRAINT [FK_Fact_Daily_Inventory_Snapshot_Snapshot_Date_Key_Dimension_Date] FOREIGN KEY ([Snapshot Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Daily_Inventory_Snapshot_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key])
)
ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Daily_Inventory_Snapshot_Grain]
    ON [Fact].[Daily Inventory Snapshot] ([Snapshot Date Key] ASC, [Stock Item Key] ASC, [Warehouse Site Key] ASC)
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Daily_Inventory_Snapshot_Exceptions]
    ON [Fact].[Daily Inventory Snapshot] ([Snapshot Date Key] ASC, [Stockout Flag] ASC, [Below Reorder Level Flag] ASC)
    INCLUDE ([Stock Value Reporting], [Days Of Cover], [Warehouse Site Key])
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Daily_Inventory_Snapshot]
    ON [Fact].[Daily Inventory Snapshot]
    ON [PS_Date] ([Snapshot Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Daily inventory position periodic snapshot, rebuilt day-at-a-time',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Daily Inventory Snapshot';
GO

/*
    Fact.Daily Sales Snapshot

    Object        : [Fact].[Daily Sales Snapshot] - periodic snapshot fact, one
                    row per salesperson per territory per day, holding the
                    day's trading position and the running month-to-date and
                    year-to-date figures that the sales bonus scheme uses.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Sale], [Fact].[Order], [Fact].[Return].
    Called by     : loaded by Integration.usp_LoadFactDailySalesSnapshot with a
                    delete-by-window incremental over the reporting window.
    Grain         : salesperson x sales territory x snapshot date.

    The running totals are stored, not derived, because the bonus scheme was
    signed off against the numbers as they stood on the day and finance refuses
    to let a restatement change a paid bonus. That is why a late correction to
    [Fact].[Sale] does not flow into rows already marked
    [Bonus Locked Flag] = 1.

    Fiscal attribution is regional: NA uses a 4-4-5 retail calendar, EU uses
    calendar months against an April-March fiscal year, APAC uses a July-June
    fiscal year. The same trading day therefore lands in three different fiscal
    periods depending on the row's region.
*/
CREATE TABLE [Fact].[Daily Sales Snapshot] (
    [Daily Sales Snapshot Key]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [Snapshot Date Key]             DATE            NOT NULL,
    [Salesperson Key]               INT             NOT NULL,
    [Sales Territory Key]           INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Customer Segment Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Fiscal Year]                   SMALLINT        NULL,
    [Fiscal Period]                 TINYINT         NULL,
    [Fiscal Week]                   TINYINT         NULL,
    [Fiscal Calendar Code]          NVARCHAR (10)   NULL,
    [Invoice Count]                 INT             NULL,
    [Order Count]                   INT             NULL,
    [Distinct Customer Count]       INT             NULL,
    [Line Count]                    INT             NULL,
    [Quantity Sold]                 DECIMAL (18, 4) NULL,
    [Gross Sales Amount]            DECIMAL (18, 2) NULL,
    [Discount Amount]               DECIMAL (18, 2) NULL,
    [Net Sales Amount]              DECIMAL (18, 2) NULL,
    [Tax Amount]                    DECIMAL (18, 2) NULL,
    [Freight Amount]                DECIMAL (18, 2) NULL,
    [Cost Of Sales Amount]          DECIMAL (18, 2) NULL,
    [Gross Margin Amount]           DECIMAL (18, 2) NULL,
    [Margin Percent]                DECIMAL (9, 4)  NULL,
    [Returns Amount]                DECIMAL (18, 2) NULL,
    [Credit Note Amount]            DECIMAL (18, 2) NULL,
    [Net Sales Amount Reporting]    DECIMAL (18, 2) NULL,
    [Average Order Value]           DECIMAL (18, 2) NULL,
    [Commissionable Revenue]        DECIMAL (18, 2) NULL,
    [Commission Rate]               DECIMAL (9, 4)  NULL,
    [Month To Date Net Sales]       DECIMAL (18, 2) NULL,
    [Year To Date Net Sales]        DECIMAL (18, 2) NULL,
    [Month To Date Margin]          DECIMAL (18, 2) NULL,
    [Quota Amount]                  DECIMAL (18, 2) NULL,
    [Quota Attainment Percent]      DECIMAL (9, 4)  NULL,
    [Commission Accrued Amount]     DECIMAL (18, 2) NULL,
    [Bonus Locked Flag]             BIT             CONSTRAINT [DF_Fact_Daily_Sales_Snapshot_Bonus_Locked_Flag] DEFAULT (0) NOT NULL,
    [Restated Flag]                 BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Daily_Sales_Snapshot] PRIMARY KEY NONCLUSTERED ([Daily Sales Snapshot Key] ASC, [Snapshot Date Key] ASC) ON [PS_Date] ([Snapshot Date Key]),
    CONSTRAINT [FK_Fact_Daily_Sales_Snapshot_Snapshot_Date_Key_Dimension_Date] FOREIGN KEY ([Snapshot Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Daily_Sales_Snapshot_Salesperson_Key_Dimension_Salesperson] FOREIGN KEY ([Salesperson Key]) REFERENCES [Dimension].[Salesperson] ([Salesperson Key])
)
ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Daily_Sales_Snapshot_Grain]
    ON [Fact].[Daily Sales Snapshot] ([Snapshot Date Key] ASC, [Salesperson Key] ASC, [Sales Territory Key] ASC)
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Daily_Sales_Snapshot_Fiscal]
    ON [Fact].[Daily Sales Snapshot] ([Region Code] ASC, [Fiscal Year] ASC, [Fiscal Period] ASC)
    INCLUDE ([Net Sales Amount Reporting], [Gross Margin Amount], [Quota Attainment Percent])
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Daily_Sales_Snapshot]
    ON [Fact].[Daily Sales Snapshot]
    ON [PS_Date] ([Snapshot Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Daily salesperson/territory trading snapshot with stored running totals',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Daily Sales Snapshot';
GO

/*
    Fact.Daily Backlog

    Object        : [Fact].[Daily Backlog] - periodic snapshot fact, one row per
                    open order line per day it remains open.
    Deploy target : WideWorldImportersDW
    Deploy order  : after [Fact].[Order] and [Fact].[Daily Inventory Snapshot].
    Called by     : loaded by Integration.usp_LoadFactDailySalesSnapshot in the
                    same nightly pass; delete-by-window on the snapshot date.
    Grain         : order line x snapshot date, restricted to lines with open
                    quantity.

    A line reappears every night until it is fully despatched or cancelled, so
    the aging of the backlog is measurable directly. This is the single largest
    row producer after the inventory snapshot and the reason the nightly run
    slipped past its window in 2019.
*/
CREATE TABLE [Fact].[Daily Backlog] (
    [Daily Backlog Key]             BIGINT          IDENTITY (1, 1) NOT NULL,
    [Snapshot Date Key]             DATE            NOT NULL,
    [Order Date Key]                DATE            NOT NULL,
    [Requested Delivery Date Key]   DATE            NULL,
    [Promised Delivery Date Key]    DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Salesperson Key]               INT             NULL,
    [Warehouse Site Key]            INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Order Number]                  NVARCHAR (20)   NOT NULL,
    [Order Line Number]             INT             NOT NULL,
    [Backorder Reason Code]         NVARCHAR (6)    NULL,
    [Quantity Ordered]              DECIMAL (18, 4) NOT NULL,
    [Quantity Despatched To Date]   DECIMAL (18, 4) NULL,
    [Quantity Open]                 DECIMAL (18, 4) NOT NULL,
    [Quantity Available At Site]    DECIMAL (18, 4) NULL,
    [Coverable Quantity]            DECIMAL (18, 4) NULL,
    [Open Value Reporting]          DECIMAL (18, 2) NULL,
    [Open Margin Reporting]         DECIMAL (18, 2) NULL,
    [Backlog Age Days]              INT             NOT NULL,
    [Days Past Promise]             INT             NULL,
    [Past Promise Flag]             BIT             NULL,
    [Stock Constrained Flag]        BIT             NULL,
    [Credit Hold Constrained Flag]  BIT             NULL,
    [Expedite Requested Flag]       BIT             NULL,
    [Backlog Aging Bucket]          NVARCHAR (10)   NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Daily_Backlog] PRIMARY KEY NONCLUSTERED ([Daily Backlog Key] ASC, [Snapshot Date Key] ASC) ON [PS_Date] ([Snapshot Date Key]),
    CONSTRAINT [FK_Fact_Daily_Backlog_Snapshot_Date_Key_Dimension_Date] FOREIGN KEY ([Snapshot Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Daily_Backlog_Stock_Item_Key_Dimension_Stock Item] FOREIGN KEY ([Stock Item Key]) REFERENCES [Dimension].[Stock Item] ([Stock Item Key])
)
ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Daily_Backlog_Grain]
    ON [Fact].[Daily Backlog] ([Snapshot Date Key] ASC, [Order Number] ASC, [Order Line Number] ASC)
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Daily_Backlog_Aging]
    ON [Fact].[Daily Backlog] ([Snapshot Date Key] ASC, [Backlog Aging Bucket] ASC, [Region Code] ASC)
    INCLUDE ([Open Value Reporting], [Quantity Open], [Stock Constrained Flag])
    ON [PS_Date] ([Snapshot Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Daily_Backlog]
    ON [Fact].[Daily Backlog]
    ON [PS_Date] ([Snapshot Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Daily open order backlog periodic snapshot with aging buckets',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Daily Backlog';
GO
