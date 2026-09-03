/*
    Fact.Movement (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Movement] (stock movement
                    transaction grain) shipped with the WideWorldImporters DW
                    sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt; before Integration.usp_LoadFactMovement.
    Called by     : deployment only. Loaded by Integration.usp_LoadFactMovement.
    Depends on    : Dimension.Warehouse Site, Dimension.Stock Item (WP05).

    The sample movement fact is a signed quantity and nothing else. The 2005
    multi-site warehouse programme added the site, bin and movement-reason
    columns; the 2011 stock-valuation project added the valued columns that
    Fact.Daily Inventory Snapshot rolls forward. Movement valuation is regional:
    NA runs weighted average, EU runs FIFO layers, APAC runs standard cost with
    a purchase price variance - hence three cost columns rather than one.
*/
SET NOCOUNT ON;
GO

IF COL_LENGTH(N'Fact.Movement', N'Warehouse Site Key') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Warehouse Site Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Region Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Bin Location') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Bin Location] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Movement Reason Code') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Movement Reason Code] NVARCHAR (6) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Movement Direction') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Movement Direction] NCHAR (1) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Quantity Base UOM') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Quantity Base UOM] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Source UOM Code') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Source UOM Code] NVARCHAR (10) NULL;
GO

/* valuation - one column per regional costing method, only one is populated */
IF COL_LENGTH(N'Fact.Movement', N'Weighted Average Cost') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Weighted Average Cost] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'FIFO Layer Cost') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [FIFO Layer Cost] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Standard Cost') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Standard Cost] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Purchase Price Variance') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Purchase Price Variance] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Movement Value Reporting') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Movement Value Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Costing Method Code') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Costing Method Code] NVARCHAR (6) NULL;
GO

/* degenerate references back to the documents that caused the movement */
IF COL_LENGTH(N'Fact.Movement', N'Invoice Number') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Invoice Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Purchase Order Number') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Purchase Order Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Stock Take Reference') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Stock Take Reference] NVARCHAR (20) NULL;
GO

/* load control */
IF COL_LENGTH(N'Fact.Movement', N'Natural Key Hash') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Natural Key Hash] BINARY (32) NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Inferred Member Flag') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Inferred Member Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Movement', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Movement] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Movement_Site_Item_Date'
                 AND object_id = OBJECT_ID(N'Fact.Movement'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Movement_Site_Item_Date]
        ON [Fact].[Movement] ([Warehouse Site Key] ASC, [Stock Item Key] ASC, [Date Key] ASC)
        INCLUDE ([Quantity Base UOM], [Movement Value Reporting])
        ON [PS_Date] ([Date Key]);
GO
