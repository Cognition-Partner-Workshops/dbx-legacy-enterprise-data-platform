/*
    Fact.Stock Holding (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Stock Holding] shipped
                    with the WideWorldImporters DW sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt; before
                    Integration.usp_LoadFactStockHolding.
    Called by     : deployment only. Loaded by
                    Integration.usp_LoadFactStockHolding.
    Depends on    : Dimension.Warehouse Site, Dimension.Stock Item (WP05).

    The sample table is a *current state* table with no date at all: it is
    truncated and reloaded every night, so history is lost. Rather than change
    its grain (which the sample's Power BI content depends on) the estate keeps
    it as the current position and carries the as-at columns needed to tell
    which run produced it. Historic positions live in
    Fact.Daily Inventory Snapshot, which is a true periodic snapshot.
*/
SET NOCOUNT ON;
GO

IF COL_LENGTH(N'Fact.Stock Holding', N'Warehouse Site Key') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Warehouse Site Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Region Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'As At Date Key') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [As At Date Key] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Quantity Allocated') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Quantity Allocated] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Quantity Available') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Quantity Available] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Quantity In Transit') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Quantity In Transit] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Quantity On Order') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Quantity On Order] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Quantity Quarantined') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Quantity Quarantined] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Stock Value At Cost') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Stock Value At Cost] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Stock Value Reporting') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Stock Value Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Days Of Cover') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Days Of Cover] DECIMAL (9, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Below Reorder Level Flag') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Below Reorder Level Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Last Movement Date Key') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Last Movement Date Key] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Last Stocktake Date Key') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Last Stocktake Date Key] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Average Daily Issues') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Average Daily Issues] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Supplier Key') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Supplier Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'FX Rate To Reporting') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [FX Rate To Reporting] DECIMAL (18, 8) NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Stock Holding', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Stock Holding] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Stock_Holding_Site_Item'
                 AND object_id = OBJECT_ID(N'Fact.Stock Holding'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Stock_Holding_Site_Item]
        ON [Fact].[Stock Holding] ([Warehouse Site Key] ASC, [Stock Item Key] ASC)
        INCLUDE ([Quantity Available], [Stock Value Reporting], [Below Reorder Level Flag]);
GO
