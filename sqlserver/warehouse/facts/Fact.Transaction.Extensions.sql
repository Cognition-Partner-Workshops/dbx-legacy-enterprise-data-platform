/*
    Fact.Transaction (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Transaction] (the combined
                    customer/supplier ledger transaction fact) shipped with the
                    WideWorldImporters DW sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt; before the AR/AP aging loads.
    Called by     : deployment only. Loaded by the finance loads; read by
                    Integration.usp_LoadFactArAgingSnapshot and
                    Integration.usp_LoadFactApAgingSnapshot.
    Depends on    : Dimension.Payment Terms, Dimension.Currency,
                    Dimension.Cost Center (WP05).

    This fact mixes AR and AP in one table because the 1990s general ledger did.
    Finance never separated them; instead each project bolted on the columns it
    needed. The aging snapshots and the finance close aggregate all read from
    here, which is why the aging bucket is stored on the row as well as being
    derivable - the nightly close needs it to be stable after the fact.
*/
SET NOCOUNT ON;
GO

IF COL_LENGTH(N'Fact.Transaction', N'Ledger Side Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Ledger Side Code] NVARCHAR (2) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Region Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Cost Center Key') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Cost Center Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Payment Terms Key') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Payment Terms Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Currency Key') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Currency Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Transaction Currency Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Transaction Currency Code] NCHAR(3) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'FX Rate To Reporting') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [FX Rate To Reporting] DECIMAL (19, 9) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Outstanding Balance Reporting') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Outstanding Balance Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Unrealised FX Gain Loss') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Unrealised FX Gain Loss] DECIMAL (18, 2) NULL;
GO

/* aging, frozen at close time */
IF COL_LENGTH(N'Fact.Transaction', N'Due Date Key') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Due Date Key] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Days Overdue') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Days Overdue] INT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Aging Bucket Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Aging Bucket Code] NVARCHAR (10) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Dispute Flag') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Dispute Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Collection Status Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Collection Status Code] NVARCHAR (6) NULL;
GO

/* general ledger attribution added when the estate started posting to Oracle GL */
IF COL_LENGTH(N'Fact.Transaction', N'GL Account Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [GL Account Code] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Fiscal Year') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Fiscal Year] SMALLINT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Fiscal Period') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Fiscal Period] TINYINT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Posting Status Code') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Posting Status Code] NVARCHAR (4) NULL;
GO

/* load control */
IF COL_LENGTH(N'Fact.Transaction', N'Natural Key Hash') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Natural Key Hash] BINARY (32) NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Transaction', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Transaction] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Transaction_Open_Items'
                 AND object_id = OBJECT_ID(N'Fact.Transaction'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Transaction_Open_Items]
        ON [Fact].[Transaction] ([Ledger Side Code] ASC, [Is Finalized] ASC, [Due Date Key] ASC)
        INCLUDE ([Outstanding Balance Reporting], [Aging Bucket Code], [Region Code])
        ON [PS_Date] ([Date Key]);
GO
