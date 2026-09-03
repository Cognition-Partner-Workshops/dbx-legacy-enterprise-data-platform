/*
    Fact.Purchase (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Purchase] (purchase order
                    line grain) shipped with the WideWorldImporters DW sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt; before Integration.usp_LoadFactPurchase.
    Called by     : deployment only. Loaded by Integration.usp_LoadFactPurchase
                    from Oracle WWI_PROC.PO_HEADER / PO_LINE via the staging
                    layer.
    Depends on    : Dimension.Vendor Contract, Dimension.Cost Center,
                    Dimension.Currency, Dimension.Warehouse Site (WP05).

    The sample fact only knew outers ordered and received. Procurement moved to
    Oracle in 2001 and the fact grew the contract, incoterm, landed-cost and
    three-way-match columns that the procure-to-pay accumulating snapshot and
    the supplier spend aggregate depend on.
*/
SET NOCOUNT ON;
GO

/* degenerate dimensions from the Oracle purchasing system */
IF COL_LENGTH(N'Fact.Purchase', N'Purchase Order Number') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Purchase Order Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Purchase Order Line Number') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Purchase Order Line Number] INT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Requisition Number') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Requisition Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Buyer Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Buyer Code] NVARCHAR (8) NULL;
GO

/* contract and accounting attribution */
IF COL_LENGTH(N'Fact.Purchase', N'Vendor Contract Key') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Vendor Contract Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Cost Center Key') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Cost Center Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Warehouse Site Key') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Warehouse Site Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Payment Terms Key') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Payment Terms Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Region Code] NVARCHAR (4) NULL;
GO

/*
    Landed cost. NA buys DDP and books duty in the unit price, EU buys from
    intra-community suppliers where the acquisition VAT is reverse charged and
    never hits cost, APAC buys FOB and books freight and customs duty
    separately - hence three columns that are mutually exclusive in practice.
*/
IF COL_LENGTH(N'Fact.Purchase', N'Incoterm Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Incoterm Code] NVARCHAR (3) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Transaction Currency Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Transaction Currency Code] NCHAR(3) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'FX Rate To Reporting') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [FX Rate To Reporting] DECIMAL (19, 9) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Unit Cost') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Unit Cost] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Extended Cost') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Extended Cost] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Freight In Amount') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Freight In Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Customs Duty Amount') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Customs Duty Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Recoverable Tax Amount') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Recoverable Tax Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Landed Cost Reporting') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Landed Cost Reporting] DECIMAL (18, 2) NULL;
GO

/* three-way match state, used by the P2P accumulating snapshot */
IF COL_LENGTH(N'Fact.Purchase', N'Quantity Received Base UOM') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Quantity Received Base UOM] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Quantity Invoiced Base UOM') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Quantity Invoiced Base UOM] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Source UOM Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Source UOM Code] NVARCHAR (10) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Match Status Code') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Match Status Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Expected Receipt Date Key') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Expected Receipt Date Key] DATE NULL;
GO

/* load control */
IF COL_LENGTH(N'Fact.Purchase', N'Natural Key Hash') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Natural Key Hash] BINARY (32) NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Inferred Member Flag') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Inferred Member Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Purchase', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Purchase] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Purchase_Po_Line'
                 AND object_id = OBJECT_ID(N'Fact.Purchase'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Purchase_Po_Line]
        ON [Fact].[Purchase] ([Purchase Order Number] ASC, [Purchase Order Line Number] ASC, [Date Key] ASC)
        ON [PS_Date] ([Date Key]);
GO
