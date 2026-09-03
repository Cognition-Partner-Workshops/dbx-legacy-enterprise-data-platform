/*
    Fact.Sale (estate extensions)

    Object        : ALTER of the pre-existing [Fact].[Sale] shipped with the
                    WideWorldImporters DW sample.
    Deploy target : WideWorldImportersDW
    Deploy order  : after wwi-dw-ssdt has created [Fact].[Sale]; before
                    Integration.usp_LoadFactSale.
    Called by     : deployment only. Loaded by Integration.usp_LoadFactSale,
                    which is driven by FACT_Load_Sale / FACT_NA_Load_Sale /
                    FACT_EU_Load_Sale / FACT_APAC_Load_Sale.
    Depends on    : Dimension.Currency, Dimension.Sales Territory,
                    Dimension.Sales Channel, Dimension.Promotion (WP05).

    The Microsoft sample fact was single-region, single-currency and carried no
    degenerate dimensions beyond [WWI Invoice ID]. Twenty years of accretion
    added, in this order (see the column comments): the 1998 multi-currency
    project, the 2004 territory realignment, the 2009 VAT/GST rework, the 2013
    web channel, the 2016 promotion attribution work and the 2019 restatement
    controls. Nothing was ever removed, which is why several columns overlap.
*/
SET NOCOUNT ON;
GO

/* ---- 1998 multi-currency project -------------------------------------- */
IF COL_LENGTH(N'Fact.Sale', N'Transaction Currency Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Transaction Currency Code] NCHAR(3) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Currency Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Currency Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'FX Rate To Reporting') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [FX Rate To Reporting] DECIMAL (19, 9) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'FX Rate Effective Date') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [FX Rate Effective Date] DATE NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'FX Rate Source Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [FX Rate Source Code] NVARCHAR (10) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Total Excluding Tax Reporting') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Total Excluding Tax Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Tax Amount Reporting') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Tax Amount Reporting] DECIMAL (18, 2) NULL;
GO

/* ---- 2004 territory realignment --------------------------------------- */
IF COL_LENGTH(N'Fact.Sale', N'Region Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Region Code] NVARCHAR (4) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Sales Territory Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Sales Territory Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Fiscal Year') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Fiscal Year] SMALLINT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Fiscal Period') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Fiscal Period] TINYINT NULL;
GO

/*
    ---- 2009 tax rework ----
    [Tax Rate] on the base table is the single rate the 1996 system knew about.
    NA still uses it as the combined state+county sales-tax rate. EU and APAC
    do not: EU splits the VAT rate out, carries the reverse-charge flag and the
    customer VAT registration, APAC carries a GST rate plus a GST-free
    indicator for exempt food lines. All three write [Tax Amount], so the
    measure is comparable even though the derivation is not.
*/
IF COL_LENGTH(N'Fact.Sale', N'Tax Regime Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Tax Regime Code] NVARCHAR (10) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'VAT Rate') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [VAT Rate] DECIMAL (18, 3) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'VAT Reverse Charge Flag') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [VAT Reverse Charge Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Customer Tax Registration') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Customer Tax Registration] NVARCHAR (25) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'GST Rate') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [GST Rate] DECIMAL (18, 3) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'GST Free Flag') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [GST Free Flag] BIT NULL;
GO

/* ---- 2013 channel / 2016 promotion attribution ------------------------ */
IF COL_LENGTH(N'Fact.Sale', N'Sales Channel Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Sales Channel Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Promotion Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Promotion Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Line Discount Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Line Discount Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Freight Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Freight Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Cost Of Sale Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Cost Of Sale Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Quantity Base UOM') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Quantity Base UOM] DECIMAL (18, 4) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Source UOM Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Source UOM Code] NVARCHAR (10) NULL;
GO

/*
    ---- 2007 amount harmonisation ----
    The group reporting project could not agree that [Total Excluding Tax] was
    the same thing in every region, so a parallel set of harmonised amounts was
    bolted on and every downstream load was pointed at these instead. The
    original sample columns were left populated for the old Access reports.
*/
IF COL_LENGTH(N'Fact.Sale', N'Gross Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Gross Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Net Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Net Amount] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Net Amount Reporting') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Net Amount Reporting] DECIMAL (18, 2) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Gross Margin Amount') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Gross Margin Amount] DECIMAL (18, 2) NULL;
GO

/* ---- 2011 segmentation project / 2019 change capture ------------------- */
IF COL_LENGTH(N'Fact.Sale', N'Customer Segment Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Customer Segment Key] INT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Source Row Version') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Source Row Version] BINARY (8) NULL;
GO

/* ---- degenerate dimensions carried on the line ------------------------ */
IF COL_LENGTH(N'Fact.Sale', N'Invoice Number') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Invoice Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Order Number') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Order Number] NVARCHAR (20) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Invoice Line Number') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Invoice Line Number] INT NULL;
GO

/*
    ---- 2019 restatement controls ----
    Fact.Sale uses the *reversal* correction pattern: a restated source line is
    never updated in place. The prior row is offset by a negated copy carrying
    [Correction Type Code] = 'REV' and the new value is inserted as 'RES'.
    (Fact.Payment uses the opposite, in-place, pattern - see that file.)
*/
IF COL_LENGTH(N'Fact.Sale', N'Natural Key Hash') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Natural Key Hash] BINARY (32) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Correction Type Code') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Correction Type Code] NVARCHAR (3) NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Corrected Sale Key') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Corrected Sale Key] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Inferred Member Flag') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Inferred Member Flag] BIT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Batch Id') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Batch Id] BIGINT NULL;
GO
IF COL_LENGTH(N'Fact.Sale', N'Load Datetime') IS NULL
    ALTER TABLE [Fact].[Sale] ADD [Load Datetime] DATETIME2 (3) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Sale_Natural_Key_Hash'
                 AND object_id = OBJECT_ID(N'Fact.Sale'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Sale_Natural_Key_Hash]
        ON [Fact].[Sale] ([Natural Key Hash] ASC, [Invoice Date Key] ASC)
        ON [PS_Date] ([Invoice Date Key]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Fact_Sale_Region_Fiscal'
                 AND object_id = OBJECT_ID(N'Fact.Sale'))
    CREATE NONCLUSTERED INDEX [IX_Fact_Sale_Region_Fiscal]
        ON [Fact].[Sale] ([Region Code] ASC, [Fiscal Year] ASC, [Fiscal Period] ASC)
        INCLUDE ([Total Excluding Tax Reporting], [Cost Of Sale Amount])
        ON [PS_Date] ([Invoice Date Key]);
GO
