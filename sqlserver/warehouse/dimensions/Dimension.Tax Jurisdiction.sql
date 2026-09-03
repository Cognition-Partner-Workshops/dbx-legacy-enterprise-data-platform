/*
    Object        : [Dimension].[Tax Jurisdiction]  (outrigger on City, Customer and Warehouse Site)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Geography.sql
    Depends on    : Sequences.TaxJurisdictionKey is NOT used - this dimension uses IDENTITY,
                    a 2007 finance-project artefact recorded in Integration.DimensionKeyRegistry
    Called by     : Integration.usp_MigrateStagedGeographyData

    An outrigger, not a dimension in its own right: facts never join to it, they
    reach it through [Dimension].[City].[Tax Jurisdiction Key] or, for a
    ship-from/ship-to pair, through both city rows.

    The grain is one row per taxing authority per effective period, which is the
    only shape that copes with all three regimes at once:

      NA    : a jurisdiction is a stack - state, county, city and sometimes a
              special district - and the rates add up. Each level is a row and
              [Parent Tax Jurisdiction Key] chains them.
      EU    : a jurisdiction is a member state with a standard, reduced,
              super-reduced and zero rate, all on one row.
      APAC  : a jurisdiction is a country, occasionally a state (India), with a
              single rate plus a separate treatment for imports.

    Rates are effective-dated here rather than versioned Type 2, because the tax
    engine needs "the rate on the transaction date" and a Type 2 lookup through
    the city would have required rekeying the sales facts.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Tax Jurisdiction', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Tax Jurisdiction]
    (
        [Tax Jurisdiction Key]          INT             IDENTITY(1, 1) NOT NULL,
        [Tax Jurisdiction Code]         NVARCHAR(20)    NOT NULL,
        [Tax Jurisdiction Name]         NVARCHAR(100)   NOT NULL,
        [Tax Regime Code]               NVARCHAR(10)    NOT NULL,   -- SALESTAX / VAT / GST / CONSUMPTION
        [Jurisdiction Level Code]       NVARCHAR(10)    NOT NULL,   -- COUNTRY / STATE / COUNTY / CITY / DISTRICT
        [Parent Tax Jurisdiction Key]   INT             NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Subdivision Code]              NVARCHAR(10)    NULL,
        [Geography Key]                 INT             NULL,

        [Standard Rate]                 DECIMAL(9, 5)   NULL,
        [Reduced Rate]                  DECIMAL(9, 5)   NULL,
        [Super Reduced Rate]            DECIMAL(9, 5)   NULL,
        [Zero Rate Applies]             BIT             NULL,
        [Import Rate]                   DECIMAL(9, 5)   NULL,
        [Combined Rate]                 DECIMAL(9, 5)   NULL,   -- NA only: the sum down the stack, maintained by the load
        [Rate Effective From]           DATE            NOT NULL,
        [Rate Effective To]             DATE            NOT NULL,

        [Tax Authority Name]            NVARCHAR(120)   NULL,
        [Filing Frequency Code]         NVARCHAR(10)    NULL,   -- MONTHLY / QUARTERLY / ANNUAL
        [Registration Threshold Amount] DECIMAL(18, 2)  NULL,
        [Registration Currency Code]    NVARCHAR(3)     NULL,
        [Reverse Charge Available]      BIT             NULL,
        [Digital Services Rule Code]    NVARCHAR(20)    NULL,
        [Nexus Rule Code]               NVARCHAR(20)    NULL,   -- NA economic-nexus thresholds post-2018
        [Is Origin Based]               BIT             NULL,   -- a handful of NA states tax at origin, not destination
        [Rounding Rule Code]            NVARCHAR(10)    NULL,
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Tax_Jurisdiction] PRIMARY KEY CLUSTERED ([Tax Jurisdiction Key] ASC),
        CONSTRAINT [UQ_Dimension_Tax_Jurisdiction_Period]
            UNIQUE ([Tax Jurisdiction Code], [Rate Effective From]),
        CONSTRAINT [CK_Dimension_Tax_Jurisdiction_Level]
            CHECK ([Jurisdiction Level Code] IN (N'COUNTRY', N'STATE', N'COUNTY', N'CITY', N'DISTRICT'))
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Tax_Jurisdiction_Lookup]
        ON [Dimension].[Tax Jurisdiction] ([Country Code] ASC, [Subdivision Code] ASC, [Rate Effective From] ASC)
        INCLUDE ([Tax Jurisdiction Key], [Combined Rate], [Standard Rate]);
END;
GO
