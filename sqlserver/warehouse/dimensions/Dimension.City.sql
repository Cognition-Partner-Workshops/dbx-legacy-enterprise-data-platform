/*
    Object        : [Dimension].[City]  (SCD Type 2 - boundary and territory changes are versioned)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql, before Dimension.Geography.sql
    Depends on    : Sequences.CityKey (WideWorldImportersDW baseline),
                    Dimension.Geography (outrigger), Dimension.Tax Jurisdiction (outrigger)
    Called by     : Integration.usp_MigrateStagedCityData

    The Microsoft sample's City dimension, extended with the address-standardisation
    outputs and the tax-jurisdiction linkage the regional loads need. Two outriggers
    hang off it: [Dimension].[Geography] (country / subregion attributes shared with
    supplier and warehouse rows) and [Dimension].[Tax Jurisdiction] (the NA
    state/county/city tax stack). They are outriggers rather than columns because
    both are maintained on their own cadence by different teams.

    Postal standardisation differs per region and each has its own column set,
    because the vendors return different things:
      NA    : CASS-style result - ZIP+4, delivery point, carrier route, DPV code.
      EU    : country-specific postcode format, locality and administrative area
              per the national addressing standard, plus a NUTS region code.
      APAC   : mixed - some countries have no postcode at all, so the load stores a
              [Postal Format Code] and leaves [Postcode Standardized] null rather
              than fabricating one.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.City', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[City]
    (
        [City Key]                      INT                 CONSTRAINT [DF_Dimension_City_City_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CityKey]) NOT NULL,
        [WWI City ID]                   INT                 NOT NULL,
        [City]                          NVARCHAR(50)        NOT NULL,
        [State Province]                NVARCHAR(50)        NOT NULL,
        [Country]                       NVARCHAR(60)        NOT NULL,
        [Continent]                     NVARCHAR(30)        NOT NULL,
        [Sales Territory]               NVARCHAR(50)        NOT NULL,
        [Region]                        NVARCHAR(30)        NOT NULL,
        [Subregion]                     NVARCHAR(30)        NOT NULL,
        [Location]                      [sys].[geography]   NULL,
        [Latest Recorded Population]    BIGINT              NOT NULL,
        [Valid From]                    DATETIME2(7)        NOT NULL,
        [Valid To]                      DATETIME2(7)        NOT NULL,
        [Lineage Key]                   INT                 NOT NULL,
        CONSTRAINT [PK_Dimension_City] PRIMARY KEY CLUSTERED ([City Key] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_City_WWICityID]
        ON [Dimension].[City] ([WWI City ID] ASC, [Valid From] ASC, [Valid To] ASC);
END;
GO

IF COL_LENGTH(N'Dimension.City', N'Geography Key') IS NULL
    ALTER TABLE [Dimension].[City] ADD
        [Geography Key]                 INT             NULL,   -- outrigger
        [Tax Jurisdiction Key]          INT             NULL,   -- outrigger
        [Country Key]                   INT             NULL,
        [Region Key]                    INT             NULL,
        [Sales Territory Key]           INT             NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Time Zone Name]                NVARCHAR(60)    NULL,
        [UTC Offset Minutes]            SMALLINT        NULL,
        [Observes Daylight Saving]      BIT             NULL;
GO

/* NA address standardisation (CASS-style vendor output). */
IF COL_LENGTH(N'Dimension.City', N'ZIP Code') IS NULL
    ALTER TABLE [Dimension].[City] ADD
        [ZIP Code]                      NVARCHAR(5)     NULL,
        [ZIP Plus Four]                 NVARCHAR(10)    NULL,
        [County Name]                   NVARCHAR(60)    NULL,
        [County FIPS Code]              NVARCHAR(5)     NULL,
        [Carrier Route Code]            NVARCHAR(9)     NULL,
        [Delivery Point Code]           NVARCHAR(4)     NULL,
        [DPV Confirmation Code]         NVARCHAR(2)     NULL,
        [Metropolitan Statistical Area] NVARCHAR(80)    NULL;
GO

/* EU address standardisation (national standards plus NUTS). */
IF COL_LENGTH(N'Dimension.City', N'Postcode Standardized') IS NULL
    ALTER TABLE [Dimension].[City] ADD
        [Postcode Standardized]         NVARCHAR(12)    NULL,
        [Postcode Format Pattern]       NVARCHAR(20)    NULL,
        [Locality Name]                 NVARCHAR(80)    NULL,
        [Administrative Area Level 1]   NVARCHAR(80)    NULL,
        [Administrative Area Level 2]   NVARCHAR(80)    NULL,
        [NUTS Level 3 Code]             NVARCHAR(10)    NULL,
        [Is EU Member State]            BIT             NULL;
GO

/* APAC address handling - deliberately sparse; several countries have no postcode. */
IF COL_LENGTH(N'Dimension.City', N'Postal Format Code') IS NULL
    ALTER TABLE [Dimension].[City] ADD
        [Postal Format Code]            NVARCHAR(10)    NULL,   -- NUM6 / NUM4 / ALPHA / NONE
        [Prefecture Or Province]        NVARCHAR(80)    NULL,
        [District Name]                 NVARCHAR(80)    NULL,
        [Local Script City Name]        NVARCHAR(120)   NULL,
        [Address Line Order Code]       NVARCHAR(10)    NULL;   -- WESTERN / REVERSED
GO

IF COL_LENGTH(N'Dimension.City', N'Effective From') IS NULL
    ALTER TABLE [Dimension].[City] ADD
        [Effective From]                DATETIME2(7)    NULL,
        [Effective To]                  DATETIME2(7)    NULL,
        [Effective From Date]           DATE            NULL,
        [Effective Sequence]            SMALLINT        NULL,
        [Is Current Row]                BIT             NULL,
        [Version Number]                INT             NULL,
        [Row Hash Type 2]               VARBINARY(32)   NULL,
        [Is Inferred Member]            BIT             NULL,
        [Last Load Batch Id]            BIGINT          NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_City_Current'
                 AND object_id = OBJECT_ID(N'Dimension.City'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_City_Current]
        ON [Dimension].[City] ([WWI City ID] ASC, [Is Current Row] ASC)
        INCLUDE ([City Key], [Geography Key], [Tax Jurisdiction Key]);
GO
