/*
    Object        : [Dimension].[Region]  (SCD Type 1, top of the geography hierarchy)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql, before Dimension.Country.sql
    Depends on    : Sequences.RegionKey
    Called by     : Integration.usp_MigrateStagedGeographyData

    Three operating regions plus a GLOBAL row for shared reference members. This is
    the dimension that records *how* each region differs, so the load procedures and
    the reports can look the behaviour up instead of hard-coding it in fifteen
    places (which they also do, in the older packages).
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Region', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Region]
    (
        [Region Key]                    INT             CONSTRAINT [DF_Dimension_Region_Key] DEFAULT (NEXT VALUE FOR [Sequences].[RegionKey]) NOT NULL,
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Region Name]                   NVARCHAR(60)    NOT NULL,
        [Operating Company Name]        NVARCHAR(100)   NULL,
        [Head Office City]              NVARCHAR(60)    NULL,
        [Reporting Currency Code]       NVARCHAR(3)     NULL,
        [Tax Regime Code]               NVARCHAR(10)    NULL,   -- SALESTAX / VAT / GST
        [Fiscal Calendar Code]          NVARCHAR(20)    NULL,   -- NA_454 / EU_CAL / APAC_MIXED
        [Fiscal Year End Month]         SMALLINT        NULL,
        [Fiscal Year Naming Rule]       NVARCHAR(20)    NULL,   -- ENDYEAR / STARTYEAR
        [Week Start Day]                SMALLINT        NULL,
        [FX Translation Method Code]    NVARCHAR(10)    NULL,
        [FX Rate Source Code]           NVARCHAR(20)    NULL,   -- CORPORATE / ECB / RBA / LOCALBANK
        [Consent Model Code]            NVARCHAR(10)    NULL,   -- OPTOUT (NA) / OPTIN (EU) / MIXED (APAC)
        [Data Protection Regime Code]   NVARCHAR(20)    NULL,
        [Default Retention Years]       SMALLINT        NULL,
        [Address Standard Code]         NVARCHAR(20)    NULL,   -- CASS / NATIONAL / MIXED
        [ETL Cutoff Local Time]         TIME(0)         NULL,
        [ETL Time Zone Name]            NVARCHAR(60)    NULL,
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Region] PRIMARY KEY CLUSTERED ([Region Key] ASC),
        CONSTRAINT [UQ_Dimension_Region_Code] UNIQUE ([Region Code])
    );
END;
GO

/*
    Seeded here rather than by a load procedure: the region list is structural and
    the ETL configuration in etl.Configuration is keyed on these codes.
*/
MERGE [Dimension].[Region] AS target
USING (VALUES
    (N'NA',     N'North America', N'WWI North America Inc.',  N'Chicago',   N'USD', N'SALESTAX', N'NA_454',    12, N'ENDYEAR',   1, N'SPOT',     N'CORPORATE', N'OPTOUT', N'CCPA',  7, N'CASS',     '23:00', N'America/Chicago'),
    (N'EU',     N'Europe',        N'WWI Europe B.V.',         N'Rotterdam', N'EUR', N'VAT',      N'EU_CAL',    12, N'ENDYEAR',   2, N'MONTHAVG', N'ECB',       N'OPTIN',  N'GDPR',  6, N'NATIONAL', '22:00', N'Europe/Amsterdam'),
    (N'APAC',   N'Asia Pacific',  N'WWI Asia Pacific Pte Ltd', N'Singapore', N'SGD', N'GST',      N'APAC_MIXED', 3, N'STARTYEAR', 2, N'CLOSING',  N'LOCALBANK', N'MIXED',  N'PDPA',  5, N'MIXED',    '21:00', N'Asia/Singapore'),
    (N'GLOBAL', N'Global',        N'WWI Holdings',            N'Chicago',   N'USD', N'SALESTAX', N'NA_454',    12, N'ENDYEAR',   1, N'SPOT',     N'CORPORATE', N'OPTOUT', N'NONE', 10, N'MIXED',    '23:30', N'UTC')
) AS source ([Region Code], [Region Name], [Operating Company Name], [Head Office City],
             [Reporting Currency Code], [Tax Regime Code], [Fiscal Calendar Code],
             [Fiscal Year End Month], [Fiscal Year Naming Rule], [Week Start Day],
             [FX Translation Method Code], [FX Rate Source Code], [Consent Model Code],
             [Data Protection Regime Code], [Default Retention Years], [Address Standard Code],
             [ETL Cutoff Local Time], [ETL Time Zone Name])
    ON target.[Region Code] = source.[Region Code]
WHEN MATCHED THEN
    UPDATE SET
        target.[Region Name]                  = source.[Region Name],
        target.[Operating Company Name]       = source.[Operating Company Name],
        target.[Head Office City]             = source.[Head Office City],
        target.[Reporting Currency Code]      = source.[Reporting Currency Code],
        target.[Tax Regime Code]              = source.[Tax Regime Code],
        target.[Fiscal Calendar Code]         = source.[Fiscal Calendar Code],
        target.[Fiscal Year End Month]        = source.[Fiscal Year End Month],
        target.[Fiscal Year Naming Rule]      = source.[Fiscal Year Naming Rule],
        target.[Week Start Day]               = source.[Week Start Day],
        target.[FX Translation Method Code]   = source.[FX Translation Method Code],
        target.[FX Rate Source Code]          = source.[FX Rate Source Code],
        target.[Consent Model Code]           = source.[Consent Model Code],
        target.[Data Protection Regime Code]  = source.[Data Protection Regime Code],
        target.[Default Retention Years]      = source.[Default Retention Years],
        target.[Address Standard Code]        = source.[Address Standard Code],
        target.[ETL Cutoff Local Time]        = source.[ETL Cutoff Local Time],
        target.[ETL Time Zone Name]           = source.[ETL Time Zone Name],
        target.[Is Active]                    = 1
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Region Code], [Region Name], [Operating Company Name], [Head Office City],
            [Reporting Currency Code], [Tax Regime Code], [Fiscal Calendar Code],
            [Fiscal Year End Month], [Fiscal Year Naming Rule], [Week Start Day],
            [FX Translation Method Code], [FX Rate Source Code], [Consent Model Code],
            [Data Protection Regime Code], [Default Retention Years], [Address Standard Code],
            [ETL Cutoff Local Time], [ETL Time Zone Name], [Is Active], [Source System Code],
            [Valid From], [Valid To], [Lineage Key])
    VALUES (source.[Region Code], source.[Region Name], source.[Operating Company Name], source.[Head Office City],
            source.[Reporting Currency Code], source.[Tax Regime Code], source.[Fiscal Calendar Code],
            source.[Fiscal Year End Month], source.[Fiscal Year Naming Rule], source.[Week Start Day],
            source.[FX Translation Method Code], source.[FX Rate Source Code], source.[Consent Model Code],
            source.[Data Protection Regime Code], source.[Default Retention Years], source.[Address Standard Code],
            source.[ETL Cutoff Local Time], source.[ETL Time Zone Name], 1, N'ESTATE',
            SYSDATETIME(), CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999'), 0);
GO
