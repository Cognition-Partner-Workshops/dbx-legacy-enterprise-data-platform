/*
    Object        : [Dimension].[Geography]  (SCD Type 1 outrigger, shared by City, Supplier and Warehouse Site)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Country.sql and Dimension.Region.sql
    Depends on    : Sequences.GeographyKey, Dimension.Country, Dimension.Region
    Called by     : Integration.usp_MigrateStagedGeographyData

    Sourced from Oracle WWI_REF.COUNTRY_REF, which the reference-data team maintain
    by hand from a spreadsheet the UN publishes. It is an outrigger: nothing joins
    a fact to it directly, but [Dimension].[City], [Dimension].[Supplier] and
    [Dimension].[Warehouse Site] all carry [Geography Key] and inherit the country,
    subregion, currency and tax-regime attributes from here so those attributes are
    maintained once.

    The grain is (country, subdivision), not country, because Canada, the US and
    Australia need state-level tax and India needs state-level GST. Countries with
    no subdivision of interest carry a single row with [Subdivision Code] = 'ALL'.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Geography', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Geography]
    (
        [Geography Key]                 INT             CONSTRAINT [DF_Dimension_Geography_Key] DEFAULT (NEXT VALUE FOR [Sequences].[GeographyKey]) NOT NULL,
        [Country Key]                   INT             NULL,
        [Region Key]                    INT             NULL,
        [Country Code]                  NVARCHAR(3)     NOT NULL,
        [Country Name]                  NVARCHAR(80)    NOT NULL,
        [Subdivision Code]              NVARCHAR(10)    NOT NULL,
        [Subdivision Name]              NVARCHAR(80)    NULL,
        [Subdivision Type]              NVARCHAR(30)    NULL,   -- State / Province / Prefecture / Territory / ALL
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Subregion Name]                NVARCHAR(60)    NULL,
        [Continent Name]                NVARCHAR(30)    NULL,

        [Local Currency Code]           NVARCHAR(3)     NULL,
        [Reporting Currency Code]       NVARCHAR(3)     NULL,
        [FX Translation Method Code]    NVARCHAR(10)    NULL,   -- SPOT / MONTHAVG / BUDGET / CLOSING
        [Tax Regime Code]               NVARCHAR(10)    NULL,   -- SALESTAX / VAT / GST / CONSUMPTION / NONE
        [Standard Tax Rate]             DECIMAL(9, 4)   NULL,
        [Reduced Tax Rate]              DECIMAL(9, 4)   NULL,
        [Tax Authority Name]            NVARCHAR(100)   NULL,
        [Tax Registration Format]       NVARCHAR(40)    NULL,

        [Fiscal Year End Month]         SMALLINT        NULL,   -- 12 in NA, 12 in most of EU, 3 in JP/IN/AU variants
        [Fiscal Calendar Code]          NVARCHAR(20)    NULL,   -- NA_454 / EU_CAL / APAC_JP / APAC_AU / APAC_IN
        [Week Start Day]                SMALLINT        NULL,   -- 1 = Sunday (NA), 2 = Monday (EU/APAC)
        [Date Format Pattern]           NVARCHAR(20)    NULL,
        [Decimal Separator]             NVARCHAR(1)     NULL,
        [Address Format Code]           NVARCHAR(10)    NULL,

        [Data Protection Regime Code]   NVARCHAR(20)    NULL,   -- GDPR / CCPA / PIPEDA / PDPA / APPI / NONE
        [Consent Model Code]            NVARCHAR(10)    NULL,   -- OPTIN / OPTOUT / MIXED
        [Retention Years]               SMALLINT        NULL,
        [Cross Border Transfer Allowed] BIT             NULL,

        [Is Trading Country]            BIT             NULL,
        [Is Sanctioned]                 BIT             NULL,
        [Trade Block Code]              NVARCHAR(10)    NULL,   -- EU / USMCA / ASEAN / CPTPP / NONE
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Geography] PRIMARY KEY CLUSTERED ([Geography Key] ASC),
        CONSTRAINT [UQ_Dimension_Geography_Country_Subdivision] UNIQUE ([Country Code], [Subdivision Code]),
        CONSTRAINT [CK_Dimension_Geography_Tax_Regime]
            CHECK ([Tax Regime Code] IS NULL
                   OR [Tax Regime Code] IN (N'SALESTAX', N'VAT', N'GST', N'CONSUMPTION', N'NONE'))
    );
END;
GO
