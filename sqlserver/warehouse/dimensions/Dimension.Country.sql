/*
    Object        : [Dimension].[Country]  (SCD Type 1, ISO reference dimension)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql, before Dimension.Geography.sql
    Depends on    : Sequences.CountryKey, Dimension.Region
    Called by     : Integration.usp_MigrateStagedGeographyData

    The country level of the geography hierarchy, kept as its own dimension because
    three cubes browse country without the subdivision grain that
    [Dimension].[Geography] carries. Codes are ISO 3166 alpha-2 and alpha-3 plus
    the numeric code, and - because the 1998 order-entry system is still alive in
    APAC - the two-character in-house code it emits, which is not always ISO.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Country', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Country]
    (
        [Country Key]                   INT             CONSTRAINT [DF_Dimension_Country_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CountryKey]) NOT NULL,
        [WWI Country ID]                INT             NULL,
        [Country Code]                  NVARCHAR(3)     NOT NULL,   -- ISO 3166-1 alpha-3
        [Country Alpha2 Code]           NVARCHAR(2)     NULL,
        [Country Numeric Code]          NVARCHAR(3)     NULL,
        [Legacy Order Entry Code]       NVARCHAR(2)     NULL,       -- 1998 in-house code, not always ISO
        [Country Name]                  NVARCHAR(80)    NOT NULL,
        [Formal Country Name]           NVARCHAR(160)   NULL,
        [Region Key]                    INT             NULL,
        [Region Code]                   NVARCHAR(10)    NOT NULL,
        [Subregion Name]                NVARCHAR(60)    NULL,
        [Continent Name]                NVARCHAR(30)    NULL,

        [Local Currency Code]           NVARCHAR(3)     NULL,
        [Calling Code]                  NVARCHAR(6)     NULL,
        [Primary Language Code]         NVARCHAR(5)     NULL,
        [Measurement System Code]       NVARCHAR(10)    NULL,   -- METRIC / IMPERIAL / MIXED
        [Drives On Code]                NVARCHAR(5)     NULL,   -- LEFT / RIGHT, used by the fleet reports

        [Tax Regime Code]               NVARCHAR(10)    NULL,
        [Customs Union Code]            NVARCHAR(10)    NULL,
        [Is EU Member State]            BIT             NULL,
        [EU Accession Date]             DATE            NULL,
        [EU Exit Date]                  DATE            NULL,   -- populated for GB; the load keeps both rows valid
        [Is Sanctioned]                 BIT             NULL,
        [Sanction Programme Code]       NVARCHAR(20)    NULL,
        [Is Trading Country]            BIT             NULL,

        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Country] PRIMARY KEY CLUSTERED ([Country Key] ASC),
        CONSTRAINT [UQ_Dimension_Country_Code] UNIQUE ([Country Code])
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Country_Legacy_Code]
        ON [Dimension].[Country] ([Legacy Order Entry Code] ASC);
END;
GO
