/*
    Object        : [Dimension].[Currency]  (SCD Type 1, ISO 4217 reference dimension)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : Sequences.CurrencyKey
    Called by     : Integration.usp_MigrateStagedCurrencyData

    Sourced from Oracle WWI_REF.CURRENCY_CODE. Rates are NOT held here - they are a
    fact ([Fact].[FX Rate], loaded by another package) because they are a daily
    time series. What is held here is how each currency is *treated*: rounding,
    minor units, whether it is a reporting currency, and which rate type the
    regional translation rules select.

    Historical currencies (DEM, FRF, ITL and the rest of the euro legacy set) are
    retained with [Superseded By Currency Code] = 'EUR' and their fixed conversion
    rate, because the 1999-2001 history was never restated.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Currency', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Currency]
    (
        [Currency Key]                  INT             CONSTRAINT [DF_Dimension_Currency_Key] DEFAULT (NEXT VALUE FOR [Sequences].[CurrencyKey]) NOT NULL,
        [Currency Code]                 NVARCHAR(3)     NOT NULL,   -- ISO 4217 alpha
        [Currency Numeric Code]         NVARCHAR(3)     NULL,
        [Currency Name]                 NVARCHAR(60)    NOT NULL,
        [Currency Symbol]               NVARCHAR(5)     NULL,
        [Minor Unit Digits]             SMALLINT        NULL,       -- 0 for JPY, 2 for most, 3 for KWD
        [Rounding Rule Code]            NVARCHAR(10)    NULL,       -- HALFUP / HALFEVEN / BANKERS / NEAREST5
        [Rounding Increment]            DECIMAL(18, 6)  NULL,       -- 0.05 for CHF cash rounding
        [Is Reporting Currency]         BIT             NULL,
        [Is Transactional Currency]     BIT             NULL,
        [Is Historical]                 BIT             NULL,
        [Superseded By Currency Code]   NVARCHAR(3)     NULL,
        [Superseded On]                 DATE            NULL,
        [Fixed Conversion Rate]         DECIMAL(18, 8)  NULL,       -- euro legacy set only
        [Default Rate Type Code]        NVARCHAR(10)    NULL,       -- SPOT / MONTHAVG / CLOSING / BUDGET
        [Rate Source Code]              NVARCHAR(20)    NULL,       -- CORPORATE / ECB / RBA / LOCALBANK
        [Inverse Quotation]             BIT             NULL,       -- quoted as units per USD rather than USD per unit
        [Decimal Separator]             NVARCHAR(1)     NULL,
        [Thousands Separator]           NVARCHAR(1)     NULL,
        [Symbol Position Code]          NVARCHAR(10)    NULL,       -- PREFIX / SUFFIX
        [Is Restricted Currency]        BIT             NULL,       -- capital controls; APAC settlement uses USD instead
        [Settlement Substitute Code]    NVARCHAR(3)     NULL,
        [Is Active]                     BIT             NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Currency] PRIMARY KEY CLUSTERED ([Currency Key] ASC),
        CONSTRAINT [UQ_Dimension_Currency_Code] UNIQUE ([Currency Code])
    );
END;
GO
