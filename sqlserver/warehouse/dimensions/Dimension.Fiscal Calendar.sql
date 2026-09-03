/*
    Object        : [Dimension].[Fiscal Calendar]  (outrigger on Date - per-country calendar and holiday detail)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Date.sql
    Depends on    : Dimension.Date, Dimension.Country
    Called by     : Integration.usp_PopulateDateDimension

    [Dimension].[Date] carries three regional calendars because three is all the
    cube can browse. The reality is per country: eleven trading countries with
    eleven public-holiday sets, four fiscal-year boundaries and two week-start
    conventions. Those live here, at (country, date) grain, and the date dimension
    columns are the regional roll-up derived from them.

    Nothing joins a fact to this table. The SLA and delivery-performance reports
    join Date -> Fiscal Calendar on the destination country to count working days,
    which is why the working-day counters are precomputed rather than derived.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Fiscal Calendar', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Fiscal Calendar]
    (
        [Fiscal Calendar Key]           BIGINT          IDENTITY(1, 1) NOT NULL,
        [Country Code]                  NVARCHAR(3)     NOT NULL,
        [Date]                          DATE            NOT NULL,
        [DateKey]                       INT             NOT NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Fiscal Calendar Code]          NVARCHAR(20)    NULL,   -- NA_454 / EU_CAL / APAC_JP / APAC_AU / APAC_IN
        [Fiscal Year]                   INT             NULL,
        [Fiscal Year Label]             NVARCHAR(10)    NULL,
        [Fiscal Quarter]                SMALLINT        NULL,
        [Fiscal Period]                 SMALLINT        NULL,
        [Fiscal Period Label]           NVARCHAR(12)    NULL,
        [Fiscal Week]                   SMALLINT        NULL,
        [Period Start Date]             DATE            NULL,
        [Period End Date]               DATE            NULL,
        [Is Period End]                 BIT             NULL,
        [Is Year End]                   BIT             NULL,

        [Is Public Holiday]             BIT             NULL,
        [Holiday Name]                  NVARCHAR(80)    NULL,
        [Holiday Scope Code]            NVARCHAR(15)    NULL,   -- NATIONAL / REGIONAL / BANK / OBSERVED
        [Holiday Subdivision Code]      NVARCHAR(10)    NULL,   -- regional holidays: which state or prefecture
        [Is Working Day]                BIT             NULL,
        [Is Warehouse Operating Day]    BIT             NULL,
        [Is Carrier Collection Day]     BIT             NULL,
        [Working Day Of Month]          SMALLINT        NULL,
        [Working Day Of Year]           SMALLINT        NULL,
        [Working Days Remaining Month]  SMALLINT        NULL,
        [Next Working Date]             DATE            NULL,
        [Previous Working Date]         DATE            NULL,

        [Tax Return Period Label]       NVARCHAR(12)    NULL,
        [Tax Return Due Date]           DATE            NULL,
        [Statutory Close Due Date]      DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Fiscal_Calendar] PRIMARY KEY CLUSTERED ([Fiscal Calendar Key] ASC),
        CONSTRAINT [UQ_Dimension_Fiscal_Calendar_Country_Date] UNIQUE ([Country Code], [Date])
    );

    CREATE NONCLUSTERED INDEX [IX_Dimension_Fiscal_Calendar_Date]
        ON [Dimension].[Fiscal Calendar] ([DateKey] ASC, [Country Code] ASC)
        INCLUDE ([Is Working Day], [Working Day Of Month], [Fiscal Period Label]);
END;
GO
