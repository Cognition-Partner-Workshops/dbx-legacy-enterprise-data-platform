/*
    Object        : [Dimension].[Date]  (static role-playing dimension, three fiscal calendars)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql, before 91_date_role_playing_views.sql
    Depends on    : the WideWorldImportersDW baseline Dimension.Date (calendar columns)
    Called by     : Integration.usp_PopulateDateDimension (populates the extension columns),
                    every role-playing view in 91_date_role_playing_views.sql

    One physical date dimension, seven roles. The Microsoft baseline supplies the
    calendar columns; this script bolts on the three regional fiscal calendars and
    the operational flags, because the regions do not share a fiscal year:

      NA_454    : 4-4-5 retail calendar, 52 or 53 weeks, year ends on the Saturday
                  nearest 31 December, period 1 starts the following Sunday. Named
                  after the calendar year it ends in.
      EU_CAL    : calendar months, January to December, ISO-8601 weeks starting
                  Monday, year named after the calendar year.
      APAC_MIXED: April to March fiscal year named after the year it *starts* in
                  (FY2019 = April 2019 to March 2020), with Japan and India on the
                  same boundary and Australia on July to June - the Australian
                  offset is carried in the [APAC AU ...] columns rather than a
                  fourth calendar, which is a 2012 compromise nobody likes.

    The dimension keys on [Date] itself, the way the Microsoft baseline and every
    fact in sqlserver/warehouse/facts do. The reserved members are therefore
    sentinel dates rather than negative keys: 1900-01-01 unknown, 1900-01-02 not
    applicable, 1900-01-03 invalid, inserted by 90_unknown_members.sql and flagged
    with [Is Reserved Member] so every fact lookup has a target.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Date', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Date]
    (
        [Date]                          DATE            NOT NULL,
        [Day Number]                    INT             NOT NULL,
        [Day]                           NVARCHAR(10)    NOT NULL,
        [Day of Week]                   NVARCHAR(20)    NOT NULL,
        [Day of Week Number]            INT             NOT NULL,
        [Month]                         NVARCHAR(10)    NOT NULL,
        [Short Month]                   NVARCHAR(3)     NOT NULL,
        [Calendar Month Number]         INT             NOT NULL,
        [Calendar Quarter Number]       INT             NOT NULL,
        [Calendar Year]                 INT             NOT NULL,
        CONSTRAINT [PK_Dimension_Date] PRIMARY KEY CLUSTERED ([Date] ASC)
    );
END;
GO

/* Surrogate-key alignment with the reserved ranges in 00_dimension_schemas_and_sequences.sql. */
IF COL_LENGTH(N'Dimension.Date', N'Is Reserved Member') IS NULL
    ALTER TABLE [Dimension].[Date] ADD
        [Is Reserved Member]            BIT             NULL,
        [Reserved Member Description]   NVARCHAR(40)    NULL,
        [Last Load Batch Id]            BIGINT          NULL;
GO

/* NA 4-4-5 retail calendar. */
IF COL_LENGTH(N'Dimension.Date', N'NA Fiscal Year') IS NULL
    ALTER TABLE [Dimension].[Date] ADD
        [NA Fiscal Year]                INT             NULL,
        [NA Fiscal Year Label]          NVARCHAR(10)    NULL,   -- FY2019
        [NA Fiscal Quarter]             SMALLINT        NULL,
        [NA Fiscal Period]              SMALLINT        NULL,   -- 1 to 12, 4-4-5 pattern
        [NA Fiscal Period Label]        NVARCHAR(12)    NULL,   -- FY2019-P07
        [NA Fiscal Week]                SMALLINT        NULL,   -- 1 to 52 or 53
        [NA Fiscal Week Label]          NVARCHAR(12)    NULL,
        [NA Fiscal Day Of Year]         SMALLINT        NULL,
        [NA Is 53 Week Year]            BIT             NULL,
        [NA Period Start Date]          DATE            NULL,
        [NA Period End Date]            DATE            NULL,
        [NA Is Period End]              BIT             NULL,
        [NA Is Quarter End]             BIT             NULL,
        [NA Is Year End]                BIT             NULL,
        [NA Same Period Last Year Date] DATE            NULL;   -- 364 days back, not one calendar year
GO

/* EU calendar-month fiscal year with ISO weeks. */
IF COL_LENGTH(N'Dimension.Date', N'EU Fiscal Year') IS NULL
    ALTER TABLE [Dimension].[Date] ADD
        [EU Fiscal Year]                INT             NULL,
        [EU Fiscal Year Label]          NVARCHAR(10)    NULL,
        [EU Fiscal Quarter]             SMALLINT        NULL,
        [EU Fiscal Period]              SMALLINT        NULL,
        [EU Fiscal Period Label]        NVARCHAR(12)    NULL,
        [EU ISO Week Number]            SMALLINT        NULL,
        [EU ISO Week Year]              INT             NULL,
        [EU ISO Week Label]             NVARCHAR(12)    NULL,   -- 2019-W27
        [EU Is Period End]              BIT             NULL,
        [EU Is Quarter End]             BIT             NULL,
        [EU Is Year End]                BIT             NULL,
        [EU VAT Return Period Label]    NVARCHAR(12)    NULL,   -- monthly or quarterly depending on the country
        [EU Same Period Last Year Date] DATE            NULL;
GO

/* APAC April-March fiscal year, plus the Australian July-June offset. */
IF COL_LENGTH(N'Dimension.Date', N'APAC Fiscal Year') IS NULL
    ALTER TABLE [Dimension].[Date] ADD
        [APAC Fiscal Year]              INT             NULL,   -- FY2019 = 2019-04-01 to 2020-03-31
        [APAC Fiscal Year Label]        NVARCHAR(10)    NULL,
        [APAC Fiscal Quarter]           SMALLINT        NULL,
        [APAC Fiscal Period]            SMALLINT        NULL,
        [APAC Fiscal Period Label]      NVARCHAR(12)    NULL,
        [APAC Is Period End]            BIT             NULL,
        [APAC Is Year End]              BIT             NULL,
        [APAC AU Fiscal Year]           INT             NULL,   -- July to June
        [APAC AU Fiscal Period]         SMALLINT        NULL,
        [APAC AU Fiscal Year Label]     NVARCHAR(10)    NULL,
        [APAC GST Return Period Label]  NVARCHAR(12)    NULL,
        [APAC Same Period Last Year Date] DATE          NULL;
GO

/*
    Operational flags. Trading days differ by region and the holiday sets are
    maintained per country in the outrigger [Dimension].[Fiscal Calendar]; the
    three flags here are the regional roll-up the ETL and the SLA reports use.
*/
IF COL_LENGTH(N'Dimension.Date', N'NA Is Trading Day') IS NULL
    ALTER TABLE [Dimension].[Date] ADD
        [NA Is Trading Day]             BIT             NULL,
        [EU Is Trading Day]             BIT             NULL,
        [APAC Is Trading Day]           BIT             NULL,
        [Is Weekend NA]                 BIT             NULL,   -- Saturday and Sunday
        [Is Weekend EU]                 BIT             NULL,   -- Saturday and Sunday
        [Is Weekend APAC]               BIT             NULL,   -- includes Friday-Saturday for some markets
        [NA Holiday Name]               NVARCHAR(60)    NULL,
        [EU Holiday Name]               NVARCHAR(60)    NULL,
        [APAC Holiday Name]             NVARCHAR(60)    NULL,
        [Is Month End Close Day]        BIT             NULL,
        [Is Quarter End Close Day]      BIT             NULL,
        [ETL Business Day Number]       SMALLINT        NULL;   -- working day within the month, drives the finance close
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = N'IX_Dimension_Date_Calendar_Month'
                 AND object_id = OBJECT_ID(N'Dimension.Date'))
    CREATE NONCLUSTERED INDEX [IX_Dimension_Date_Calendar_Month]
        ON [Dimension].[Date] ([Calendar Year] ASC, [Calendar Month Number] ASC)
        INCLUDE ([Date]);
GO
