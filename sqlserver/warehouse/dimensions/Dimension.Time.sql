/*
    Object        : [Dimension].[Time]  (static dimension, one row per minute of the day)
    Deploy target : WideWorldImportersDW
    Deploy order  : after 01_dimension_key_registry.sql
    Depends on    : none
    Called by     : Integration.usp_PopulateTimeDimension

    1,440 rows, one per minute, keyed hhmm as an integer so the fact loads can
    derive the key arithmetically instead of joining. Reserved members -1
    (unknown) and -2 (not applicable) are inserted by 90_unknown_members.sql;
    the not-applicable member matters because several facts are date-grain only
    and still carry a [Time Key] column for uniformity.

    The shift and daypart bands differ by region: NA warehouses run two shifts
    plus a twilight shift, EU sites run three eight-hour shifts under the working
    time directive, and APAC sites run a six-day pattern with a split shift. Each
    is a separate column rather than a parameterised lookup, in the style of the
    rest of the estate.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Time', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Time]
    (
        [Time Key]                      INT             NOT NULL,   -- hhmm, e.g. 1430; negative for reserved members
        [Time Of Day]                   TIME(0)         NULL,
        [Hour 24]                       SMALLINT        NULL,
        [Hour 12]                       SMALLINT        NULL,
        [Minute]                        SMALLINT        NULL,
        [AM PM]                         NVARCHAR(2)     NULL,
        [Time Label 24]                 NVARCHAR(5)     NULL,       -- 14:30
        [Time Label 12]                 NVARCHAR(8)     NULL,       -- 02:30 PM
        [Hour Label]                    NVARCHAR(8)     NULL,
        [Half Hour Label]               NVARCHAR(8)     NULL,
        [Quarter Hour Label]            NVARCHAR(8)     NULL,
        [Minutes Since Midnight]        SMALLINT        NULL,
        [Is Reserved Member]            BIT             NULL,
        [Reserved Member Description]   NVARCHAR(40)    NULL,

        [Daypart Code]                  NVARCHAR(15)    NULL,   -- OVERNIGHT / MORNING / MIDDAY / AFTERNOON / EVENING
        [NA Shift Code]                 NVARCHAR(10)    NULL,   -- DAY / SWING / TWILIGHT
        [EU Shift Code]                 NVARCHAR(10)    NULL,   -- EARLY / LATE / NIGHT
        [APAC Shift Code]               NVARCHAR(10)    NULL,   -- AM / SPLIT / PM
        [NA Is Core Business Hour]      BIT             NULL,   -- 08:00-17:00 local
        [EU Is Core Business Hour]      BIT             NULL,   -- 09:00-17:30 local
        [APAC Is Core Business Hour]    BIT             NULL,   -- 09:00-18:00 local
        [Is Order Cutoff Minute]        BIT             NULL,   -- same-day dispatch cutoff
        [Is Batch Window]               BIT             NULL,   -- nightly ETL window; used by the operational reports
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Time] PRIMARY KEY CLUSTERED ([Time Key] ASC)
    );
END;
GO
