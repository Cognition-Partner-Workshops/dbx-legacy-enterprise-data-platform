/*
    Object        : [Integration].[usp_PopulateDateDimension]
    Deploy target : WideWorldImportersDW
    Depends on    : Dimension.Date, Dimension.Fiscal Calendar, ref.PublicHoliday,
                    the etl control framework
    Called by     : DIM_Populate_Date, run once a year in the December release and
                    on demand when a new market is onboarded

    Populates the single physical date dimension that every role-playing view in
    91_date_role_playing_views.sql sits on, plus the country-grain
    [Dimension].[Fiscal Calendar] outrigger.

    Three fiscal calendars live side by side on the one row, which is the estate's
    biggest dimensional compromise and the reason the three regional finance packs
    disagree about what "period 7" means:

      NA    4-4-5 retail calendar, 52 or 53 weeks, year ends the Saturday nearest
            31 December. Prior-year comparison is 364 days back, not one calendar
            year, so like-for-like weeks line up.
      EU    calendar months, ISO-8601 weeks with a Monday start, VAT return period
            monthly or quarterly depending on the country.
      APAC  April to March by default; Australia is July to June and gets its own
            pair of columns rather than a fourth calendar, which was the cheap fix
            in 2009 and is still here.

    The row-by-row loop over dates is deliberate estate history: it was written
    before the set-based tally-table rewrite everyone talks about and never
    replaced, because it runs once a year at three in the morning.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_PopulateDateDimension]
    @BatchId            BIGINT,
    @FromDate           DATE,
    @ToDate             DATE,
    @PackageName        NVARCHAR(200) = N'DIM_Populate_Date',
    @RebuildFiscal      BIT           = 1,
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @FiscalCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);
    DECLARE @vDate              DATE;

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DatePopulation',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF @FromDate IS NULL OR @ToDate IS NULL OR @ToDate < @FromDate
        BEGIN
            RAISERROR(N'A valid from/to date range is required to populate the date dimension.', 16, 1);
            RETURN;
        END;

        SET @vDate = @FromDate;

        WHILE @vDate <= @ToDate
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM [Dimension].[Date] WHERE [Date] = @vDate)
            BEGIN
                INSERT INTO [Dimension].[Date]
                    ([Date], [DateKey], [Day Number], [Day], [Day of Week], [Day of Week Number],
                     [Month], [Short Month], [Calendar Month Number], [Calendar Quarter Number],
                     [Calendar Year], [Is Reserved Member], [Last Load Batch Id])
                VALUES
                    (@vDate,
                     (YEAR(@vDate) * 10000) + (MONTH(@vDate) * 100) + DAY(@vDate),
                     DAY(@vDate),
                     CONVERT(NVARCHAR(10), DAY(@vDate)),
                     DATENAME(WEEKDAY, @vDate),
                     /* Monday = 1 regardless of the server DATEFIRST setting; the
                        EU ISO week columns depend on it and the server setting has
                        been changed under this warehouse twice. */
                     ((DATEDIFF(DAY, N'1900-01-01', @vDate) + 0) % 7) + 1,
                     DATENAME(MONTH, @vDate),
                     LEFT(DATENAME(MONTH, @vDate), 3),
                     MONTH(@vDate),
                     DATEPART(QUARTER, @vDate),
                     YEAR(@vDate),
                     0,
                     @BatchId);

                SET @InsertedCount = @InsertedCount + 1;
            END;

            SET @vDate = DATEADD(DAY, 1, @vDate);
        END;

        /* -------------------------------------------------- NA 4-4-5 calendar */
        /* Fiscal year ends the Saturday nearest 31 December; week 1 starts the
           Sunday after. The 53-week years are the ones the like-for-like report
           has to special-case. */
        UPDATE d
        SET d.[NA Fiscal Year]        = x.[Fiscal Year],
            d.[NA Fiscal Year Label]  = CONCAT(N'FY', x.[Fiscal Year]),
            d.[NA Fiscal Week]        = x.[Fiscal Week],
            d.[NA Fiscal Week Label]  = CONCAT(N'FY', x.[Fiscal Year], N'-W',
                                               RIGHT(CONCAT(N'0', x.[Fiscal Week]), 2)),
            d.[NA Fiscal Period]      = x.[Fiscal Period],
            d.[NA Fiscal Period Label]= CONCAT(N'FY', x.[Fiscal Year], N'-P',
                                               RIGHT(CONCAT(N'0', x.[Fiscal Period]), 2)),
            d.[NA Fiscal Quarter]     = ((x.[Fiscal Period] - 1) / 3) + 1,
            d.[NA Fiscal Day Of Year] = x.[Fiscal Day],
            d.[NA Is 53 Week Year]    = x.[Is 53 Week Year],
            d.[NA Same Period Last Year Date] = DATEADD(DAY, -364, d.[Date]),
            d.[Last Load Batch Id]    = @BatchId
        FROM [Dimension].[Date] AS d
        CROSS APPLY (
            SELECT  [Year Start] = DATEADD(DAY,
                        -((DATEDIFF(DAY, N'2000-01-02', DATEFROMPARTS(YEAR(d.[Date]), 1, 1)) % 7 + 7) % 7),
                        DATEFROMPARTS(CASE WHEN MONTH(d.[Date]) = 12 AND DAY(d.[Date]) > 28
                                           THEN YEAR(d.[Date]) + 1 ELSE YEAR(d.[Date]) END, 1, 1))
        ) AS ys
        CROSS APPLY (
            SELECT  [Fiscal Year]  = YEAR(ys.[Year Start]),
                    [Fiscal Day]   = CONVERT(SMALLINT, DATEDIFF(DAY, ys.[Year Start], d.[Date]) + 1),
                    [Fiscal Week]  = CONVERT(SMALLINT, (DATEDIFF(DAY, ys.[Year Start], d.[Date]) / 7) + 1),
                    [Fiscal Period]= CONVERT(SMALLINT,
                        CASE
                            WHEN (DATEDIFF(DAY, ys.[Year Start], d.[Date]) / 7) + 1 > 52 THEN 12
                            ELSE (((DATEDIFF(DAY, ys.[Year Start], d.[Date]) / 7) / 13) * 3)
                                 + CASE WHEN ((DATEDIFF(DAY, ys.[Year Start], d.[Date]) / 7) % 13) < 4 THEN 1
                                        WHEN ((DATEDIFF(DAY, ys.[Year Start], d.[Date]) / 7) % 13) < 8 THEN 2
                                        ELSE 3 END
                        END),
                    [Is 53 Week Year] = CONVERT(BIT,
                        CASE WHEN DATEDIFF(DAY, ys.[Year Start],
                                  DATEADD(YEAR, 1, ys.[Year Start])) > 364 THEN 1 ELSE 0 END)
        ) AS x
        WHERE d.[Date] BETWEEN @FromDate AND @ToDate;

        SET @UpdatedCount = @@ROWCOUNT;

        /* -------------------------------------------------- EU ISO calendar */
        UPDATE d
        SET d.[EU Fiscal Year]        = YEAR(d.[Date]),
            d.[EU Fiscal Year Label]  = CONCAT(N'FY', YEAR(d.[Date])),
            d.[EU Fiscal Period]      = MONTH(d.[Date]),
            d.[EU Fiscal Period Label]= CONCAT(N'FY', YEAR(d.[Date]), N'-P',
                                               RIGHT(CONCAT(N'0', MONTH(d.[Date])), 2)),
            d.[EU Fiscal Quarter]     = DATEPART(QUARTER, d.[Date]),
            d.[EU ISO Week Number]    = DATEPART(ISO_WEEK, d.[Date]),
            d.[EU ISO Week Year]      = CASE
                                            WHEN MONTH(d.[Date]) = 1  AND DATEPART(ISO_WEEK, d.[Date]) > 50
                                                THEN YEAR(d.[Date]) - 1
                                            WHEN MONTH(d.[Date]) = 12 AND DATEPART(ISO_WEEK, d.[Date]) = 1
                                                THEN YEAR(d.[Date]) + 1
                                            ELSE YEAR(d.[Date])
                                        END,
            d.[EU ISO Week Label]     = CONCAT(YEAR(d.[Date]), N'-W',
                                               RIGHT(CONCAT(N'0', DATEPART(ISO_WEEK, d.[Date])), 2)),
            d.[EU Is Period End]      = CASE WHEN d.[Date] = EOMONTH(d.[Date]) THEN 1 ELSE 0 END,
            d.[EU Is Quarter End]     = CASE WHEN d.[Date] = EOMONTH(d.[Date])
                                                  AND MONTH(d.[Date]) IN (3, 6, 9, 12) THEN 1 ELSE 0 END,
            d.[EU Is Year End]        = CASE WHEN MONTH(d.[Date]) = 12 AND DAY(d.[Date]) = 31 THEN 1 ELSE 0 END,
            /* Quarterly filers get a quarter label, monthly filers a month label.
               Which a country is depends on turnover, so the label here is the
               quarterly form and the country-grain outrigger overrides it. */
            d.[EU VAT Return Period Label] = CONCAT(YEAR(d.[Date]), N'-Q', DATEPART(QUARTER, d.[Date])),
            d.[EU Same Period Last Year Date] = DATEADD(YEAR, -1, d.[Date]),
            d.[Last Load Batch Id]    = @BatchId
        FROM [Dimension].[Date] AS d
        WHERE d.[Date] BETWEEN @FromDate AND @ToDate;

        /* -------------------------------------------------- APAC calendars */
        UPDATE d
        SET d.[APAC Fiscal Year]       = CASE WHEN MONTH(d.[Date]) >= 4
                                              THEN YEAR(d.[Date]) ELSE YEAR(d.[Date]) - 1 END,
            d.[APAC Fiscal Year Label] = CONCAT(N'FY', CASE WHEN MONTH(d.[Date]) >= 4
                                                            THEN YEAR(d.[Date]) ELSE YEAR(d.[Date]) - 1 END),
            d.[APAC Fiscal Period]     = CASE WHEN MONTH(d.[Date]) >= 4
                                              THEN MONTH(d.[Date]) - 3 ELSE MONTH(d.[Date]) + 9 END,
            d.[APAC Fiscal Period Label] = CONCAT(N'P',
                                              RIGHT(CONCAT(N'0', CASE WHEN MONTH(d.[Date]) >= 4
                                                                      THEN MONTH(d.[Date]) - 3
                                                                      ELSE MONTH(d.[Date]) + 9 END), 2)),
            d.[APAC Fiscal Quarter]    = ((CASE WHEN MONTH(d.[Date]) >= 4
                                                THEN MONTH(d.[Date]) - 4 ELSE MONTH(d.[Date]) + 8 END) / 3) + 1,
            d.[APAC Is Period End]     = CASE WHEN d.[Date] = EOMONTH(d.[Date]) THEN 1 ELSE 0 END,
            d.[APAC Is Year End]       = CASE WHEN MONTH(d.[Date]) = 3 AND DAY(d.[Date]) = 31 THEN 1 ELSE 0 END,
            d.[APAC AU Fiscal Year]    = CASE WHEN MONTH(d.[Date]) >= 7
                                              THEN YEAR(d.[Date]) + 1 ELSE YEAR(d.[Date]) END,
            d.[APAC AU Fiscal Year Label] = CONCAT(N'FY', CASE WHEN MONTH(d.[Date]) >= 7
                                                               THEN YEAR(d.[Date]) + 1 ELSE YEAR(d.[Date]) END),
            d.[APAC AU Fiscal Period]  = CASE WHEN MONTH(d.[Date]) >= 7
                                              THEN MONTH(d.[Date]) - 6 ELSE MONTH(d.[Date]) + 6 END,
            d.[APAC GST Return Period Label] = CONCAT(YEAR(d.[Date]), N'-',
                                              RIGHT(CONCAT(N'0', MONTH(d.[Date])), 2)),
            d.[APAC Same Period Last Year Date] = DATEADD(YEAR, -1, d.[Date]),
            d.[Last Load Batch Id]     = @BatchId
        FROM [Dimension].[Date] AS d
        WHERE d.[Date] BETWEEN @FromDate AND @ToDate;

        /* -------------------------------------------------- trading days */
        UPDATE d
        SET d.[Is Weekend NA]   = CASE WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 1 ELSE 0 END,
            d.[Is Weekend EU]   = CASE WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 1 ELSE 0 END,
            /* Several APAC markets treat Friday-Saturday as the weekend; the flag
               is the regional default and the country outrigger is authoritative. */
            d.[Is Weekend APAC] = CASE WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 1 ELSE 0 END,
            d.[NA Is Trading Day]   = CASE WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 0 ELSE 1 END,
            d.[EU Is Trading Day]   = CASE WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 0 ELSE 1 END,
            d.[APAC Is Trading Day] = CASE WHEN d.[Day of Week] = N'Sunday' THEN 0 ELSE 1 END,
            d.[Is Month End Close Day]   = CASE WHEN d.[Date] = EOMONTH(d.[Date]) THEN 1 ELSE 0 END,
            d.[Is Quarter End Close Day] = CASE WHEN d.[Date] = EOMONTH(d.[Date])
                                                     AND MONTH(d.[Date]) IN (3, 6, 9, 12) THEN 1 ELSE 0 END,
            d.[Last Load Batch Id]  = @BatchId
        FROM [Dimension].[Date] AS d
        WHERE d.[Date] BETWEEN @FromDate AND @ToDate;

        /* Working day within the month drives the finance close calendar - the
           close pack is due on working day 5 in NA, 6 in EU, 4 in APAC. */
        UPDATE d
        SET d.[ETL Business Day Number] = x.[Business Day]
        FROM [Dimension].[Date] AS d
        INNER JOIN (
            SELECT [Date],
                   CONVERT(SMALLINT, ROW_NUMBER() OVER (
                       PARTITION BY YEAR([Date]), MONTH([Date]) ORDER BY [Date])) AS [Business Day]
            FROM [Dimension].[Date]
            WHERE [Date] BETWEEN DATEFROMPARTS(YEAR(@FromDate), MONTH(@FromDate), 1) AND @ToDate
              AND [Day of Week] NOT IN (N'Saturday', N'Sunday')
        ) AS x
            ON x.[Date] = d.[Date];

        /* -------------------------------------------------- fiscal outrigger */
        IF @RebuildFiscal = 1
        BEGIN
            DELETE FROM [Dimension].[Fiscal Calendar]
            WHERE [Date] BETWEEN @FromDate AND @ToDate;

            INSERT INTO [Dimension].[Fiscal Calendar]
                ([Country Code], [Date], [DateKey], [Region Code], [Fiscal Calendar Code],
                 [Fiscal Year], [Fiscal Year Label], [Fiscal Quarter], [Fiscal Period],
                 [Fiscal Period Label], [Fiscal Week], [Is Period End], [Is Year End],
                 [Is Public Holiday], [Holiday Name], [Holiday Scope Code],
                 [Holiday Subdivision Code], [Is Working Day], [Is Warehouse Operating Day],
                 [Is Carrier Collection Day], [Tax Return Period Label], [Source System Code],
                 [Last Load Batch Id])
            SELECT
                  c.[Country Code]
                , d.[Date]
                , d.[DateKey]
                , c.[Region Code]
                , c.[Fiscal Calendar Code]
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'NA_454'  THEN d.[NA Fiscal Year]
                      WHEN N'EU_CAL'  THEN d.[EU Fiscal Year]
                      WHEN N'APAC_AU' THEN d.[APAC AU Fiscal Year]
                      ELSE d.[APAC Fiscal Year]
                  END
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'NA_454'  THEN d.[NA Fiscal Year Label]
                      WHEN N'EU_CAL'  THEN d.[EU Fiscal Year Label]
                      WHEN N'APAC_AU' THEN d.[APAC AU Fiscal Year Label]
                      ELSE d.[APAC Fiscal Year Label]
                  END
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'NA_454' THEN d.[NA Fiscal Quarter]
                      WHEN N'EU_CAL' THEN d.[EU Fiscal Quarter]
                      ELSE d.[APAC Fiscal Quarter]
                  END
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'NA_454'  THEN d.[NA Fiscal Period]
                      WHEN N'EU_CAL'  THEN d.[EU Fiscal Period]
                      WHEN N'APAC_AU' THEN d.[APAC AU Fiscal Period]
                      ELSE d.[APAC Fiscal Period]
                  END
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'NA_454' THEN d.[NA Fiscal Period Label]
                      WHEN N'EU_CAL' THEN d.[EU Fiscal Period Label]
                      ELSE d.[APAC Fiscal Period Label]
                  END
                , CASE WHEN c.[Fiscal Calendar Code] = N'NA_454'
                       THEN d.[NA Fiscal Week] ELSE d.[EU ISO Week Number] END
                , CASE WHEN d.[Date] = EOMONTH(d.[Date]) THEN 1 ELSE 0 END
                , CASE c.[Fiscal Calendar Code]
                      WHEN N'EU_CAL'  THEN CASE WHEN MONTH(d.[Date]) = 12 AND DAY(d.[Date]) = 31 THEN 1 ELSE 0 END
                      WHEN N'APAC_AU' THEN CASE WHEN MONTH(d.[Date]) = 6  AND DAY(d.[Date]) = 30 THEN 1 ELSE 0 END
                      WHEN N'NA_454'  THEN d.[NA Is Year End]
                      ELSE CASE WHEN MONTH(d.[Date]) = 3 AND DAY(d.[Date]) = 31 THEN 1 ELSE 0 END
                  END
                , CASE WHEN h.[HolidayName] IS NULL THEN 0 ELSE 1 END
                , h.[HolidayName]
                , UPPER(h.[HolidayScopeCode])
                , UPPER(h.[SubdivisionCode])
                , CASE WHEN h.[HolidayName] IS NOT NULL THEN 0
                       WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 0
                       ELSE 1 END
                , CASE WHEN h.[HolidayName] IS NOT NULL THEN 0
                       WHEN d.[Day of Week] = N'Sunday' THEN 0
                       ELSE 1 END
                , CASE WHEN h.[HolidayName] IS NOT NULL THEN 0
                       WHEN d.[Day of Week] IN (N'Saturday', N'Sunday') THEN 0
                       ELSE 1 END
                , CASE c.[Region Code]
                      WHEN N'EU'   THEN CONCAT(YEAR(d.[Date]), N'-Q', DATEPART(QUARTER, d.[Date]))
                      WHEN N'APAC' THEN d.[APAC GST Return Period Label]
                      ELSE CONCAT(YEAR(d.[Date]), N'-', RIGHT(CONCAT(N'0', MONTH(d.[Date])), 2))
                  END
                , N'REF_CALENDAR'
                , @BatchId
            FROM [Dimension].[Date] AS d
            CROSS JOIN (
                SELECT DISTINCT g.[Country Code], g.[Region Code],
                       CASE
                           WHEN g.[Region Code] = N'NA'                      THEN N'NA_454'
                           WHEN g.[Region Code] = N'EU'                      THEN N'EU_CAL'
                           WHEN g.[Country Code] = N'AUS'                    THEN N'APAC_AU'
                           WHEN g.[Country Code] = N'JPN'                    THEN N'APAC_JP'
                           WHEN g.[Country Code] = N'IND'                    THEN N'APAC_IN'
                           ELSE N'APAC_AM'
                       END AS [Fiscal Calendar Code]
                FROM [Dimension].[Geography] AS g
                WHERE g.[Geography Key] > 0
            ) AS c
            LEFT OUTER JOIN [ref].[PublicHoliday] AS h
                ON  h.[CountryCode]  = c.[Country Code]
                AND h.[HolidayDate]  = d.[Date]
            WHERE d.[Date] BETWEEN @FromDate AND @ToDate;

            SET @FiscalCount = @@ROWCOUNT;

            /* Working-day sequences are computed after the holidays land, because
               a public holiday is not a working day and the finance close counts
               from the first working day of the month. */
            UPDATE f
            SET f.[Working Day Of Month] = x.[Working Day Of Month],
                f.[Working Day Of Year]  = x.[Working Day Of Year]
            FROM [Dimension].[Fiscal Calendar] AS f
            INNER JOIN (
                SELECT [Fiscal Calendar Key],
                       CONVERT(SMALLINT, ROW_NUMBER() OVER (
                           PARTITION BY [Country Code], YEAR([Date]), MONTH([Date])
                           ORDER BY [Date])) AS [Working Day Of Month],
                       CONVERT(SMALLINT, ROW_NUMBER() OVER (
                           PARTITION BY [Country Code], YEAR([Date])
                           ORDER BY [Date])) AS [Working Day Of Year]
                FROM [Dimension].[Fiscal Calendar]
                WHERE [Is Working Day] = 1
                  AND [Date] BETWEEN @FromDate AND @ToDate
            ) AS x
                ON x.[Fiscal Calendar Key] = f.[Fiscal Calendar Key];

            UPDATE f
            SET f.[Next Working Date]     = nx.[Date],
                f.[Previous Working Date] = pv.[Date]
            FROM [Dimension].[Fiscal Calendar] AS f
            OUTER APPLY (
                SELECT TOP (1) n.[Date]
                FROM [Dimension].[Fiscal Calendar] AS n
                WHERE n.[Country Code]   = f.[Country Code]
                  AND n.[Date]           > f.[Date]
                  AND n.[Is Working Day] = 1
                ORDER BY n.[Date]
            ) AS nx
            OUTER APPLY (
                SELECT TOP (1) p.[Date]
                FROM [Dimension].[Fiscal Calendar] AS p
                WHERE p.[Country Code]   = f.[Country Code]
                  AND p.[Date]           < f.[Date]
                  AND p.[Is Working Day] = 1
                ORDER BY p.[Date] DESC
            ) AS pv
            WHERE f.[Date] BETWEEN @FromDate AND @ToDate;
        END;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Date', N'GLOBAL', @BatchId, @PackageExecutionId,
             DATEDIFF(DAY, @FromDate, @ToDate) + 1, @UpdatedCount, @InsertedCount,
             N'StaticPopulate', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Date',
             @SourceRowCount     = @InsertedCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount;

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Fiscal Calendar',
             @SourceRowCount     = @FiscalCount,
             @InsertRowCount     = @FiscalCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @InsertedCount,
             @RowsInserted       = @InsertedCount + @FiscalCount,
             @RowsUpdated        = @UpdatedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Date',
             @ProcedureName      = N'Integration.usp_PopulateDateDimension',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @InsertedCount;

        THROW;
    END CATCH;
END;
GO
