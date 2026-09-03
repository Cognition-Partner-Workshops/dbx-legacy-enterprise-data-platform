/*
    Object        : [Integration].[usp_PopulateTimeDimension]
    Deploy target : WideWorldImportersDW
    Depends on    : Dimension.Time, the etl control framework
    Called by     : DIM_Populate_Time, run once at deployment

    Fills the minute-grain time dimension the [Dimension].[vw_OrderTime] and
    [Dimension].[vw_ShipTime] role-playing views sit on. [Time Key] is hhmm, not a
    surrogate, which is why the reserved members are negative and out of that
    pattern.

    The shift codes differ per region and are the whole reason this dimension has
    regional columns at all: warehouse productivity is reported by shift and the
    three regions never agreed a common shift definition.

        NA    DAY 06:00-14:00, SWING 14:00-22:00, TWILIGHT 22:00-06:00
        EU    EARLY 05:00-13:00, LATE 13:00-21:00, NIGHT 21:00-05:00
        APAC  AM 07:00-12:00, SPLIT 12:00-15:00 (unpaid break in most markets),
              PM 15:00-23:00

    The order cutoff minute is the same-day dispatch cutoff; it also differs, and
    the version here is the NA one with the EU and APAC cutoffs bolted on.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_PopulateTimeDimension]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Populate_Time',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'TimePopulation',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        ;WITH [Minutes] AS
        (
            SELECT TOP (1440)
                   CONVERT(INT, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1) AS [Minute Of Day]
            FROM sys.all_columns AS a
            CROSS JOIN sys.all_columns AS b
        )
        INSERT INTO [Dimension].[Time]
            ([Time Key], [Time Of Day], [Hour 24], [Hour 12], [Minute], [AM PM],
             [Time Label 24], [Time Label 12], [Hour Label], [Half Hour Label],
             [Quarter Hour Label], [Minutes Since Midnight], [Is Reserved Member],
             [Daypart Code], [NA Shift Code], [EU Shift Code], [APAC Shift Code],
             [NA Is Core Business Hour], [EU Is Core Business Hour],
             [APAC Is Core Business Hour], [Is Order Cutoff Minute], [Is Batch Window],
             [Last Load Batch Id])
        SELECT
              (m.[Minute Of Day] / 60) * 100 + (m.[Minute Of Day] % 60)
            , TIMEFROMPARTS(m.[Minute Of Day] / 60, m.[Minute Of Day] % 60, 0, 0, 0)
            , CONVERT(SMALLINT, m.[Minute Of Day] / 60)
            , CONVERT(SMALLINT, CASE WHEN (m.[Minute Of Day] / 60) % 12 = 0
                                     THEN 12 ELSE (m.[Minute Of Day] / 60) % 12 END)
            , CONVERT(SMALLINT, m.[Minute Of Day] % 60)
            , CASE WHEN m.[Minute Of Day] < 720 THEN N'AM' ELSE N'PM' END
            , CONCAT(RIGHT(CONCAT(N'0', m.[Minute Of Day] / 60), 2), N':',
                     RIGHT(CONCAT(N'0', m.[Minute Of Day] % 60), 2))
            , CONCAT(RIGHT(CONCAT(N'0', CASE WHEN (m.[Minute Of Day] / 60) % 12 = 0
                                             THEN 12 ELSE (m.[Minute Of Day] / 60) % 12 END), 2), N':',
                     RIGHT(CONCAT(N'0', m.[Minute Of Day] % 60), 2), N' ',
                     CASE WHEN m.[Minute Of Day] < 720 THEN N'AM' ELSE N'PM' END)
            , CONCAT(RIGHT(CONCAT(N'0', m.[Minute Of Day] / 60), 2), N':00')
            , CONCAT(RIGHT(CONCAT(N'0', m.[Minute Of Day] / 60), 2), N':',
                     CASE WHEN m.[Minute Of Day] % 60 < 30 THEN N'00' ELSE N'30' END)
            , CONCAT(RIGHT(CONCAT(N'0', m.[Minute Of Day] / 60), 2), N':',
                     RIGHT(CONCAT(N'0', ((m.[Minute Of Day] % 60) / 15) * 15), 2))
            , CONVERT(SMALLINT, m.[Minute Of Day])
            , 0
            , CASE
                  WHEN m.[Minute Of Day] < 360   THEN N'OVERNIGHT'
                  WHEN m.[Minute Of Day] < 720   THEN N'MORNING'
                  WHEN m.[Minute Of Day] < 840   THEN N'MIDDAY'
                  WHEN m.[Minute Of Day] < 1080  THEN N'AFTERNOON'
                  ELSE N'EVENING'
              END
            , CASE
                  WHEN m.[Minute Of Day] >= 360  AND m.[Minute Of Day] < 840  THEN N'DAY'
                  WHEN m.[Minute Of Day] >= 840  AND m.[Minute Of Day] < 1320 THEN N'SWING'
                  ELSE N'TWILIGHT'
              END
            , CASE
                  WHEN m.[Minute Of Day] >= 300  AND m.[Minute Of Day] < 780  THEN N'EARLY'
                  WHEN m.[Minute Of Day] >= 780  AND m.[Minute Of Day] < 1260 THEN N'LATE'
                  ELSE N'NIGHT'
              END
            , CASE
                  WHEN m.[Minute Of Day] >= 420  AND m.[Minute Of Day] < 720  THEN N'AM'
                  WHEN m.[Minute Of Day] >= 720  AND m.[Minute Of Day] < 900  THEN N'SPLIT'
                  WHEN m.[Minute Of Day] >= 900  AND m.[Minute Of Day] < 1380 THEN N'PM'
                  ELSE N'PM'
              END
            , CASE WHEN m.[Minute Of Day] >= 480 AND m.[Minute Of Day] < 1020 THEN 1 ELSE 0 END
            , CASE WHEN m.[Minute Of Day] >= 540 AND m.[Minute Of Day] < 1050 THEN 1 ELSE 0 END
            , CASE WHEN m.[Minute Of Day] >= 540 AND m.[Minute Of Day] < 1080 THEN 1 ELSE 0 END
            /* Same-day dispatch cutoffs: NA 14:00, EU 15:30, APAC 12:00. */
            , CASE WHEN m.[Minute Of Day] IN (840, 930, 720) THEN 1 ELSE 0 END
            , CASE WHEN m.[Minute Of Day] >= 1320 OR m.[Minute Of Day] < 330 THEN 1 ELSE 0 END
            , @BatchId
        FROM [Minutes] AS m
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Time] AS t
                          WHERE t.[Time Key] = (m.[Minute Of Day] / 60) * 100 + (m.[Minute Of Day] % 60));

        SET @InsertedCount = @@ROWCOUNT;

        /* Reserved members: negative because the key is hhmm and 0000 is midnight. */
        INSERT INTO [Dimension].[Time]
            ([Time Key], [Is Reserved Member], [Reserved Member Description], [Last Load Batch Id])
        SELECT r.[Key Value], 1, r.[Description], @BatchId
        FROM (VALUES (-1, N'Unknown'), (-2, N'Not Applicable'), (-3, N'Invalid'), (-9, N'Error'))
             AS r ([Key Value], [Description])
        WHERE NOT EXISTS (SELECT 1 FROM [Dimension].[Time] AS t WHERE t.[Time Key] = r.[Key Value]);

        SET @InsertedCount = @InsertedCount + @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [New Member Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Time', N'GLOBAL', @BatchId, @PackageExecutionId, 1444, @InsertedCount,
             N'StaticPopulate', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Time',
             @SourceRowCount     = @InsertedCount,
             @InsertRowCount     = @InsertedCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @InsertedCount,
             @RowsInserted       = @InsertedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Time',
             @ProcedureName      = N'Integration.usp_PopulateTimeDimension',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @InsertedCount;

        THROW;
    END CATCH;
END;
GO
