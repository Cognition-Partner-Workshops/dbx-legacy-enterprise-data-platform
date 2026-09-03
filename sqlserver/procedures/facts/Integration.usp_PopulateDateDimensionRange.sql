/*
    Integration.usp_PopulateDateDimensionRange

    Object        : Integration.usp_PopulateDateDimensionRange
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Date (built on the dimension branch) and
                    Integration.GenerateDateDimensionColumnsV2.
    Called by     : DIM_Populate_Date_Range (annually in December, and by hand
                    whenever a fact arrives with a date past the end of the
                    dimension).
    Reads         : Dimension.Date.
    Depends on    : the etl control procedures.

    Extends Dimension.Date forward. It lives with the facts rather than the
    dimensions because it is the fact loads that run off the end of the
    calendar - twice, in 2019 and again in 2023, when the date dimension
    stopped at 31 December and the January loads all keyed to the unknown
    member.

    Row by row on purpose: the fiscal attributes for the three regional
    calendars are carried forward from the previous day rather than computed,
    so that the APAC 4-4-5 period boundaries stay where finance put them.
      NA   fiscal year = calendar year.
      EU   fiscal year starts 1 April and is named for the ending year.
      APAC fiscal year starts 1 July, thirteen four-week periods.
*/
IF OBJECT_ID(N'Integration.usp_PopulateDateDimensionRange', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_PopulateDateDimensionRange;
GO

CREATE PROCEDURE Integration.usp_PopulateDateDimensionRange
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FromDate           DATE = NULL,
    @ToDate             DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @CurrentDate    DATE;
    DECLARE @ApacPeriod     INT;
    DECLARE @ApacPeriodDay  INT;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'DIM_Populate_Date_Range',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'PopulateDateDimensionRange',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        IF @FromDate IS NULL
            SELECT @FromDate = DATEADD(DAY, 1, MAX([Date])) FROM Dimension.[Date];

        SET @FromDate = ISNULL(@FromDate, CONVERT(DATE, '2013-01-01'));
        SET @ToDate   = ISNULL(@ToDate, DATEFROMPARTS(YEAR(SYSDATETIME()) + 2, 12, 31));

        SET @CurrentDate = @FromDate;

        SELECT @ApacPeriod = ISNULL(MAX([Apac Fiscal Period]), 1),
               @ApacPeriodDay = 0
        FROM Dimension.[Date]
        WHERE [Date] = DATEADD(DAY, -1, @FromDate);

        WHILE @CurrentDate <= @ToDate
        BEGIN
            /* APAC periods are four weeks long and roll on the 28th day. */
            SET @ApacPeriodDay = @ApacPeriodDay + 1;

            IF MONTH(@CurrentDate) = 7 AND DAY(@CurrentDate) = 1
            BEGIN
                SET @ApacPeriod = 1;
                SET @ApacPeriodDay = 1;
            END
            ELSE IF @ApacPeriodDay > 28
            BEGIN
                SET @ApacPeriod = @ApacPeriod + 1;
                SET @ApacPeriodDay = 1;
            END;

            INSERT INTO Dimension.[Date]
            (
                [Date], [Day Number], [Day], [Month], [Short Month], [Calendar Month Number],
                [Calendar Month Label], [Calendar Year], [Calendar Year Label],
                [Fiscal Month Number], [Fiscal Month Label], [Fiscal Year],
                [Fiscal Year Label], [ISO Week Number], [Eu Fiscal Year],
                [Eu Fiscal Period], [Apac Fiscal Year], [Apac Fiscal Period],
                [Is Weekday], [Is Month End], [Is Quarter End], [Is Year End],
                [Lineage Key]
            )
            SELECT
                @CurrentDate,
                DAY(@CurrentDate),
                DATENAME(WEEKDAY, @CurrentDate),
                DATENAME(MONTH, @CurrentDate),
                LEFT(DATENAME(MONTH, @CurrentDate), 3),
                MONTH(@CurrentDate),
                CONCAT(DATENAME(MONTH, @CurrentDate), N' ', YEAR(@CurrentDate)),
                YEAR(@CurrentDate),
                CONCAT(N'CY', YEAR(@CurrentDate)),
                /* The base fiscal columns follow the NA calendar, which is why
                   every EU report has to use the Eu Fiscal columns instead. */
                MONTH(@CurrentDate),
                CONCAT(N'FY', YEAR(@CurrentDate), N'-P', MONTH(@CurrentDate)),
                YEAR(@CurrentDate),
                CONCAT(N'FY', YEAR(@CurrentDate)),
                DATEPART(ISO_WEEK, @CurrentDate),
                CASE WHEN MONTH(@CurrentDate) >= 4 THEN YEAR(@CurrentDate) + 1
                     ELSE YEAR(@CurrentDate) END,
                CASE WHEN MONTH(@CurrentDate) >= 4 THEN MONTH(@CurrentDate) - 3
                     ELSE MONTH(@CurrentDate) + 9 END,
                CASE WHEN MONTH(@CurrentDate) >= 7 THEN YEAR(@CurrentDate) + 1
                     ELSE YEAR(@CurrentDate) END,
                @ApacPeriod,
                CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 0 ELSE 1 END,
                CASE WHEN @CurrentDate = EOMONTH(@CurrentDate) THEN 1 ELSE 0 END,
                CASE WHEN MONTH(@CurrentDate) IN (3, 6, 9, 12)
                          AND @CurrentDate = EOMONTH(@CurrentDate) THEN 1 ELSE 0 END,
                CASE WHEN MONTH(@CurrentDate) = 12 AND DAY(@CurrentDate) = 31 THEN 1 ELSE 0 END,
                0
            WHERE NOT EXISTS (SELECT 1 FROM Dimension.[Date] AS d WHERE d.[Date] = @CurrentDate);

            SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;
            SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
        END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Dimension.Date',
            @InsertRowCount     = @InsertRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsInserted       = @InsertRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Dimension.Date',
            @SourceComponent    = N'Calendar extension',
            @ProcedureName      = N'Integration.usp_PopulateDateDimensionRange',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
