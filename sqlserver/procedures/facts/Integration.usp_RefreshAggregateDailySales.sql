/*
    Integration.usp_RefreshAggregateDailySales

    Object        : Integration.usp_RefreshAggregateDailySales
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Daily Summaries and the fact loads.
    Called by     : AGG_Refresh_Daily_Sales (nightly) and AGG_Refresh_Intraday
                    (every two hours for the current day only).
    Reads         : Fact.Sale, Fact.Return, Dimension.Date, Dimension.Stock Item.
    Depends on    : the etl control procedures.

    Incremental rebuild by day. The trailing window is longer than it looks
    necessary because Fact.Sale accepts back-dated invoices for five days and
    the correction procedure can restate anything in the open period.

    Prior-year comparison is done on the same calendar date shifted 364 days,
    not 365, so that like weekday compares with like weekday. Finance signed
    that off in 2015 and every daily trend in the estate assumes it.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateDailySales', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateDailySales;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateDailySales
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FromDate           DATE = NULL,
    @ToDate             DATE = NULL,
    @TrailingDays       INT = 10
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;

    SET @ToDate   = ISNULL(@ToDate, CONVERT(DATE, SYSDATETIME()));
    SET @FromDate = ISNULL(@FromDate, DATEADD(DAY, -@TrailingDays, @ToDate));

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Daily_Sales',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateDailySales',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Daily Sales Summary]
        WHERE [Sales Date] BETWEEN @FromDate AND @ToDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Daily Sales Summary]
        (
            [Sales Date], [Stock Item Key], [Product Category Key], [Sales Territory Key],
            [Sales Channel Key], [Region Code], [Fiscal Year], [Fiscal Period],
            [Invoice Count], [Line Count], [Distinct Customer Count], [Quantity Sold Base UOM],
            [Gross Sales Amount], [Line Discount Amount], [Promotion Discount Amount],
            [Net Sales Amount], [Tax Amount], [Freight Amount], [Cost Of Sales Amount],
            [Gross Margin Amount], [Margin Percent], [Returns Amount],
            [Net Sales Amount Reporting], [Prior Year Net Sales], [Prior Year Variance Percent],
            [Refresh Batch Id], [Refreshed Datetime], [Source Row Count]
        )
        SELECT
            s.[Invoice Date Key],
            s.[Stock Item Key],
            ISNULL(item.[Product Category Key], 0),
            s.[Sales Territory Key],
            s.[Sales Channel Key],
            s.[Region Code],
            /* Fiscal attributes follow the region, so the same day appears in
               three different fiscal periods depending on the row. */
            CASE s.[Region Code]
                WHEN N'EU'   THEN d.[Eu Fiscal Year]
                WHEN N'APAC' THEN d.[Apac Fiscal Year]
                ELSE d.[Fiscal Year]
            END,
            CASE s.[Region Code]
                WHEN N'EU'   THEN d.[Eu Fiscal Period]
                WHEN N'APAC' THEN d.[Apac Fiscal Period]
                ELSE d.[Fiscal Month Number]
            END,
            COUNT(DISTINCT s.[Invoice Number]),
            COUNT_BIG(*),
            COUNT(DISTINCT s.[Customer Key]),
            SUM(s.[Quantity Base UOM]),
            SUM(s.[Gross Amount]),
            SUM(s.[Line Discount Amount]),
            SUM(CASE WHEN s.[Promotion Key] > 0 THEN s.[Line Discount Amount] ELSE 0 END),
            SUM(s.[Net Amount]),
            SUM(s.[Tax Amount]),
            SUM(ISNULL(s.[Freight Amount], 0)),
            SUM(s.[Cost Of Sale Amount]),
            SUM(s.[Gross Margin Amount]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            ISNULL(ret.ReturnsAmount, 0),
            SUM(s.[Net Amount Reporting]),
            NULL,
            NULL,
            @BatchId, SYSDATETIME(), COUNT_BIG(*)
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = s.[Stock Item Key]
        OUTER APPLY
        (
            SELECT SUM(ABS(r.[Net Credit Amount])) AS ReturnsAmount
            FROM Fact.[Return] AS r
            WHERE r.[Return Date Key] = s.[Invoice Date Key]
              AND r.[Stock Item Key] = s.[Stock Item Key]
              AND r.[Sales Territory Key] = s.[Sales Territory Key]
        ) AS ret
        WHERE s.[Invoice Date Key] BETWEEN @FromDate AND @ToDate
        GROUP BY
            s.[Invoice Date Key], s.[Stock Item Key], item.[Product Category Key],
            s.[Sales Territory Key], s.[Sales Channel Key], s.[Region Code],
            d.[Fiscal Year], d.[Fiscal Month Number], d.[Eu Fiscal Year],
            d.[Eu Fiscal Period], d.[Apac Fiscal Year], d.[Apac Fiscal Period],
            ret.ReturnsAmount;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Prior year, 364 days back, applied as a second pass because the
           aggregate has to be complete before it can look at itself. */
        UPDATE agg
        SET [Prior Year Net Sales] = py.[Net Sales Amount],
            [Prior Year Variance Percent] =
                CASE WHEN ISNULL(py.[Net Sales Amount], 0) = 0 THEN NULL
                     ELSE ROUND(100.0 * (agg.[Net Sales Amount] - py.[Net Sales Amount])
                                / py.[Net Sales Amount], 2) END
        FROM Aggregate.[Daily Sales Summary] AS agg
        INNER JOIN Aggregate.[Daily Sales Summary] AS py
            ON py.[Sales Date] = DATEADD(DAY, -364, agg.[Sales Date])
           AND py.[Stock Item Key] = agg.[Stock Item Key]
           AND py.[Sales Territory Key] = agg.[Sales Territory Key]
           AND py.[Sales Channel Key] = agg.[Sales Channel Key]
        WHERE agg.[Sales Date] BETWEEN @FromDate AND @ToDate;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Daily Sales Summary',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Aggregate.Daily Sales Summary',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateDailySales',
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
