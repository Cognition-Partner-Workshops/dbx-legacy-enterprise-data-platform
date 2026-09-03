/*
    Integration.usp_RefreshAggregateInventoryHealth

    Object        : Integration.usp_RefreshAggregateInventoryHealth
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Daily Summaries and
                    Integration.usp_LoadFactDailyInventorySnapshot.
    Called by     : AGG_Refresh_Inventory_Health (nightly, plus an intraday run
                    at 13:00 that only touches today's row and increments
                    [Intraday Refresh Count]).
    Reads         : Fact.Daily Inventory Snapshot, Fact.Movement, Fact.Sale.
    Depends on    : the etl control procedures.

    Site x category grain. The obsolescence provision is calculated here rather
    than in finance because the ageing buckets only exist in the warehouse; the
    percentages differ by region and were last agreed in 2016.

    Inventory turns are annualised from a 90-day issue rate, not a 365-day one,
    so seasonal SKUs read high in December. Everybody knows; nobody has changed
    it.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateInventoryHealth', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateInventoryHealth;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateInventoryHealth
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SnapshotDate       DATE = NULL,
    @IntradayRun        BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;

    SET @SnapshotDate = ISNULL(@SnapshotDate, DATEADD(DAY, -1, CONVERT(DATE, SYSDATETIME())));

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Inventory_Health',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateInventoryHealth',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Daily Inventory Health]
        WHERE [Snapshot Date] = @SnapshotDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Daily Inventory Health]
        (
            [Snapshot Date], [Warehouse Site Key], [Product Category Key], [Region Code],
            [Sku Count], [Sku Stocked Count], [Stockout Sku Count], [Below Reorder Sku Count],
            [Excess Sku Count], [Slow Moving Sku Count], [Quarantined Sku Count],
            [Total Quantity On Hand], [Total Stock Value Reporting],
            [Excess Stock Value Reporting], [Obsolescence Provision Amount],
            [Average Days Of Cover], [Service Level Percent], [Inventory Turns Annualised],
            [Days Inventory Outstanding], [Stockout Rate Percent],
            [Refresh Batch Id], [Refreshed Datetime], [Intraday Refresh Count]
        )
        SELECT
            @SnapshotDate,
            snap.[Warehouse Site Key],
            ISNULL(item.[Product Category Key], 0),
            snap.[Region Code],
            COUNT_BIG(*),
            SUM(CASE WHEN snap.[Quantity On Hand] > 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN snap.[Stockout Flag] = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN snap.[Below Reorder Level Flag] = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN snap.[Excess Stock Flag] = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN snap.[Stock Age Bucket Code] IN (N'A180', N'A365', N'AOLD')
                     THEN 1 ELSE 0 END),
            SUM(CASE WHEN snap.[Quantity Quarantined] > 0 THEN 1 ELSE 0 END),
            SUM(snap.[Quantity On Hand]),
            SUM(snap.[Stock Value Reporting]),
            SUM(CASE WHEN snap.[Excess Stock Flag] = 1
                     THEN snap.[Stock Value Reporting] ELSE 0 END),
            /* Regional provision policy. NA provides on the oldest bucket only,
               EU has a statutory sliding scale, APAC provides flat because the
               auditors there would not accept the sliding scale. */
            SUM(CASE snap.[Region Code]
                    WHEN N'NA' THEN CASE WHEN snap.[Stock Age Bucket Code] = N'AOLD'
                                         THEN snap.[Stock Value Reporting] * 0.50
                                         ELSE 0 END
                    WHEN N'EU' THEN CASE snap.[Stock Age Bucket Code]
                                        WHEN N'A180' THEN snap.[Stock Value Reporting] * 0.10
                                        WHEN N'A365' THEN snap.[Stock Value Reporting] * 0.35
                                        WHEN N'AOLD' THEN snap.[Stock Value Reporting] * 0.75
                                        ELSE 0 END
                    ELSE CASE WHEN snap.[Stock Age Bucket Code] IN (N'A365', N'AOLD')
                              THEN snap.[Stock Value Reporting] * 0.40
                              ELSE 0 END
                END),
            AVG(CONVERT(DECIMAL(18, 2), snap.[Days Of Cover])),
            ROUND(100.0 * SUM(CASE WHEN snap.[Stockout Flag] = 0 THEN 1 ELSE 0 END)
                  / NULLIF(COUNT_BIG(*), 0), 2),
            CASE WHEN SUM(snap.[Stock Value Reporting]) = 0 THEN NULL
                 ELSE ROUND(SUM(ISNULL(issued.IssueValue, 0)) * 4.0
                            / SUM(snap.[Stock Value Reporting]), 2) END,
            CASE WHEN SUM(ISNULL(issued.IssueValue, 0)) = 0 THEN NULL
                 ELSE ROUND(90.0 * SUM(snap.[Stock Value Reporting])
                            / SUM(ISNULL(issued.IssueValue, 0)), 1) END,
            ROUND(100.0 * SUM(CASE WHEN snap.[Stockout Flag] = 1 THEN 1 ELSE 0 END)
                  / NULLIF(COUNT_BIG(*), 0), 2),
            @BatchId, SYSDATETIME(), CASE WHEN @IntradayRun = 1 THEN 1 ELSE 0 END
        FROM Fact.[Daily Inventory Snapshot] AS snap
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = snap.[Stock Item Key]
        OUTER APPLY
        (
            SELECT SUM(ABS(m.[Movement Value Reporting])) AS IssueValue
            FROM Fact.[Movement] AS m
            WHERE m.[Stock Item Key] = snap.[Stock Item Key]
              AND m.[Warehouse Site Key] = snap.[Warehouse Site Key]
              AND m.[Movement Direction] = N'OUT'
              AND m.[Date Key] > DATEADD(DAY, -90, @SnapshotDate)
              AND m.[Date Key] <= @SnapshotDate
        ) AS issued
        WHERE snap.[Snapshot Date Key] = @SnapshotDate
        GROUP BY snap.[Warehouse Site Key], item.[Product Category Key], snap.[Region Code];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Daily Inventory Health',
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

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Aggregate.Daily Inventory Health',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateInventoryHealth',
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
