/*
    Integration.usp_PublishReportingLayer

    Object        : Integration.usp_PublishReportingLayer
    Deploy target : WideWorldImportersDW
    Deploy order  : last, after every aggregate refresh procedure.
    Called by     : AGG_Publish_Reporting_Layer (end of the nightly batch).
    Reads         : etl.PackageExecution, etl.RowCountAudit, the aggregates.
    Depends on    : the etl control procedures.

    The gate between the warehouse and the BI layer. It runs the aggregate
    refreshes in dependency order, checks a small set of publish rules, and
    only then flips Report.PublishState so the reporting views start returning
    the new day. If a rule fails the previous day stays published and the BI
    layer never sees the half-built data.

    The refresh list is a table variable driven by dynamic EXEC because the
    order changed often enough that hard-coding it stopped being worth it -
    though the order below is still hard-coded, just in one place.
*/
IF OBJECT_ID(N'Integration.usp_PublishReportingLayer', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_PublishReportingLayer;
GO

CREATE PROCEDURE Integration.usp_PublishReportingLayer
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SkipRefresh        BIT = 0,
    @ForcePublish       BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @UpdateRowCount BIGINT = 0;
    DECLARE @FailedRules    INT = 0;
    DECLARE @StepOrder      INT;
    DECLARE @ProcedureName  NVARCHAR(200);
    DECLARE @Sql            NVARCHAR(MAX);
    DECLARE @StepError      NVARCHAR(MAX);

    DECLARE @RefreshStep TABLE
    (
        StepOrder     INT NOT NULL,
        ProcedureName NVARCHAR(200) NOT NULL
    );

    INSERT INTO @RefreshStep (StepOrder, ProcedureName)
    VALUES
        (10, N'Integration.usp_RefreshAggregateDailySales'),
        (20, N'Integration.usp_RefreshAggregateInventoryHealth'),
        (30, N'Integration.usp_RefreshAggregateMonthlySales'),
        (40, N'Integration.usp_RefreshAggregateMarginAnalysis'),
        (50, N'Integration.usp_RefreshAggregateProductPerformance'),
        (60, N'Integration.usp_RefreshAggregateSupplierPerformance'),
        (70, N'Integration.usp_RefreshAggregateRegionalSales'),
        (80, N'Integration.usp_RefreshAggregateDeliveryPerformance'),
        (90, N'Integration.usp_RefreshAggregatePromotionEffectiveness'),
        (100, N'Integration.usp_RefreshAggregateCustomer360'),
        (110, N'Integration.usp_RefreshAggregateFinanceClose');

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Publish_Reporting_Layer',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'PublishReportingLayer',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        IF @SkipRefresh = 0
        BEGIN
            DECLARE refresh_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT StepOrder, ProcedureName FROM @RefreshStep ORDER BY StepOrder;

            OPEN refresh_cursor;
            FETCH NEXT FROM refresh_cursor INTO @StepOrder, @ProcedureName;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @Sql = N'EXECUTE ' + @ProcedureName
                         + N' @BatchId = @BatchIdIn, @PackageExecutionId = @ExecIdIn;';

                BEGIN TRY
                    EXECUTE sp_executesql @Sql,
                        N'@BatchIdIn BIGINT, @ExecIdIn BIGINT',
                        @BatchIdIn = @BatchId, @ExecIdIn = @PackageExecutionId;
                END TRY
                BEGIN CATCH
                    /* A single aggregate failing does not stop the others, but
                       it does stop the publish. */
                    SET @StepError = ERROR_MESSAGE();
                    SET @FailedRules = @FailedRules + 1;

                    EXECUTE etl.usp_LogError
                        @PackageExecutionId = @PackageExecutionId,
                        @BatchId            = @BatchId,
                        @ErrorSeverity      = N'Warning',
                        @ErrorCode          = 0,
                        @SourceName         = @ProcedureName,
                        @SourceComponent    = N'Aggregate refresh step',
                        @ProcedureName      = N'Integration.usp_PublishReportingLayer',
                        @ErrorDescription   = @StepError;
                END CATCH;

                FETCH NEXT FROM refresh_cursor INTO @StepOrder, @ProcedureName;
            END;

            CLOSE refresh_cursor;
            DEALLOCATE refresh_cursor;
        END;

        /* Publish rules. Crude, but they have caught a blank day twice. */
        IF NOT EXISTS (SELECT 1 FROM Aggregate.[Daily Sales Summary]
                       WHERE [Sales Date] = DATEADD(DAY, -1, CONVERT(DATE, SYSDATETIME())))
            SET @FailedRules = @FailedRules + 1;

        IF NOT EXISTS (SELECT 1 FROM Aggregate.[Daily Inventory Health]
                       WHERE [Snapshot Date] = DATEADD(DAY, -1, CONVERT(DATE, SYSDATETIME())))
            SET @FailedRules = @FailedRules + 1;

        IF EXISTS (SELECT 1 FROM Aggregate.[Customer 360]
                   WHERE [Region Code] = N'EU'
                     AND [Anonymised Flag] = 0
                     AND [Retention Expiry Date] < CONVERT(DATE, SYSDATETIME()))
            SET @FailedRules = @FailedRules + 1;

        IF @FailedRules = 0 OR @ForcePublish = 1
        BEGIN
            IF OBJECT_ID(N'Report.PublishState', N'U') IS NOT NULL
            BEGIN
                UPDATE Report.PublishState
                SET PublishedBatchId  = @BatchId,
                    PublishedDatetime = SYSDATETIME(),
                    PublishStatusCode = CASE WHEN @FailedRules = 0 THEN N'OK' ELSE N'FORCED' END,
                    FailedRuleCount   = @FailedRules
                WHERE PublishScopeCode = N'DW';

                SET @UpdateRowCount = @@ROWCOUNT;
            END;
        END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Report.PublishState',
            @UpdateRowCount     = @UpdateRowCount,
            @RejectRowCount     = @FailedRules;

        IF @OwnsExecution = 1
            DECLARE @StatusValue NVARCHAR(200) = CASE WHEN @FailedRules = 0 THEN N'Succeeded' ELSE N'Warning' END;
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = @StatusValue,
                @RowsUpdated        = @UpdateRowCount,
                @RowsRejected       = @FailedRules;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'refresh_cursor') >= 0
        BEGIN
            CLOSE refresh_cursor;
            DEALLOCATE refresh_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Report.PublishState',
            @SourceComponent    = N'Publish',
            @ProcedureName      = N'Integration.usp_PublishReportingLayer',
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
