/*
    Object        : [Integration].[usp_EnrichInferredMembers]
    Deploy target : WideWorldImportersDW
    Depends on    : Integration.InferredMemberQueue, Integration.DimensionKeyRegistry,
                    the Integration.usp_MigrateStaged*Data procedures,
                    the etl control framework
    Called by     : DIM_Enrich_InferredMembers, run after the dimension loads and
                    before the aggregate refresh

    The enrichment half of the late-arriving pattern, and the sweeper for stubs the
    owning dimension load did not manage to fill.

    Each dimension load enriches its own stubs in place; this procedure exists for
    the ones that were still pending afterwards, which happens when the business
    key never appears in the source at all - a promotion code typed wrong on an
    order, a customer number from a system that was decommissioned in 2013.

    Policy, unchanged since it was argued out in 2012:

        pending  < 7 days   leave alone, the source may still catch up
        7 to 30 days        re-invoke the owning dimension load for that key only
        > 30 days           abandon the stub; it stays in the dimension so the
                            facts keep their key, but it is no longer chased

    The re-invoke is dynamic because the owning procedure differs per dimension and
    the mapping lives in a table-valued constructor here rather than in the
    registry, which is an inconsistency nobody has been paid to fix.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_EnrichInferredMembers]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Enrich_InferredMembers',
    @AbandonAfterDays   INT           = 30,
    @RetryAfterDays     INT           = 7,
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @PendingCount       BIGINT = 0;
    DECLARE @RetriedCount       BIGINT = 0;
    DECLARE @EnrichedCount      BIGINT = 0;
    DECLARE @AbandonedCount     BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);
    DECLARE @vDimension         NVARCHAR(100);
    DECLARE @vProcedure         NVARCHAR(200);
    DECLARE @vRegion            NVARCHAR(10);
    DECLARE @Sql                NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'InferredMemberEnrichment',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#OwningProcedure') IS NOT NULL DROP TABLE #OwningProcedure;

        CREATE TABLE #OwningProcedure
        (
            [Dimension Name]  NVARCHAR(100) NOT NULL,
            [Procedure Name]  NVARCHAR(200) NOT NULL,
            [Takes Region]    BIT           NOT NULL
        );

        INSERT INTO #OwningProcedure ([Dimension Name], [Procedure Name], [Takes Region])
        VALUES
            (N'Dimension.Customer',   N'Integration.usp_MigrateStagedCustomerDataV2', 1),
            (N'Dimension.Supplier',   N'Integration.usp_MigrateStagedSupplierDataV2', 1),
            (N'Dimension.Stock Item', N'Integration.usp_MigrateStagedStockItemData',  1),
            (N'Dimension.City',       N'Integration.usp_MigrateStagedCityData',       0),
            (N'Dimension.Geography',  N'Integration.usp_MigrateStagedGeographyData',  0),
            (N'Dimension.Promotion',  N'Integration.usp_MigrateStagedPromotionData',  1);

        SET @PendingCount = (SELECT COUNT_BIG(*)
                             FROM [Integration].[InferredMemberQueue]
                             WHERE [Enrichment Status] = N'Pending');

        /* Stubs the dimension loads already filled in but whose queue row was left
           behind - the 2016 rewrite of the customer load used to miss these. */
        UPDATE q
        SET q.[Enrichment Status] = N'Enriched',
            q.[Enriched On]       = @Now,
            q.[Last Attempt Note] = N'Closed by the enrichment sweep; dimension row is no longer a stub.'
        FROM [Integration].[InferredMemberQueue] AS q
        WHERE q.[Enrichment Status] = N'Pending'
          AND q.[Dimension Name]    = N'Dimension.Customer'
          AND EXISTS (SELECT 1
                      FROM [Dimension].[Customer] AS d
                      WHERE d.[Customer Key]       = q.[Surrogate Key]
                        AND ISNULL(d.[Is Inferred Member], 0) = 0);

        SET @EnrichedCount = @@ROWCOUNT;

        UPDATE q
        SET q.[Enrichment Status] = N'Enriched',
            q.[Enriched On]       = @Now,
            q.[Last Attempt Note] = N'Closed by the enrichment sweep; dimension row is no longer a stub.'
        FROM [Integration].[InferredMemberQueue] AS q
        WHERE q.[Enrichment Status] = N'Pending'
          AND q.[Dimension Name]    = N'Dimension.Stock Item'
          AND EXISTS (SELECT 1
                      FROM [Dimension].[Stock Item] AS d
                      WHERE d.[Stock Item Key]     = q.[Surrogate Key]
                        AND ISNULL(d.[Is Inferred Member], 0) = 0);

        SET @EnrichedCount = @EnrichedCount + @@ROWCOUNT;

        /* Re-invoke the owning load for the dimensions that still have stubs in the
           retry window. One call per dimension and region, not per key: the loads
           are set-based and a per-key call was tried in 2015 and took four hours. */
        DECLARE curPending CURSOR LOCAL FAST_FORWARD FOR
            SELECT DISTINCT q.[Dimension Name], p.[Procedure Name],
                   CASE WHEN p.[Takes Region] = 1 THEN ISNULL(q.[Region Code], N'GLOBAL') END
            FROM [Integration].[InferredMemberQueue] AS q
            INNER JOIN #OwningProcedure AS p
                ON p.[Dimension Name] = q.[Dimension Name]
            WHERE q.[Enrichment Status] = N'Pending'
              AND DATEDIFF(DAY, q.[Requested On], @Now) BETWEEN @RetryAfterDays AND @AbandonAfterDays;

        OPEN curPending;
        FETCH NEXT FROM curPending INTO @vDimension, @vProcedure, @vRegion;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Sql = N'EXEC ' + @vProcedure + N' @BatchId = @BatchIdParam'
                     + CASE WHEN @vRegion IS NULL THEN N'' ELSE N', @RegionCode = @RegionParam' END
                     + N', @LineageKey = @LineageParam;';

            BEGIN TRY
                EXEC sys.sp_executesql
                     @Sql,
                     N'@BatchIdParam BIGINT, @RegionParam NVARCHAR(10), @LineageParam INT',
                     @BatchIdParam = @BatchId,
                     @RegionParam  = @vRegion,
                     @LineageParam = @LineageKey;

                SET @RetriedCount = @RetriedCount + 1;
            END TRY
            BEGIN CATCH
                /* A failed retry must not take the sweep down: the remaining
                   dimensions still need their stubs chased. */
                SET @ErrorMessage = ERROR_MESSAGE();

                EXEC [etl].[usp_LogError]
                     @PackageExecutionId = @PackageExecutionId,
                     @BatchId            = @BatchId,
                     @ErrorSeverity      = N'Warning',
                     @SourceName         = @PackageName,
                     @SourceComponent    = @vDimension,
                     @ProcedureName      = @vProcedure,
                     @ErrorDescription   = @ErrorMessage;

                UPDATE [Integration].[InferredMemberQueue]
                SET [Enrichment Attempts] = [Enrichment Attempts] + 1,
                    [Last Attempt Note]   = LEFT(CONCAT(N'Retry failed: ', @ErrorMessage), 500)
                WHERE [Dimension Name]    = @vDimension
                  AND [Enrichment Status] = N'Pending';
            END CATCH;

            FETCH NEXT FROM curPending INTO @vDimension, @vProcedure, @vRegion;
        END;

        CLOSE curPending;
        DEALLOCATE curPending;

        UPDATE [Integration].[InferredMemberQueue]
        SET [Enrichment Status]   = N'Abandoned',
            [Enrichment Attempts] = [Enrichment Attempts] + 1,
            [Last Attempt Note]   = CONCAT(N'Abandoned after ', @AbandonAfterDays,
                                           N' days; business key never arrived. Stub row retained so facts keep their key.')
        WHERE [Enrichment Status] = N'Pending'
          AND DATEDIFF(DAY, [Requested On], @Now) > @AbandonAfterDays;

        SET @AbandonedCount = @@ROWCOUNT;

        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, q.[Source System Code], q.[Dimension Name],
               q.[Business Key], N'INFERRED_MEMBER_ABANDONED',
               N'Inferred member was never enriched; the business key did not arrive from the source.',
               N'Dimension',
               CONCAT(N'SurrogateKey=', q.[Surrogate Key], N'|RequestedOn=',
                      CONVERT(NVARCHAR(19), q.[Requested On], 126))
        FROM [Integration].[InferredMemberQueue] AS q
        WHERE q.[Enrichment Status] = N'Abandoned'
          AND q.[Enriched On] IS NULL
          AND DATEDIFF(DAY, q.[Requested On], @Now) BETWEEN @AbandonAfterDays AND @AbandonAfterDays + 1;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Inferred Enriched Count], [Reject Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'(inferred member sweep)', N'GLOBAL', @BatchId, @PackageExecutionId,
             @PendingCount, @EnrichedCount, @AbandonedCount, N'InferredSweep',
             @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Integration.InferredMemberQueue',
             @SourceRowCount     = @PendingCount,
             @UpdateRowCount     = @EnrichedCount,
             @RejectRowCount     = @AbandonedCount;

        DECLARE @RowsUpdatedValue BIGINT = @EnrichedCount + @RetriedCount;
        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @PendingCount,
             @RowsUpdated        = @RowsUpdatedValue,
             @RowsRejected       = @AbandonedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF CURSOR_STATUS(N'local', N'curPending') >= 0
        BEGIN
            CLOSE curPending;
            DEALLOCATE curPending;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Integration.InferredMemberQueue',
             @ProcedureName      = N'Integration.usp_EnrichInferredMembers',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @PendingCount;

        THROW;
    END CATCH;
END;
GO
