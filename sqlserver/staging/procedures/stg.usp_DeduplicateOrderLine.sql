/*
    stg.usp_DeduplicateOrderLine

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_DEDUP_ORDERLINE (SSIS), after STG_LOAD_ORDERLINE
    Reads/writes  : stg.OrderLine
    Writes        : err.RejectedOrderLine
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecord, etl.usp_LogError

    Order lines duplicate for two unrelated reasons and the survivorship rule is
    different for each:

      1. Re-extraction. The same OrderLineID lands twice because the extract
         window overlapped. The rule is last-one-wins on the batch row identity:
         the highest StagingOrderLineId is kept and the earlier copy is deleted
         outright, because it is byte-for-byte redundant.

      2. Genuine re-keying in the OLTP. The order was cancelled and re-entered,
         so the same order, stock item and quantity appear under two different
         OrderLineIDs. These are NOT deleted - finance still expects to see the
         cancelled line - but the later line is marked with the shared
         DuplicateGroupId and the earlier one is flagged WARN, and a reject row
         is written so the order-management team can confirm the cancellation.

    Case 2 is evaluated with a cursor over the affected orders. It processes a
    few hundred orders on a normal night and the loop is what the order-management
    team reads in the log when they check the morning exceptions.
*/

IF OBJECT_ID(N'stg.usp_DeduplicateOrderLine', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_DeduplicateOrderLine;
GO

CREATE PROCEDURE stg.usp_DeduplicateOrderLine
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.OrderLine';
    DECLARE @TargetRows    BIGINT = 0;
    DECLARE @DeletedRows   BIGINT = 0;
    DECLARE @FlaggedRows   BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;

    DECLARE @OrderKey      NVARCHAR(100);
    DECLARE @GroupSeed     BIGINT = 0;

    BEGIN TRY
        SELECT @TargetRows = COUNT_BIG(*)
        FROM stg.OrderLine AS l
        WHERE l.BatchId = @BatchId;

        BEGIN TRANSACTION;

        --  Case 1: identical business key re-extracted. Keep the newest row.
        WITH ExactDuplicate AS
        (
            SELECT
                l.StagingOrderLineId,
                CopyRank = ROW_NUMBER() OVER
                (
                    PARTITION BY l.OrderLineBusinessKey
                    ORDER BY l.StagingOrderLineId DESC
                )
            FROM stg.OrderLine AS l
            WHERE l.BatchId = @BatchId
        )
        DELETE FROM stg.OrderLine
        WHERE StagingOrderLineId IN (SELECT StagingOrderLineId FROM ExactDuplicate WHERE CopyRank > 1);

        SET @DeletedRows = @@ROWCOUNT;

        --  Case 2: candidate re-keys, one order at a time.
        SELECT DISTINCT l.OrderBusinessKey
        INTO #ReKeyOrder
        FROM stg.OrderLine AS l
        WHERE l.BatchId = @BatchId
        GROUP BY l.OrderBusinessKey, l.StockItemBusinessKey, l.OrderedQuantity, l.UnitPriceAmount
        HAVING COUNT(*) > 1;

        SELECT @GroupSeed = ISNULL(MAX(l.DuplicateGroupId), 0)
        FROM stg.OrderLine AS l
        WHERE l.BatchId = @BatchId;

        DECLARE ReKeyCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT o.OrderBusinessKey FROM #ReKeyOrder AS o ORDER BY o.OrderBusinessKey;

        OPEN ReKeyCursor;
        FETCH NEXT FROM ReKeyCursor INTO @OrderKey;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @GroupSeed = @GroupSeed + 1;

            UPDATE l
            SET l.DuplicateGroupId = @GroupSeed,
                l.DqStatusCode     = CASE WHEN g.KeepRank = 1 THEN l.DqStatusCode ELSE N'WARN' END
            FROM stg.OrderLine AS l
            INNER JOIN
            (
                SELECT
                    x.StagingOrderLineId,
                    KeepRank = ROW_NUMBER() OVER
                    (
                        PARTITION BY x.StockItemBusinessKey, x.OrderedQuantity, x.UnitPriceAmount
                        ORDER BY x.LineNumber DESC, x.StagingOrderLineId DESC
                    )
                FROM stg.OrderLine AS x
                WHERE x.BatchId          = @BatchId
                  AND x.OrderBusinessKey = @OrderKey
            ) AS g
                ON g.StagingOrderLineId = l.StagingOrderLineId
            WHERE l.BatchId = @BatchId
              AND EXISTS
                  (
                      SELECT 1
                      FROM stg.OrderLine AS p
                      WHERE p.BatchId              = @BatchId
                        AND p.OrderBusinessKey     = @OrderKey
                        AND p.StockItemBusinessKey = l.StockItemBusinessKey
                        AND p.OrderedQuantity      = l.OrderedQuantity
                        AND p.UnitPriceAmount      = l.UnitPriceAmount
                        AND p.StagingOrderLineId  <> l.StagingOrderLineId
                  );

            SET @FlaggedRows = @FlaggedRows + @@ROWCOUNT;

            INSERT INTO err.RejectedOrderLine
            (
                BatchId, PackageExecutionId, SourceSystemCode, OrderBusinessKey, OrderLineBusinessKey,
                LineNumber, StockItemReference, OrderedQuantityText, UnitPriceText,
                RejectReasonCode, RejectReason, RejectStage, RecordPayload
            )
            SELECT
                @BatchId, @PackageExecutionId, l.SourceSystemCode, l.OrderBusinessKey,
                l.OrderLineBusinessKey, CONVERT(NVARCHAR(20), l.LineNumber), LEFT(l.StockItemBusinessKey, 60),
                CONVERT(NVARCHAR(50), l.OrderedQuantity), CONVERT(NVARCHAR(50), l.UnitPriceAmount),
                N'DUPLICATE_LINE',
                N'same stock item, quantity and price appear more than once on this order; confirm re-key',
                N'Dedup',
                CONCAT(N'{"OrderLineBusinessKey":"', l.OrderLineBusinessKey,
                       N'","DuplicateGroupId":', CONVERT(NVARCHAR(20), @GroupSeed), N'}')
            FROM stg.OrderLine AS l
            WHERE l.BatchId          = @BatchId
              AND l.OrderBusinessKey = @OrderKey
              AND l.DuplicateGroupId = @GroupSeed
              AND l.DqStatusCode     = N'WARN';

            SET @RejectedRows = @RejectedRows + @@ROWCOUNT;

            FETCH NEXT FROM ReKeyCursor INTO @OrderKey;
        END;

        CLOSE ReKeyCursor;
        DEALLOCATE ReKeyCursor;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @TargetRows,
            @TargetRowCount     = @TargetRows - @DeletedRows,
            @UpdateRowCount     = @FlaggedRows,
            @DeleteRowCount     = @DeletedRows,
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedOrderLine',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'OrderLineBusinessKey',
                @RejectStage        = N'Dedup';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        IF CURSOR_STATUS('local', 'ReKeyCursor') >= 0
        BEGIN
            CLOSE ReKeyCursor;
            DEALLOCATE ReKeyCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_DEDUP_ORDERLINE',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_DeduplicateOrderLine';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
