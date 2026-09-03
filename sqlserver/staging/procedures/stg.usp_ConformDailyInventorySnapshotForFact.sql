/*
    stg.usp_ConformDailyInventorySnapshotForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_DailyInventorySnapshot (SSIS), after
                    stg.usp_ConformStockHoldingForFact
    Reads         : stg.StockHolding, work.InventoryPositionDaily, stg.StockMovement
    Writes        : stg.DailyInventorySnapshot
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The periodic snapshot fact carries the valuation view of the same position
    that stg.StockHolding carries in units: value, cover, aged quantity and the
    obsolescence provision. It is a second table rather than more columns on the
    holding because the provision is restated at period end and the unit
    position is not.

    The provision rate is a regional accounting policy and the three regions have
    never agreed one:

      * NA  - 25% of the value aged over 90 days.
      * EU  - 50% over 90 days, as the statutory auditors require.
      * APAC- 10% over 90 days and 100% over 365, following the local practice
              of writing off rather than providing.
*/

IF OBJECT_ID(N'stg.usp_ConformDailyInventorySnapshotForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformDailyInventorySnapshotForFact;
GO

CREATE PROCEDURE stg.usp_ConformDailyInventorySnapshotForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @PositionDate       DATE = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.DailyInventorySnapshot';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        IF @PositionDate IS NULL
            SELECT @PositionDate = MAX(sh.PositionDate)
            FROM stg.StockHolding AS sh
            WHERE sh.BatchId = @BatchId;

        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.StockHolding AS sh
        WHERE sh.BatchId      = @BatchId
          AND sh.PositionDate = @PositionDate;

        DELETE FROM stg.DailyInventorySnapshot
        WHERE BatchId      = @BatchId
          AND PositionDate = @PositionDate;

        BEGIN TRANSACTION;

        WITH AgedStock AS
        (
            SELECT
                sh.StockItemBusinessKey,
                sh.WarehouseCode,
                sh.PositionDate,
                sh.QuantityOnHand,
                sh.UnitCostAmount,
                sh.RegionCode,
                sh.DqStatusCode,
                StockValueAmount = ROUND(ISNULL(sh.QuantityOnHand, 0)
                                         * ISNULL(sh.UnitCostAmount, 0), 2),
                -- Cover is measured against the last 90 days of issues, which is what
                -- the planners see in their own report.
                IssuedLast90 = ISNULL(issued.IssuedQuantity, 0),
                LastReceiptDate = received.LastReceiptDate
            FROM stg.StockHolding AS sh
            OUTER APPLY
            (
                SELECT IssuedQuantity = SUM(ABS(ISNULL(sm.Quantity, 0)))
                FROM stg.StockMovement AS sm
                WHERE sm.BatchId              = @BatchId
                  AND sm.StockItemBusinessKey = sh.StockItemBusinessKey
                  AND sm.WarehouseCode        = sh.WarehouseCode
                  AND sm.MovementDirection    = N'OUT'
                  AND sm.MovementDate         > DATEADD(DAY, -90, sh.PositionDate)
            ) AS issued
            OUTER APPLY
            (
                SELECT LastReceiptDate = MAX(sm.MovementDate)
                FROM stg.StockMovement AS sm
                WHERE sm.BatchId              = @BatchId
                  AND sm.StockItemBusinessKey = sh.StockItemBusinessKey
                  AND sm.WarehouseCode        = sh.WarehouseCode
                  AND sm.MovementDirection    = N'IN'
            ) AS received
            WHERE sh.BatchId      = @BatchId
              AND sh.PositionDate = @PositionDate
        )
        INSERT INTO stg.DailyInventorySnapshot
        (
            StockItemBusinessKey, WarehouseCode, PositionDate, SourceSystemCode,
            QuantityOnHand, StockValueAmount, DaysOfCover, QuantityAgedOver90Days,
            ObsolescenceProvisionAmount, ProvisionRuleCode, RegionCode, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            ast.StockItemBusinessKey,
            ast.WarehouseCode,
            ast.PositionDate,
            @SourceSystemCode,
            ast.QuantityOnHand,
            ast.StockValueAmount,
            CASE
                WHEN ast.IssuedLast90 <= 0 THEN NULL
                ELSE ROUND(ISNULL(ast.QuantityOnHand, 0) / (ast.IssuedLast90 / 90.0), 2)
            END,
            CASE
                WHEN ast.LastReceiptDate IS NULL
                     OR DATEDIFF(DAY, ast.LastReceiptDate, ast.PositionDate) > 90
                THEN ast.QuantityOnHand
                ELSE 0
            END,
            CASE ISNULL(ast.RegionCode, N'NA')
                WHEN N'EU'   THEN ROUND(CASE WHEN DATEDIFF(DAY, ISNULL(ast.LastReceiptDate,
                                                 DATEADD(DAY, -999, ast.PositionDate)),
                                                 ast.PositionDate) > 90
                                             THEN ast.StockValueAmount * 0.50 ELSE 0 END, 2)
                WHEN N'APAC' THEN ROUND(CASE
                                            WHEN DATEDIFF(DAY, ISNULL(ast.LastReceiptDate,
                                                     DATEADD(DAY, -999, ast.PositionDate)),
                                                     ast.PositionDate) > 365
                                                 THEN ast.StockValueAmount
                                            WHEN DATEDIFF(DAY, ISNULL(ast.LastReceiptDate,
                                                     DATEADD(DAY, -999, ast.PositionDate)),
                                                     ast.PositionDate) > 90
                                                 THEN ast.StockValueAmount * 0.10
                                            ELSE 0
                                        END, 2)
                ELSE ROUND(CASE WHEN DATEDIFF(DAY, ISNULL(ast.LastReceiptDate,
                                        DATEADD(DAY, -999, ast.PositionDate)),
                                        ast.PositionDate) > 90
                                THEN ast.StockValueAmount * 0.25 ELSE 0 END, 2)
            END,
            CASE ISNULL(ast.RegionCode, N'NA')
                WHEN N'EU'   THEN N'EU_50PCT_90D'
                WHEN N'APAC' THEN N'APAC_10PCT_90D_100PCT_365D'
                ELSE              N'NA_25PCT_90D'
            END,
            ast.RegionCode,
            CASE
                WHEN ast.StockItemBusinessKey IS NULL THEN N'FAIL'
                WHEN ast.QuantityOnHand < 0           THEN N'WARN'
                ELSE ast.DqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(ast.StockItemBusinessKey, N'|', ast.WarehouseCode, N'|',
                       ast.PositionDate, N'|', ast.QuantityOnHand, N'|',
                       ast.StockValueAmount)),
            @BatchId,
            @PackageExecutionId
        FROM AgedStock AS ast;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.DailyInventorySnapshot AS dis
        WHERE dis.BatchId      = @BatchId
          AND dis.DqStatusCode = N'FAIL';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @FailRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'FACT_Load_DailyInventorySnapshot',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformDailyInventorySnapshotForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
