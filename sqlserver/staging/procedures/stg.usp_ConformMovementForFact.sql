/*
    stg.usp_ConformMovementForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_Movement (SSIS)
    Reads         : stg.StockMovement, ref.ReasonCode
    Writes        : stg.Movement
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    FACT_Movement reads a from/to location pair and a signed quantity, which is
    not how the OLTP records a movement: it records one row with a direction and
    a single warehouse. The pair is derived here from the direction and the
    movement type, so the fact package does not have to know the direction
    convention.

    Reversals are matched back to the movement they reverse by reason code and
    equal-and-opposite quantity on the same stock item and day. The match is not
    guaranteed - the OLTP has never held a reversal reference - so a reversal
    that finds no partner is published with the reference left NULL rather than
    dropped.
*/

IF OBJECT_ID(N'stg.usp_ConformMovementForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformMovementForFact;
GO

CREATE PROCEDURE stg.usp_ConformMovementForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Movement';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.StockMovement AS sm
        WHERE sm.BatchId = @BatchId;

        DELETE FROM stg.Movement
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH SignedMovement AS
        (
            SELECT
                sm.StockMovementBusinessKey,
                sm.StockItemBusinessKey,
                sm.MovementTypeCode,
                sm.MovementReasonCode,
                sm.MovementDirection,
                sm.WarehouseCode,
                sm.BinCode,
                sm.MovementDate,
                sm.MovementDateTimeUtc,
                sm.UnitCostAmount,
                sm.ExtendedCostAmountUsd,
                sm.DqStatusCode,
                sm.LoadedAtUtc,
                SignedQuantity = CASE
                                     WHEN sm.MovementDirection = N'OUT' THEN -ABS(ISNULL(sm.Quantity, 0))
                                     ELSE ABS(ISNULL(sm.Quantity, 0))
                                 END,
                RowRank        = ROW_NUMBER() OVER (PARTITION BY sm.StockMovementBusinessKey
                                                    ORDER BY sm.StagingStockMovementId DESC)
            FROM stg.StockMovement AS sm
            WHERE sm.BatchId = @BatchId
        )
        INSERT INTO stg.Movement
        (
            MovementBusinessKey, SourceSystemCode, StockItemBusinessKey, MovementTypeCode,
            MovementDate, QuantityMoved, MovementValueAmount, FromLocationCode, ToLocationCode,
            ReasonCode, ReversesMovementKey, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            sm.StockMovementBusinessKey,
            @SourceSystemCode,
            sm.StockItemBusinessKey,
            sm.MovementTypeCode,
            sm.MovementDate,
            sm.SignedQuantity,
            ROUND(sm.SignedQuantity * ISNULL(sm.UnitCostAmount, 0), 2),
            CASE
                WHEN sm.MovementDirection = N'OUT'      THEN sm.WarehouseCode
                WHEN sm.MovementTypeCode  = N'TRANSFER' THEN sm.WarehouseCode
                ELSE NULL
            END,
            CASE
                WHEN sm.MovementDirection = N'IN'       THEN sm.WarehouseCode
                WHEN sm.MovementTypeCode  = N'TRANSFER' THEN sm.BinCode
                ELSE NULL
            END,
            COALESCE(rc.ConformedReasonCode, sm.MovementReasonCode),
            reversed.StockMovementBusinessKey,
            LEFT(sm.WarehouseCode, 2),
            CONVERT(DATETIME2(3), COALESCE(sm.MovementDateTimeUtc, sm.LoadedAtUtc)),
            CASE
                WHEN sm.StockItemBusinessKey IS NULL THEN N'FAIL'
                WHEN sm.MovementDate IS NULL         THEN N'FAIL'
                WHEN sm.SignedQuantity = 0           THEN N'WARN'
                ELSE sm.DqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(sm.StockMovementBusinessKey, N'|', sm.SignedQuantity, N'|',
                       sm.MovementDate, N'|', sm.WarehouseCode)),
            @BatchId,
            @PackageExecutionId
        FROM SignedMovement AS sm
        LEFT JOIN ref.ReasonCode AS rc
            ON  rc.ReasonDomainCode = N'STOCK_MOVEMENT'
            AND rc.ConformedReasonCode = sm.MovementReasonCode
        OUTER APPLY
        (
            SELECT TOP (1) prior.StockMovementBusinessKey
            FROM SignedMovement AS prior
            WHERE prior.StockItemBusinessKey      = sm.StockItemBusinessKey
              AND prior.MovementDate              = sm.MovementDate
              AND prior.SignedQuantity            = -sm.SignedQuantity
              AND prior.StockMovementBusinessKey <> sm.StockMovementBusinessKey
              AND sm.MovementReasonCode LIKE N'REV%'
            ORDER BY prior.StockMovementBusinessKey
        ) AS reversed
        WHERE sm.RowRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.Movement AS m
        WHERE m.BatchId      = @BatchId
          AND m.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_Movement',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformMovementForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
