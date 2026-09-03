/*
    work.usp_BuildInventoryPositionDaily

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : WRK_BUILD_INVENTORY_POSITION (SSIS), after STG_LOAD_STOCKMOVEMENT
    Reads         : stg.StockMovement, stg.StockItem, work.InventoryPositionDaily
    Writes        : work.InventoryPositionDaily
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Daily inventory roll-forward per stock item and warehouse:

        closing = opening + receipts - issues +/- adjustments + transfers

    The opening balance comes from the previous position date already in
    work.InventoryPositionDaily rather than from a fresh sum of history, so a gap
    in the movement extract propagates forwards. That is exactly what
    RollForwardBrokenFlag exists to record: when the previous closing balance for
    a key cannot be found, the day is seeded from the movement sum and the flag
    is set so the warehouse team knows the number is a reconstruction.

    The date walk is a WHILE loop over @FromDate..@ToDate. It has to be
    sequential - each day's opening is the previous day's closing - and the loop
    is the reason this step takes forty minutes at month end.

    Average cost is a simple weighted average of the receipt lines in the day,
    carried forward when there are no receipts. It is not the ERP's costing
    method; finance knows, and uses the ERP number for anything that matters.
*/

IF OBJECT_ID(N'work.usp_BuildInventoryPositionDaily', N'P') IS NOT NULL
    DROP PROCEDURE work.usp_BuildInventoryPositionDaily;
GO

CREATE PROCEDURE work.usp_BuildInventoryPositionDaily
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FromDate           DATE = NULL,
    @ToDate             DATE = NULL,
    @DaysOfCoverWindow  SMALLINT = 28
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'work.InventoryPositionDaily';
    DECLARE @PositionDate DATE;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @BrokenRows   BIGINT = 0;
    DECLARE @MovementRows BIGINT = 0;

    BEGIN TRY
        SELECT
            @FromDate = ISNULL(@FromDate, MIN(m.MovementDate)),
            @ToDate   = ISNULL(@ToDate,   MAX(m.MovementDate)),
            @MovementRows = COUNT_BIG(*)
        FROM stg.StockMovement AS m
        WHERE m.BatchId = @BatchId;

        IF @FromDate IS NULL
        BEGIN
            EXEC etl.usp_LogRowCount
                @PackageExecutionId = @PackageExecutionId,
                @ObjectName         = @ObjectName,
                @SourceRowCount     = 0,
                @InsertRowCount     = 0;

            RETURN 0;
        END;

        DELETE FROM work.InventoryPositionDaily
        WHERE BatchId = @BatchId;

        SET @PositionDate = @FromDate;

        WHILE @PositionDate <= @ToDate
        BEGIN
            INSERT INTO work.InventoryPositionDaily
            (
                BatchId, PackageExecutionId, PositionDate, StockItemBusinessKey, WarehouseCode,
                OpeningQuantity, ReceiptQuantity, IssueQuantity, AdjustmentQuantity,
                TransferInQuantity, TransferOutQuantity, ClosingQuantity, ClosingValueUsd,
                AverageUnitCostUsd, DaysOfCoverEstimate, NegativeBalanceFlag, RollForwardBrokenFlag
            )
            SELECT
                @BatchId,
                @PackageExecutionId,
                @PositionDate,
                d.StockItemBusinessKey,
                d.WarehouseCode,
                ISNULL(prior.ClosingQuantity, d.SeedQuantity),
                d.ReceiptQuantity,
                d.IssueQuantity,
                d.AdjustmentQuantity,
                d.TransferInQuantity,
                d.TransferOutQuantity,
                Closing = ISNULL(prior.ClosingQuantity, d.SeedQuantity)
                        + d.ReceiptQuantity - d.IssueQuantity + d.AdjustmentQuantity
                        + d.TransferInQuantity - d.TransferOutQuantity,
                CONVERT(DECIMAL(19,4),
                    (ISNULL(prior.ClosingQuantity, d.SeedQuantity)
                     + d.ReceiptQuantity - d.IssueQuantity + d.AdjustmentQuantity
                     + d.TransferInQuantity - d.TransferOutQuantity)
                    * ISNULL(d.DayAverageUnitCostUsd, prior.AverageUnitCostUsd)),
                ISNULL(d.DayAverageUnitCostUsd, prior.AverageUnitCostUsd),
                CASE
                    WHEN cover.AverageDailyIssue IS NULL OR cover.AverageDailyIssue = 0 THEN NULL
                    ELSE CONVERT(DECIMAL(9,2),
                            (ISNULL(prior.ClosingQuantity, d.SeedQuantity)
                             + d.ReceiptQuantity - d.IssueQuantity + d.AdjustmentQuantity
                             + d.TransferInQuantity - d.TransferOutQuantity)
                            / cover.AverageDailyIssue)
                END,
                CASE
                    WHEN ISNULL(prior.ClosingQuantity, d.SeedQuantity)
                       + d.ReceiptQuantity - d.IssueQuantity + d.AdjustmentQuantity
                       + d.TransferInQuantity - d.TransferOutQuantity < 0 THEN 1
                    ELSE 0
                END,
                CASE WHEN prior.ClosingQuantity IS NULL AND @PositionDate > @FromDate THEN 1 ELSE 0 END
            FROM
            (
                SELECT
                    m.StockItemBusinessKey,
                    WarehouseCode      = ISNULL(m.WarehouseCode, N'UNKNOWN'),
                    ReceiptQuantity    = SUM(CASE WHEN m.MovementTypeCode = N'RECEIPT'  THEN ABS(m.Quantity) ELSE 0 END),
                    IssueQuantity      = SUM(CASE WHEN m.MovementTypeCode = N'ISSUE'    THEN ABS(m.Quantity) ELSE 0 END),
                    AdjustmentQuantity = SUM(CASE WHEN m.MovementTypeCode = N'ADJUST'   THEN m.Quantity      ELSE 0 END),
                    TransferInQuantity = SUM(CASE WHEN m.MovementTypeCode = N'TRANSFER' AND m.MovementDirection = 1
                                                  THEN ABS(m.Quantity) ELSE 0 END),
                    TransferOutQuantity= SUM(CASE WHEN m.MovementTypeCode = N'TRANSFER' AND m.MovementDirection = -1
                                                  THEN ABS(m.Quantity) ELSE 0 END),
                    --  On the very first day of the window there is nothing to
                    --  roll forward from, so the day seeds itself from zero.
                    SeedQuantity       = 0,
                    DayAverageUnitCostUsd =
                        CASE
                            WHEN SUM(CASE WHEN m.MovementTypeCode = N'RECEIPT' THEN ABS(m.Quantity) ELSE 0 END) = 0
                                THEN NULL
                            ELSE CONVERT(DECIMAL(19,6),
                                    SUM(CASE WHEN m.MovementTypeCode = N'RECEIPT'
                                             THEN ISNULL(m.ExtendedCostAmountUsd, 0) ELSE 0 END)
                                  / SUM(CASE WHEN m.MovementTypeCode = N'RECEIPT' THEN ABS(m.Quantity) ELSE 0 END))
                        END
                FROM stg.StockMovement AS m
                WHERE m.BatchId              = @BatchId
                  AND m.MovementDate         = @PositionDate
                  AND m.StockItemBusinessKey IS NOT NULL
                GROUP BY m.StockItemBusinessKey, ISNULL(m.WarehouseCode, N'UNKNOWN')
            ) AS d
            OUTER APPLY
            (
                SELECT TOP (1) p.ClosingQuantity, p.AverageUnitCostUsd
                FROM work.InventoryPositionDaily AS p
                WHERE p.StockItemBusinessKey = d.StockItemBusinessKey
                  AND p.WarehouseCode        = d.WarehouseCode
                  AND p.PositionDate         < @PositionDate
                ORDER BY p.PositionDate DESC
            ) AS prior
            OUTER APPLY
            (
                SELECT AverageDailyIssue =
                    CONVERT(DECIMAL(18,4),
                        SUM(CASE WHEN h.MovementTypeCode = N'ISSUE' THEN ABS(h.Quantity) ELSE 0 END)
                        / NULLIF(CONVERT(DECIMAL(9,2), @DaysOfCoverWindow), 0))
                FROM stg.StockMovement AS h
                WHERE h.BatchId              = @BatchId
                  AND h.StockItemBusinessKey = d.StockItemBusinessKey
                  AND ISNULL(h.WarehouseCode, N'UNKNOWN') = d.WarehouseCode
                  AND h.MovementDate BETWEEN DATEADD(DAY, -@DaysOfCoverWindow, @PositionDate) AND @PositionDate
            ) AS cover;

            SET @InsertedRows = @InsertedRows + @@ROWCOUNT;
            SET @PositionDate = DATEADD(DAY, 1, @PositionDate);
        END;

        SELECT @BrokenRows = COUNT_BIG(*)
        FROM work.InventoryPositionDaily AS p
        WHERE p.BatchId = @BatchId
          AND (p.RollForwardBrokenFlag = 1 OR p.NegativeBalanceFlag = 1);

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @MovementRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @BrokenRows;
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'WRK_BUILD_INVENTORY_POSITION',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'work.usp_BuildInventoryPositionDaily';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
