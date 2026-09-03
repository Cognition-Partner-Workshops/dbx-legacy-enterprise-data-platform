/*
    stg.usp_ConformStockHoldingForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_StockHolding (SSIS), after work.usp_BuildInventoryPositionDaily
    Reads         : work.InventoryPositionDaily, stg.StockItem, stg.OrderLine,
                    stg.PurchaseOrderLine, stg.Receipt
    Writes        : stg.StockHolding
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The position roll-forward is already built per day and warehouse in
    work.InventoryPositionDaily. This load adds the three quantities the roll
    forward does not carry - allocated, on order and the last stocktake - and
    publishes one row per stock item, warehouse and position date, which is the
    grain FACT_StockHolding reads.

    A negative closing balance is kept rather than floored. The warehouse teams
    use the negatives to find the missing receipts, and zeroing them here has
    twice hidden a receipting backlog.
*/

IF OBJECT_ID(N'stg.usp_ConformStockHoldingForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformStockHoldingForFact;
GO

CREATE PROCEDURE stg.usp_ConformStockHoldingForFact
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

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.StockHolding';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        IF @PositionDate IS NULL
            SELECT @PositionDate = MAX(ipd.PositionDate)
            FROM work.InventoryPositionDaily AS ipd
            WHERE ipd.BatchId = @BatchId;

        SELECT @SourceRows = COUNT_BIG(*)
        FROM work.InventoryPositionDaily AS ipd
        WHERE ipd.BatchId      = @BatchId
          AND ipd.PositionDate = @PositionDate;

        DELETE FROM stg.StockHolding
        WHERE BatchId      = @BatchId
          AND PositionDate = @PositionDate;

        BEGIN TRANSACTION;

        INSERT INTO stg.StockHolding
        (
            StockItemBusinessKey, WarehouseCode, PositionDate, SourceSystemCode,
            QuantityOnHand, QuantityAllocated, QuantityOnOrder, UnitCostAmount,
            StockValueAmountUsd, ReorderLevel, TargetStockLevel, LastStocktakeDate,
            NegativeBalanceFlag, RegionCode, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            ipd.StockItemBusinessKey,
            ipd.WarehouseCode,
            ipd.PositionDate,
            @SourceSystemCode,
            ipd.ClosingQuantity,
            ISNULL(alloc.AllocatedQuantity, 0),
            ISNULL(onorder.OnOrderQuantity, 0),
            ipd.AverageUnitCostUsd,
            ipd.ClosingValueUsd,
            -- Neither level is held in the OLTP; both follow the lead-time ladder
            -- the planners have used since the 2012 reorder policy.
            CONVERT(DECIMAL(18,4), ISNULL(si.LeadTimeDays, 7) * 10),
            CONVERT(DECIMAL(18,4), ISNULL(si.LeadTimeDays, 7) * 25),
            stocktake.LastStocktakeDate,
            ipd.NegativeBalanceFlag,
            LEFT(ipd.WarehouseCode, 2),
            CASE
                WHEN ipd.StockItemBusinessKey IS NULL     THEN N'FAIL'
                WHEN ipd.RollForwardBrokenFlag = 1        THEN N'WARN'
                WHEN ipd.NegativeBalanceFlag = 1          THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(ipd.StockItemBusinessKey, N'|', ipd.WarehouseCode, N'|',
                       ipd.PositionDate, N'|', ipd.ClosingQuantity, N'|',
                       ipd.ClosingValueUsd)),
            @BatchId,
            @PackageExecutionId
        FROM work.InventoryPositionDaily AS ipd
        LEFT JOIN stg.StockItem AS si
            ON  si.StockItemBusinessKey = ipd.StockItemBusinessKey
            AND si.BatchId              = @BatchId
        OUTER APPLY
        (
            SELECT AllocatedQuantity = SUM(ISNULL(ol.PickedQuantity, 0))
            FROM stg.OrderLine AS ol
            WHERE ol.BatchId              = @BatchId
              AND ol.StockItemBusinessKey = ipd.StockItemBusinessKey
              AND ol.LineStatusCode       IN (N'OPEN', N'PICKING', N'ALLOCATED')
        ) AS alloc
        OUTER APPLY
        (
            SELECT OnOrderQuantity = SUM(ISNULL(pol.OpenQuantity, 0))
            FROM stg.PurchaseOrderLine AS pol
            INNER JOIN stg.StockItem AS si2
                ON  si2.ProductBusinessKey  = pol.ProductBusinessKey
                AND si2.BatchId             = @BatchId
                AND si2.StockItemBusinessKey = ipd.StockItemBusinessKey
            WHERE pol.BatchId        = @BatchId
              AND pol.LineStatusCode <> N'CLOSED'
        ) AS onorder
        OUTER APPLY
        (
            SELECT LastStocktakeDate = MAX(r.InspectionDate)
            FROM stg.Receipt AS r
            WHERE r.BatchId       = @BatchId
              AND r.WarehouseCode = ipd.WarehouseCode
        ) AS stocktake
        WHERE ipd.BatchId      = @BatchId
          AND ipd.PositionDate = @PositionDate;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.StockHolding AS sh
        WHERE sh.BatchId      = @BatchId
          AND sh.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_StockHolding',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformStockHoldingForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
