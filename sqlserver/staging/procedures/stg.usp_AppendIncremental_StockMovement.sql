/*
    stg.usp_AppendIncremental_StockMovement

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_STOCKMOVEMENT (SSIS), before WORK_BUILD_INVENTORY_POSITION
    Reads         : raw.SqlStockMovement, stg.StockItem, ref.CodeCrosswalk,
                    ref.ReasonCode, ref.FxRateDaily
    Writes        : stg.StockMovement
    Control       : etl.usp_GetWatermark, etl.usp_SetWatermark, etl.usp_LogRowCount,
                    etl.usp_LogError

    Highest-volume staging load in the estate, so it is deliberately plain: one
    set-based insert, no reject table, no per-row logging. Movements that cannot
    be typed are given MovementTypeCode = UNKNOWN and DqStatusCode = FAIL and
    still load, because inventory that disappears from the roll-forward causes
    more support calls than inventory with a bad reason code.

    Direction is derived rather than trusted. The OLTP sends the quantity signed
    for issues and unsigned for adjustments, which the 2009 conversion never
    fixed, so the sign is taken from the transaction type and the quantity is
    stored as an absolute value with a separate direction column.
*/

IF OBJECT_ID(N'stg.usp_AppendIncremental_StockMovement', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_AppendIncremental_StockMovement;
GO

CREATE PROCEDURE stg.usp_AppendIncremental_StockMovement
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP',
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.StockMovement';
    DECLARE @WatermarkFrom NVARCHAR(50);
    DECLARE @WatermarkTo   NVARCHAR(50);
    DECLARE @FromUtc       DATETIME2(3);
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @FailRows      BIGINT = 0;
    DECLARE @MaxOccurred   DATETIME2(3);

    BEGIN TRY
        EXEC etl.usp_GetWatermark
            @SourceSystemCode  = @SourceSystemCode,
            @ObjectName        = @ObjectName,
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo   OUTPUT;

        SET @FromUtc = ISNULL(TRY_CONVERT(DATETIME2(3), @WatermarkFrom, 126), CONVERT(DATETIME2(3), '1900-01-01'));

        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.SqlStockMovement AS m
        WHERE m.BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO stg.StockMovement
        (
            StockMovementBusinessKey, SourceSystemCode, StockItemBusinessKey, MovementTypeCode,
            MovementReasonCode, MovementDirection, CustomerBusinessKey, SupplierBusinessKey,
            SaleBusinessKey, PurchaseOrderBusinessKey, WarehouseCode, BinCode, MovementDate,
            MovementDateTimeUtc, Quantity, UnitCostAmount, ExtendedCostAmountUsd, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.StockItemTransactionID, 1),
            @SourceSystemCode,
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.StockItemID, 1),
            t.MovementTypeCode,
            COALESCE(rc.ConformedReasonCode, NULLIF(UPPER(LTRIM(RTRIM(m.MovementReasonCode))), N'')),
            t.MovementDirection,
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.CustomerID, 1),
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.SupplierID, 1),
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.InvoiceID, 1),
            stg.ufn_SourceSystemKey(m.SourceSystemCode, m.PurchaseOrderID, 1),
            NULLIF(UPPER(LTRIM(RTRIM(m.WarehouseCode))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(m.BinCode))), N''),
            CONVERT(DATE, stg.ufn_SafeDate(m.TransactionOccurredWhen, N'NA')),
            CONVERT(DATETIME2(3), stg.ufn_SafeDate(m.TransactionOccurredWhen, N'NA')),
            ABS(CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(m.Quantity, N'.'))),
            CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(m.UnitCost, N'.')),
            CONVERT(DECIMAL(19,4),
                ABS(stg.ufn_SafeDecimal(m.Quantity, N'.'))
                * ISNULL(stg.ufn_SafeDecimal(m.UnitCost, N'.'), si.StandardCostAmountUsd)),
            CASE
                WHEN t.MovementTypeCode = N'UNKNOWN'                          THEN N'FAIL'
                WHEN stg.ufn_SafeDecimal(m.Quantity, N'.') IS NULL            THEN N'FAIL'
                WHEN stg.ufn_SafeDate(m.TransactionOccurredWhen, N'NA') IS NULL THEN N'FAIL'
                WHEN si.StockItemBusinessKey IS NULL                          THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(m.StockItemTransactionID, N'|', m.Quantity, N'|', m.UnitCost, N'|',
                       m.TransactionOccurredWhen, N'|', m.WarehouseCode)),
            @BatchId,
            @PackageExecutionId
        FROM raw.SqlStockMovement AS m
        CROSS APPLY
        (
            SELECT
                MovementTypeCode =
                    CASE
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%RECEIPT%'    THEN N'RECEIPT'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%ISSUE%'      THEN N'ISSUE'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%INVOICE%'    THEN N'ISSUE'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%ADJUST%'     THEN N'ADJUST'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%TRANSFER%'   THEN N'TRANSFER'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%RETURN%'     THEN N'RETURN'
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%STOCKTAKE%'  THEN N'ADJUST'
                        ELSE N'UNKNOWN'
                    END,
                MovementDirection =
                    CASE
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%RECEIPT%'  THEN 1
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%RETURN%'   THEN 1
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%ISSUE%'    THEN -1
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%INVOICE%'  THEN -1
                        WHEN UPPER(m.TransactionTypeName) LIKE N'%TRANSFER%'
                             THEN CASE WHEN stg.ufn_SafeDecimal(m.Quantity, N'.') < 0 THEN -1 ELSE 1 END
                        ELSE CASE WHEN stg.ufn_SafeDecimal(m.Quantity, N'.') < 0 THEN -1 ELSE 1 END
                    END
        ) AS t
        LEFT JOIN ref.ReasonCode AS rc
            ON  rc.ReasonDomainCode    = N'ADJUSTMENT'
            AND rc.ConformedReasonCode = UPPER(LTRIM(RTRIM(m.MovementReasonCode)))
        LEFT JOIN stg.StockItem AS si
            ON  si.StockItemBusinessKey = stg.ufn_SourceSystemKey(m.SourceSystemCode, m.StockItemID, 1)
            AND si.BatchId              = @BatchId
        WHERE m.BatchId = @BatchId
          AND (
                  @ReloadFullHistory = 1
               OR ISNULL(CONVERT(DATETIME2(3), stg.ufn_SafeDate(m.LastEditedWhen, N'NA')),
                         CONVERT(DATETIME2(3), '9999-12-31')) > @FromUtc
              );

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.StockMovement AS s
        WHERE s.BatchId      = @BatchId
          AND s.DqStatusCode = N'FAIL';

        COMMIT TRANSACTION;

        SELECT @MaxOccurred = MAX(s.MovementDateTimeUtc)
        FROM stg.StockMovement AS s
        WHERE s.BatchId = @BatchId;

        IF @MaxOccurred IS NOT NULL
            SET @WatermarkTo = CONVERT(NVARCHAR(50), @MaxOccurred, 126);

        EXEC etl.usp_SetWatermark
            @SourceSystemCode   = @SourceSystemCode,
            @ObjectName         = @ObjectName,
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

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
            @SourceName         = N'STG_LOAD_STOCKMOVEMENT',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_AppendIncremental_StockMovement';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
