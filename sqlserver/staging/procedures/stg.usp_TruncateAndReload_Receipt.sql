/*
    stg.usp_TruncateAndReload_Receipt

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_Load_Receipt (SSIS)
    Reads         : raw.OracleReceiptLine, stg.PurchaseOrderLine, ref.UomConversion,
                    ref.FxRateDaily, ref.ReasonCode
    Writes        : stg.Receipt
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    Receipts are reloaded whole rather than appended. The ERP restates a receipt
    in place when inspection finishes, and the correction carries the original
    receipt line identifier, so an incremental append duplicates every inspected
    line. The volume is small enough that nobody has ever revisited it.

    Landed cost is converted to USD at the receipt-date rate. Where the receipt
    date falls on a day the rate feed did not run - which happens over local
    holidays in each region - the last rate on or before the receipt date is
    used, and the row is marked WARN so the cost accountants can see it.

    A receipt line whose purchase order line cannot be resolved is rejected and
    queued for late arrival: the purchase order extract and the receipt extract
    run in different windows, and the receipt regularly lands first.
*/

IF OBJECT_ID(N'stg.usp_TruncateAndReload_Receipt', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateAndReload_Receipt;
GO

CREATE PROCEDURE stg.usp_TruncateAndReload_Receipt
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Receipt';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleReceiptLine AS rl
        WHERE rl.BatchId = @BatchId;

        TRUNCATE TABLE stg.Receipt;

        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            @ObjectName,
            rl.RECEIPT_LINE_ID,
            N'PurchaseOrderLine',
            N'PO_LINE_ID',
            rl.PO_LINE_ID,
            @SourceSystemCode,
            N'LOOKUP_MISS',
            N'Receipt line does not resolve to a staged purchase order line.',
            N'Stage',
            0,
            1,
            CONCAT(rl.RECEIPT_LINE_ID, N'|', rl.PO_NUMBER, N'|', rl.RECEIVED_QTY)
        FROM raw.OracleReceiptLine AS rl
        LEFT JOIN stg.PurchaseOrderLine AS pol
            ON  pol.BatchId                      = @BatchId
            AND pol.PurchaseOrderLineBusinessKey = stg.ufn_SourceSystemKey(@SourceSystemCode,
                                                       rl.PO_LINE_ID, 1)
        WHERE rl.BatchId = @BatchId
          AND pol.PurchaseOrderLineBusinessKey IS NULL;

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        WITH RankedReceipt AS
        (
            SELECT
                rl.*,
                RowRank = ROW_NUMBER() OVER
                (
                    PARTITION BY rl.RECEIPT_LINE_ID
                    ORDER BY     stg.ufn_SafeDate(rl.LAST_UPDATE_DT, N'NA') DESC,
                                 rl.SourceRowNumber DESC
                )
            FROM raw.OracleReceiptLine AS rl
            WHERE rl.BatchId = @BatchId
        )
        INSERT INTO stg.Receipt
        (
            ReceiptLineBusinessKey, SourceSystemCode, ReceiptNumber, PurchaseOrderNumber,
            PurchaseOrderLineBusinessKey, ProductBusinessKey, ReceiptDate, ReceiptDateTimeUtc,
            ReceivedQuantity, ReceivedUomCode, ReceivedQuantityBaseUom, AcceptedQuantity,
            RejectedQuantity, RejectReasonCode, LotNumber, InspectionDate,
            InspectionResultCode, WarehouseCode, BinCode, ReceiverEmployeeKey,
            LandedCostAmount, LandedCostAmountUsd, DaysLateVsPromised, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            stg.ufn_SourceSystemKey(@SourceSystemCode, rr.RECEIPT_LINE_ID, 1),
            @SourceSystemCode,
            stg.ufn_CleanString(rr.RECEIPT_NUM, 1),
            stg.ufn_CleanString(rr.PO_NUMBER, 1),
            pol.PurchaseOrderLineBusinessKey,
            pol.ProductBusinessKey,
            CONVERT(DATE, stg.ufn_SafeDate(rr.RECEIPT_DT, N'NA')),
            stg.ufn_SafeDate(rr.RECEIPT_DT, N'NA'),
            stg.ufn_SafeDecimal(rr.RECEIVED_QTY, N'.'),
            UPPER(LTRIM(RTRIM(rr.RECEIVED_UOM_CD))),
            ROUND(stg.ufn_SafeDecimal(rr.RECEIVED_QTY, N'.')
                  * ISNULL(uc.ConversionFactor, 1), 4),
            stg.ufn_SafeDecimal(rr.ACCEPTED_QTY, N'.'),
            stg.ufn_SafeDecimal(rr.REJECTED_QTY, N'.'),
            COALESCE(rc.ConformedReasonCode, UPPER(LTRIM(RTRIM(rr.REJECT_REASON_CD)))),
            NULLIF(LTRIM(RTRIM(rr.LOT_NUMBER)), N''),
            CONVERT(DATE, stg.ufn_SafeDate(rr.INSPECTION_DT, N'NA')),
            UPPER(LTRIM(RTRIM(rr.INSPECTION_RESULT_CD))),
            UPPER(LTRIM(RTRIM(rr.WAREHOUSE_CD))),
            UPPER(LTRIM(RTRIM(rr.BIN_CD))),
            stg.ufn_SourceSystemKey(@SourceSystemCode, rr.RECEIVER_ID, 1),
            stg.ufn_SafeDecimal(rr.LANDED_COST_AMT, N'.'),
            ROUND(stg.ufn_SafeDecimal(rr.LANDED_COST_AMT, N'.')
                  * ISNULL(fx.ConversionRate, 1), 4),
            DATEDIFF(DAY, po.PromisedDate, stg.ufn_SafeDate(rr.RECEIPT_DT, N'NA')),
            CASE
                WHEN stg.ufn_SafeDate(rr.RECEIPT_DT, N'NA') IS NULL          THEN N'FAIL'
                WHEN stg.ufn_SafeDecimal(rr.RECEIVED_QTY, N'.') IS NULL      THEN N'FAIL'
                WHEN fx.ConversionRate IS NULL                               THEN N'WARN'
                WHEN ISNULL(stg.ufn_SafeDecimal(rr.REJECTED_QTY, N'.'), 0) > 0 THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(rr.RECEIPT_LINE_ID, N'|', rr.RECEIVED_QTY, N'|', rr.ACCEPTED_QTY, N'|',
                       rr.RECEIPT_DT, N'|', rr.LANDED_COST_AMT)),
            @BatchId,
            @PackageExecutionId
        FROM RankedReceipt AS rr
        INNER JOIN stg.PurchaseOrderLine AS pol
            ON  pol.BatchId                      = @BatchId
            AND pol.PurchaseOrderLineBusinessKey = stg.ufn_SourceSystemKey(@SourceSystemCode,
                                                       rr.PO_LINE_ID, 1)
        LEFT JOIN stg.PurchaseOrder AS po
            ON  po.PurchaseOrderBusinessKey = pol.PurchaseOrderBusinessKey
            AND po.BatchId                  = @BatchId
        LEFT JOIN ref.UomConversion AS uc
            ON  uc.FromUomCode = UPPER(LTRIM(RTRIM(rr.RECEIVED_UOM_CD)))
            AND uc.ToUomCode   = pol.OrderUomCode
            AND uc.StockItemBusinessKey = N'*'
        LEFT JOIN ref.ReasonCode AS rc
            ON  rc.ReasonDomainCode    = N'RECEIPT_REJECT'
            AND rc.ConformedReasonCode = UPPER(LTRIM(RTRIM(rr.REJECT_REASON_CD)))
        OUTER APPLY
        (
            SELECT TOP (1) fxr.ConversionRate
            FROM ref.FxRateDaily AS fxr
            WHERE fxr.FromCurrencyCode = ISNULL(po.TransactionCurrencyCode, N'USD')
              AND fxr.ToCurrencyCode   = N'USD'
              AND fxr.RateTypeCode     = N'CORPORATE'
              AND fxr.RateDate        <= CONVERT(DATE, stg.ufn_SafeDate(rr.RECEIPT_DT, N'NA'))
            ORDER BY fxr.RateDate DESC
        ) AS fx
        WHERE rr.RowRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Stage',
                @RejectReasonCode   = N'LOOKUP_MISS',
                @SourceTable        = N'err.RejectedLookupFailure',
                @SourceFilter       = N'SourceObjectName = N''stg.Receipt''';

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_Load_Receipt',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TruncateAndReload_Receipt';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
