/*
    stg.usp_ConformPurchaseReceiptForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_PurchaseReceipt (SSIS), after stg.usp_ConformPurchaseForFact
    Reads         : stg.Receipt, work.PurchaseLineEnriched, stg.PurchaseOrder,
                    stg.PurchaseOrderLine, stg.ApInvoice, stg.ApInvoiceLine, stg.Payment
    Writes        : stg.PurchaseReceipt
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    stg.Receipt is the typed receipt-line landing shape and is the right source
    here, but it is not the accumulating snapshot the fact wants: the fact needs
    the order, receipt, invoice and settlement dates on one row so the
    procure-to-pay lag can be measured. This load walks that chain and publishes
    the result under the name the fact package selects.

    Settlement comes off the payment applied to the invoice; where a payment
    covers several invoices the date of the last applied payment is used, which
    is how the working capital report has always counted it.
*/

IF OBJECT_ID(N'stg.usp_ConformPurchaseReceiptForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformPurchaseReceiptForFact;
GO

CREATE PROCEDURE stg.usp_ConformPurchaseReceiptForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.PurchaseReceipt';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.Receipt AS r
        WHERE r.BatchId = @BatchId;

        DELETE FROM stg.PurchaseReceipt
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO stg.PurchaseReceipt
        (
            ReceiptBusinessKey, SourceSystemCode, ReceiptNumber, PurchaseOrderNumber,
            PurchaseOrderLineNumber, SupplierBusinessKey, StockItemBusinessKey,
            OrderRaisedDate, GoodsReceivedDate, InvoiceReceivedDate, PaymentSettledDate,
            QuantityOrdered, QuantityReceived, ReceivedCostAmount, InvoicedAmount,
            TransactionCurrency, ReceivedCostAmountUsd, DaysOrderToReceipt,
            DaysReceiptToInvoice, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            r.ReceiptLineBusinessKey,
            @SourceSystemCode,
            r.ReceiptNumber,
            COALESCE(r.PurchaseOrderNumber, po.PurchaseOrderNumber),
            pol.LineNumber,
            ple.SupplierBusinessKey,
            si.StockItemBusinessKey,
            po.OrderDate,
            r.ReceiptDate,
            inv.InvoiceDate,
            pay.SettlementDate,
            pol.OrderQuantity,
            ISNULL(r.ReceivedQuantityBaseUom, r.ReceivedQuantity),
            r.LandedCostAmount,
            inv.InvoiceAmount,
            LEFT(COALESCE(po.TransactionCurrencyCode, N'USD'), 3),
            r.LandedCostAmountUsd,
            DATEDIFF(DAY, po.OrderDate, r.ReceiptDate),
            DATEDIFF(DAY, r.ReceiptDate, inv.InvoiceDate),
            CONVERT(DATETIME2(3), COALESCE(r.ReceiptDateTimeUtc, r.LoadedAtUtc)),
            CASE
                WHEN ple.SupplierBusinessKey IS NULL                        THEN N'FAIL'
                WHEN r.ReceiptDate IS NULL                                  THEN N'FAIL'
                WHEN ISNULL(r.RejectedQuantity, 0) > 0                      THEN N'WARN'
                WHEN inv.InvoiceDate IS NOT NULL AND inv.InvoiceDate < r.ReceiptDate THEN N'WARN'
                ELSE r.DqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(r.ReceiptLineBusinessKey, N'|', r.ReceivedQuantity, N'|',
                       r.ReceiptDate, N'|', inv.InvoiceDate, N'|', pay.SettlementDate)),
            @BatchId,
            @PackageExecutionId
        FROM stg.Receipt AS r
        LEFT JOIN stg.PurchaseOrderLine AS pol
            ON  pol.PurchaseOrderLineBusinessKey = r.PurchaseOrderLineBusinessKey
            AND pol.BatchId                      = @BatchId
        LEFT JOIN work.PurchaseLineEnriched AS ple
            ON  ple.PurchaseOrderLineBusinessKey = r.PurchaseOrderLineBusinessKey
            AND ple.BatchId                      = @BatchId
        LEFT JOIN stg.PurchaseOrder AS po
            ON  po.PurchaseOrderBusinessKey = ple.PurchaseOrderBusinessKey
            AND po.BatchId                  = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) si2.StockItemBusinessKey
            FROM stg.StockItem AS si2
            WHERE si2.ProductBusinessKey = r.ProductBusinessKey
              AND si2.BatchId            = @BatchId
            ORDER BY si2.StagingStockItemId
        ) AS si
        OUTER APPLY
        (
            SELECT TOP (1)
                ai.ApInvoiceBusinessKey,
                ai.InvoiceDate,
                ai.InvoiceAmount,
                ai.InvoiceNumber
            FROM stg.ApInvoiceLine AS ail
            INNER JOIN stg.ApInvoice AS ai
                ON  ai.ApInvoiceBusinessKey = ail.ApInvoiceBusinessKey
                AND ai.BatchId              = @BatchId
            WHERE ail.BatchId              = @BatchId
              AND ail.ReceiptLineBusinessKey = r.ReceiptLineBusinessKey
            ORDER BY ai.InvoiceDate
        ) AS inv
        OUTER APPLY
        (
            SELECT SettlementDate = MAX(p.PaymentDate)
            FROM work.PaymentMatched AS pm
            INNER JOIN stg.Payment AS p
                ON  p.PaymentBusinessKey = pm.PaymentBusinessKey
                AND p.BatchId            = @BatchId
            WHERE pm.BatchId               = @BatchId
              AND pm.ApInvoiceBusinessKey  = inv.ApInvoiceBusinessKey
              AND pm.IsFinalAllocation     = 1
        ) AS pay
        WHERE r.BatchId = @BatchId;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.PurchaseReceipt AS pr
        WHERE pr.BatchId      = @BatchId
          AND pr.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_PurchaseReceipt',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformPurchaseReceiptForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
