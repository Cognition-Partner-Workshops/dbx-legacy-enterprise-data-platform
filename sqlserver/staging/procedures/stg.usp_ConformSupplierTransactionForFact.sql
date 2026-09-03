/*
    stg.usp_ConformSupplierTransactionForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_SupplierTransaction (SSIS)
    Reads         : stg.ApInvoice, stg.Payment, ref.Region
    Writes        : stg.SupplierTransaction
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The AP sub-ledger is published as one row per invoice and one row per
    payment, which is how the finance pack has always read it. Accruals are the
    invoices with IsAccrual on their lines, and they are marked rather than
    filtered because the month-end pack reports them separately.

    The accounting period label is taken from the ERP where the invoice carries
    one. Where it does not, it is derived from the fiscal calendar of the
    region: NA and APAC run to the calendar month, the EU ledgers run to the
    445 calendar and so their period is stamped from the ledger code instead.
*/

IF OBJECT_ID(N'stg.usp_ConformSupplierTransactionForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformSupplierTransactionForFact;
GO

CREATE PROCEDURE stg.usp_ConformSupplierTransactionForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.SupplierTransaction';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.ApInvoice AS ai
        WHERE ai.BatchId = @BatchId;

        DELETE FROM stg.SupplierTransaction
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH ApLedger AS
        (
            SELECT
                BusinessKey     = ai.ApInvoiceBusinessKey,
                TypeCode        = CASE WHEN ai.InvoiceTypeCode = N'CREDIT' THEN N'CRN' ELSE N'INV' END,
                SupplierKey     = ai.SupplierBusinessKey,
                DocumentNumber  = ai.InvoiceNumber,
                TransactionDate = ai.InvoiceDate,
                DueDate         = ai.DueDate,
                Amount          = ai.InvoiceAmount,
                OpenAmount      = ai.OpenAmount,
                CurrencyCode    = ai.TransactionCurrencyCode,
                AmountUsd       = ai.InvoiceAmountUsd,
                FiscalPeriod    = ai.FiscalPeriodLabel,
                LedgerCode      = ai.LedgerCode,
                RegionCode      = ai.RegionCode,
                IsAccrual       = CASE WHEN EXISTS (SELECT 1
                                                    FROM stg.ApInvoiceLine AS al
                                                    WHERE al.ApInvoiceBusinessKey = ai.ApInvoiceBusinessKey
                                                      AND al.BatchId              = @BatchId
                                                      AND al.IsAccrual            = 1)
                                       THEN 1 ELSE 0 END,
                ModifiedAt      = ISNULL(ai.SourceModifiedDate, ai.LoadedAtUtc)
            FROM stg.ApInvoice AS ai
            WHERE ai.BatchId = @BatchId

            UNION ALL

            SELECT
                BusinessKey     = p.PaymentBusinessKey,
                TypeCode        = N'PMT',
                SupplierKey     = p.SupplierBusinessKey,
                DocumentNumber  = p.PaymentNumber,
                TransactionDate = p.PaymentDate,
                DueDate         = p.PaymentDate,
                Amount          = -ABS(ISNULL(p.PaymentAmount, 0)),
                OpenAmount      = ISNULL(p.UnappliedAmount, 0),
                CurrencyCode    = p.TransactionCurrencyCode,
                AmountUsd       = -ABS(ISNULL(p.PaymentAmountUsd, 0)),
                FiscalPeriod    = CONVERT(NVARCHAR(20), FORMAT(p.PaymentDate, N'yyyy-MM')),
                LedgerCode      = p.LedgerCode,
                RegionCode      = p.RegionCode,
                IsAccrual       = 0,
                ModifiedAt      = ISNULL(p.SourceModifiedDate, p.LoadedAtUtc)
            FROM stg.Payment AS p
            WHERE p.BatchId = @BatchId
              AND p.PaymentStatusCode <> N'VOID'
        )
        INSERT INTO stg.SupplierTransaction
        (
            SupplierTransactionBusinessKey, SourceSystemCode, SupplierBusinessKey,
            TransactionTypeCode, SupplierInvoiceNumber, TransactionDate, DueDate,
            TransactionAmount, OutstandingBalance, TransactionCurrency, TransactionAmountUsd,
            IsAccrual, AccountingPeriodCode, LedgerCode, RegionCode, LastModifiedAt,
            DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            TRY_CONVERT(BIGINT, RIGHT(al.BusinessKey,
                CHARINDEX(N'|', REVERSE(al.BusinessKey) + N'|') - 1)),
            @SourceSystemCode,
            al.SupplierKey,
            al.TypeCode,
            al.DocumentNumber,
            al.TransactionDate,
            al.DueDate,
            al.Amount,
            al.OpenAmount,
            LEFT(ISNULL(al.CurrencyCode, N'USD'), 3),
            al.AmountUsd,
            al.IsAccrual,
            COALESCE(al.FiscalPeriod,
                     CASE
                         WHEN al.RegionCode = N'EU'
                              THEN CONCAT(al.LedgerCode, N'-', FORMAT(al.TransactionDate, N'yyyyMM'))
                         ELSE FORMAT(al.TransactionDate, N'yyyy-MM')
                     END),
            al.LedgerCode,
            al.RegionCode,
            al.ModifiedAt,
            CASE
                WHEN al.SupplierKey IS NULL                                     THEN N'FAIL'
                WHEN al.TransactionDate IS NULL                                 THEN N'FAIL'
                WHEN TRY_CONVERT(BIGINT, RIGHT(al.BusinessKey,
                         CHARINDEX(N'|', REVERSE(al.BusinessKey) + N'|') - 1)) IS NULL THEN N'FAIL'
                WHEN al.DueDate < al.TransactionDate                            THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(al.BusinessKey, N'|', al.TypeCode, N'|', al.Amount, N'|',
                       al.TransactionDate, N'|', al.CurrencyCode)),
            @BatchId,
            @PackageExecutionId
        FROM ApLedger AS al
        WHERE TRY_CONVERT(BIGINT, RIGHT(al.BusinessKey,
                  CHARINDEX(N'|', REVERSE(al.BusinessKey) + N'|') - 1)) IS NOT NULL;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.SupplierTransaction AS st
        WHERE st.BatchId      = @BatchId
          AND st.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_SupplierTransaction',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformSupplierTransactionForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
