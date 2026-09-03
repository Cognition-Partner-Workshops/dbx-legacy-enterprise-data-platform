/*
    stg.usp_ConformSupplierPaymentForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_SupplierPayment (SSIS), after work.usp_MatchPaymentsToInvoices
    Reads         : work.PaymentMatched, stg.Payment, stg.ApInvoice
    Writes        : stg.SupplierPayment
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The fact is at invoice-settlement grain: one row per payment applied to one
    invoice, which is what work.PaymentMatched already holds after the matching
    pass. This load publishes the final allocations under the shape the fact
    package selects and leaves the unmatched payments behind - they are still
    visible in work.PaymentMatched with an unmatched reason code.

    The payment run code is not held anywhere. It is reconstructed from the
    payment method and the payment date, which is how the treasury team names
    the runs: one ACH run a week in NA, one SEPA run a week in EU, and a daily
    BPAY/BACS run in APAC.
*/

IF OBJECT_ID(N'stg.usp_ConformSupplierPaymentForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformSupplierPaymentForFact;
GO

CREATE PROCEDURE stg.usp_ConformSupplierPaymentForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.SupplierPayment';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM work.PaymentMatched AS pm
        WHERE pm.BatchId = @BatchId;

        DELETE FROM stg.SupplierPayment
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO stg.SupplierPayment
        (
            SupplierPaymentBusinessKey, SourceSystemCode, PaymentReference,
            SupplierBusinessKey, SupplierInvoiceNumber, InvoiceDate, SettlementDate,
            InvoiceAmount, SettledAmount, SettlementDiscountAmount, TransactionCurrency,
            SettledAmountUsd, RealizedFxGainLossUsd, PaymentRunCode, PaymentMethodCode,
            MatchStatusCode, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(pm.PaymentBusinessKey, N'|', pm.ApInvoiceBusinessKey),
            @SourceSystemCode,
            p.PaymentNumber,
            pm.SupplierBusinessKey,
            ai.InvoiceNumber,
            ai.InvoiceDate,
            p.PaymentDate,
            ai.InvoiceAmount,
            pm.AppliedAmount,
            pm.DiscountTakenAmount,
            LEFT(ISNULL(p.TransactionCurrencyCode, N'USD'), 3),
            pm.AppliedAmountUsd,
            pm.FxDifferenceUsd,
            CASE p.RegionCode
                WHEN N'EU'   THEN CONCAT(N'SEPA-', DATEPART(YEAR, p.PaymentDate), N'W',
                                         RIGHT(N'0' + CONVERT(NVARCHAR(2),
                                             DATEPART(ISO_WEEK, p.PaymentDate)), 2))
                WHEN N'APAC' THEN CONCAT(N'AP-', FORMAT(p.PaymentDate, N'yyyyMMdd'))
                ELSE              CONCAT(N'ACH-', DATEPART(YEAR, p.PaymentDate), N'W',
                                         RIGHT(N'0' + CONVERT(NVARCHAR(2),
                                             DATEPART(WEEK, p.PaymentDate)), 2))
            END,
            p.PaymentMethodCode,
            COALESCE(pm.MatchRuleCode, p.MatchStatusCode),
            p.RegionCode,
            CONVERT(DATETIME2(3), COALESCE(p.SourceModifiedDate, p.LoadedAtUtc)),
            CASE
                WHEN pm.SupplierBusinessKey IS NULL                     THEN N'FAIL'
                WHEN ai.ApInvoiceBusinessKey IS NULL                    THEN N'FAIL'
                WHEN pm.WithinToleranceFlag = 0                         THEN N'WARN'
                WHEN p.PaymentDate < ai.InvoiceDate                     THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(pm.PaymentBusinessKey, N'|', pm.ApInvoiceBusinessKey, N'|',
                       pm.AppliedAmount, N'|', p.PaymentDate)),
            @BatchId,
            @PackageExecutionId
        FROM work.PaymentMatched AS pm
        INNER JOIN stg.Payment AS p
            ON  p.PaymentBusinessKey = pm.PaymentBusinessKey
            AND p.BatchId            = @BatchId
        LEFT JOIN stg.ApInvoice AS ai
            ON  ai.ApInvoiceBusinessKey = pm.ApInvoiceBusinessKey
            AND ai.BatchId              = @BatchId
        WHERE pm.BatchId           = @BatchId
          AND pm.IsFinalAllocation = 1;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.SupplierPayment AS sp
        WHERE sp.BatchId      = @BatchId
          AND sp.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_SupplierPayment',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformSupplierPaymentForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
