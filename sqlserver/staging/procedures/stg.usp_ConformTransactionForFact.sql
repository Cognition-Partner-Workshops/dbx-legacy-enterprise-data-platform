/*
    stg.usp_ConformTransactionForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_Transaction (SSIS), after the customer and supplier
                    transaction conform procedures
    Reads         : stg.CustomerTransaction, stg.SupplierTransaction
    Writes        : stg.[Transaction]
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    FACT_Transaction is the combined sub-ledger the group reporting pack reads:
    one row per AR and AP document, keyed by party rather than by customer or
    supplier. It is built from the two conformed sub-ledgers rather than from the
    sources again, so a correction made in either one carries through without a
    second reconciliation.

    The tax split only exists on the AR side; AP documents carry their tax on the
    invoice line, so the amount excluding tax is the document amount less the
    header tax where one is held and the document amount otherwise.
*/

IF OBJECT_ID(N'stg.usp_ConformTransactionForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformTransactionForFact;
GO

CREATE PROCEDURE stg.usp_ConformTransactionForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Transaction';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows =
        (
            SELECT COUNT_BIG(*) FROM stg.CustomerTransaction WHERE BatchId = @BatchId
        )
        +
        (
            SELECT COUNT_BIG(*) FROM stg.SupplierTransaction WHERE BatchId = @BatchId
        );

        DELETE FROM stg.[Transaction]
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        WITH CombinedLedger AS
        (
            SELECT
                TransactionBusinessKey = CONCAT(N'AR|', ct.CustomerTransactionBusinessKey),
                TransactionTypeCode    = ct.TransactionTypeCode,
                PartyBusinessKey       = ct.CustomerBusinessKey,
                PartyTypeCode          = N'CUSTOMER',
                TransactionDate        = ct.TransactionDate,
                TaxAmount              = ISNULL(ct.TaxAmount, 0),
                TransactionAmount      = ISNULL(ct.TransactionAmount, 0),
                TransactionCurrency    = ct.TransactionCurrency,
                TransactionAmountUsd   = ct.TransactionAmountUsd,
                SourceDocumentNumber   = ct.InvoiceNumber,
                AccountingPeriodCode   = ct.AccountingPeriodCode,
                RegionCode             = ct.RegionCode,
                LastModifiedAt         = ct.LastModifiedAt,
                SourceDqStatusCode     = ct.DqStatusCode
            FROM stg.CustomerTransaction AS ct
            WHERE ct.BatchId = @BatchId

            UNION ALL

            SELECT
                TransactionBusinessKey = CONCAT(N'AP|', st.SupplierTransactionBusinessKey),
                TransactionTypeCode    = st.TransactionTypeCode,
                PartyBusinessKey       = st.SupplierBusinessKey,
                PartyTypeCode          = N'SUPPLIER',
                TransactionDate        = st.TransactionDate,
                TaxAmount              = 0,
                TransactionAmount      = ISNULL(st.TransactionAmount, 0),
                TransactionCurrency    = st.TransactionCurrency,
                TransactionAmountUsd   = st.TransactionAmountUsd,
                SourceDocumentNumber   = st.SupplierInvoiceNumber,
                AccountingPeriodCode   = st.AccountingPeriodCode,
                RegionCode             = st.RegionCode,
                LastModifiedAt         = st.LastModifiedAt,
                SourceDqStatusCode     = st.DqStatusCode
            FROM stg.SupplierTransaction AS st
            WHERE st.BatchId = @BatchId
        )
        INSERT INTO stg.[Transaction]
        (
            TransactionBusinessKey, SourceSystemCode, TransactionTypeCode, PartyBusinessKey,
            PartyTypeCode, TransactionDate, AmountExcludingTax, TaxAmount, TransactionAmount,
            TransactionCurrency, TransactionAmountUsd, SourceDocumentNumber,
            AccountingPeriodCode, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            cl.TransactionBusinessKey,
            @SourceSystemCode,
            cl.TransactionTypeCode,
            cl.PartyBusinessKey,
            cl.PartyTypeCode,
            cl.TransactionDate,
            cl.TransactionAmount - cl.TaxAmount,
            cl.TaxAmount,
            cl.TransactionAmount,
            LEFT(ISNULL(cl.TransactionCurrency, N'USD'), 3),
            cl.TransactionAmountUsd,
            cl.SourceDocumentNumber,
            cl.AccountingPeriodCode,
            cl.RegionCode,
            cl.LastModifiedAt,
            CASE
                WHEN cl.SourceDqStatusCode = N'FAIL'   THEN N'FAIL'
                WHEN cl.PartyBusinessKey IS NULL       THEN N'FAIL'
                WHEN cl.AccountingPeriodCode IS NULL   THEN N'WARN'
                ELSE cl.SourceDqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(cl.TransactionBusinessKey, N'|', cl.TransactionAmount, N'|',
                       cl.TaxAmount, N'|', cl.TransactionDate)),
            @BatchId,
            @PackageExecutionId
        FROM CombinedLedger AS cl;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.[Transaction] AS t
        WHERE t.BatchId      = @BatchId
          AND t.DqStatusCode = N'FAIL';

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
            @SourceName         = N'FACT_Load_Transaction',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformTransactionForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
