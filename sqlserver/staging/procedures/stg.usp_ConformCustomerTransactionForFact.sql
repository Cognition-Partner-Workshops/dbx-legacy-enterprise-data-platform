/*
    stg.usp_ConformCustomerTransactionForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_CustomerTransaction (SSIS)
    Reads         : stg.Sale, stg.CreditNote, stg.Customer, ref.Region
    Writes        : stg.CustomerTransaction
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    The AR sub-ledger is not landed as a ledger anywhere: invoices arrive as
    sales and credits arrive as credit notes, and the balance is whatever is
    left after the credits are applied. This load flattens both into the one
    transaction shape the fact package selects, and computes the due date from
    the regional standard terms because the OLTP has never carried one.

    Terms by region, as maintained by the credit control team: NA net 30 from
    invoice date, EU net 30 from the end of the invoice month, APAC net 60. The
    fact package builds its aging ladder on top of these, so changing them here
    moves the aging buckets in the warehouse.
*/

IF OBJECT_ID(N'stg.usp_ConformCustomerTransactionForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformCustomerTransactionForFact;
GO

CREATE PROCEDURE stg.usp_ConformCustomerTransactionForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.CustomerTransaction';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.Sale AS s
        WHERE s.BatchId = @BatchId;

        DELETE FROM stg.CustomerTransaction
        WHERE BatchId = @BatchId;

        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            @ObjectName,
            s.SaleBusinessKey,
            N'NumericKey',
            N'SaleBusinessKey',
            s.SaleBusinessKey,
            @SourceSystemCode,
            N'NON_NUMERIC_KEY',
            N'Sale business key has no numeric segment, so no AR transaction key can be derived.',
            N'Transform',
            0,
            CONCAT(s.SaleBusinessKey, N'|', s.CustomerBusinessKey, N'|', s.InvoiceDate)
        FROM stg.Sale AS s
        WHERE s.BatchId = @BatchId
          AND TRY_CONVERT(BIGINT, RIGHT(s.SaleBusinessKey,
                  CHARINDEX(N'|', REVERSE(s.SaleBusinessKey) + N'|') - 1)) IS NULL;

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        WITH CreditApplied AS
        (
            SELECT
                cn.AppliedToSaleBusinessKey,
                CreditAmount = SUM(ISNULL(cn.GrossAmount, 0))
            FROM stg.CreditNote AS cn
            WHERE cn.BatchId = @BatchId
              AND cn.AppliedToSaleBusinessKey IS NOT NULL
            GROUP BY cn.AppliedToSaleBusinessKey
        ),
        SaleTransaction AS
        (
            SELECT
                BusinessKey      = s.SaleBusinessKey,
                NumericKey       = TRY_CONVERT(BIGINT, RIGHT(s.SaleBusinessKey,
                                       CHARINDEX(N'|', REVERSE(s.SaleBusinessKey) + N'|') - 1)),
                CustomerKey      = s.CustomerBusinessKey,
                TypeCode         = CASE WHEN s.IsCreditNote = 1 THEN N'CRN' ELSE N'INV' END,
                DocumentNumber   = s.SourceInvoiceId,
                TransactionDate  = s.InvoiceDate,
                GrossAmount      = ISNULL(s.SaleGrossAmount, 0),
                TaxAmount        = ISNULL(s.SaleTaxAmount, 0),
                AmountUsd        = s.SaleNetAmountUsd,
                CurrencyCode     = s.TransactionCurrencyCode,
                RegionCode       = s.RegionCode,
                FiscalPeriod     = s.FiscalPeriodLabel,
                CreditAmount     = ISNULL(ca.CreditAmount, 0),
                ModifiedAt       = ISNULL(s.SourceModifiedDate, s.LoadedAtUtc),
                RowRank          = ROW_NUMBER() OVER (PARTITION BY s.SaleBusinessKey
                                                      ORDER BY s.StagingSaleId DESC)
            FROM stg.Sale AS s
            LEFT JOIN CreditApplied AS ca
                ON ca.AppliedToSaleBusinessKey = s.SaleBusinessKey
            WHERE s.BatchId = @BatchId
        )
        INSERT INTO stg.CustomerTransaction
        (
            CustomerTransactionBusinessKey, SourceSystemCode, CustomerBusinessKey,
            TransactionTypeCode, InvoiceNumber, TransactionDate, DueDate, TransactionAmount,
            OutstandingBalance, TaxAmount, TransactionCurrency, TransactionAmountUsd,
            AccountingPeriodCode, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            st.NumericKey,
            @SourceSystemCode,
            st.CustomerKey,
            st.TypeCode,
            st.DocumentNumber,
            st.TransactionDate,
            -- Standard terms differ by region and always have.
            CASE ISNULL(st.RegionCode, N'NA')
                WHEN N'EU'   THEN DATEADD(DAY, 30, EOMONTH(st.TransactionDate))
                WHEN N'APAC' THEN DATEADD(DAY, 60, st.TransactionDate)
                ELSE              DATEADD(DAY, 30, st.TransactionDate)
            END,
            st.GrossAmount,
            CASE
                WHEN st.TypeCode = N'CRN' THEN -ABS(st.GrossAmount)
                ELSE st.GrossAmount - st.CreditAmount
            END,
            st.TaxAmount,
            LEFT(ISNULL(st.CurrencyCode, N'USD'), 3),
            st.AmountUsd,
            st.FiscalPeriod,
            st.RegionCode,
            st.ModifiedAt,
            CASE
                WHEN st.CustomerKey IS NULL                     THEN N'FAIL'
                WHEN st.TransactionDate IS NULL                 THEN N'FAIL'
                WHEN st.GrossAmount - st.CreditAmount < 0
                     AND st.TypeCode = N'INV'                   THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(st.BusinessKey, N'|', st.GrossAmount, N'|', st.CreditAmount, N'|',
                       st.TransactionDate, N'|', st.CurrencyCode)),
            @BatchId,
            @PackageExecutionId
        FROM SaleTransaction AS st
        WHERE st.RowRank = 1
          AND st.NumericKey IS NOT NULL;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Transform',
                @RejectReasonCode   = N'NON_NUMERIC_KEY',
                @SourceTable        = N'err.RejectedLookupFailure',
                @SourceFilter       = N'SourceObjectName = N''stg.CustomerTransaction''';

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
            @SourceName         = N'FACT_Load_CustomerTransaction',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformCustomerTransactionForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
