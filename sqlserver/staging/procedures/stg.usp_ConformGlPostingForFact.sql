/*
    stg.usp_ConformGlPostingForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_GLPosting (SSIS)
    Reads         : stg.GlJournalLine, ref.Region
    Writes        : stg.GLPosting
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    FACT_GLPosting selects a flatter shape than stg.GlJournalLine holds - one
    debit and one credit column, a legal entity instead of a ledger, and a
    posting date instead of an effective date - and it selects it under the
    GLPosting name. That shape is produced here rather than in the fact package
    so the general ledger reconciliation runs against a table it can query.

    Only posted lines are published. Unposted lines move between runs and the
    period reconciliation has never been able to agree with a ledger that
    includes them; they are logged as rejects so the count is still visible.

    The legal entity is the first segment of the ERP ledger code, except for the
    EU ledgers, which carry the entity in the ledger code itself because the
    2009 statutory split was never reflected in the segment structure.
*/

IF OBJECT_ID(N'stg.usp_ConformGlPostingForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformGlPostingForFact;
GO

CREATE PROCEDURE stg.usp_ConformGlPostingForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.GLPosting';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.GlJournalLine AS gl
        WHERE gl.BatchId = @BatchId;

        DELETE FROM stg.GLPosting
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
            gl.GlJournalLineBusinessKey,
            N'PostedFlag',
            N'IsPosted',
            CONVERT(NVARCHAR(10), gl.IsPosted),
            @SourceSystemCode,
            N'UNPOSTED_LINE',
            N'Journal line is not posted; excluded from the GL posting fact source.',
            N'Transform',
            0,
            CONCAT(gl.GlJournalLineBusinessKey, N'|', gl.LedgerCode, N'|', gl.FiscalPeriodLabel)
        FROM stg.GlJournalLine AS gl
        WHERE gl.BatchId  = @BatchId
          AND gl.IsPosted = 0;

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        INSERT INTO stg.GLPosting
        (
            GlPostingBusinessKey, SourceSystemCode, JournalBatchNumber, JournalLineNumber,
            GlAccountCode, CostCentreCode, LegalEntityCode, PostingDate, AccountingPeriodCode,
            DebitAmount, CreditAmount, TransactionCurrency, NetAmountUsd, JournalSourceCode,
            IsPosted, RegionCode, LastModifiedAt, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            gl.GlJournalLineBusinessKey,
            @SourceSystemCode,
            gl.JournalName,
            gl.LineNumber,
            gl.GlAccountCode,
            gl.CostCenterCode,
            CASE
                WHEN gl.RegionCode = N'EU' THEN gl.LedgerCode
                ELSE COALESCE(gl.GlAccountSegment1, LEFT(gl.LedgerCode, 4))
            END,
            COALESCE(gl.PostedDate, gl.EffectiveDate),
            gl.FiscalPeriodLabel,
            ISNULL(gl.AccountedDebitAmount, 0),
            ISNULL(gl.AccountedCreditAmount, 0),
            LEFT(ISNULL(gl.TransactionCurrencyCode, N'USD'), 3),
            gl.NetAmountUsd,
            gl.JournalSourceCode,
            gl.IsPosted,
            gl.RegionCode,
            CONVERT(DATETIME2(3), COALESCE(gl.PostedDate, gl.EffectiveDate, gl.LoadedAtUtc)),
            CASE
                WHEN gl.GlAccountCode IS NULL                                       THEN N'FAIL'
                WHEN gl.FiscalPeriodLabel IS NULL                                   THEN N'FAIL'
                WHEN ISNULL(gl.AccountedDebitAmount, 0) <> 0
                     AND ISNULL(gl.AccountedCreditAmount, 0) <> 0                   THEN N'WARN'
                WHEN gl.IsReversal = 1                                              THEN N'WARN'
                ELSE gl.DqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(gl.GlJournalLineBusinessKey, N'|', gl.AccountedDebitAmount, N'|',
                       gl.AccountedCreditAmount, N'|', gl.GlAccountCode, N'|',
                       gl.FiscalPeriodLabel)),
            @BatchId,
            @PackageExecutionId
        FROM stg.GlJournalLine AS gl
        WHERE gl.BatchId  = @BatchId
          AND gl.IsPosted = 1;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Transform',
                @RejectReasonCode   = N'UNPOSTED_LINE',
                @SourceTable        = N'err.RejectedLookupFailure',
                @SourceFilter       = N'SourceObjectName = N''stg.GLPosting''';

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
            @SourceName         = N'FACT_Load_GLPosting',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformGlPostingForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
