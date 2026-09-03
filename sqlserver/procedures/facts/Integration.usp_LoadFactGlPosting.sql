/*
    Integration.usp_LoadFactGlPosting

    Object        : Integration.usp_LoadFactGlPosting
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.GL Posting.
    Called by     : FACT_Load_Gl_Posting, FIN_Load_GL (nightly and at close).
    Reads         : stg.GlJournalLine, stg.GlJournalHeader, stg.LedgerCalendar.
    Depends on    : the etl control procedures.

    Journal lines are loaded a journal at a time, and a journal is only loaded
    once it is POSTED and its period is open or newly closed. Unbalanced
    journals are rejected as a whole - a half-loaded journal makes the trial
    balance wrong, which is worse than missing it.

    Three ledgers with three calendars:
      NA   - calendar year, 12 periods, period 13 for adjustments.
      EU   - April to March, 12 periods, statutory adjustment in period 12.
      APAC - July to June, 13 four-week periods, so the period number cannot be
             derived from the month and has to come from stg.LedgerCalendar.

    Debits are positive and credits negative in [Signed Amount]; the separate
    debit and credit columns are kept because the statutory reports in EU have
    to show them gross.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactGlPosting', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactGlPosting;
GO

CREATE PROCEDURE Integration.usp_LoadFactGlPosting
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @LedgerCode         NVARCHAR(10) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution   BIT = 0;
    DECLARE @SourceRowCount  BIGINT = 0;
    DECLARE @InsertRowCount  BIGINT = 0;
    DECLARE @RejectRowCount  BIGINT = 0;
    DECLARE @WatermarkFrom   NVARCHAR(50);
    DECLARE @WatermarkTo     NVARCHAR(50);
    DECLARE @JournalNumber   NVARCHAR(30);
    DECLARE @JournalImbalance DECIMAL(19, 2);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Gl_Posting',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactGlPosting',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'FIN_GL',
            @ObjectName       = N'Fact.GL Posting',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        /* Unbalanced journals out first, one reject per journal. */
        DECLARE imbalance_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT l.JournalNumber,
                   SUM(ISNULL(l.DebitAmount, 0) - ISNULL(l.CreditAmount, 0))
            FROM stg.GlJournalLine AS l
            INNER JOIN stg.GlJournalHeader AS h
                ON h.JournalNumber = l.JournalNumber
            WHERE h.PostedDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
              AND h.JournalStatusCode = N'POSTED'
              AND (@LedgerCode IS NULL OR h.LedgerCode = @LedgerCode)
            GROUP BY l.JournalNumber
            HAVING ABS(SUM(ISNULL(l.DebitAmount, 0) - ISNULL(l.CreditAmount, 0))) > 0.005;

        OPEN imbalance_cursor;
        FETCH NEXT FROM imbalance_cursor INTO @JournalNumber, @JournalImbalance;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @RejectDetail NVARCHAR(400) =
                CONCAT(N'Journal does not balance; difference ',
                       CONVERT(NVARCHAR(30), @JournalImbalance));

            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'FIN_GL',
                @ObjectName         = N'Fact.GL Posting',
                @BusinessKey        = @JournalNumber,
                @RejectReasonCode   = N'JNL_UNBALANCED',
                @RejectReason       = @RejectDetail,
                @RejectStage        = N'Fact';

            SET @RejectRowCount = @RejectRowCount + 1;
            FETCH NEXT FROM imbalance_cursor INTO @JournalNumber, @JournalImbalance;
        END;

        CLOSE imbalance_cursor;
        DEALLOCATE imbalance_cursor;

        INSERT INTO Fact.[GL Posting]
        (
            [Posting Date Key], [Document Date Key], [Gl Account Key], [Cost Center Key],
            [Profit Center Key], [Customer Key], [Supplier Key], [Currency Key],
            [Employee Key], [Region Code], [Ledger Code], [Journal Number],
            [Journal Line Number], [Journal Source Code], [Journal Category Code],
            [Document Number], [Invoice Number], [Po Number], [Line Description],
            [Fiscal Year], [Fiscal Period], [Period Status Code], [Debit Amount],
            [Credit Amount], [Signed Amount], [Transaction Currency Code],
            [Fx Rate], [Signed Amount Reporting], [Functional Currency Code],
            [Signed Amount Functional], [Manual Journal Flag], [Reversal Flag],
            [Reversal Of Journal Number], [Accrual Flag], [Intercompany Flag],
            [Natural Key Hash], [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            h.PostingDate,
            h.DocumentDate,
            ISNULL(acct.[Gl Account Key], 0),
            CASE WHEN l.CostCenterCode IS NULL THEN -1 ELSE ISNULL(cc.[Cost Center Key], 0) END,
            CASE WHEN l.ProfitCenterCode IS NULL THEN -1
                 ELSE ISNULL(pc.[Profit Center Key], 0) END,
            CASE WHEN l.CustomerBusinessKey IS NULL THEN -1
                 ELSE ISNULL(cust.[Customer Key], 0) END,
            CASE WHEN l.SupplierBusinessKey IS NULL THEN -1
                 ELSE ISNULL(sup.[Supplier Key], 0) END,
            ISNULL(cur.[Currency Key], 0),
            CASE WHEN h.PostedByCode IS NULL THEN -1 ELSE ISNULL(emp.[Employee Key], 0) END,
            h.RegionCode,
            h.LedgerCode,
            l.JournalNumber,
            l.JournalLineNumber,
            h.JournalSourceCode,
            h.JournalCategoryCode,
            h.DocumentNumber,
            l.InvoiceNumber,
            l.PoNumber,
            l.LineDescription,
            cal.FiscalYear,
            cal.FiscalPeriod,
            cal.PeriodStatusCode,
            ISNULL(l.DebitAmount, 0),
            ISNULL(l.CreditAmount, 0),
            ISNULL(l.DebitAmount, 0) - ISNULL(l.CreditAmount, 0),
            h.CurrencyCode,
            ISNULL(h.FxRate, 1.0),
            ROUND((ISNULL(l.DebitAmount, 0) - ISNULL(l.CreditAmount, 0)) * ISNULL(h.FxRate, 1.0), 2),
            CASE h.RegionCode WHEN N'NA' THEN N'USD' WHEN N'EU' THEN N'EUR' ELSE N'AUD' END,
            ROUND((ISNULL(l.DebitAmount, 0) - ISNULL(l.CreditAmount, 0))
                  * ISNULL(h.FunctionalFxRate, ISNULL(h.FxRate, 1.0)), 2),
            CASE WHEN h.JournalSourceCode = N'MANUAL' THEN 1 ELSE 0 END,
            CASE WHEN h.ReversalOfJournalNumber IS NOT NULL THEN 1 ELSE 0 END,
            h.ReversalOfJournalNumber,
            CASE WHEN h.JournalCategoryCode IN (N'ACCR', N'PREPAY') THEN 1 ELSE 0 END,
            CASE WHEN l.IntercompanyPartnerCode IS NOT NULL THEN 1 ELSE 0 END,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(h.LedgerCode, N'|', l.JournalNumber, N'|', l.JournalLineNumber))),
            0, @BatchId, SYSDATETIME()
        FROM stg.GlJournalLine AS l
        INNER JOIN stg.GlJournalHeader AS h
            ON h.JournalNumber = l.JournalNumber
        LEFT JOIN stg.LedgerCalendar AS cal
            ON cal.LedgerCode = h.LedgerCode
           AND h.PostingDate BETWEEN cal.PeriodStartDate AND cal.PeriodEndDate
        LEFT JOIN Dimension.[Gl Account] AS acct
            ON acct.[Account Code] = l.GlAccountCode
           AND acct.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Cost Center] AS cc
            ON cc.[Cost Center Code] = l.CostCenterCode
           AND h.PostingDate >= cc.[Valid From] AND h.PostingDate < cc.[Valid To]
        LEFT JOIN Dimension.[Profit Center] AS pc
            ON pc.[Profit Center Code] = l.ProfitCenterCode
        LEFT JOIN Dimension.[Customer] AS cust
            ON cust.[WWI Customer ID] = TRY_CONVERT(INT, l.CustomerBusinessKey)
           AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Supplier] AS sup
            ON sup.[Supplier Reference] = l.SupplierBusinessKey
           AND sup.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        LEFT JOIN Dimension.[Currency] AS cur
            ON cur.[Currency Code] = h.CurrencyCode
        LEFT JOIN Dimension.[Employee] AS emp
            ON emp.[Employee Code] = h.PostedByCode
           AND emp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        WHERE h.PostedDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND h.JournalStatusCode = N'POSTED'
          AND (@LedgerCode IS NULL OR h.LedgerCode = @LedgerCode)
          AND NOT EXISTS
          (
              SELECT 1
              FROM stg.GlJournalLine AS x
              WHERE x.JournalNumber = l.JournalNumber
              GROUP BY x.JournalNumber
              HAVING ABS(SUM(ISNULL(x.DebitAmount, 0) - ISNULL(x.CreditAmount, 0))) > 0.005
          )
          AND NOT EXISTS
          (
              SELECT 1 FROM Fact.[GL Posting] AS f
              WHERE f.[Ledger Code] = h.LedgerCode
                AND f.[Journal Number] = l.JournalNumber
                AND f.[Journal Line Number] = l.JournalLineNumber
          );

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.GL Posting',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'FIN_GL',
            @ObjectName         = N'Fact.GL Posting',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'imbalance_cursor') >= 0
        BEGIN
            CLOSE imbalance_cursor;
            DEALLOCATE imbalance_cursor;
        END;

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.GL Posting',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactGlPosting',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
