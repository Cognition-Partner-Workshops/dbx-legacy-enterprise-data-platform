/*
    Integration.usp_RefreshAggregateFinanceClose

    Object        : Integration.usp_RefreshAggregateFinanceClose
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Finance Close Summary and
                    Integration.usp_LoadFactGlPosting.
    Called by     : AGG_Refresh_Finance_Close (every hour during close week,
                    nightly otherwise).
    Reads         : Fact.GL Posting, Fact.Monthly Ar Aging, Fact.Monthly Ap Aging.
    Depends on    : the etl control procedures.

    The close dashboard. It compares the GL balance with the sub-ledger balance
    per account group and flags anything outside tolerance. Tolerance is
    absolute, not relative, and differs by region because the entities are
    different sizes; the values came out of a 2012 email and are hard-coded
    below.

    A period is only marked CLOSED here when the ledger calendar says so - the
    warehouse never closes a period on its own.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateFinanceClose', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateFinanceClose;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateFinanceClose
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @FiscalYear         INT = NULL,
    @FiscalPeriod       INT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;

    IF @FiscalYear IS NULL OR @FiscalPeriod IS NULL
        SELECT @FiscalYear = ISNULL(@FiscalYear, MAX([Fiscal Year])),
               @FiscalPeriod = ISNULL(@FiscalPeriod, MAX([Fiscal Period]))
        FROM Fact.[GL Posting]
        WHERE [Posting Date Key] <= CONVERT(DATE, SYSDATETIME());

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Finance_Close',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateFinanceClose',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Finance Close Summary]
        WHERE [Fiscal Year] = @FiscalYear
          AND [Fiscal Period] = @FiscalPeriod;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Finance Close Summary]
        (
            [Fiscal Year], [Fiscal Period], [Legal Entity Code], [Region Code],
            [Account Group Code], [Ledger Currency Code], [Period End Date],
            [Opening Balance Local], [Period Debits Local], [Period Credits Local],
            [Closing Balance Local], [Closing Balance Reporting], [Consolidation Rate],
            [Sub Ledger Balance Reporting], [Sub Ledger To GL Difference], [Tolerance Amount],
            [Within Tolerance Flag], [Manual Journal Count], [Manual Journal Value],
            [Late Posting Count], [Unposted Journal Count], [Ar Balance Reporting],
            [Ap Balance Reporting], [Grni Accrual Reporting], [Bad Debt Provision Reporting],
            [Close Status Code], [Close Completed Datetime], [Days To Close],
            [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            @FiscalYear,
            @FiscalPeriod,
            g.[Ledger Code],
            g.[Region Code],
            ISNULL(acct.[Account Group Code], N'UNCL'),
            MAX(g.[Ledger Currency Code]),
            MAX(g.[Posting Date Key]),
            0,
            SUM(g.[Debit Amount]),
            SUM(g.[Credit Amount]),
            SUM(g.[Signed Amount]),
            SUM(g.[Signed Amount Reporting]),
            AVG(g.[Consolidation Rate]),
            NULL, NULL,
            CASE g.[Region Code] WHEN N'NA' THEN 500.00
                                 WHEN N'EU' THEN 250.00
                                 ELSE 1000.00 END,
            0,
            SUM(CASE WHEN g.[Manual Journal Flag] = 1 THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.[Manual Journal Flag] = 1
                     THEN ABS(g.[Signed Amount Reporting]) ELSE 0 END),
            SUM(CASE WHEN DATEDIFF(DAY, g.[Effective Date Key], g.[Posting Date Key]) > 5
                     THEN 1 ELSE 0 END),
            0,
            NULL, NULL, NULL, NULL,
            MAX(g.[Period Status Code]),
            NULL, NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[GL Posting] AS g
        LEFT JOIN Dimension.[Gl Account] AS acct
            ON acct.[Gl Account Key] = g.[Gl Account Key]
        WHERE g.[Fiscal Year] = @FiscalYear
          AND g.[Fiscal Period] = @FiscalPeriod
        GROUP BY g.[Ledger Code], g.[Region Code], acct.[Account Group Code];

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Sub-ledger comparison: AR from the aging fact, AP from its own. */
        UPDATE agg
        SET [Ar Balance Reporting] = ar.ArBalance,
            [Ap Balance Reporting] = ap.ApBalance,
            [Sub Ledger Balance Reporting] =
                CASE agg.[Account Group Code]
                    WHEN N'AR' THEN ar.ArBalance
                    WHEN N'AP' THEN ap.ApBalance
                    ELSE NULL
                END
        FROM Aggregate.[Finance Close Summary] AS agg
        OUTER APPLY
        (
            SELECT SUM(a.[Balance Reporting]) AS ArBalance
            FROM Fact.[Monthly Ar Aging] AS a
            WHERE a.[Fiscal Year] = agg.[Fiscal Year]
              AND a.[Fiscal Period] = agg.[Fiscal Period]
              AND a.[Region Code] = agg.[Region Code]
        ) AS ar
        OUTER APPLY
        (
            SELECT SUM(p.[Balance Reporting]) AS ApBalance
            FROM Fact.[Monthly Ap Aging] AS p
            WHERE p.[Fiscal Year] = agg.[Fiscal Year]
              AND p.[Fiscal Period] = agg.[Fiscal Period]
              AND p.[Region Code] = agg.[Region Code]
        ) AS ap
        WHERE agg.[Fiscal Year] = @FiscalYear
          AND agg.[Fiscal Period] = @FiscalPeriod;

        UPDATE Aggregate.[Finance Close Summary]
        SET [Sub Ledger To GL Difference] =
                ISNULL([Sub Ledger Balance Reporting], 0) - [Closing Balance Reporting],
            [Within Tolerance Flag] =
                CASE WHEN [Sub Ledger Balance Reporting] IS NULL THEN 1
                     WHEN ABS(ISNULL([Sub Ledger Balance Reporting], 0)
                              - [Closing Balance Reporting]) <= [Tolerance Amount]
                     THEN 1 ELSE 0 END,
            [Days To Close] = DATEDIFF(DAY, [Period End Date], CONVERT(DATE, SYSDATETIME())),
            [Close Completed Datetime] = CASE WHEN [Close Status Code] = N'CLOSED'
                                            THEN SYSDATETIME() ELSE NULL END
        WHERE [Fiscal Year] = @FiscalYear
          AND [Fiscal Period] = @FiscalPeriod;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Finance Close Summary',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Aggregate.Finance Close Summary',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateFinanceClose',
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
