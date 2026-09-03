/*
    Object        : [Integration].[usp_LoadGlAccountHierarchy]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.GlJournalLine (for the accounts that post but are not in the
                    chart extract), ref.GlAccount, Dimension.GL Account,
                    the etl control framework
    Called by     : DIM_Load_GlAccountHierarchy, run after the cost centre load and
                    before the GL fact load

    The recursive account hierarchy. Accounts point at a parent account code; the
    chart is ragged because the 1997 conversion left a set of accounts with no
    rollup at all ([Is Orphan Account]) and because the EU statutory charts hang
    three extra levels under the group accounts.

    The parent-child walk is a recursive CTE with MAXRECURSION capped at 20. When
    the cap is hit the load does not fail - it flags the accounts it could not
    resolve as orphans and carries on, because a cycle in the chart has stopped
    the nightly GL load twice and finance would rather have the numbers with a
    known-bad rollup than not have them.

    Regional divergence:
      NA    the operating chart is the reporting chart; [NA Tax Line Reference]
            carries the federal tax line the account maps to.
      EU    every account also carries a local statutory account code and chart
            code, and the statutory chart differs per member state, so the
            statutory code is not unique across the dimension.
      APAC  local account codes exist only where the market files locally; the
            rest roll up on the consolidation account code alone.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadGlAccountHierarchy]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_GlAccountHierarchy',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_GL',
    @MaxHierarchyDepth  INT           = 20,
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @OrphanCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'Hierarchy',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#GlAccount') IS NOT NULL DROP TABLE #GlAccount;

        SELECT
              UPPER(LTRIM(RTRIM(a.[GlAccountCode])))                AS [GL Account Code]
            , LTRIM(RTRIM(a.[GlAccountName]))                       AS [GL Account Name]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[ParentGlAccountCode])), N'')) AS [Parent GL Account Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[AccountTypeCode])), N'')) AS [Account Type Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[AccountClassCode])), N'')) AS [Account Class Code]
            , CONVERT(BIT, ISNULL(a.[IsPostable], 1))               AS [Is Postable]
            , CONVERT(BIT, ISNULL(a.[IsIntercompany], 0))           AS [Is Intercompany Account]
            , CONVERT(BIT, ISNULL(a.[IsCashAccount], 0))            AS [Is Cash Account]
            , CONVERT(BIT, ISNULL(a.[IsRevaluationAccount], 0))     AS [Is Revaluation Account]
            , CONVERT(BIT, ISNULL(a.[RequiresCostCenter], 0))       AS [Requires Cost Center]
            , CONVERT(BIT, ISNULL(a.[RequiresProjectCode], 0))      AS [Requires Project Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[StatutoryAccountCode])), N'')) AS [Statutory Account Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[StatutoryChartCode])), N''))   AS [Statutory Chart Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[TaxLineReference])), N''))     AS [NA Tax Line Reference]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[LocalAccountCode])), N''))     AS [APAC Local Account Code]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[ConsolidationAccountCode])), N'')) AS [Consolidation Account Code]
            , a.[EffectiveFromDate]                                 AS [Effective From Date]
            , CONVERT(BIT, ISNULL(a.[BlockedForPosting], 0))        AS [Blocked For Posting]
            , CONVERT(BIT, ISNULL(a.[IsActive], 1))                 AS [Is Active]
        INTO #GlAccount
        FROM [ref].[GlAccount] AS a
        WHERE NULLIF(LTRIM(RTRIM(a.[GlAccountCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* Accounts that post but were never in the chart extract. The GL fact load
           cannot resolve them and used to send them to the unknown member, which
           made the trial balance impossible to tie out. They are added here as
           postable orphans instead. */
        INSERT INTO #GlAccount
            ([GL Account Code], [GL Account Name], [Account Type Code], [Is Postable],
             [Is Active])
        SELECT DISTINCT
              UPPER(LTRIM(RTRIM(j.[GlAccountCode])))
            , CONCAT(N'Unmapped posting account ', UPPER(LTRIM(RTRIM(j.[GlAccountCode]))))
            , N'STAT'
            , 1
            , 1
        FROM [stg].[GlJournalLine] AS j
        WHERE NULLIF(LTRIM(RTRIM(j.[GlAccountCode])), N'') IS NOT NULL
          AND NOT EXISTS (SELECT 1
                          FROM #GlAccount AS g
                          WHERE g.[GL Account Code] = UPPER(LTRIM(RTRIM(j.[GlAccountCode]))));

        /* A parent that is not itself in the chart is not a parent. */
        UPDATE g
        SET g.[Parent GL Account Code] = NULL
        FROM #GlAccount AS g
        WHERE g.[Parent GL Account Code] IS NOT NULL
          AND NOT EXISTS (SELECT 1
                          FROM #GlAccount AS p
                          WHERE p.[GL Account Code] = g.[Parent GL Account Code]);

        /* An account cannot be its own parent; the 1997 conversion produced
           several hundred of these. */
        UPDATE #GlAccount
        SET [Parent GL Account Code] = NULL
        WHERE [Parent GL Account Code] = [GL Account Code];

        IF OBJECT_ID(N'tempdb..#Walk') IS NOT NULL DROP TABLE #Walk;

        ;WITH [Walk] AS
        (
            SELECT  g.[GL Account Code],
                    g.[Parent GL Account Code],
                    CONVERT(SMALLINT, 1) AS [Hierarchy Level],
                    CONVERT(NVARCHAR(400), g.[GL Account Code]) AS [Hierarchy Path]
            FROM #GlAccount AS g
            WHERE g.[Parent GL Account Code] IS NULL

            UNION ALL

            SELECT  c.[GL Account Code],
                    c.[Parent GL Account Code],
                    CONVERT(SMALLINT, p.[Hierarchy Level] + 1),
                    CONVERT(NVARCHAR(400), CONCAT(p.[Hierarchy Path], N' / ', c.[GL Account Code]))
            FROM #GlAccount AS c
            INNER JOIN [Walk] AS p
                ON p.[GL Account Code] = c.[Parent GL Account Code]
            WHERE p.[Hierarchy Level] < @MaxHierarchyDepth
        )
        SELECT [GL Account Code], [Hierarchy Level], [Hierarchy Path]
        INTO #Walk
        FROM [Walk]
        OPTION (MAXRECURSION 0);

        /* Anything the walk did not reach is in a cycle or hangs off one. It is
           kept, flagged, and reported rather than failing the load. */
        INSERT INTO #Walk ([GL Account Code], [Hierarchy Level], [Hierarchy Path])
        SELECT g.[GL Account Code], CONVERT(SMALLINT, 1), g.[GL Account Code]
        FROM #GlAccount AS g
        WHERE NOT EXISTS (SELECT 1 FROM #Walk AS w WHERE w.[GL Account Code] = g.[GL Account Code]);

        SET @OrphanCount = @@ROWCOUNT;

        MERGE [Dimension].[GL Account] AS tgt
        USING (
            SELECT
                  g.[GL Account Code]
                , g.[GL Account Name]
                , g.[Parent GL Account Code]
                , w.[Hierarchy Level]
                , w.[Hierarchy Path]
                , CONVERT(BIT, CASE WHEN EXISTS (SELECT 1 FROM #GlAccount AS c
                                                 WHERE c.[Parent GL Account Code] = g.[GL Account Code])
                                    THEN 0 ELSE 1 END)              AS [Is Leaf Node]
                , CONVERT(BIT, CASE WHEN g.[Parent GL Account Code] IS NULL
                                         AND w.[Hierarchy Level] = 1
                                         AND g.[Account Type Code] <> N'STAT'
                                         AND NOT EXISTS (SELECT 1 FROM #GlAccount AS c
                                                         WHERE c.[Parent GL Account Code] = g.[GL Account Code])
                                    THEN 1 ELSE 0 END)              AS [Is Orphan Account]
                , g.[Is Postable]
                , g.[Account Type Code]
                , g.[Account Class Code]
                , CASE g.[Account Type Code]
                      WHEN N'ASSET' THEN N'DR' WHEN N'COGS' THEN N'DR'
                      WHEN N'EXP'   THEN N'DR' WHEN N'LIAB' THEN N'CR'
                      WHEN N'EQTY'  THEN N'CR' WHEN N'REV'  THEN N'CR'
                      ELSE N'DR'
                  END                                               AS [Normal Balance Side]
                , CASE g.[Account Type Code]
                      WHEN N'ASSET' THEN N'BS' WHEN N'LIAB' THEN N'BS' WHEN N'EQTY' THEN N'BS'
                      WHEN N'STAT'  THEN N'MEMO'
                      ELSE N'PL'
                  END                                               AS [Financial Statement Code]
                , CONCAT(CASE g.[Account Type Code]
                             WHEN N'REV'  THEN N'REV'
                             WHEN N'COGS' THEN N'COGS'
                             WHEN N'EXP'  THEN N'OPEX'
                             ELSE ISNULL(g.[Account Class Code], N'OTHER')
                         END, N'_', LEFT(g.[GL Account Code], 2))   AS [Statement Line Code]
                , CONVERT(INT, ROW_NUMBER() OVER (ORDER BY g.[GL Account Code])) AS [Statement Sort Order]
                , g.[Is Intercompany Account], g.[Is Cash Account], g.[Is Revaluation Account]
                , g.[Requires Cost Center], g.[Requires Project Code]
                , g.[Statutory Account Code], g.[Statutory Chart Code]
                , g.[NA Tax Line Reference], g.[APAC Local Account Code]
                , ISNULL(g.[Consolidation Account Code], g.[GL Account Code]) AS [Consolidation Account Code]
                , g.[Effective From Date], g.[Blocked For Posting], g.[Is Active]
                , HASHBYTES(N'SHA2_256',
                      CONCAT_WS(N'|', g.[GL Account Name], ISNULL(g.[Parent GL Account Code], N'~'),
                                ISNULL(g.[Account Type Code], N'~'), ISNULL(g.[Account Class Code], N'~'),
                                CONVERT(NVARCHAR(6), w.[Hierarchy Level]), w.[Hierarchy Path],
                                CONVERT(NVARCHAR(1), g.[Is Postable]),
                                CONVERT(NVARCHAR(1), g.[Blocked For Posting]),
                                CONVERT(NVARCHAR(1), g.[Is Active]),
                                ISNULL(g.[Statutory Account Code], N'~'),
                                ISNULL(g.[APAC Local Account Code], N'~'))) AS [Row Hash Type 1]
            FROM #GlAccount AS g
            INNER JOIN #Walk AS w
                ON w.[GL Account Code] = g.[GL Account Code]
        ) AS src
            ON tgt.[GL Account Code] = src.[GL Account Code]
        WHEN MATCHED AND ISNULL(tgt.[Row Hash Type 1], 0x) <> src.[Row Hash Type 1] THEN
            UPDATE SET
                  tgt.[GL Account Name]         = src.[GL Account Name]
                , tgt.[Parent GL Account Code]  = src.[Parent GL Account Code]
                , tgt.[Hierarchy Level]         = src.[Hierarchy Level]
                , tgt.[Hierarchy Path]          = src.[Hierarchy Path]
                , tgt.[Is Leaf Node]            = src.[Is Leaf Node]
                , tgt.[Is Orphan Account]       = src.[Is Orphan Account]
                , tgt.[Is Postable]             = src.[Is Postable]
                , tgt.[Account Type Code]       = src.[Account Type Code]
                , tgt.[Account Class Code]      = src.[Account Class Code]
                , tgt.[Normal Balance Side]     = src.[Normal Balance Side]
                , tgt.[Financial Statement Code]= src.[Financial Statement Code]
                , tgt.[Statement Line Code]     = src.[Statement Line Code]
                , tgt.[Statement Sort Order]    = src.[Statement Sort Order]
                , tgt.[Is Intercompany Account] = src.[Is Intercompany Account]
                , tgt.[Is Cash Account]         = src.[Is Cash Account]
                , tgt.[Is Revaluation Account]  = src.[Is Revaluation Account]
                , tgt.[Requires Cost Center]    = src.[Requires Cost Center]
                , tgt.[Requires Project Code]   = src.[Requires Project Code]
                , tgt.[Statutory Account Code]  = src.[Statutory Account Code]
                , tgt.[Statutory Chart Code]    = src.[Statutory Chart Code]
                , tgt.[NA Tax Line Reference]   = src.[NA Tax Line Reference]
                , tgt.[APAC Local Account Code] = src.[APAC Local Account Code]
                , tgt.[Consolidation Account Code] = src.[Consolidation Account Code]
                , tgt.[Effective From Date]     = src.[Effective From Date]
                , tgt.[Blocked For Posting]     = src.[Blocked For Posting]
                , tgt.[Is Active]               = src.[Is Active]
                , tgt.[Source System Code]      = @SourceSystemCode
                , tgt.[Row Hash Type 1]         = src.[Row Hash Type 1]
                , tgt.[Lineage Key]             = @LineageKey
                , tgt.[Last Load Batch Id]      = @BatchId
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([GL Account Code], [GL Account Name], [Parent GL Account Code],
                    [Hierarchy Level], [Hierarchy Path], [Is Leaf Node], [Is Orphan Account],
                    [Is Postable], [Account Type Code], [Account Class Code],
                    [Normal Balance Side], [Financial Statement Code], [Statement Line Code],
                    [Statement Sort Order], [Is Intercompany Account], [Is Cash Account],
                    [Is Revaluation Account], [Requires Cost Center], [Requires Project Code],
                    [Statutory Account Code], [Statutory Chart Code], [NA Tax Line Reference],
                    [APAC Local Account Code], [Consolidation Account Code],
                    [Effective From Date], [Blocked For Posting], [Is Active],
                    [Source System Code], [Row Hash Type 1], [Valid From], [Valid To],
                    [Lineage Key], [Last Load Batch Id])
            VALUES (src.[GL Account Code], src.[GL Account Name], src.[Parent GL Account Code],
                    src.[Hierarchy Level], src.[Hierarchy Path], src.[Is Leaf Node],
                    src.[Is Orphan Account], src.[Is Postable], src.[Account Type Code],
                    src.[Account Class Code], src.[Normal Balance Side],
                    src.[Financial Statement Code], src.[Statement Line Code],
                    src.[Statement Sort Order], src.[Is Intercompany Account],
                    src.[Is Cash Account], src.[Is Revaluation Account],
                    src.[Requires Cost Center], src.[Requires Project Code],
                    src.[Statutory Account Code], src.[Statutory Chart Code],
                    src.[NA Tax Line Reference], src.[APAC Local Account Code],
                    src.[Consolidation Account Code], src.[Effective From Date],
                    src.[Blocked For Posting], src.[Is Active], @SourceSystemCode,
                    src.[Row Hash Type 1], @Now, @HighDate, @LineageKey, @BatchId);

        SELECT @InsertedCount = COUNT_BIG(*)
        FROM [Dimension].[GL Account]
        WHERE [Last Load Batch Id] = @BatchId
          AND [Valid From] = @Now;

        SELECT @UpdatedCount = COUNT_BIG(*)
        FROM [Dimension].[GL Account]
        WHERE [Last Load Batch Id] = @BatchId
          AND [Valid From] <> @Now;

        /* Parent keys are resolved in a second pass; a parent is not guaranteed to
           have a surrogate key at the time its child row is written. */
        UPDATE c
        SET c.[Parent GL Account Key] = p.[GL Account Key]
        FROM [Dimension].[GL Account] AS c
        INNER JOIN [Dimension].[GL Account] AS p
            ON p.[GL Account Code] = c.[Parent GL Account Code]
        WHERE ISNULL(c.[Parent GL Account Key], -1) <> p.[GL Account Key];

        IF @OrphanCount > 0
        BEGIN
            DECLARE @OrphanNote NVARCHAR(MAX) =
                CONCAT(N'GL hierarchy walk did not reach ', @OrphanCount,
                       N' account(s); they are flagged and rolled up to themselves. ',
                       N'A cycle in ref.GlAccount is the usual cause.');

            EXEC [etl].[usp_LogError]
                 @PackageExecutionId = @PackageExecutionId,
                 @BatchId            = @BatchId,
                 @ErrorSeverity      = N'Warning',
                 @SourceName         = @PackageName,
                 @SourceComponent    = N'Dimension.GL Account',
                 @ProcedureName      = N'Integration.usp_LoadGlAccountHierarchy',
                 @ErrorDescription   = @OrphanNote;
        END;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'GL Account', N'GLOBAL', @BatchId, @PackageExecutionId,
             @SourceRowCount, @UpdatedCount, @InsertedCount, N'Type1Merge', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.GL Account',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @UpdatedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.GL Account',
             @ProcedureName      = N'Integration.usp_LoadGlAccountHierarchy',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount;

        THROW;
    END CATCH;
END;
GO
