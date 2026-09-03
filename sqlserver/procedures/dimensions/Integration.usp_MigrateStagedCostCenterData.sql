/*
    Object        : [Integration].[usp_MigrateStagedCostCenterData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.CostCenter, Dimension.Cost Center, Dimension.Region,
                    the etl control framework
    Called by     : REF_Load_CostCenter (weekly), and by the finance close
                    orchestration when a cost centre is opened mid-period

    Type 2 with a hand-rolled close-out, followed by an iterative rollup pass
    that flattens the cost centre hierarchy into six fixed levels. The hierarchy
    is ragged - NA runs four levels, EU six, APAC three - so the deepest
    populated level is repeated downwards, which is what the finance cube
    expects and what every report written since 2010 assumes.

    Same-day changes are ordered by the source change timestamp and given an
    effective sequence, because the Oracle GL sends a cost centre twice on the
    day a budget is reallocated and the second row must win.

    Regional divergence: EU carries the statutory chart of accounts code (the
    local GAAP mapping), APAC carries the intercompany partner code used for
    the transfer pricing entries, NA carries the departmental analysis code that
    the 1999 general ledger still emits.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCostCenterData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'REF_Load_CostCenter',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_GL',
    @MaximumDepth       INT           = 6,
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @Today              DATE         = CONVERT(DATE, SYSDATETIME());
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @ClosedCount        BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @SameDayCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @Depth              INT    = 1;
    DECLARE @Affected           INT    = 1;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#CostCenterSource') IS NOT NULL DROP TABLE #CostCenterSource;

        SELECT
              UPPER(LTRIM(RTRIM(c.[CostCenterCode])))       AS [Cost Center Code]
            , c.[CostCenterName]                            AS [Cost Center Name]
            , UPPER(NULLIF(LTRIM(RTRIM(c.[ParentCostCenterCode])), N'')) AS [Parent Cost Center Code]
            , UPPER(ISNULL(c.[CostCenterTypeCode], N'COST')) AS [Cost Center Type Code]
            , UPPER(c.[FunctionCode])                       AS [Function Code]
            , UPPER(c.[CompanyCode])                        AS [Company Code]
            , UPPER(c.[LegalEntityCode])                    AS [Legal Entity Code]
            , UPPER(ISNULL(c.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , UPPER(c.[CountryCode])                        AS [Country Code]
            , UPPER(c.[FunctionalCurrencyCode])             AS [Functional Currency Code]
            , c.[ManagerEmployeeNumber]                     AS [Manager Employee Number]
            , c.[BudgetOwnerEmployeeNumber]                 AS [Budget Owner Employee Number]
            , c.[AnnualBudgetAmount]                        AS [Annual Budget Amount]
            , CONVERT(SMALLINT, c.[BudgetFiscalYear])       AS [Budget Fiscal Year]
            , UPPER(ISNULL(c.[AllocationMethodCode], N'NONE')) AS [Allocation Method Code]
            , c.[AllocationPercentage]                      AS [Allocation Percentage]
            , c.[EuStatutoryChartCode]                      AS [EU Statutory Chart Code]
            , c.[ApacIntercompanyPartnerCode]               AS [APAC Intercompany Partner Code]
            , c.[NaDepartmentAnalysisCode]                  AS [NA Department Analysis Code]
            , ISNULL(c.[IsActive], 1)                       AS [Is Active]
            , ISNULL(c.[BlockedForPosting], 0)              AS [Blocked For Posting]
            , ISNULL(c.[SourceChangedOn], @Now)             AS [Source Changed On]
            , CONVERT(SMALLINT, 1)                          AS [Effective Sequence]
        INTO #CostCenterSource
        FROM [stg].[CostCenter] AS c
        WHERE NULLIF(LTRIM(RTRIM(c.[CostCenterCode])), N'') IS NOT NULL
          AND (@RegionCode = N'GLOBAL' OR UPPER(ISNULL(c.[RegionCode], N'GLOBAL')) = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* Same-day sequencing. The GL sends the reallocation twice; the later
           change timestamp wins and the earlier one is kept as sequence 1 so the
           close-out ordering is deterministic. */
        UPDATE s
        SET s.[Effective Sequence] = x.[Sequence Number]
        FROM #CostCenterSource AS s
        INNER JOIN (
            SELECT [Cost Center Code], [Source Changed On],
                   CONVERT(SMALLINT, ROW_NUMBER() OVER (PARTITION BY [Cost Center Code]
                                                        ORDER BY [Source Changed On])) AS [Sequence Number]
            FROM #CostCenterSource
        ) AS x
            ON  x.[Cost Center Code]  = s.[Cost Center Code]
            AND x.[Source Changed On] = s.[Source Changed On];

        SET @SameDayCount = (SELECT COUNT_BIG(*) FROM #CostCenterSource WHERE [Effective Sequence] > 1);

        /* Allocation percentages that do not sum to 100 within a parent leave
           unallocated overhead sitting in the parent. Reported, never blocked. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Cost Center',
               s.[Parent Cost Center Code], N'ALLOCATION_NOT_100',
               N'Child allocation percentages do not sum to 100 for the parent cost centre.',
               N'Reference', CONCAT(N'Sum=', CONVERT(NVARCHAR(20), s.[Allocated Total]))
        FROM (
            SELECT [Parent Cost Center Code], SUM(ISNULL([Allocation Percentage], 0)) AS [Allocated Total]
            FROM #CostCenterSource
            WHERE [Parent Cost Center Code] IS NOT NULL
              AND [Allocation Method Code] = N'PERCENT'
            GROUP BY [Parent Cost Center Code]
        ) AS s
        WHERE ABS(s.[Allocated Total] - 100) > 0.01;

        SET @RejectCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           Type 2 close-out, written by hand. Only the attributes finance
           agreed to version are in the hash: name, parent, manager, budget,
           allocation and the posting block.
           --------------------------------------------------------------- */
        IF OBJECT_ID(N'tempdb..#CostCenterChanged') IS NOT NULL DROP TABLE #CostCenterChanged;

        SELECT s.*
             , HASHBYTES(N'SHA2_256',
                 CONCAT_WS(N'|', ISNULL(s.[Cost Center Name], N''),
                           ISNULL(s.[Parent Cost Center Code], N''),
                           ISNULL(s.[Manager Employee Number], N''),
                           ISNULL(CONVERT(NVARCHAR(30), s.[Annual Budget Amount]), N''),
                           ISNULL(s.[Allocation Method Code], N''),
                           ISNULL(CONVERT(NVARCHAR(12), s.[Allocation Percentage]), N''),
                           ISNULL(CONVERT(NVARCHAR(1), s.[Blocked For Posting]), N''),
                           ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))) AS [Row Hash Type 2]
        INTO #CostCenterChanged
        FROM #CostCenterSource AS s;

        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = @Now,
            d.[Valid To]           = @Now,
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Cost Center] AS d
        INNER JOIN #CostCenterChanged AS s
            ON s.[Cost Center Code] = d.[Cost Center Code]
        WHERE d.[Cost Center Key] > 0
          AND d.[Is Current Row]   = 1
          AND (d.[Row Hash Type 2] IS NULL OR d.[Row Hash Type 2] <> s.[Row Hash Type 2]);

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Cost Center]
            ([Cost Center Code], [Cost Center Name], [Parent Cost Center Code],
             [Cost Center Type Code], [Function Code], [Company Code], [Legal Entity Code],
             [Region Key], [Region Code], [Country Code], [Functional Currency Code],
             [Manager Employee Number], [Budget Owner Employee Number], [Annual Budget Amount],
             [Budget Fiscal Year], [Allocation Method Code], [Allocation Percentage],
             [EU Statutory Chart Code], [APAC Intercompany Partner Code],
             [NA Department Analysis Code], [Is Active], [Blocked For Posting],
             [Source System Code], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[Cost Center Code]
            , ISNULL(s.[Cost Center Name], s.[Cost Center Code])
            , s.[Parent Cost Center Code]
            , s.[Cost Center Type Code]
            , s.[Function Code]
            , s.[Company Code]
            , s.[Legal Entity Code]
            , r.[Region Key]
            , s.[Region Code]
            , s.[Country Code]
            , ISNULL(s.[Functional Currency Code], r.[Reporting Currency Code])
            , s.[Manager Employee Number]
            , ISNULL(s.[Budget Owner Employee Number], s.[Manager Employee Number])
            , s.[Annual Budget Amount]
            , s.[Budget Fiscal Year]
            , s.[Allocation Method Code]
            , s.[Allocation Percentage]
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[EU Statutory Chart Code] END
            , CASE WHEN s.[Region Code] = N'APAC' THEN s.[APAC Intercompany Partner Code] END
            , CASE WHEN s.[Region Code] = N'NA'   THEN s.[NA Department Analysis Code] END
            , s.[Is Active]
            , s.[Blocked For Posting]
            , @SourceSystemCode
            , @Now
            , @HighDate
            , @Today
            , s.[Effective Sequence]
            , 1
            , ISNULL((SELECT MAX(p.[Version Number]) + 1
                      FROM [Dimension].[Cost Center] AS p
                      WHERE p.[Cost Center Code] = s.[Cost Center Code]
                        AND p.[Cost Center Key]  > 0), 1)
            , s.[Row Hash Type 2]
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #CostCenterChanged AS s
        LEFT OUTER JOIN [Dimension].[Region] AS r
            ON r.[Region Code] = s.[Region Code]
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Cost Center] AS d
                          WHERE d.[Cost Center Code] = s.[Cost Center Code]
                            AND d.[Cost Center Key]  > 0
                            AND d.[Is Current Row]   = 1
                            AND d.[Row Hash Type 2]  = s.[Row Hash Type 2]);

        SET @InsertedCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           Parent keys and the flattened rollup. Iterative, not a recursive
           CTE - the 2010 author did not trust MAXRECURSION after an incident
           with a self-parented cost centre.
           --------------------------------------------------------------- */
        UPDATE d
        SET d.[Parent Cost Center Key] = p.[Cost Center Key]
        FROM [Dimension].[Cost Center] AS d
        INNER JOIN [Dimension].[Cost Center] AS p
            ON  p.[Cost Center Code] = d.[Parent Cost Center Code]
            AND p.[Is Current Row]   = 1
            AND p.[Cost Center Key]  > 0
        WHERE d.[Is Current Row]  = 1
          AND d.[Cost Center Key] > 0;

        UPDATE [Dimension].[Cost Center]
        SET [Hierarchy Level]     = 1,
            [Hierarchy Path]      = N'/' + [Cost Center Code],
            [Rollup Level 1 Code] = [Cost Center Code],
            [Rollup Level 2 Code] = NULL,
            [Rollup Level 3 Code] = NULL,
            [Rollup Level 4 Code] = NULL,
            [Rollup Level 5 Code] = NULL,
            [Rollup Level 6 Code] = NULL
        WHERE [Is Current Row]         = 1
          AND [Cost Center Key]        > 0
          AND [Parent Cost Center Key] IS NULL;

        WHILE @Depth < @MaximumDepth AND @Affected > 0
        BEGIN
            UPDATE d
            SET d.[Hierarchy Level]     = CONVERT(SMALLINT, p.[Hierarchy Level] + 1),
                d.[Hierarchy Path]      = p.[Hierarchy Path] + N'/' + d.[Cost Center Code],
                d.[Rollup Level 1 Code] = p.[Rollup Level 1 Code],
                d.[Rollup Level 2 Code] = CASE WHEN p.[Hierarchy Level] + 1 = 2 THEN d.[Cost Center Code]
                                               ELSE p.[Rollup Level 2 Code] END,
                d.[Rollup Level 3 Code] = CASE WHEN p.[Hierarchy Level] + 1 = 3 THEN d.[Cost Center Code]
                                               ELSE p.[Rollup Level 3 Code] END,
                d.[Rollup Level 4 Code] = CASE WHEN p.[Hierarchy Level] + 1 = 4 THEN d.[Cost Center Code]
                                               ELSE p.[Rollup Level 4 Code] END,
                d.[Rollup Level 5 Code] = CASE WHEN p.[Hierarchy Level] + 1 = 5 THEN d.[Cost Center Code]
                                               ELSE p.[Rollup Level 5 Code] END,
                d.[Rollup Level 6 Code] = CASE WHEN p.[Hierarchy Level] + 1 = 6 THEN d.[Cost Center Code]
                                               ELSE p.[Rollup Level 6 Code] END
            FROM [Dimension].[Cost Center] AS d
            INNER JOIN [Dimension].[Cost Center] AS p
                ON p.[Cost Center Key] = d.[Parent Cost Center Key]
            WHERE d.[Is Current Row]      = 1
              AND d.[Cost Center Key]     > 0
              AND p.[Hierarchy Level]     = @Depth
              AND (d.[Hierarchy Level] IS NULL OR d.[Hierarchy Level] <> @Depth + 1);

            SET @Affected = @@ROWCOUNT;
            SET @Depth    = @Depth + 1;
        END;

        /* Ragged branches: repeat the deepest populated code downwards so the
           finance cube always has six levels to slice on. */
        UPDATE [Dimension].[Cost Center]
        SET [Rollup Level 2 Code] = ISNULL([Rollup Level 2 Code], [Rollup Level 1 Code]),
            [Rollup Level 3 Code] = ISNULL([Rollup Level 3 Code], ISNULL([Rollup Level 2 Code], [Rollup Level 1 Code])),
            [Rollup Level 4 Code] = ISNULL([Rollup Level 4 Code], ISNULL([Rollup Level 3 Code], [Rollup Level 1 Code])),
            [Rollup Level 5 Code] = ISNULL([Rollup Level 5 Code], ISNULL([Rollup Level 4 Code], [Rollup Level 1 Code])),
            [Rollup Level 6 Code] = ISNULL([Rollup Level 6 Code], ISNULL([Rollup Level 5 Code], [Rollup Level 1 Code])),
            [Is Leaf Node]        = CASE
                                        WHEN EXISTS (SELECT 1
                                                     FROM [Dimension].[Cost Center] AS c
                                                     WHERE c.[Parent Cost Center Key] = [Dimension].[Cost Center].[Cost Center Key]
                                                       AND c.[Is Current Row] = 1)
                                        THEN 0 ELSE 1
                                    END
        WHERE [Is Current Row]  = 1
          AND [Cost Center Key] > 0;

        /* Cost centres left without a level after the traversal are cyclic. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Cost Center',
               d.[Cost Center Code], N'HIERARCHY_UNRESOLVED',
               N'Cost centre could not be placed in the hierarchy within the maximum depth; probable cycle.',
               N'Dimension', CONCAT(N'Parent=', d.[Parent Cost Center Code])
        FROM [Dimension].[Cost Center] AS d
        WHERE d.[Is Current Row]  = 1
          AND d.[Cost Center Key] > 0
          AND d.[Hierarchy Level] IS NULL;

        SET @RejectCount = @RejectCount + @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Same Day Change Count], [Reject Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Cost Center', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @SameDayCount, @RejectCount, N'Type2Hierarchy',
             @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Cost Center',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @ClosedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @ClosedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Cost Center',
             @ProcedureName      = N'Integration.usp_MigrateStagedCostCenterData',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount,
             @RowsRejected       = @RejectCount;

        THROW;
    END CATCH;
END;
GO
