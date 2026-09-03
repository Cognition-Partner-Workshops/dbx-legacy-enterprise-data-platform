/*
    Object        : [Integration].[usp_LoadProductHierarchy]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.ProductHierarchyNode, Dimension.Product Hierarchy,
                    Dimension.Product Category, the etl control framework
    Called by     : DIM_Load_ProductHierarchy

    The recursive/ragged hierarchy of the estate. Nodes point at their parent by
    code; the depth is not uniform because the NA merchandising tree has six
    levels, the EU tree four, and the APAC tree three with regional extension
    nodes hung off level two. Cubes cannot consume a parent-child hierarchy, so
    the six flattened level columns are rebuilt on every run and the levels below
    a node's own depth repeat the leaf - the standard ragged-flattening trick that
    keeps the cube from showing empty members.

    The load is a full rebuild of the structural columns in place. Surrogate keys
    are preserved because the stock item dimension points at them; a node that
    disappears from the extract is deactivated, not deleted.

    Cycle protection: the recursion is capped and any node still unresolved after
    the cap is logged as a reject and left attached to the unknown parent. The
    2015 incident where SEASONAL pointed at CHRISTMAS and CHRISTMAS pointed at
    SEASONAL is the reason the cap exists.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadProductHierarchy]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_ProductHierarchy',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_MDM',
    @MaximumDepth       INT           = 6,
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
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#HierarchySource') IS NOT NULL
            DROP TABLE #HierarchySource;
        IF OBJECT_ID(N'tempdb..#HierarchyResolved') IS NOT NULL
            DROP TABLE #HierarchyResolved;

        SELECT
              n.[NodeCode]              AS [Hierarchy Node Code]
            , n.[NodeName]              AS [Hierarchy Node Name]
            , n.[ParentNodeCode]        AS [Parent Node Code]
            , n.[NodeTypeCode]          AS [Node Type Code]
            , n.[OwningRegionCode]      AS [Owning Region Code]
            , n.[CategoryCode]          AS [Category Code]
            , n.[SortOrder]             AS [Hierarchy Sort Order]
            , n.[IsRegionalExtension]   AS [Is Regional Extension]
        INTO #HierarchySource
        FROM [ref].[ProductHierarchyNode] AS n
        WHERE NULLIF(LTRIM(RTRIM(n.[NodeCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A parent that is not itself a node in the extract cannot be walked. */
        UPDATE s
        SET s.[Parent Node Code] = NULL
        FROM #HierarchySource AS s
        WHERE s.[Parent Node Code] IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM #HierarchySource AS p
                          WHERE p.[Hierarchy Node Code] = s.[Parent Node Code]);

        CREATE TABLE #HierarchyResolved
        (
            [Hierarchy Node Code]   NVARCHAR(30)    NOT NULL,
            [Hierarchy Path]        NVARCHAR(400)   NOT NULL,
            [Hierarchy Level]       INT             NOT NULL,
            [Level 1 Code]          NVARCHAR(30)    NULL,
            [Level 2 Code]          NVARCHAR(30)    NULL,
            [Level 3 Code]          NVARCHAR(30)    NULL,
            [Level 4 Code]          NVARCHAR(30)    NULL,
            [Level 5 Code]          NVARCHAR(30)    NULL,
            [Level 6 Code]          NVARCHAR(30)    NULL
        );

        /*
            Iterative walk rather than a recursive CTE. The 2007 author wrote it
            this way because the CTE plan spilled on the full merchandising tree
            and because the iteration makes the cycle cap explicit.
        */
        DECLARE @Depth INT = 1;

        INSERT INTO #HierarchyResolved
            ([Hierarchy Node Code], [Hierarchy Path], [Hierarchy Level], [Level 1 Code])
        SELECT s.[Hierarchy Node Code], N'\' + s.[Hierarchy Node Code], 1, s.[Hierarchy Node Code]
        FROM #HierarchySource AS s
        WHERE s.[Parent Node Code] IS NULL;

        WHILE @Depth < @MaximumDepth
        BEGIN
            INSERT INTO #HierarchyResolved
                ([Hierarchy Node Code], [Hierarchy Path], [Hierarchy Level],
                 [Level 1 Code], [Level 2 Code], [Level 3 Code],
                 [Level 4 Code], [Level 5 Code], [Level 6 Code])
            SELECT
                  c.[Hierarchy Node Code]
                , p.[Hierarchy Path] + N'\' + c.[Hierarchy Node Code]
                , p.[Hierarchy Level] + 1
                , p.[Level 1 Code]
                , CASE WHEN p.[Hierarchy Level] + 1 = 2 THEN c.[Hierarchy Node Code] ELSE p.[Level 2 Code] END
                , CASE WHEN p.[Hierarchy Level] + 1 = 3 THEN c.[Hierarchy Node Code] ELSE p.[Level 3 Code] END
                , CASE WHEN p.[Hierarchy Level] + 1 = 4 THEN c.[Hierarchy Node Code] ELSE p.[Level 4 Code] END
                , CASE WHEN p.[Hierarchy Level] + 1 = 5 THEN c.[Hierarchy Node Code] ELSE p.[Level 5 Code] END
                , CASE WHEN p.[Hierarchy Level] + 1 = 6 THEN c.[Hierarchy Node Code] ELSE p.[Level 6 Code] END
            FROM #HierarchySource AS c
            INNER JOIN #HierarchyResolved AS p
                ON p.[Hierarchy Node Code] = c.[Parent Node Code]
            WHERE p.[Hierarchy Level] = @Depth
              AND NOT EXISTS (SELECT 1 FROM #HierarchyResolved AS r
                              WHERE r.[Hierarchy Node Code] = c.[Hierarchy Node Code]);

            SET @Depth = @Depth + 1;
        END;

        /* Anything unresolved after the cap is in a cycle or is too deep. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Product Hierarchy',
               s.[Hierarchy Node Code], N'UNRESOLVED_NODE',
               N'Node could not be placed within the maximum depth; cycle or over-deep branch.',
               N'Dimension', CONCAT(N'Parent=', s.[Parent Node Code], N'; Type=', s.[Node Type Code])
        FROM #HierarchySource AS s
        WHERE NOT EXISTS (SELECT 1 FROM #HierarchyResolved AS r
                          WHERE r.[Hierarchy Node Code] = s.[Hierarchy Node Code]);

        SET @RejectCount = @@ROWCOUNT;

        /* Ragged flattening: repeat the deepest code down the empty levels. */
        UPDATE #HierarchyResolved
        SET [Level 2 Code] = ISNULL([Level 2 Code], [Level 1 Code]);
        UPDATE #HierarchyResolved
        SET [Level 3 Code] = ISNULL([Level 3 Code], [Level 2 Code]);
        UPDATE #HierarchyResolved
        SET [Level 4 Code] = ISNULL([Level 4 Code], [Level 3 Code]);
        UPDATE #HierarchyResolved
        SET [Level 5 Code] = ISNULL([Level 5 Code], [Level 4 Code]);
        UPDATE #HierarchyResolved
        SET [Level 6 Code] = ISNULL([Level 6 Code], [Level 5 Code]);

        UPDATE [Dimension].[Product Hierarchy]
        SET [Is Active]          = 0,
            [Last Load Batch Id] = @BatchId
        WHERE [Product Hierarchy Key] > 0;

        INSERT INTO [Dimension].[Product Hierarchy]
            ([Hierarchy Node Code], [Hierarchy Node Name], [Parent Node Code], [Node Type Code],
             [Hierarchy Level], [Owning Region Code], [Product Category Key], [Hierarchy Sort Order],
             [Is Regional Extension], [Is Active], [Source System Code], [Valid From], [Valid To],
             [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[Hierarchy Node Code]
            , ISNULL(s.[Hierarchy Node Name], s.[Hierarchy Node Code])
            , s.[Parent Node Code]
            , s.[Node Type Code]
            /* unresolved nodes park at level 1 until procurement fixes the parent */
            , CONVERT(SMALLINT, ISNULL(r.[Hierarchy Level], 1))
            , s.[Owning Region Code]
            , ISNULL(cat.[Product Category Key], -1)
            , s.[Hierarchy Sort Order]
            , ISNULL(s.[Is Regional Extension], 0)
            , 1
            , @SourceSystemCode
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #HierarchySource AS s
        LEFT OUTER JOIN #HierarchyResolved AS r
            ON r.[Hierarchy Node Code] = s.[Hierarchy Node Code]
        LEFT OUTER JOIN [Dimension].[Product Category] AS cat
            ON cat.[Category Code] = s.[Category Code]
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Product Hierarchy] AS d
            WHERE d.[Hierarchy Node Code] = s.[Hierarchy Node Code]
        );

        SET @InsertedCount = @@ROWCOUNT;

        UPDATE d
        SET d.[Hierarchy Node Name]    = ISNULL(s.[Hierarchy Node Name], d.[Hierarchy Node Name]),
            d.[Parent Node Code]       = s.[Parent Node Code],
            d.[Node Type Code]         = s.[Node Type Code],
            d.[Owning Region Code]     = s.[Owning Region Code],
            d.[Hierarchy Sort Order]   = s.[Hierarchy Sort Order],
            d.[Is Regional Extension]  = ISNULL(s.[Is Regional Extension], 0),
            d.[Hierarchy Path]         = r.[Hierarchy Path],
            d.[Hierarchy Level]        = CONVERT(SMALLINT, ISNULL(r.[Hierarchy Level], d.[Hierarchy Level])),
            d.[Level 1 Code]           = r.[Level 1 Code],
            d.[Level 2 Code]           = r.[Level 2 Code],
            d.[Level 3 Code]           = r.[Level 3 Code],
            d.[Level 4 Code]           = r.[Level 4 Code],
            d.[Level 5 Code]           = r.[Level 5 Code],
            d.[Level 6 Code]           = r.[Level 6 Code],
            d.[Is Ragged Branch]       = CASE WHEN ISNULL(r.[Hierarchy Level], 1) < @MaximumDepth THEN 1 ELSE 0 END,
            d.[Is Active]              = 1,
            d.[Last Load Batch Id]     = @BatchId
        FROM [Dimension].[Product Hierarchy] AS d
        INNER JOIN #HierarchySource AS s
            ON s.[Hierarchy Node Code] = d.[Hierarchy Node Code]
        LEFT OUTER JOIN #HierarchyResolved AS r
            ON r.[Hierarchy Node Code] = s.[Hierarchy Node Code]
        WHERE d.[Product Hierarchy Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        /* Parent keys and leaf flags, once every node has a key. */
        UPDATE d
        SET d.[Parent Product Hierarchy Key] = ISNULL(p.[Product Hierarchy Key], -2)
        FROM [Dimension].[Product Hierarchy] AS d
        LEFT OUTER JOIN [Dimension].[Product Hierarchy] AS p
            ON p.[Hierarchy Node Code] = d.[Parent Node Code]
        WHERE d.[Product Hierarchy Key] > 0;

        UPDATE d
        SET d.[Is Leaf Node]          = CASE WHEN kids.[Child Count] = 0 THEN 1 ELSE 0 END,
            d.[Leaf Descendant Count] = kids.[Child Count]
        FROM [Dimension].[Product Hierarchy] AS d
        CROSS APPLY
        (
            SELECT COUNT_BIG(*) AS [Child Count]
            FROM [Dimension].[Product Hierarchy] AS c
            WHERE c.[Parent Node Code] = d.[Hierarchy Node Code]
              AND c.[Is Active]        = 1
        ) AS kids
        WHERE d.[Product Hierarchy Key] > 0;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Product Hierarchy', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'HierarchyRebuild', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Product Hierarchy',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @UpdatedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Product Hierarchy',
             @ProcedureName      = N'Integration.usp_LoadProductHierarchy',
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
