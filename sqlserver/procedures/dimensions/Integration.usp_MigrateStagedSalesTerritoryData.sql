/*
    Object        : [Integration].[usp_MigrateStagedSalesTerritoryData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.SalesTerritory, Dimension.Sales Territory, Dimension.Region,
                    Dimension.Employee Territory Bridge, the etl control framework
    Called by     : DIM_Load_SalesTerritory

    Type 1 with an alignment year. Territories are re-aligned every January and
    the business explicitly does not want history on the territory row itself -
    it wants the current alignment plus the alignment year, and the historical
    coverage is reconstructed from the fact rows. That decision was taken in 2009
    and has been regretted since, which is why the employee-to-territory bridge
    is Type 2 even though the territory itself is not.

    Coverage is expressed differently per region and stored in separate columns
    rather than a normalised coverage table, because each region's sales
    operations team maintains its own list in its own format:

      NA   - state list plus ZIP prefix ranges.
      EU   - country list plus NUTS level 2 codes.
      APAC - country list plus a named city list, because the territories are
             city-based in the larger markets and country-based elsewhere.

    A territory that appears in the extract but is missing from the current
    alignment year is retired rather than deleted.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedSalesTerritoryData]
    @BatchId            BIGINT,
    @AlignmentYear      SMALLINT      = NULL,
    @PackageName        NVARCHAR(200) = N'DIM_Load_SalesTerritory',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_SLS',
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
    DECLARE @RetiredCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    IF @AlignmentYear IS NULL
        SET @AlignmentYear = CONVERT(SMALLINT, YEAR(@Now));

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#TerritorySource') IS NOT NULL DROP TABLE #TerritorySource;

        SELECT
              UPPER(LTRIM(RTRIM(t.[SalesTerritoryCode])))   AS [Sales Territory Code]
            , t.[WWISalesTerritoryID]                       AS [WWI Sales Territory ID]
            , t.[SalesTerritoryName]                        AS [Sales Territory]
            , UPPER(ISNULL(t.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , CONVERT(SMALLINT, ISNULL(t.[AlignmentYear], @AlignmentYear)) AS [Alignment Year]
            , UPPER(NULLIF(LTRIM(RTRIM(t.[ParentTerritoryCode])), N'')) AS [Parent Territory Code]
            , UPPER(ISNULL(t.[TerritoryLevelCode], N'TERR')) AS [Territory Level Code]
            , t.[TerritoryManagerEmployeeNo]                AS [Territory Manager Employee No]
            , t.[SalesOfficeCode]                           AS [Sales Office Code]
            , UPPER(ISNULL(t.[CoverageModelCode], N'DIRECT')) AS [Coverage Model Code]
            , t.[CoverageList]                              AS [Coverage List]
            , t.[SecondaryCoverageList]                     AS [Secondary Coverage List]
            , t.[AnnualTargetAmount]                        AS [Annual Target Amount]
            , UPPER(t.[TargetCurrencyCode])                 AS [Target Currency Code]
            , CONVERT(SMALLINT, t.[TargetFiscalYear])       AS [Target Fiscal Year]
            , ISNULL(t.[IsActive], 1)                       AS [Is Active]
        INTO #TerritorySource
        FROM [stg].[SalesTerritory] AS t
        WHERE NULLIF(LTRIM(RTRIM(t.[SalesTerritoryCode])), N'') IS NOT NULL
          AND CONVERT(SMALLINT, ISNULL(t.[AlignmentYear], @AlignmentYear)) = @AlignmentYear;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A territory with a target but no manager has nobody to carry the quota.
           Sales operations get this list every January. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Sales Territory',
               s.[Sales Territory Code], N'TERRITORY_NO_MANAGER',
               N'Territory carries an annual target but has no manager employee number for the alignment year.',
               N'Dimension', CONCAT(N'Region=', s.[Region Code], N'|Target=',
                                    CONVERT(NVARCHAR(30), s.[Annual Target Amount]))
        FROM #TerritorySource AS s
        WHERE ISNULL(s.[Annual Target Amount], 0) > 0
          AND NULLIF(s.[Territory Manager Employee No], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* A parent territory code that is not in this alignment breaks the
           rollup, so it is dropped to NULL and the territory reports at region
           level for the year. The 2013 author left a note asking for this to be
           rejected instead; it never was. */
        UPDATE s
        SET s.[Parent Territory Code] = NULL
        FROM #TerritorySource AS s
        WHERE s.[Parent Territory Code] IS NOT NULL
          AND NOT EXISTS (SELECT 1
                          FROM #TerritorySource AS p
                          WHERE p.[Sales Territory Code] = s.[Parent Territory Code]);

        UPDATE d
        SET d.[Sales Territory]               = s.[Sales Territory],
            d.[Region Code]                   = s.[Region Code],
            d.[Region Key]                    = r.[Region Key],
            d.[Alignment Year]                = s.[Alignment Year],
            d.[Parent Territory Code]         = s.[Parent Territory Code],
            d.[Territory Level Code]          = s.[Territory Level Code],
            d.[Territory Manager Employee No] = s.[Territory Manager Employee No],
            d.[Sales Office Code]             = s.[Sales Office Code],
            d.[Coverage Model Code]           = s.[Coverage Model Code],
            d.[NA State Province List]        = CASE WHEN s.[Region Code] = N'NA' THEN s.[Coverage List] END,
            d.[NA ZIP Prefix List]            = CASE WHEN s.[Region Code] = N'NA' THEN s.[Secondary Coverage List] END,
            d.[EU Country List]               = CASE WHEN s.[Region Code] = N'EU' THEN s.[Coverage List] END,
            d.[EU NUTS Code List]             = CASE WHEN s.[Region Code] = N'EU' THEN s.[Secondary Coverage List] END,
            d.[APAC Country List]             = CASE WHEN s.[Region Code] = N'APAC' THEN s.[Coverage List] END,
            d.[APAC City List]                = CASE WHEN s.[Region Code] = N'APAC' THEN s.[Secondary Coverage List] END,
            d.[Annual Target Amount]          = s.[Annual Target Amount],
            d.[Target Currency Code]          = ISNULL(s.[Target Currency Code], r.[Reporting Currency Code]),
            d.[Target Fiscal Year]            = ISNULL(s.[Target Fiscal Year], s.[Alignment Year]),
            d.[Is Active]                     = s.[Is Active],
            d.[Retired On]                    = CASE WHEN s.[Is Active] = 0 THEN CONVERT(DATE, @Now) ELSE NULL END,
            d.[Source System Code]            = @SourceSystemCode,
            d.[Row Hash Type 1]               = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Sales Territory], N''),
                            ISNULL(s.[Parent Territory Code], N''),
                            ISNULL(s.[Territory Manager Employee No], N''),
                            ISNULL(s.[Coverage List], N''),
                            ISNULL(CONVERT(NVARCHAR(30), s.[Annual Target Amount]), N''),
                            ISNULL(CONVERT(NVARCHAR(6), s.[Alignment Year]), N''))),
            d.[Last Load Batch Id]            = @BatchId
        FROM [Dimension].[Sales Territory] AS d
        INNER JOIN #TerritorySource AS s
            ON s.[Sales Territory Code] = d.[Sales Territory Code]
        LEFT OUTER JOIN [Dimension].[Region] AS r
            ON r.[Region Code] = s.[Region Code]
        WHERE d.[Sales Territory Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Sales Territory]
            ([WWI Sales Territory ID], [Sales Territory Code], [Sales Territory], [Region Key],
             [Region Code], [Alignment Year], [Parent Territory Code], [Territory Level Code],
             [Territory Manager Employee No], [Sales Office Code], [Coverage Model Code],
             [NA State Province List], [NA ZIP Prefix List], [EU Country List], [EU NUTS Code List],
             [APAC Country List], [APAC City List], [Annual Target Amount], [Target Currency Code],
             [Target Fiscal Year], [Is Active], [Source System Code], [Row Hash Type 1],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Sales Territory ID]
            , s.[Sales Territory Code]
            , ISNULL(s.[Sales Territory], s.[Sales Territory Code])
            , r.[Region Key]
            , s.[Region Code]
            , s.[Alignment Year]
            , s.[Parent Territory Code]
            , s.[Territory Level Code]
            , s.[Territory Manager Employee No]
            , s.[Sales Office Code]
            , s.[Coverage Model Code]
            , CASE WHEN s.[Region Code] = N'NA'   THEN s.[Coverage List] END
            , CASE WHEN s.[Region Code] = N'NA'   THEN s.[Secondary Coverage List] END
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[Coverage List] END
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[Secondary Coverage List] END
            , CASE WHEN s.[Region Code] = N'APAC' THEN s.[Coverage List] END
            , CASE WHEN s.[Region Code] = N'APAC' THEN s.[Secondary Coverage List] END
            , s.[Annual Target Amount]
            , ISNULL(s.[Target Currency Code], r.[Reporting Currency Code])
            , ISNULL(s.[Target Fiscal Year], s.[Alignment Year])
            , s.[Is Active]
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Sales Territory], N''),
                          ISNULL(s.[Parent Territory Code], N''),
                          ISNULL(s.[Territory Manager Employee No], N''),
                          ISNULL(s.[Coverage List], N''),
                          ISNULL(CONVERT(NVARCHAR(30), s.[Annual Target Amount]), N''),
                          ISNULL(CONVERT(NVARCHAR(6), s.[Alignment Year]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #TerritorySource AS s
        LEFT OUTER JOIN [Dimension].[Region] AS r
            ON r.[Region Code] = s.[Region Code]
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Sales Territory] AS d
                          WHERE d.[Sales Territory Code] = s.[Sales Territory Code]
                            AND d.[Sales Territory Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        /* Territories that survived the re-alignment but are no longer in the
           extract are retired, and their bridge rows are closed on the same day. */
        UPDATE d
        SET d.[Is Active]          = 0,
            d.[Retired On]         = CONVERT(DATE, @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Sales Territory] AS d
        WHERE d.[Sales Territory Key] > 0
          AND d.[Is Active] = 1
          AND NOT EXISTS (SELECT 1
                          FROM #TerritorySource AS s
                          WHERE s.[Sales Territory Code] = d.[Sales Territory Code]);

        SET @RetiredCount = @@ROWCOUNT;

        UPDATE b
        SET b.[Is Current Coverage] = 0,
            b.[Coverage To]         = CONVERT(DATE, @Now),
            b.[Last Load Batch Id]  = @BatchId
        FROM [Dimension].[Employee Territory Bridge] AS b
        INNER JOIN [Dimension].[Sales Territory] AS d
            ON d.[Sales Territory Key] = b.[Sales Territory Key]
        WHERE b.[Is Current Coverage] = 1
          AND d.[Is Active]           = 0;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Sales Territory', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, @RetiredCount, @InsertedCount, @RejectCount, N'Type1Realignment',
             @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Sales Territory',
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
             @SourceComponent    = N'Dimension.Sales Territory',
             @ProcedureName      = N'Integration.usp_MigrateStagedSalesTerritoryData',
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
