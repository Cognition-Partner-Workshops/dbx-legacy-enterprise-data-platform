/*
    Object        : [Integration].[usp_MigrateStagedStockItemData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Product, ref.ProductRegionalListing, Dimension.Stock Item,
                    Dimension.Product Category, Dimension.Product Hierarchy,
                    Dimension.Supplier, the etl control framework
    Called by     : DIM_Load_StockItem

    Type 2 on price band, cost band, supplier, tax category, hazard class and the
    discontinued flag; Type 1 on the merchandising text.

    The grain is (SKU, listing region), not SKU: the same SKU is listed separately
    in NA, EU and APAC with different tax categories, different hazard
    classifications and, since 2016, different pack sizes. A SKU therefore has up
    to three current rows. Fact loads must join on both the SKU and the region, and
    the 2017 margin restatement happened because one of them did not.

    Price banding rather than price versioning: the actual price is on the sales
    fact, and the dimension carries the band. The band boundaries are region
    specific and hard-coded in the CASE expressions below - they were parameters
    once, in a configuration table, until the 2014 audit found that a change to the
    table silently restated three years of banding.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedStockItemData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @PackageName        NVARCHAR(200) = N'DIM_Load_StockItem',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_WHS',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @ClosedCount        BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @Type1Count         BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @WatermarkFrom      NVARCHAR(50);
    DECLARE @WatermarkTo        NVARCHAR(50);
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        EXEC [etl].[usp_GetWatermark]
             @SourceSystemCode = @SourceSystemCode,
             @ObjectName       = N'Dimension.Stock Item',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#StockItemSource') IS NOT NULL
            DROP TABLE #StockItemSource;

        SELECT
              p.[WWIStockItemID]            AS [WWI Stock Item ID]
            , p.[StockItemName]             AS [Stock Item]
            , p.[SkuCode]                   AS [Source Product Reference]
            , @RegionCode                   AS [Listing Region Code]
            , p.[Color]                     AS [Color]
            , p.[Brand]                     AS [Brand]
            , p.[Size]                      AS [Size]
            , p.[SellingPackage]            AS [Selling Package]
            , p.[BuyingPackage]             AS [Buying Package]
            , p.[QuantityPerOuter]          AS [Quantity Per Outer]
            , p.[LeadTimeDays]              AS [Lead Time Days]
            , p.[IsChillerStock]            AS [Is Chiller Stock]
            , p.[TypicalWeightPerUnit]      AS [Typical Weight Per Unit]
            , p.[UnitPrice]                 AS [Unit Price]
            , p.[RecommendedRetailPrice]    AS [Recommended Retail Price]
            , p.[StandardCost]              AS [Standard Cost]
            , p.[CategoryCode]              AS [Category Code]
            , p.[HierarchyNodeCode]         AS [Hierarchy Node Code]
            , p.[PrimarySupplierId]         AS [Primary Supplier Id]
            , p.[ProductStatusCode]         AS [Product Status Code]
            , p.[IsDiscontinued]            AS [Is Discontinued]
            , p.[DiscontinuedOn]            AS [Discontinued On]
            , p.[HazardClassCode]           AS [Hazard Class Code]
            , p.[IsSerialTracked]           AS [Is Serialized]
            , p.[IsBatchTracked]            AS [Is Batch Tracked]
            , p.[ShelfLifeDays]             AS [Shelf Life Days]
            , p.[MarketingDescription]      AS [Marketing Description]
            , p.[SearchKeywords]            AS [Search Keywords]
            , p.[ImageUrl]                  AS [Image URL]
            , p.[MerchandisingNotes]        AS [Merchandising Notes]
            , l.[TaxCategoryCode]           AS [Source Tax Category Code]
            , l.[HarmonisedSystemCode]      AS [EU Intrastat Commodity Code]
            , l.[LocalPackSize]             AS [Pack Size Quantity]
            , p.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(NVARCHAR(15), NULL)   AS [Listing Price Band]
            , CONVERT(NVARCHAR(15), NULL)   AS [Standard Cost Band]
            , CONVERT(NVARCHAR(15), NULL)   AS [Tax Category Code]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 1]
        INTO #StockItemSource
        FROM [stg].[Product] AS p
        LEFT OUTER JOIN [ref].[ProductRegionalListing] AS l
            ON  l.[SkuCode]    = p.[SkuCode]
            AND l.[RegionCode] = @RegionCode
        WHERE p.[SourceChangedOn] > CONVERT(DATETIME2(7), @WatermarkFrom)
          AND EXISTS (SELECT 1 FROM [ref].[ProductRegionalListing] AS x
                      WHERE x.[SkuCode] = p.[SkuCode] AND x.[RegionCode] = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* Price bands. Different boundaries and different currencies per region. */
        IF @RegionCode = N'NA'
            UPDATE #StockItemSource
            SET [Listing Price Band] =
                    CASE WHEN [Unit Price] IS NULL      THEN N'UNKNOWN'
                         WHEN [Unit Price] < 10         THEN N'USD_0_10'
                         WHEN [Unit Price] < 50         THEN N'USD_10_50'
                         WHEN [Unit Price] < 250        THEN N'USD_50_250'
                         WHEN [Unit Price] < 1000       THEN N'USD_250_1K'
                         ELSE N'USD_1K_PLUS' END,
                [Standard Cost Band] =
                    CASE WHEN [Standard Cost] < 25 THEN N'LOW'
                         WHEN [Standard Cost] < 500 THEN N'MID'
                         ELSE N'HIGH' END,
                [Tax Category Code] = ISNULL([Source Tax Category Code], N'TAXABLE');

        IF @RegionCode = N'EU'
            UPDATE #StockItemSource
            SET [Listing Price Band] =
                    CASE WHEN [Unit Price] IS NULL      THEN N'UNKNOWN'
                         WHEN [Unit Price] < 15         THEN N'EUR_0_15'
                         WHEN [Unit Price] < 75         THEN N'EUR_15_75'
                         WHEN [Unit Price] < 300        THEN N'EUR_75_300'
                         ELSE N'EUR_300_PLUS' END,
                [Standard Cost Band] =
                    CASE WHEN [Standard Cost] < 30 THEN N'LOW'
                         WHEN [Standard Cost] < 600 THEN N'MID'
                         ELSE N'HIGH' END,
                -- VAT category, not a taxable flag: STD, RED, SUP, ZER or EXE
                [Tax Category Code] =
                    CASE WHEN [Source Tax Category Code] IN (N'STD', N'RED', N'SUP', N'ZER', N'EXE')
                         THEN [Source Tax Category Code]
                         WHEN [Is Chiller Stock] = 1 THEN N'RED'
                         ELSE N'STD' END;

        IF @RegionCode = N'APAC'
            UPDATE #StockItemSource
            SET [Listing Price Band] =
                    CASE WHEN [Unit Price] IS NULL      THEN N'UNKNOWN'
                         WHEN [Unit Price] < 20         THEN N'SGD_0_20'
                         WHEN [Unit Price] < 100        THEN N'SGD_20_100'
                         ELSE N'SGD_100_PLUS' END,
                [Standard Cost Band] =
                    CASE WHEN [Standard Cost] < 40 THEN N'LOW' ELSE N'HIGH' END,
                [Tax Category Code] = ISNULL([Source Tax Category Code], N'GST_STD');

        /* A SKU with no category cannot be reported on and is rejected outright. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Stock Item',
               s.[Source Product Reference], N'MISSING_CATEGORY',
               N'Stock item has no product category; merchandising reporting would drop it silently.',
               N'Dimension', CONCAT(N'Name=', s.[Stock Item], N'; Region=', @RegionCode)
        FROM #StockItemSource AS s
        WHERE s.[Category Code] IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        DELETE FROM #StockItemSource WHERE [Category Code] IS NULL;

        UPDATE #StockItemSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Stock Item], N''), ISNULL([Listing Price Band], N''),
                          ISNULL([Standard Cost Band], N''), ISNULL(CONVERT(NVARCHAR(12), [Primary Supplier Id]), N''),
                          ISNULL([Tax Category Code], N''), ISNULL([Hazard Class Code], N''),
                          ISNULL(CONVERT(NVARCHAR(1), [Is Discontinued]), N''),
                          ISNULL([Category Code], N''), ISNULL(CONVERT(NVARCHAR(12), [Pack Size Quantity]), N''))),
            [Row Hash Type 1] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Marketing Description], N''), ISNULL([Search Keywords], N''),
                          ISNULL([Image URL], N''), ISNULL([Merchandising Notes], N'')));

        /* Type 1 overwrite across all versions of the SKU in this region. */
        UPDATE d
        SET d.[Marketing Description] = s.[Marketing Description],
            d.[Search Keywords]       = s.[Search Keywords],
            d.[Image URL]             = s.[Image URL],
            d.[Merchandising Notes]   = s.[Merchandising Notes],
            d.[Row Hash Type 1]       = s.[Row Hash Type 1],
            d.[Last Load Batch Id]    = @BatchId
        FROM [Dimension].[Stock Item] AS d
        INNER JOIN #StockItemSource AS s
            ON  s.[WWI Stock Item ID]   = d.[WWI Stock Item ID]
            AND s.[Listing Region Code] = d.[Listing Region Code]
        WHERE d.[Stock Item Key] > 0
          AND (d.[Row Hash Type 1] IS NULL OR d.[Row Hash Type 1] <> s.[Row Hash Type 1]);

        SET @Type1Count = @@ROWCOUNT;

        /* Type 2 close-out, hand-rolled. */
        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Stock Item] AS d
        INNER JOIN #StockItemSource AS s
            ON  s.[WWI Stock Item ID]   = d.[WWI Stock Item ID]
            AND s.[Listing Region Code] = d.[Listing Region Code]
        WHERE d.[Is Current Row]   = 1
          AND d.[Stock Item Key]   > 0
          AND d.[Row Hash Type 2] <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Stock Item]
            ([WWI Stock Item ID], [Stock Item], [Source Product Reference], [Listing Region Code], [Color],
             [Brand], [Size], [Selling Package], [Buying Package], [Quantity Per Outer],
             [Lead Time Days], [Is Chiller Stock], [Typical Weight Per Unit], [Unit Price],
             [Recommended Retail Price], [Tax Rate], [Listing Price Band], [Standard Cost Band],
             [Product Category Key], [Product Hierarchy Key], [Primary Supplier Key],
             [Product Status Code], [Is Discontinued], [Discontinued On],
             [Hazard Class Code], [Is Serialized], [Is Batch Tracked], [Shelf Life Days],
             [NA Sales Tax Category Code], [EU VAT Rate Category], [APAC GST Treatment Code],
             [EU Intrastat Commodity Code], [Pack Size Quantity],
             [Marketing Description], [Search Keywords], [Image URL], [Merchandising Notes],
             [Source System Code], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Row Hash Type 1], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id], [Last Load Package Execution Id])
        SELECT
              s.[WWI Stock Item ID]
            , s.[Stock Item]
            , s.[Source Product Reference]
            , s.[Listing Region Code]
            , ISNULL(s.[Color], N'N/A')
            , ISNULL(s.[Brand], N'N/A')
            , ISNULL(s.[Size], N'N/A')
            , ISNULL(s.[Selling Package], N'Each')
            , ISNULL(s.[Buying Package], N'Each')
            , ISNULL(s.[Quantity Per Outer], 1)
            , ISNULL(s.[Lead Time Days], 0)
            , ISNULL(s.[Is Chiller Stock], 0)
            , ISNULL(s.[Typical Weight Per Unit], 0)
            , ISNULL(s.[Unit Price], 0)
            , s.[Recommended Retail Price]
            , 0                                     -- tax rate lives on the tax jurisdiction now
            , s.[Listing Price Band]
            , s.[Standard Cost Band]
            , ISNULL(cat.[Product Category Key], -1)
            , ISNULL(h.[Product Hierarchy Key], -1)
            , ISNULL(sup.[Supplier Key], -1)
            , s.[Product Status Code]
            , ISNULL(s.[Is Discontinued], 0)
            , s.[Discontinued On]
            , s.[Hazard Class Code]
            , s.[Is Serialized]
            , s.[Is Batch Tracked]
            , s.[Shelf Life Days]
            , CASE WHEN s.[Listing Region Code] = N'NA'   THEN s.[Tax Category Code] END
            , CASE WHEN s.[Listing Region Code] = N'EU'   THEN s.[Tax Category Code] END
            , CASE WHEN s.[Listing Region Code] = N'APAC' THEN s.[Tax Category Code] END
            , s.[EU Intrastat Commodity Code]
            , s.[Pack Size Quantity]
            , s.[Marketing Description]
            , s.[Search Keywords]
            , s.[Image URL]
            , s.[Merchandising Notes]
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
            , 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , s.[Row Hash Type 1]
            , 0
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
            , @PackageExecutionId
        FROM #StockItemSource AS s
        LEFT OUTER JOIN [Dimension].[Product Category] AS cat
            ON cat.[Category Code] = s.[Category Code]
        LEFT OUTER JOIN [Dimension].[Product Hierarchy] AS h
            ON  h.[Hierarchy Node Code] = s.[Hierarchy Node Code]
            AND h.[Is Active]           = 1
        LEFT OUTER JOIN [Dimension].[Supplier] AS sup
            ON  sup.[WWI Supplier ID] = s.[Primary Supplier Id]
            AND sup.[Is Current Row]  = 1
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Stock Item] AS d
            WHERE d.[WWI Stock Item ID]   = s.[WWI Stock Item ID]
              AND d.[Listing Region Code] = s.[Listing Region Code]
              AND d.[Stock Item Key]      > 0
        ) AS prior
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Stock Item] AS cur
            WHERE cur.[WWI Stock Item ID]   = s.[WWI Stock Item ID]
              AND cur.[Listing Region Code] = s.[Listing Region Code]
              AND cur.[Is Current Row]      = 1
              AND cur.[Stock Item Key]      > 0
              AND cur.[Row Hash Type 2]     = s.[Row Hash Type 2]
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Stock Item', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             @Type1Count, @ClosedCount, @InsertedCount, @RejectCount, N'HybridSCD', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Stock Item',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @Type1Count,
             @RejectRowCount     = @RejectCount;

        SELECT @WatermarkTo = CONVERT(NVARCHAR(50), MAX([Source Changed On]), 126)
        FROM #StockItemSource;

        IF @WatermarkTo IS NOT NULL
            EXEC [etl].[usp_SetWatermark]
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Stock Item',
                 @WatermarkTo        = @WatermarkTo,
                 @PackageExecutionId = @PackageExecutionId;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @Type1Count,
             @RowsRejected       = @RejectCount,
             @WatermarkFrom      = @WatermarkFrom,
             @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Stock Item',
             @ProcedureName      = N'Integration.usp_MigrateStagedStockItemData',
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
