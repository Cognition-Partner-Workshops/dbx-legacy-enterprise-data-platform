/*
    Object        : [Integration].[usp_MigrateStagedProductData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.ProductCategory, Dimension.Product Category,
                    the etl control framework
    Called by     : DIM_Load_ProductCategory

    Type 1 flattened merchandising hierarchy: department, class, subclass in one
    row with a slash-delimited path. The path is rebuilt on every load because
    the merchandising team renames levels without changing codes.

    The regional tax categories are derived here rather than in the stock item
    load, so that a category re-classification moves every SKU at once. The NA
    taxability, the EU VAT rate category and the APAC GST category are three
    unrelated code sets and are derived by three separate CASE expressions - the
    2013 attempt to drive all three from one lookup table produced zero-rated
    children's clothing in Singapore and was reverted.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedProductData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_ProductCategory',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_MDM',
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
        IF OBJECT_ID(N'tempdb..#ProductCategorySource') IS NOT NULL
            DROP TABLE #ProductCategorySource;

        SELECT
              r.[CategoryCode]              AS [Category Code]
            , r.[WWIProductCategoryID]      AS [WWI Product Category ID]
            , r.[CategoryName]              AS [Product Category]
            , r.[DepartmentCode]            AS [Department Code]
            , r.[DepartmentName]            AS [Department Name]
            , r.[ClassCode]                 AS [Class Code]
            , r.[ClassName]                 AS [Class Name]
            , r.[SubclassCode]              AS [Subclass Code]
            , r.[SubclassName]              AS [Subclass Name]
            , r.[HarmonisedSystemCode]      AS [Harmonised System Code]
            , r.[BuyingTeamCode]            AS [Buying Team Code]
            , r.[MerchandisingManager]      AS [Merchandising Manager]
            , r.[TargetMarginPercentage]    AS [Target Margin Percentage]
            , r.[SeasonalityCode]           AS [Seasonality Code]
            , r.[MarkdownPolicyCode]        AS [Markdown Policy Code]
            , r.[IsOwnBrand]                AS [Is Own Brand]
            , r.[IsPerishable]              AS [Is Perishable]
            , r.[IsAgeRestricted]           AS [Is Age Restricted]
            , r.[MinimumAge]                AS [Minimum Age]
            , CONVERT(NVARCHAR(200), NULL)  AS [Category Path]
            , CONVERT(NVARCHAR(15), NULL)   AS [NA Tax Category Code]
            , CONVERT(NVARCHAR(15), NULL)   AS [EU VAT Rate Category]
            , CONVERT(NVARCHAR(15), NULL)   AS [APAC GST Category Code]
        INTO #ProductCategorySource
        FROM [ref].[ProductCategory] AS r;

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE #ProductCategorySource
        SET [Category Path] =
                CONCAT_WS(N'/', ISNULL([Department Code], N'???'),
                          ISNULL([Class Code], N'???'), ISNULL([Subclass Code], N'???'));

        /* NA: taxable unless the department is grocery or the item is a gift card. */
        UPDATE #ProductCategorySource
        SET [NA Tax Category Code] =
                CASE WHEN [Department Code] = N'GRC' THEN N'FOOD_EXEMPT'
                     WHEN [Class Code]      = N'GFTC' THEN N'NON_TAXABLE'
                     WHEN [Is Age Restricted] = 1 AND [Class Code] IN (N'BEER', N'WINE', N'SPIR')
                          THEN N'EXCISE'
                     ELSE N'TAXABLE' END;

        /* EU: five VAT rate categories, driven by department and perishability. */
        UPDATE #ProductCategorySource
        SET [EU VAT Rate Category] =
                CASE WHEN [Department Code] = N'GRC' AND [Is Perishable] = 1 THEN N'RED'
                     WHEN [Department Code] = N'GRC' THEN N'SUP'
                     WHEN [Department Code] = N'BKS' THEN N'RED'
                     WHEN [Class Code] = N'GFTC' THEN N'EXE'
                     WHEN [Department Code] = N'MED' THEN N'ZER'
                     ELSE N'STD' END;

        /* APAC: GST standard, zero-rated exports and the concessional food list. */
        UPDATE #ProductCategorySource
        SET [APAC GST Category Code] =
                CASE WHEN [Department Code] = N'GRC' THEN N'GST_CONC'
                     WHEN [Class Code] = N'GFTC' THEN N'GST_OUT'
                     WHEN [Department Code] = N'MED' THEN N'GST_ZERO'
                     ELSE N'GST_STD' END;

        /* A subclass with no parent class breaks the merchandising rollup. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Product Category',
               s.[Category Code], N'ORPHAN_SUBCLASS',
               N'Subclass supplied without a parent class code; rollup will be incomplete.',
               N'Reference', CONCAT(N'Path=', s.[Category Path])
        FROM #ProductCategorySource AS s
        WHERE s.[Subclass Code] IS NOT NULL
          AND s.[Class Code] IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        UPDATE d
        SET d.[Product Category]           = s.[Product Category],
            d.[Department Code]            = s.[Department Code],
            d.[Department Name]            = s.[Department Name],
            d.[Class Code]                 = s.[Class Code],
            d.[Class Name]                 = s.[Class Name],
            d.[Subclass Code]              = s.[Subclass Code],
            d.[Subclass Name]              = s.[Subclass Name],
            d.[Category Path]              = s.[Category Path],
            d.[Harmonised System Code]     = s.[Harmonised System Code],
            d.[Buying Team Code]           = s.[Buying Team Code],
            d.[Merchandising Manager]      = s.[Merchandising Manager],
            d.[Target Margin Percentage]   = s.[Target Margin Percentage],
            d.[Seasonality Code]           = s.[Seasonality Code],
            d.[Markdown Policy Code]       = s.[Markdown Policy Code],
            d.[Is Own Brand]               = s.[Is Own Brand],
            d.[Is Perishable]              = s.[Is Perishable],
            d.[Is Age Restricted]          = s.[Is Age Restricted],
            d.[Minimum Age]                = s.[Minimum Age],
            d.[NA Tax Category Code]       = s.[NA Tax Category Code],
            d.[EU VAT Rate Category]       = s.[EU VAT Rate Category],
            d.[APAC GST Category Code]     = s.[APAC GST Category Code],
            d.[Is Active]                  = 1,
            d.[Row Hash Type 1]            = HASHBYTES(N'SHA2_256',
                    CONCAT_WS(N'|', ISNULL(s.[Product Category], N''), ISNULL(s.[Category Path], N''),
                              ISNULL(s.[NA Tax Category Code], N''), ISNULL(s.[EU VAT Rate Category], N''),
                              ISNULL(s.[APAC GST Category Code], N''))),
            d.[Last Load Batch Id]         = @BatchId
        FROM [Dimension].[Product Category] AS d
        INNER JOIN #ProductCategorySource AS s
            ON s.[Category Code] = d.[Category Code]
        WHERE d.[Product Category Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Product Category]
            ([WWI Product Category ID], [Product Category], [Category Code], [Department Code],
             [Department Name], [Class Code], [Class Name], [Subclass Code], [Subclass Name],
             [Category Path], [Harmonised System Code], [Buying Team Code], [Merchandising Manager],
             [Target Margin Percentage], [Seasonality Code], [Markdown Policy Code], [Is Own Brand],
             [Is Perishable], [Is Age Restricted], [Minimum Age], [NA Tax Category Code],
             [EU VAT Rate Category], [APAC GST Category Code], [Is Active], [Source System Code],
             [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Product Category ID]
            , ISNULL(s.[Product Category], s.[Category Code])
            , s.[Category Code]
            , s.[Department Code]
            , s.[Department Name]
            , s.[Class Code]
            , s.[Class Name]
            , s.[Subclass Code]
            , s.[Subclass Name]
            , s.[Category Path]
            , s.[Harmonised System Code]
            , s.[Buying Team Code]
            , s.[Merchandising Manager]
            , s.[Target Margin Percentage]
            , s.[Seasonality Code]
            , s.[Markdown Policy Code]
            , s.[Is Own Brand]
            , s.[Is Perishable]
            , s.[Is Age Restricted]
            , s.[Minimum Age]
            , s.[NA Tax Category Code]
            , s.[EU VAT Rate Category]
            , s.[APAC GST Category Code]
            , 1
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Product Category], N''), ISNULL(s.[Category Path], N''),
                          ISNULL(s.[NA Tax Category Code], N''), ISNULL(s.[EU VAT Rate Category], N''),
                          ISNULL(s.[APAC GST Category Code], N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #ProductCategorySource AS s
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Product Category] AS d
            WHERE d.[Category Code] = s.[Category Code]
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Product Category', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1UpdateInsert', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Product Category',
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
             @SourceComponent    = N'Dimension.Product Category',
             @ProcedureName      = N'Integration.usp_MigrateStagedProductData',
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
