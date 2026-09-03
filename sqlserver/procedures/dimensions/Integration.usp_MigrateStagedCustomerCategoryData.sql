/*
    Object        : [Integration].[usp_MigrateStagedCustomerCategoryData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.CustomerClassification, Dimension.Customer Category,
                    the etl control framework
    Called by     : DIM_Load_CustomerCategory

    Small Type 1 dimension loaded with a single MERGE. Nothing here is versioned:
    when a category is re-described the description simply changes and every
    historical sale re-reports under the new wording. Finance has asked twice for
    this to become Type 2 and it has been declined twice on the grounds that the
    category codes themselves never change.

    The legacy classification code and name are carried alongside the current
    ones. They come from the 1996 order-entry system, they are not maintained,
    and three reports still group on them.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCustomerCategoryData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_CustomerCategory',
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
    DECLARE @MergeLog TABLE ([Action] NVARCHAR(10) NOT NULL);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        SELECT @SourceRowCount = COUNT_BIG(*) FROM [ref].[CustomerClassification];

        /* Duplicate codes are a data-entry fault in the Oracle reference screen. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Customer Category',
               r.[CategoryCode], N'DUPLICATE_CODE',
               N'Category code appears more than once in the reference extract; last one wins.',
               N'Reference', CONCAT(N'Occurrences=', CONVERT(NVARCHAR(10), COUNT_BIG(*)))
        FROM [ref].[CustomerClassification] AS r
        GROUP BY r.[CategoryCode]
        HAVING COUNT_BIG(*) > 1;

        SET @RejectCount = @@ROWCOUNT;

        MERGE [Dimension].[Customer Category] AS tgt
        USING
        (
            SELECT
                  r.[CategoryCode]
                , MAX(r.[WWICustomerCategoryID])    AS [WWICustomerCategoryID]
                , MAX(r.[CategoryName])             AS [CategoryName]
                , MAX(r.[CategoryGroup])            AS [CategoryGroup]
                , MAX(r.[LegacyClassificationCode]) AS [LegacyClassificationCode]
                , MAX(r.[LegacyClassificationName]) AS [LegacyClassificationName]
                , MAX(r.[NaSegmentCode])            AS [NaSegmentCode]
                , MAX(r.[EuSectorCode])             AS [EuSectorCode]
                , MAX(r.[ApacTradeCode])            AS [ApacTradeCode]
                , MAX(r.[DefaultCreditLimitAmount]) AS [DefaultCreditLimitAmount]
                , MAX(r.[DefaultDiscountPercentage]) AS [DefaultDiscountPercentage]
                , MAX(r.[DefaultPaymentTermsCode])  AS [DefaultPaymentTermsCode]
                , MAX(CONVERT(TINYINT, r.[IsGovernment]))       AS [IsGovernment]
                , MAX(CONVERT(TINYINT, r.[IsInternal]))         AS [IsInternal]
                , MAX(CONVERT(TINYINT, r.[IsRetail]))           AS [IsRetail]
                , MAX(CONVERT(TINYINT, r.[IsWholesale]))        AS [IsWholesale]
                , MAX(CONVERT(TINYINT, r.[TaxExemptByDefault])) AS [TaxExemptByDefault]
                , MAX(CONVERT(TINYINT, r.[RequiresPurchaseOrder])) AS [RequiresPurchaseOrder]
                , MAX(r.[SourceEffectiveFrom])      AS [SourceEffectiveFrom]
                , HASHBYTES(N'SHA2_256',
                    CONCAT_WS(N'|', MAX(ISNULL(r.[CategoryName], N'')),
                              MAX(ISNULL(r.[CategoryGroup], N'')),
                              MAX(ISNULL(r.[DefaultPaymentTermsCode], N'')),
                              MAX(ISNULL(CONVERT(NVARCHAR(20), r.[DefaultCreditLimitAmount]), N'')),
                              MAX(ISNULL(CONVERT(NVARCHAR(20), r.[DefaultDiscountPercentage]), N'')),
                              MAX(ISNULL(r.[NaSegmentCode], N'')),
                              MAX(ISNULL(r.[EuSectorCode], N'')),
                              MAX(ISNULL(r.[ApacTradeCode], N'')))) AS [RowHash]
            FROM [ref].[CustomerClassification] AS r
            GROUP BY r.[CategoryCode]
        ) AS src
            ON tgt.[Category Code] = src.[CategoryCode]
        WHEN MATCHED AND (tgt.[Row Hash Type 1] IS NULL OR tgt.[Row Hash Type 1] <> src.[RowHash])
            THEN UPDATE SET
                  tgt.[Customer Category]           = src.[CategoryName]
                , tgt.[Category Group]              = src.[CategoryGroup]
                , tgt.[Legacy Classification Code]  = src.[LegacyClassificationCode]
                , tgt.[Legacy Classification Name]  = src.[LegacyClassificationName]
                , tgt.[NA Segment Code]             = src.[NaSegmentCode]
                , tgt.[EU Sector Code]              = src.[EuSectorCode]
                , tgt.[APAC Trade Code]             = src.[ApacTradeCode]
                , tgt.[Default Credit Limit Amount] = src.[DefaultCreditLimitAmount]
                , tgt.[Default Discount Percentage] = src.[DefaultDiscountPercentage]
                , tgt.[Default Payment Terms Code]  = src.[DefaultPaymentTermsCode]
                , tgt.[Is Government]               = src.[IsGovernment]
                , tgt.[Is Internal]                 = src.[IsInternal]
                , tgt.[Is Retail]                   = src.[IsRetail]
                , tgt.[Is Wholesale]                = src.[IsWholesale]
                , tgt.[Tax Exempt By Default]       = src.[TaxExemptByDefault]
                , tgt.[Requires Purchase Order]     = src.[RequiresPurchaseOrder]
                , tgt.[Is Active]                   = 1
                , tgt.[Row Hash Type 1]             = src.[RowHash]
                , tgt.[Last Load Batch Id]          = @BatchId
        WHEN NOT MATCHED BY TARGET
            THEN INSERT
                ([WWI Customer Category ID], [Customer Category], [Category Code], [Category Group],
                 [Legacy Classification Code], [Legacy Classification Name], [NA Segment Code],
                 [EU Sector Code], [APAC Trade Code], [Default Credit Limit Amount],
                 [Default Discount Percentage], [Default Payment Terms Code], [Is Government],
                 [Is Internal], [Is Retail], [Is Wholesale], [Tax Exempt By Default],
                 [Requires Purchase Order], [Is Active], [Source Effective From],
                 [Source System Code], [Row Hash Type 1], [Valid From], [Valid To],
                 [Lineage Key], [Last Load Batch Id])
            VALUES
                (src.[WWICustomerCategoryID], src.[CategoryName], src.[CategoryCode],
                 src.[CategoryGroup], src.[LegacyClassificationCode], src.[LegacyClassificationName],
                 src.[NaSegmentCode], src.[EuSectorCode], src.[ApacTradeCode],
                 src.[DefaultCreditLimitAmount], src.[DefaultDiscountPercentage],
                 src.[DefaultPaymentTermsCode], src.[IsGovernment], src.[IsInternal],
                 src.[IsRetail], src.[IsWholesale], src.[TaxExemptByDefault],
                 src.[RequiresPurchaseOrder], 1, src.[SourceEffectiveFrom], @SourceSystemCode,
                 src.[RowHash], @Now, @HighDate, @LineageKey, @BatchId)
        /*
            Categories that disappear from the reference extract are deactivated,
            never deleted: customers still point at them.
        */
        WHEN NOT MATCHED BY SOURCE AND tgt.[Customer Category Key] > 0 AND tgt.[Is Active] = 1
            THEN UPDATE SET
                  tgt.[Is Active]          = 0
                , tgt.[Last Load Batch Id] = @BatchId
        OUTPUT $action INTO @MergeLog([Action]);

        SELECT @InsertedCount = SUM(CASE WHEN [Action] = N'INSERT' THEN 1 ELSE 0 END),
               @UpdatedCount  = SUM(CASE WHEN [Action] = N'UPDATE' THEN 1 ELSE 0 END)
        FROM @MergeLog;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Customer Category', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             ISNULL(@UpdatedCount, 0), 0, ISNULL(@InsertedCount, 0), @RejectCount,
             N'Type1Merge', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Customer Category',
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
             @SourceComponent    = N'Dimension.Customer Category',
             @ProcedureName      = N'Integration.usp_MigrateStagedCustomerCategoryData',
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
