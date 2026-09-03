/*
    Object        : [Integration].[usp_LoadSupplierCategoryDimension]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.SupplierCategory, Dimension.Supplier Category,
                    the etl control framework
    Called by     : DIM_Load_SupplierCategory

    Type 1, loaded by delete-and-reinsert rather than by MERGE. The table is a
    few hundred rows and the procurement team re-codes it wholesale twice a year,
    so the 2008 author decided a full rebuild was simpler than change detection.
    The surrogate keys are therefore not stable across loads for categories that
    were dropped and re-added, which is why Dimension.Supplier carries the
    category code as well as the key and why the supplier load joins on the code.

    Only the rows that would actually change are touched: the delete is restricted
    to codes present in the extract, so a failed extract cannot empty the table.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadSupplierCategoryDimension]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_SupplierCategory',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_PUR',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @DeletedCount       BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#SupplierCategorySource') IS NOT NULL
            DROP TABLE #SupplierCategorySource;

        SELECT
              UPPER(LTRIM(RTRIM(r.[SupplierCategoryCode]))) AS [Supplier Category Code]
            , r.[WWISupplierCategoryID]     AS [WWI Supplier Category ID]
            , r.[SupplierCategoryName]      AS [Supplier Category]
            , r.[SpendCategoryGroup]        AS [Spend Category Group]
            , r.[UnspscSegmentCode]         AS [UNSPSC Segment Code]
            , r.[LegacyPurchasingCode]      AS [Legacy Purchasing Code]
            , r.[IsDirectSpend]             AS [Is Direct Spend]
            , r.[IsIndirectSpend]           AS [Is Indirect Spend]
            , r.[IsCapitalSpend]            AS [Is Capital Spend]
            , r.[RequiresContract]          AS [Requires Contract]
            , r.[RequiresQualityAudit]      AS [Requires Quality Audit]
            , r.[ApprovalRouteCode]         AS [Approval Route Code]
            , r.[ApprovalThresholdAmount]   AS [Approval Threshold Amount]
            , r.[ApprovalThresholdCurrency] AS [Approval Threshold Currency]
            , r.[DefaultGlAccountCode]      AS [Default GL Account Code]
            , r.[DefaultCostCenterCode]     AS [Default Cost Center Code]
            , r.[NaSourcingTeamCode]        AS [NA Sourcing Team Code]
            , r.[EuFrameworkAgreementRef]   AS [EU Framework Agreement Ref]
            , r.[ApacLocalContentRequired]  AS [APAC Local Content Required]
        INTO #SupplierCategorySource
        FROM [ref].[SupplierCategory] AS r
        WHERE NULLIF(LTRIM(RTRIM(r.[SupplierCategoryCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* Capital spend without an approval route is a control gap, not a load failure. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Supplier Category',
               s.[Supplier Category Code], N'CAPEX_NO_APPROVAL_ROUTE',
               N'Capital spend category has no approval route; loaded but flagged for procurement control.',
               N'Reference', CONCAT(N'Name=', s.[Supplier Category])
        FROM #SupplierCategorySource AS s
        WHERE s.[Is Capital Spend] = 1
          AND NULLIF(s.[Approval Route Code], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* Deactivate everything, then rebuild only the codes the extract carries. */
        UPDATE [Dimension].[Supplier Category]
        SET [Is Active]          = 0,
            [Last Load Batch Id] = @BatchId
        WHERE [Supplier Category Key] > 0;

        DELETE d
        FROM [Dimension].[Supplier Category] AS d
        INNER JOIN #SupplierCategorySource AS s
            ON s.[Supplier Category Code] = d.[Supplier Category Code]
        WHERE d.[Supplier Category Key] > 0;

        SET @DeletedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Supplier Category]
            ([WWI Supplier Category ID], [Supplier Category], [Supplier Category Code],
             [Spend Category Group], [UNSPSC Segment Code], [Legacy Purchasing Code],
             [Is Direct Spend], [Is Indirect Spend], [Is Capital Spend], [Requires Contract],
             [Requires Quality Audit], [Approval Route Code], [Approval Threshold Amount],
             [Approval Threshold Currency], [Default GL Account Code], [Default Cost Center Code],
             [NA Sourcing Team Code], [EU Framework Agreement Ref], [APAC Local Content Required],
             [Is Active], [Source System Code], [Row Hash Type 1], [Valid From], [Valid To],
             [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Supplier Category ID]
            , ISNULL(s.[Supplier Category], s.[Supplier Category Code])
            , s.[Supplier Category Code]
            , s.[Spend Category Group]
            , s.[UNSPSC Segment Code]
            , s.[Legacy Purchasing Code]
            , s.[Is Direct Spend]
            , s.[Is Indirect Spend]
            , s.[Is Capital Spend]
            , s.[Requires Contract]
            , s.[Requires Quality Audit]
            , s.[Approval Route Code]
            , s.[Approval Threshold Amount]
            , s.[Approval Threshold Currency]
            , s.[Default GL Account Code]
            , s.[Default Cost Center Code]
            , s.[NA Sourcing Team Code]
            , s.[EU Framework Agreement Ref]
            , s.[APAC Local Content Required]
            , 1
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Supplier Category], N''),
                          ISNULL(s.[Spend Category Group], N''),
                          ISNULL(s.[Approval Route Code], N''),
                          ISNULL(CONVERT(NVARCHAR(20), s.[Approval Threshold Amount]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #SupplierCategorySource AS s;

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Supplier Category', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @DeletedCount, @InsertedCount, @RejectCount, N'FullRebuild', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Supplier Category',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @DeleteRowCount     = @DeletedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsDeleted        = @DeletedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Supplier Category',
             @ProcedureName      = N'Integration.usp_LoadSupplierCategoryDimension',
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
