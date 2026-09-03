/*
    Object        : [Integration].[usp_MigrateStagedBuyingGroupData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.BuyingGroup, Dimension.Buying Group, the etl control framework
    Called by     : DIM_Load_BuyingGroup

    Type 2 on the rebate terms. A buying group whose rebate percentage or tier
    threshold changes must not restate last year's accruals, so the old version
    is closed and a new one opened, and the accrual fact points at the version
    that was current on the accrual date.

    Dissolutions and mergers are handled here rather than by a delete: the group
    is marked dissolved, the successor code is recorded, and the membership bridge
    is left alone. The bridge load closes the memberships on its next run, which
    means there is a window of up to a day where a customer belongs to a dissolved
    group. Reporting has always tolerated this.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedBuyingGroupData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Load_BuyingGroup',
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
    DECLARE @ClosedCount        BIGINT = 0;
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
        IF OBJECT_ID(N'tempdb..#BuyingGroupSource') IS NOT NULL
            DROP TABLE #BuyingGroupSource;

        SELECT
              g.[BuyingGroupCode]           AS [Buying Group Code]
            , g.[WWIBuyingGroupID]          AS [WWI Buying Group ID]
            , g.[BuyingGroupName]           AS [Buying Group]
            , g.[BuyingGroupTypeCode]       AS [Buying Group Type Code]
            , g.[NegotiatingEntityName]     AS [Negotiating Entity Name]
            , g.[HomeCountryCode]           AS [Home Country Code]
            , g.[RegionCode]                AS [Region Code]
            , g.[AgreementStartDate]        AS [Agreement Start Date]
            , g.[AgreementEndDate]          AS [Agreement End Date]
            , g.[RebateBasisCode]           AS [Rebate Basis Code]
            , g.[RebatePercentage]          AS [Rebate Percentage]
            , g.[RebateTierThresholdAmount] AS [Rebate Tier Threshold Amount]
            , g.[RebateAgreementReference]  AS [Rebate Agreement Reference]
            , g.[RebateAccrualAccountCode]  AS [Rebate Accrual Account Code]
            , g.[SettlementCurrencyCode]    AS [Settlement Currency Code]
            , g.[IsDissolved]               AS [Is Dissolved]
            , g.[MergedOn]                  AS [Merged On]
            , g.[SuccessorBuyingGroupCode]  AS [Successor Buying Group Code]
            , g.[GroupPurchasingOrgCode]    AS [NA Group Purchasing Org Code]
            , g.[CompetitionDeclarationRef] AS [EU Competition Declaration Ref]
            , g.[IsCompetitionDeclared]     AS [EU Is Declared]
            , g.[LocalRegistrationNo]       AS [APAC Local Registration No]
            , g.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
        INTO #BuyingGroupSource
        FROM [stg].[BuyingGroup] AS g
        WHERE (@RegionCode = N'GLOBAL' OR g.[RegionCode] = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /*
            An EU group with a rebate above the notification threshold must carry
            a competition declaration reference. The row still loads - blocking it
            would lose the sales - but compliance gets the reject report.
        */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Buying Group',
               s.[Buying Group Code], N'EU_DECLARATION_MISSING',
               N'EU buying group above the rebate notification threshold has no competition declaration reference.',
               N'Dimension', CONCAT(N'Rebate=', CONVERT(NVARCHAR(20), s.[Rebate Percentage]))
        FROM #BuyingGroupSource AS s
        WHERE s.[Region Code] = N'EU'
          AND s.[Rebate Percentage] > 5.0
          AND NULLIF(s.[EU Competition Declaration Ref], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* A successor code that does not exist would orphan the merged group. */
        UPDATE s
        SET s.[Successor Buying Group Code] = NULL
        FROM #BuyingGroupSource AS s
        WHERE s.[Successor Buying Group Code] IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM #BuyingGroupSource AS x
                          WHERE x.[Buying Group Code] = s.[Successor Buying Group Code])
          AND NOT EXISTS (SELECT 1 FROM [Dimension].[Buying Group] AS d
                          WHERE d.[Buying Group Code] = s.[Successor Buying Group Code]);

        UPDATE #BuyingGroupSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Buying Group], N''), ISNULL([Buying Group Type Code], N''),
                          ISNULL([Rebate Basis Code], N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Rebate Percentage]), N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Rebate Tier Threshold Amount]), N''),
                          ISNULL([Settlement Currency Code], N''),
                          ISNULL(CONVERT(NVARCHAR(1), [Is Dissolved]), N''),
                          ISNULL([Successor Buying Group Code], N''),
                          ISNULL(CONVERT(NVARCHAR(30), [Agreement End Date], 126), N'')));

        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Buying Group] AS d
        INNER JOIN #BuyingGroupSource AS s
            ON s.[Buying Group Code] = d.[Buying Group Code]
        WHERE d.[Is Current Row]    = 1
          AND d.[Buying Group Key]  > 0
          AND d.[Row Hash Type 2]  <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Buying Group]
            ([WWI Buying Group ID], [Buying Group], [Buying Group Code], [Buying Group Type Code],
             [Negotiating Entity Name], [Home Country Code], [Region Code],
             [Agreement Start Date], [Agreement End Date], [Rebate Basis Code], [Rebate Percentage],
             [Rebate Tier Threshold Amount], [Rebate Agreement Reference],
             [Rebate Accrual Account Code], [Settlement Currency Code], [Is Dissolved], [Merged On],
             [Successor Buying Group Code], [NA Group Purchasing Org Code],
             [EU Competition Declaration Ref], [EU Is Declared], [APAC Local Registration No],
             [Source System Code], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Buying Group ID]
            , ISNULL(s.[Buying Group], s.[Buying Group Code])
            , s.[Buying Group Code]
            , s.[Buying Group Type Code]
            , s.[Negotiating Entity Name]
            , s.[Home Country Code]
            , ISNULL(s.[Region Code], @RegionCode)
            , s.[Agreement Start Date]
            , s.[Agreement End Date]
            , s.[Rebate Basis Code]
            , s.[Rebate Percentage]
            , s.[Rebate Tier Threshold Amount]
            , s.[Rebate Agreement Reference]
            , s.[Rebate Accrual Account Code]
            , s.[Settlement Currency Code]
            , ISNULL(s.[Is Dissolved], 0)
            , s.[Merged On]
            , s.[Successor Buying Group Code]
            , CASE WHEN s.[Region Code] = N'NA'   THEN s.[NA Group Purchasing Org Code] END
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[EU Competition Declaration Ref] END
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[EU Is Declared] END
            , CASE WHEN s.[Region Code] = N'APAC' THEN s.[APAC Local Registration No] END
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
            , ISNULL(sameday.[Sequence], 0) + 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #BuyingGroupSource AS s
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Buying Group] AS d
            WHERE d.[Buying Group Code] = s.[Buying Group Code]
              AND d.[Buying Group Key]  > 0
        ) AS prior
        OUTER APPLY
        (
            SELECT MAX(d.[Effective Sequence]) AS [Sequence]
            FROM [Dimension].[Buying Group] AS d
            WHERE d.[Buying Group Code]   = s.[Buying Group Code]
              AND d.[Effective From Date] = CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
        ) AS sameday
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Buying Group] AS cur
            WHERE cur.[Buying Group Code] = s.[Buying Group Code]
              AND cur.[Is Current Row]    = 1
              AND cur.[Buying Group Key]  > 0
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Buying Group', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @RejectCount, N'Type2', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Buying Group',
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
             @SourceComponent    = N'Dimension.Buying Group',
             @ProcedureName      = N'Integration.usp_MigrateStagedBuyingGroupData',
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
