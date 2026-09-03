/*
    Object        : [Integration].[usp_MigrateStagedPromotionData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Promotion, Dimension.Promotion, Dimension.Sales Channel,
                    Dimension.Product Category, Integration.InferredMemberQueue,
                    the etl control framework
    Called by     : DIM_Load_Promotion

    Type 2 with same-day amendment handling and inferred-member enrichment.

    Promotions are amended while they are running - extended, discounted further,
    re-scoped - and marketing amend them more than once on a launch day, so the
    close-out orders by the source amendment timestamp and stamps an effective
    sequence. Two rows for the same promotion on the same day are legitimate and
    the effectiveness report picks the highest sequence for the day.

    Promotion codes also reach the sales fact before marketing publish the
    promotion record, which is common at the start of a campaign. The fact load
    inserts a stub through Integration.InferredMemberQueue; this procedure
    enriches the stub in place so the surrogate key that the already-loaded sale
    rows point at survives. An enriched stub is not versioned - there was nothing
    to version, the row was empty.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedPromotionData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Load_Promotion',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_SLS',
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
    DECLARE @EnrichedCount      BIGINT = 0;
    DECLARE @SameDayCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#PromotionSource') IS NOT NULL DROP TABLE #PromotionSource;

        SELECT
              UPPER(LTRIM(RTRIM(p.[PromotionCode])))        AS [Promotion Code]
            , p.[WWIPromotionID]                            AS [WWI Promotion ID]
            , p.[PromotionName]                             AS [Promotion Name]
            , UPPER(p.[CampaignCode])                       AS [Campaign Code]
            , p.[CampaignName]                              AS [Campaign Name]
            , UPPER(ISNULL(p.[PromotionTypeCode], N'PRICE')) AS [Promotion Type Code]
            , UPPER(ISNULL(p.[MechanicCode], N'PCTOFF'))    AS [Mechanic Code]
            , p.[Parameter1]                                AS [Parameter 1]
            , p.[Parameter2]                                AS [Parameter 2]
            , p.[Parameter3]                                AS [Parameter 3]
            , UPPER(p.[ParameterCurrencyCode])              AS [Parameter Currency Code]
            , UPPER(ISNULL(p.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , UPPER(p.[SalesChannelCode])                   AS [Sales Channel Code]
            , UPPER(ISNULL(p.[ChannelScopeCode], N'ALL'))   AS [Channel Scope Code]
            , UPPER(ISNULL(p.[ProductScopeCode], N'ALL'))   AS [Product Scope Code]
            , UPPER(p.[ProductCategoryCode])                AS [Product Category Code]
            , UPPER(ISNULL(p.[CustomerScopeCode], N'ALL'))  AS [Customer Scope Code]
            , UPPER(p.[CustomerSegmentCode])                AS [Customer Segment Code]
            , p.[StartDate]                                 AS [Start Date]
            , p.[EndDate]                                   AS [End Date]
            , p.[RedemptionLimit]                           AS [Redemption Limit]
            , p.[RedemptionLimitPerCustomer]                AS [Redemption Limit Per Customer]
            , ISNULL(p.[IsStackable], 0)                    AS [Is Stackable]
            , CONVERT(SMALLINT, p.[PriorityOrder])          AS [Priority Order]
            , UPPER(ISNULL(p.[FundingSourceCode], N'INTERNAL')) AS [Funding Source Code]
            , p.[VendorFundingPercentage]                   AS [Vendor Funding Percentage]
            , p.[BudgetAmount]                              AS [Budget Amount]
            , p.[AccrualGlAccountCode]                      AS [Accrual GL Account Code]
            , p.[PriorPriceReferenceDate]                   AS [EU Prior Price Reference Date]
            , p.[CouponSeriesCode]                          AS [NA Coupon Series Code]
            , p.[MarketplaceCampaignId]                     AS [APAC Marketplace Campaign Id]
            , UPPER(p.[AmendmentReasonCode])                AS [Amendment Reason Code]
            , ISNULL(p.[SourceChangedOn], @Now)             AS [Source Changed On]
            , CONVERT(SMALLINT, 1)                          AS [Effective Sequence]
            , CONVERT(BIT, 0)                               AS [EU Price Display Compliant]
        INTO #PromotionSource
        FROM [stg].[Promotion] AS p
        WHERE NULLIF(LTRIM(RTRIM(p.[PromotionCode])), N'') IS NOT NULL
          AND (@RegionCode = N'GLOBAL' OR UPPER(ISNULL(p.[RegionCode], N'GLOBAL')) = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* EU price display: a price promotion must reference the lowest price in
           the previous 30 days. Marketing supply the reference date; where it is
           missing or too recent the promotion is marked non-compliant and the
           legal team get it in the reject file. */
        UPDATE s
        SET s.[EU Price Display Compliant] =
                CASE
                    WHEN s.[Promotion Type Code] <> N'PRICE' THEN 1
                    WHEN s.[EU Prior Price Reference Date] IS NULL THEN 0
                    WHEN DATEDIFF(DAY, s.[EU Prior Price Reference Date],
                                  ISNULL(s.[Start Date], @Today)) < 30 THEN 0
                    ELSE 1
                END
        FROM #PromotionSource AS s
        WHERE s.[Region Code] = N'EU';

        /* NA coupon promotions are frequently vendor funded; a funding split of
           zero on a VENDOR-funded promotion means the accrual is never raised. */
        UPDATE s
        SET s.[Vendor Funding Percentage] = ISNULL(s.[Vendor Funding Percentage], 0),
            s.[Accrual GL Account Code]   = ISNULL(s.[Accrual GL Account Code], N'2450')
        FROM #PromotionSource AS s
        WHERE s.[Region Code] = N'NA'
          AND s.[Funding Source Code] IN (N'VENDOR', N'SHARED');

        /* APAC marketplace-funded promotions carry the marketplace campaign id
           and are not accrued at all - the marketplace nets the funding off the
           remittance, which the finance close reconciles separately. */
        UPDATE s
        SET s.[Funding Source Code]     = CASE
                                              WHEN s.[APAC Marketplace Campaign Id] IS NOT NULL
                                              THEN N'MARKETPLACE' ELSE s.[Funding Source Code]
                                          END,
            s.[Accrual GL Account Code] = CASE
                                              WHEN s.[APAC Marketplace Campaign Id] IS NOT NULL
                                              THEN NULL ELSE s.[Accrual GL Account Code]
                                          END
        FROM #PromotionSource AS s
        WHERE s.[Region Code] = N'APAC';

        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Promotion',
               s.[Promotion Code],
               CASE
                   WHEN s.[End Date] IS NOT NULL AND s.[End Date] < s.[Start Date] THEN N'PROMO_DATES_REVERSED'
                   WHEN s.[Region Code] = N'EU' AND s.[EU Price Display Compliant] = 0 THEN N'EU_PRICE_DISPLAY_NONCOMPLIANT'
                   ELSE N'PROMO_MECHANIC_PARAM_MISSING'
               END,
               CASE
                   WHEN s.[End Date] IS NOT NULL AND s.[End Date] < s.[Start Date]
                       THEN N'Promotion end date precedes its start date.'
                   WHEN s.[Region Code] = N'EU' AND s.[EU Price Display Compliant] = 0
                       THEN N'EU price promotion has no valid 30-day prior price reference date.'
                   ELSE N'Promotion mechanic requires a first parameter and none was supplied.'
               END,
               N'Dimension',
               CONCAT(N'Mechanic=', s.[Mechanic Code], N'|P1=',
                      ISNULL(CONVERT(NVARCHAR(30), s.[Parameter 1]), N'(null)'),
                      N'|Start=', CONVERT(NVARCHAR(10), s.[Start Date], 23))
        FROM #PromotionSource AS s
        WHERE (s.[End Date] IS NOT NULL AND s.[End Date] < s.[Start Date])
           OR (s.[Region Code] = N'EU' AND s.[EU Price Display Compliant] = 0)
           OR (s.[Parameter 1] IS NULL);

        SET @RejectCount = @@ROWCOUNT;

        /* Same-day amendments: sequence within the promotion and the calendar day
           of the change, not within the whole extract. */
        UPDATE s
        SET s.[Effective Sequence] = x.[Sequence Number]
        FROM #PromotionSource AS s
        INNER JOIN (
            SELECT [Promotion Code], [Source Changed On],
                   CONVERT(SMALLINT, ROW_NUMBER() OVER (
                       PARTITION BY [Promotion Code], CONVERT(DATE, [Source Changed On])
                       ORDER BY [Source Changed On])) AS [Sequence Number]
            FROM #PromotionSource
        ) AS x
            ON  x.[Promotion Code]    = s.[Promotion Code]
            AND x.[Source Changed On] = s.[Source Changed On];

        SET @SameDayCount = (SELECT COUNT_BIG(*) FROM #PromotionSource WHERE [Effective Sequence] > 1);

        IF OBJECT_ID(N'tempdb..#PromotionHashed') IS NOT NULL DROP TABLE #PromotionHashed;

        SELECT s.*
             , sc.[Sales Channel Key]
             , pc.[Product Category Key]
             , HASHBYTES(N'SHA2_256',
                 CONCAT_WS(N'|', ISNULL(s.[Promotion Name], N''),
                           ISNULL(s.[Mechanic Code], N''),
                           ISNULL(CONVERT(NVARCHAR(30), s.[Parameter 1]), N''),
                           ISNULL(CONVERT(NVARCHAR(30), s.[Parameter 2]), N''),
                           ISNULL(CONVERT(NVARCHAR(30), s.[Parameter 3]), N''),
                           ISNULL(CONVERT(NVARCHAR(10), s.[Start Date], 23), N''),
                           ISNULL(CONVERT(NVARCHAR(10), s.[End Date], 23), N''),
                           ISNULL(s.[Product Scope Code], N''),
                           ISNULL(s.[Customer Scope Code], N''),
                           ISNULL(s.[Funding Source Code], N''),
                           ISNULL(CONVERT(NVARCHAR(20), s.[Budget Amount]), N''))) AS [Row Hash Type 2]
        INTO #PromotionHashed
        FROM #PromotionSource AS s
        LEFT OUTER JOIN [Dimension].[Sales Channel] AS sc
            ON  sc.[Sales Channel Code] = s.[Sales Channel Code]
            AND sc.[Region Code]        = s.[Region Code]
        LEFT OUTER JOIN [Dimension].[Product Category] AS pc
            ON pc.[Category Code] = s.[Product Category Code];

        /* ---------------------------------------------------------------
           Inferred members first. A stub row keeps its surrogate key and is
           filled in place, so the sale rows already pointing at it stay put.
           --------------------------------------------------------------- */
        UPDATE d
        SET d.[WWI Promotion ID]              = s.[WWI Promotion ID],
            d.[Promotion Name]                = s.[Promotion Name],
            d.[Campaign Code]                 = s.[Campaign Code],
            d.[Campaign Name]                 = s.[Campaign Name],
            d.[Promotion Type Code]           = s.[Promotion Type Code],
            d.[Mechanic Code]                 = s.[Mechanic Code],
            d.[Parameter 1]                   = s.[Parameter 1],
            d.[Parameter 2]                   = s.[Parameter 2],
            d.[Parameter 3]                   = s.[Parameter 3],
            d.[Parameter Currency Code]       = s.[Parameter Currency Code],
            d.[Region Code]                   = s.[Region Code],
            d.[Sales Channel Key]             = ISNULL(s.[Sales Channel Key], -1),
            d.[Channel Scope Code]            = s.[Channel Scope Code],
            d.[Product Scope Code]            = s.[Product Scope Code],
            d.[Product Category Key]          = ISNULL(s.[Product Category Key], -1),
            d.[Customer Scope Code]           = s.[Customer Scope Code],
            d.[Customer Segment Code]         = s.[Customer Segment Code],
            d.[Start Date]                    = s.[Start Date],
            d.[End Date]                      = s.[End Date],
            d.[Redemption Limit]              = s.[Redemption Limit],
            d.[Redemption Limit Per Customer] = s.[Redemption Limit Per Customer],
            d.[Is Stackable]                  = s.[Is Stackable],
            d.[Priority Order]                = s.[Priority Order],
            d.[Funding Source Code]           = s.[Funding Source Code],
            d.[Vendor Funding Percentage]     = s.[Vendor Funding Percentage],
            d.[Budget Amount]                 = s.[Budget Amount],
            d.[Accrual GL Account Code]       = s.[Accrual GL Account Code],
            d.[EU Prior Price Reference Date] = CASE WHEN s.[Region Code] = N'EU'
                                                     THEN s.[EU Prior Price Reference Date] END,
            d.[EU Price Display Compliant]    = CASE WHEN s.[Region Code] = N'EU'
                                                     THEN s.[EU Price Display Compliant] END,
            d.[NA Coupon Series Code]         = CASE WHEN s.[Region Code] = N'NA'
                                                     THEN s.[NA Coupon Series Code] END,
            d.[APAC Marketplace Campaign Id]  = CASE WHEN s.[Region Code] = N'APAC'
                                                     THEN s.[APAC Marketplace Campaign Id] END,
            d.[Amendment Reason Code]         = N'INFERRED_ENRICHED',
            d.[Row Hash Type 2]               = s.[Row Hash Type 2],
            d.[Is Inferred Member]            = 0,
            d.[Source System Code]            = @SourceSystemCode,
            d.[Last Load Batch Id]            = @BatchId
        FROM [Dimension].[Promotion] AS d
        INNER JOIN #PromotionHashed AS s
            ON s.[Promotion Code] = d.[Promotion Code]
        WHERE d.[Promotion Key]      > 0
          AND d.[Is Inferred Member] = 1
          AND d.[Is Current Row]     = 1;

        SET @EnrichedCount = @@ROWCOUNT;

        UPDATE q
        SET q.[Enrichment Status] = N'Enriched',
            q.[Enriched On]       = SYSDATETIME(),
            q.[Last Attempt Note] = CONCAT(N'Enriched by batch ', CONVERT(NVARCHAR(20), @BatchId))
        FROM [Integration].[InferredMemberQueue] AS q
        INNER JOIN #PromotionHashed AS s
            ON s.[Promotion Code] = q.[Business Key]
        WHERE q.[Dimension Name]     = N'Promotion'
          AND q.[Enrichment Status]  = N'Pending';

        /* ---------------------------------------------------------------
           Hand-rolled Type 2 close-out for the real changes.
           --------------------------------------------------------------- */
        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = @Now,
            d.[Valid To]           = @Now,
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Promotion] AS d
        INNER JOIN #PromotionHashed AS s
            ON s.[Promotion Code] = d.[Promotion Code]
        WHERE d.[Promotion Key]      > 0
          AND d.[Is Current Row]     = 1
          AND d.[Is Inferred Member] = 0
          AND (d.[Row Hash Type 2] IS NULL OR d.[Row Hash Type 2] <> s.[Row Hash Type 2]);

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Promotion]
            ([WWI Promotion ID], [Promotion Code], [Promotion Name], [Campaign Code],
             [Campaign Name], [Promotion Type Code], [Mechanic Code], [Parameter 1],
             [Parameter 2], [Parameter 3], [Parameter Currency Code], [Region Code],
             [Sales Channel Key], [Channel Scope Code], [Product Scope Code],
             [Product Category Key], [Customer Scope Code], [Customer Segment Code],
             [Start Date], [End Date], [Redemption Limit], [Redemption Limit Per Customer],
             [Is Stackable], [Priority Order], [Funding Source Code], [Vendor Funding Percentage],
             [Budget Amount], [Accrual GL Account Code], [EU Prior Price Reference Date],
             [EU Price Display Compliant], [NA Coupon Series Code], [APAC Marketplace Campaign Id],
             [Amendment Reason Code], [Source System Code], [Effective From], [Effective To],
             [Effective From Date], [Effective Sequence], [Is Current Row], [Version Number],
             [Row Hash Type 2], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        SELECT
              s.[WWI Promotion ID]
            , s.[Promotion Code]
            , ISNULL(s.[Promotion Name], s.[Promotion Code])
            , s.[Campaign Code]
            , s.[Campaign Name]
            , s.[Promotion Type Code]
            , s.[Mechanic Code]
            , s.[Parameter 1]
            , s.[Parameter 2]
            , s.[Parameter 3]
            , s.[Parameter Currency Code]
            , s.[Region Code]
            , ISNULL(s.[Sales Channel Key], -1)
            , s.[Channel Scope Code]
            , s.[Product Scope Code]
            , ISNULL(s.[Product Category Key], -1)
            , s.[Customer Scope Code]
            , s.[Customer Segment Code]
            , s.[Start Date]
            , s.[End Date]
            , s.[Redemption Limit]
            , s.[Redemption Limit Per Customer]
            , s.[Is Stackable]
            , s.[Priority Order]
            , s.[Funding Source Code]
            , s.[Vendor Funding Percentage]
            , s.[Budget Amount]
            , s.[Accrual GL Account Code]
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[EU Prior Price Reference Date] END
            , CASE WHEN s.[Region Code] = N'EU'   THEN s.[EU Price Display Compliant] END
            , CASE WHEN s.[Region Code] = N'NA'   THEN s.[NA Coupon Series Code] END
            , CASE WHEN s.[Region Code] = N'APAC' THEN s.[APAC Marketplace Campaign Id] END
            , s.[Amendment Reason Code]
            , @SourceSystemCode
            , @Now
            , @HighDate
            , @Today
            , s.[Effective Sequence]
            , 1
            , ISNULL((SELECT MAX(p.[Version Number]) + 1
                      FROM [Dimension].[Promotion] AS p
                      WHERE p.[Promotion Code] = s.[Promotion Code]
                        AND p.[Promotion Key]  > 0), 1)
            , s.[Row Hash Type 2]
            , 0
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #PromotionHashed AS s
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Promotion] AS d
                          WHERE d.[Promotion Code]  = s.[Promotion Code]
                            AND d.[Promotion Key]   > 0
                            AND d.[Is Current Row]  = 1
                            AND d.[Row Hash Type 2] = s.[Row Hash Type 2]);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Inferred Enriched Count], [Same Day Change Count],
             [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Promotion', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @EnrichedCount, @SameDayCount, @RejectCount,
             N'Type2WithInferred', @Now, SYSDATETIME());

        DECLARE @UpdateRowCountValue BIGINT = @ClosedCount + @EnrichedCount;
        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Promotion',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdateRowCountValue,
             @RejectRowCount     = @RejectCount;

        DECLARE @RowsUpdatedValue BIGINT = @ClosedCount + @EnrichedCount;
        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @RowsUpdatedValue,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Promotion',
             @ProcedureName      = N'Integration.usp_MigrateStagedPromotionData',
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
