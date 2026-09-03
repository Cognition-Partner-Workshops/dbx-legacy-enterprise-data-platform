/*
    Object        : [Integration].[usp_MigrateStagedLoyaltyTierData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.LoyaltyTier, Dimension.Loyalty Tier,
                    the etl control framework
    Called by     : REF_Load_LoyaltyTier

    Type 1 MERGE on programme plus tier code. Three programmes that were never
    merged, so the load applies a different rule block to each and the same tier
    code (GOLD, say) legitimately exists several times with different thresholds
    and currencies.

      NA_REWARDS  - spend based, four tiers, 12 month qualification window,
                    points expire 24 months after earning, no consent needed
                    because the programme is contractual.
      EU_CLUB     - points based, three tiers, enrolment requires marketing
                    opt-in, points expire at the end of the following calendar
                    year, 24 month retention after the last activity.
      APAC_*      - one programme per country, thresholds in local currency,
                    consent model varies by country so the basis is carried
                    from the source and defaulted to notice-based.

    Tier ranks must be dense within a programme. A gap means a tier was retired
    without re-ranking and the tier progression report silently skips a level,
    so the gap is rejected for review rather than closed automatically.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedLoyaltyTierData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_LoyaltyTier',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_CRM',
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
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#LoyaltyTierSource') IS NOT NULL DROP TABLE #LoyaltyTierSource;

        SELECT
              UPPER(LTRIM(RTRIM(t.[ProgrammeCode])))        AS [Programme Code]
            , UPPER(LTRIM(RTRIM(t.[LoyaltyTierCode])))      AS [Loyalty Tier Code]
            , t.[WWILoyaltyTierID]                          AS [WWI Loyalty Tier ID]
            , t.[ProgrammeName]                             AS [Programme Name]
            , t.[LoyaltyTierName]                           AS [Loyalty Tier]
            , CONVERT(SMALLINT, t.[TierRank])               AS [Tier Rank]
            , UPPER(ISNULL(t.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , UPPER(t.[CountryCode])                        AS [Country Code]
            , UPPER(ISNULL(t.[QualificationBasisCode], N'SPEND')) AS [Qualification Basis Code]
            , t.[QualificationAmount]                       AS [Qualification Amount]
            , UPPER(t.[QualificationCurrencyCode])          AS [Qualification Currency Code]
            , t.[QualificationPoints]                       AS [Qualification Points]
            , CONVERT(SMALLINT, t.[QualificationPeriodMonths]) AS [Qualification Period Months]
            , t.[RetentionAmount]                           AS [Retention Amount]
            , CONVERT(SMALLINT, t.[DowngradeGraceMonths])   AS [Downgrade Grace Months]
            , t.[EarnRatePointsPerUnit]                     AS [Earn Rate Points Per Unit]
            , t.[RedeemRateValuePerPoint]                   AS [Redeem Rate Value Per Point]
            , CONVERT(NVARCHAR(15), NULL)                   AS [Points Expiry Rule Code]
            , CONVERT(SMALLINT, NULL)                       AS [Points Expiry Months]
            , t.[DiscountPercentage]                        AS [Discount Percentage]
            , t.[FreeShippingThreshold]                     AS [Free Shipping Threshold]
            , ISNULL(t.[HasDedicatedSupport], 0)            AS [Has Dedicated Support]
            , ISNULL(t.[HasEarlyAccess], 0)                 AS [Has Early Access]
            , t.[BenefitSummary]                            AS [Benefit Summary]
            , CONVERT(BIT, 0)                               AS [Requires Marketing Consent]
            , t.[ConsentBasisCode]                          AS [Consent Basis Code]
            , CONVERT(SMALLINT, NULL)                       AS [Data Retention Months]
            , ISNULL(t.[IsActive], 1)                       AS [Is Active]
            , t.[LaunchedOn]                                AS [Launched On]
            , t.[RetiredOn]                                 AS [Retired On]
        INTO #LoyaltyTierSource
        FROM [ref].[LoyaltyTier] AS t
        WHERE NULLIF(LTRIM(RTRIM(t.[ProgrammeCode])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(t.[LoyaltyTierCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE s
        SET s.[Qualification Basis Code]     = N'SPEND',
            s.[Qualification Currency Code]  = ISNULL(s.[Qualification Currency Code], N'USD'),
            s.[Qualification Period Months]  = ISNULL(s.[Qualification Period Months], 12),
            s.[Points Expiry Rule Code]      = N'MONTHS24',
            s.[Points Expiry Months]         = 24,
            s.[Downgrade Grace Months]       = ISNULL(s.[Downgrade Grace Months], 3),
            s.[Retention Amount]             = ISNULL(s.[Retention Amount],
                                                   CONVERT(DECIMAL(18, 2), s.[Qualification Amount] * 0.75)),
            s.[Requires Marketing Consent]   = 0,
            s.[Consent Basis Code]           = ISNULL(s.[Consent Basis Code], N'CONTRACT'),
            s.[Data Retention Months]        = 84
        FROM #LoyaltyTierSource AS s
        WHERE s.[Programme Code] = N'NA_REWARDS';

        UPDATE s
        SET s.[Qualification Basis Code]     = N'POINTS',
            s.[Qualification Currency Code]  = N'EUR',
            s.[Qualification Period Months]  = ISNULL(s.[Qualification Period Months], 12),
            s.[Points Expiry Rule Code]      = N'ENDNEXTYEAR',
            s.[Points Expiry Months]         = NULL,
            s.[Downgrade Grace Months]       = ISNULL(s.[Downgrade Grace Months], 0),
            s.[Requires Marketing Consent]   = 1,
            s.[Consent Basis Code]           = N'GDPR_CONSENT',
            s.[Data Retention Months]        = 24
        FROM #LoyaltyTierSource AS s
        WHERE s.[Programme Code] = N'EU_CLUB';

        /* APAC. Country programmes, local thresholds, and a consent basis that
           differs by market - notice-based where the source did not say. */
        UPDATE s
        SET s.[Qualification Basis Code]     = ISNULL(NULLIF(s.[Qualification Basis Code], N''), N'SPEND'),
            s.[Qualification Currency Code]  = ISNULL(s.[Qualification Currency Code],
                                                   CASE s.[Country Code]
                                                       WHEN N'JPN' THEN N'JPY'
                                                       WHEN N'AUS' THEN N'AUD'
                                                       WHEN N'SGP' THEN N'SGD'
                                                       WHEN N'HKG' THEN N'HKD'
                                                       WHEN N'NZL' THEN N'NZD'
                                                       ELSE N'SGD'
                                                   END),
            s.[Points Expiry Rule Code]      = ISNULL(s.[Points Expiry Rule Code], N'MONTHS12'),
            s.[Points Expiry Months]         = 12,
            s.[Qualification Period Months]  = ISNULL(s.[Qualification Period Months], 12),
            s.[Requires Marketing Consent]   = CASE WHEN s.[Country Code] IN (N'SGP', N'MYS', N'PHL')
                                                    THEN 1 ELSE 0 END,
            s.[Consent Basis Code]           = ISNULL(s.[Consent Basis Code], N'PDPA_NOTICE'),
            s.[Data Retention Months]        = 60
        FROM #LoyaltyTierSource AS s
        WHERE s.[Programme Code] LIKE N'APAC[_]%';

        /* Anything outside the three known programme families keeps whatever the
           source sent and is flagged, because nobody owns it. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Loyalty Tier',
               CONCAT(s.[Programme Code], N'/', s.[Loyalty Tier Code]), N'UNKNOWN_PROGRAMME',
               N'Loyalty programme code is not one of the three known programme families; regional rules not applied.',
               N'Reference', CONCAT(N'Region=', s.[Region Code], N'|Country=', s.[Country Code])
        FROM #LoyaltyTierSource AS s
        WHERE s.[Programme Code] NOT IN (N'NA_REWARDS', N'EU_CLUB')
          AND s.[Programme Code] NOT LIKE N'APAC[_]%';

        SET @RejectCount = @@ROWCOUNT;

        /* Rank gaps within a programme. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Loyalty Tier',
               g.[Programme Code], N'TIER_RANK_GAP',
               N'Tier ranks within the programme are not dense; the tier progression report will skip a level.',
               N'Reference', CONCAT(N'Ranks=', CONVERT(NVARCHAR(10), g.[Rank Count]),
                                    N'|Max=', CONVERT(NVARCHAR(10), g.[Maximum Rank]))
        FROM (
            SELECT [Programme Code],
                   COUNT_BIG(DISTINCT [Tier Rank]) AS [Rank Count],
                   MAX([Tier Rank])                AS [Maximum Rank]
            FROM #LoyaltyTierSource
            WHERE [Is Active] = 1
              AND [Tier Rank] IS NOT NULL
            GROUP BY [Programme Code]
        ) AS g
        WHERE g.[Rank Count] <> g.[Maximum Rank];

        SET @RejectCount = @RejectCount + @@ROWCOUNT;

        MERGE [Dimension].[Loyalty Tier] WITH (HOLDLOCK) AS tgt
        USING #LoyaltyTierSource AS src
            ON  tgt.[Programme Code]   = src.[Programme Code]
            AND tgt.[Loyalty Tier Code] = src.[Loyalty Tier Code]
            AND tgt.[Loyalty Tier Key] > 0
        WHEN MATCHED AND (tgt.[Row Hash Type 1] IS NULL
                          OR tgt.[Row Hash Type 1] <> HASHBYTES(N'SHA2_256',
                                CONCAT_WS(N'|', ISNULL(src.[Loyalty Tier], N''),
                                          ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Amount]), N''),
                                          ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Points]), N''),
                                          ISNULL(src.[Points Expiry Rule Code], N''),
                                          ISNULL(CONVERT(NVARCHAR(12), src.[Discount Percentage]), N''),
                                          ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N''))))
            THEN UPDATE SET
                  tgt.[Programme Name]               = src.[Programme Name]
                , tgt.[Loyalty Tier]                 = src.[Loyalty Tier]
                , tgt.[Tier Rank]                    = src.[Tier Rank]
                , tgt.[Region Code]                  = src.[Region Code]
                , tgt.[Country Code]                 = src.[Country Code]
                , tgt.[Qualification Basis Code]     = src.[Qualification Basis Code]
                , tgt.[Qualification Amount]         = src.[Qualification Amount]
                , tgt.[Qualification Currency Code]  = src.[Qualification Currency Code]
                , tgt.[Qualification Points]         = src.[Qualification Points]
                , tgt.[Qualification Period Months]  = src.[Qualification Period Months]
                , tgt.[Retention Amount]             = src.[Retention Amount]
                , tgt.[Downgrade Grace Months]       = src.[Downgrade Grace Months]
                , tgt.[Earn Rate Points Per Unit]    = src.[Earn Rate Points Per Unit]
                , tgt.[Redeem Rate Value Per Point]  = src.[Redeem Rate Value Per Point]
                , tgt.[Points Expiry Rule Code]      = src.[Points Expiry Rule Code]
                , tgt.[Points Expiry Months]         = src.[Points Expiry Months]
                , tgt.[Discount Percentage]          = src.[Discount Percentage]
                , tgt.[Free Shipping Threshold]      = src.[Free Shipping Threshold]
                , tgt.[Has Dedicated Support]        = src.[Has Dedicated Support]
                , tgt.[Has Early Access]             = src.[Has Early Access]
                , tgt.[Benefit Summary]              = src.[Benefit Summary]
                , tgt.[Requires Marketing Consent]   = src.[Requires Marketing Consent]
                , tgt.[Consent Basis Code]           = src.[Consent Basis Code]
                , tgt.[Data Retention Months]        = src.[Data Retention Months]
                , tgt.[Is Active]                    = src.[Is Active]
                , tgt.[Launched On]                  = src.[Launched On]
                , tgt.[Retired On]                   = src.[Retired On]
                , tgt.[Source System Code]           = @SourceSystemCode
                , tgt.[Row Hash Type 1]              = HASHBYTES(N'SHA2_256',
                        CONCAT_WS(N'|', ISNULL(src.[Loyalty Tier], N''),
                                  ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Amount]), N''),
                                  ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Points]), N''),
                                  ISNULL(src.[Points Expiry Rule Code], N''),
                                  ISNULL(CONVERT(NVARCHAR(12), src.[Discount Percentage]), N''),
                                  ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N'')))
                , tgt.[Last Load Batch Id]           = @BatchId
        WHEN NOT MATCHED BY TARGET THEN INSERT
            ([WWI Loyalty Tier ID], [Programme Code], [Programme Name], [Loyalty Tier Code],
             [Loyalty Tier], [Tier Rank], [Region Code], [Country Code],
             [Qualification Basis Code], [Qualification Amount], [Qualification Currency Code],
             [Qualification Points], [Qualification Period Months], [Retention Amount],
             [Downgrade Grace Months], [Earn Rate Points Per Unit], [Redeem Rate Value Per Point],
             [Points Expiry Rule Code], [Points Expiry Months], [Discount Percentage],
             [Free Shipping Threshold], [Has Dedicated Support], [Has Early Access],
             [Benefit Summary], [Requires Marketing Consent], [Consent Basis Code],
             [Data Retention Months], [Is Active], [Launched On], [Retired On],
             [Source System Code], [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (src.[WWI Loyalty Tier ID], src.[Programme Code], src.[Programme Name],
             src.[Loyalty Tier Code], ISNULL(src.[Loyalty Tier], src.[Loyalty Tier Code]),
             src.[Tier Rank], src.[Region Code], src.[Country Code],
             src.[Qualification Basis Code], src.[Qualification Amount],
             src.[Qualification Currency Code], src.[Qualification Points],
             src.[Qualification Period Months], src.[Retention Amount],
             src.[Downgrade Grace Months], src.[Earn Rate Points Per Unit],
             src.[Redeem Rate Value Per Point], src.[Points Expiry Rule Code],
             src.[Points Expiry Months], src.[Discount Percentage], src.[Free Shipping Threshold],
             src.[Has Dedicated Support], src.[Has Early Access], src.[Benefit Summary],
             src.[Requires Marketing Consent], src.[Consent Basis Code],
             src.[Data Retention Months], src.[Is Active], src.[Launched On], src.[Retired On],
             @SourceSystemCode,
             HASHBYTES(N'SHA2_256',
                 CONCAT_WS(N'|', ISNULL(src.[Loyalty Tier], N''),
                           ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Amount]), N''),
                           ISNULL(CONVERT(NVARCHAR(20), src.[Qualification Points]), N''),
                           ISNULL(src.[Points Expiry Rule Code], N''),
                           ISNULL(CONVERT(NVARCHAR(12), src.[Discount Percentage]), N''),
                           ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N''))),
             @Now, @HighDate, @LineageKey, @BatchId);

        SET @UpdatedCount  = (SELECT COUNT_BIG(*) FROM [Dimension].[Loyalty Tier]
                              WHERE [Last Load Batch Id] = @BatchId AND [Valid From] < @Now);
        SET @InsertedCount = (SELECT COUNT_BIG(*) FROM [Dimension].[Loyalty Tier]
                              WHERE [Last Load Batch Id] = @BatchId AND [Valid From] = @Now);

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Loyalty Tier', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1Merge', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Loyalty Tier',
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
             @SourceComponent    = N'Dimension.Loyalty Tier',
             @ProcedureName      = N'Integration.usp_MigrateStagedLoyaltyTierData',
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
