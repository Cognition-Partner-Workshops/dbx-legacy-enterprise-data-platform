/*
    Object        : [Integration].[usp_MigrateStagedSalesChannelData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.SalesChannel, Dimension.Sales Channel,
                    the etl control framework
    Called by     : REF_Load_SalesChannel

    Type 1 MERGE on channel code plus region, because the same channel code is
    reused in more than one region with different behaviour - WEB is the own
    web store in NA and EU but a marketplace storefront in two APAC markets.

    The channel drives three things the fact loads depend on and that differ by
    region: the order number prefix (used to route an order back to its capture
    system), the returns policy code (EU is statutory 14 days regardless of what
    the channel says), and the sell-through reporting lag that makes APAC
    distributor revenue land up to nine days late.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedSalesChannelData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_SalesChannel',
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
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#SalesChannelSource') IS NOT NULL DROP TABLE #SalesChannelSource;

        SELECT
              UPPER(LTRIM(RTRIM(c.[SalesChannelCode])))     AS [Sales Channel Code]
            , UPPER(ISNULL(c.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , c.[WWISalesChannelID]                         AS [WWI Sales Channel ID]
            , c.[SalesChannelName]                          AS [Sales Channel]
            , UPPER(ISNULL(c.[ChannelGroupCode], N'DIRECT')) AS [Channel Group Code]
            , UPPER(ISNULL(c.[ChannelTypeCode], N'INSIDE')) AS [Channel Type Code]
            , UPPER(c.[OperatingCountryCode])               AS [Operating Country Code]
            , c.[PartnerName]                               AS [Partner Name]
            , ISNULL(c.[IsOwnChannel], 1)                   AS [Is Own Channel]
            , ISNULL(c.[IsMarketplace], 0)                  AS [Is Marketplace]
            , c.[MarketplaceCommissionPct]                  AS [Marketplace Commission Pct]
            , ISNULL(c.[ReportsSellThrough], 0)             AS [Reports Sell Through]
            , CONVERT(SMALLINT, c.[ReportingLagDays])       AS [Reporting Lag Days]
            , c.[OrderCaptureSystemCode]                    AS [Order Capture System Code]
            , LEFT(UPPER(c.[OrderNumberPrefix]), 5)         AS [Order Number Prefix]
            , UPPER(c.[DefaultPaymentMethodCode])           AS [Default Payment Method Code]
            , ISNULL(c.[AllowsBackorder], 1)                AS [Allows Backorder]
            , ISNULL(c.[AllowsPartialShipment], 1)          AS [Allows Partial Shipment]
            , UPPER(c.[ReturnsPolicyCode])                  AS [Returns Policy Code]
            , c.[PriceListCode]                             AS [Price List Code]
            , UPPER(c.[AttributionModelCode])               AS [Attribution Model Code]
            , ISNULL(c.[IsActive], 1)                       AS [Is Active]
            , c.[LaunchedOn]                                AS [Launched On]
            , c.[RetiredOn]                                 AS [Retired On]
        INTO #SalesChannelSource
        FROM [ref].[SalesChannel] AS c
        WHERE NULLIF(LTRIM(RTRIM(c.[SalesChannelCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE s
        SET s.[Returns Policy Code]     = ISNULL(s.[Returns Policy Code], N'30D'),
            s.[Attribution Model Code]  = ISNULL(s.[Attribution Model Code],
                                              CASE WHEN s.[Channel Type Code] = N'WEB'
                                                   THEN N'LASTCLICK' ELSE N'NONE' END),
            s.[Reporting Lag Days]      = ISNULL(s.[Reporting Lag Days], 0)
        FROM #SalesChannelSource AS s
        WHERE s.[Region Code] = N'NA';

        /* EU consumer rights: the statutory 14-day withdrawal applies to every
           distance-selling channel and overrides whatever the channel record
           says. Field sales is a face-to-face sale and is left alone. */
        UPDATE s
        SET s.[Returns Policy Code]     = CASE WHEN s.[Channel Type Code] = N'FIELD'
                                               THEN ISNULL(s.[Returns Policy Code], N'30D')
                                               ELSE N'14D' END,
            s.[Attribution Model Code]  = ISNULL(s.[Attribution Model Code], N'FIRSTCLICK'),
            s.[Reporting Lag Days]      = ISNULL(s.[Reporting Lag Days], 0)
        FROM #SalesChannelSource AS s
        WHERE s.[Region Code] = N'EU';

        UPDATE s
        SET s.[Returns Policy Code]     = ISNULL(s.[Returns Policy Code], N'7D'),
            s.[Is Own Channel]          = CASE WHEN s.[Channel Type Code] IN (N'MKTPL', N'DIST')
                                               THEN 0 ELSE s.[Is Own Channel] END,
            s.[Reports Sell Through]    = CASE WHEN s.[Channel Type Code] = N'DIST'
                                               THEN 1 ELSE s.[Reports Sell Through] END,
            s.[Reporting Lag Days]      = CASE WHEN s.[Channel Type Code] = N'DIST'
                                               THEN ISNULL(NULLIF(s.[Reporting Lag Days], 0), 9)
                                               ELSE ISNULL(s.[Reporting Lag Days], 0) END,
            s.[Attribution Model Code]  = ISNULL(s.[Attribution Model Code], N'NONE')
        FROM #SalesChannelSource AS s
        WHERE s.[Region Code] = N'APAC';

        /* A marketplace channel with no commission percentage understates cost
           of sale on every order it captures. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Sales Channel',
               CONCAT(s.[Sales Channel Code], N'/', s.[Region Code]), N'MARKETPLACE_NO_COMMISSION',
               N'Marketplace channel carries no commission percentage; channel cost of sale will be understated.',
               N'Reference', CONCAT(N'Partner=', s.[Partner Name])
        FROM #SalesChannelSource AS s
        WHERE s.[Is Marketplace] = 1
          AND ISNULL(s.[Marketplace Commission Pct], 0) = 0;

        SET @RejectCount = @@ROWCOUNT;

        MERGE [Dimension].[Sales Channel] WITH (HOLDLOCK) AS tgt
        USING #SalesChannelSource AS src
            ON  tgt.[Sales Channel Code] = src.[Sales Channel Code]
            AND tgt.[Region Code]        = src.[Region Code]
            AND tgt.[Sales Channel Key]  > 0
        WHEN MATCHED THEN UPDATE SET
              tgt.[WWI Sales Channel ID]        = src.[WWI Sales Channel ID]
            , tgt.[Sales Channel]               = ISNULL(src.[Sales Channel], src.[Sales Channel Code])
            , tgt.[Channel Group Code]          = src.[Channel Group Code]
            , tgt.[Channel Type Code]           = src.[Channel Type Code]
            , tgt.[Operating Country Code]      = src.[Operating Country Code]
            , tgt.[Partner Name]                = src.[Partner Name]
            , tgt.[Is Own Channel]              = src.[Is Own Channel]
            , tgt.[Is Marketplace]              = src.[Is Marketplace]
            , tgt.[Marketplace Commission Pct]  = src.[Marketplace Commission Pct]
            , tgt.[Reports Sell Through]        = src.[Reports Sell Through]
            , tgt.[Reporting Lag Days]          = src.[Reporting Lag Days]
            , tgt.[Order Capture System Code]   = src.[Order Capture System Code]
            , tgt.[Order Number Prefix]         = src.[Order Number Prefix]
            , tgt.[Default Payment Method Code] = src.[Default Payment Method Code]
            , tgt.[Allows Backorder]            = src.[Allows Backorder]
            , tgt.[Allows Partial Shipment]     = src.[Allows Partial Shipment]
            , tgt.[Returns Policy Code]         = src.[Returns Policy Code]
            , tgt.[Price List Code]             = src.[Price List Code]
            , tgt.[Attribution Model Code]      = src.[Attribution Model Code]
            , tgt.[Is Active]                   = src.[Is Active]
            , tgt.[Launched On]                 = src.[Launched On]
            , tgt.[Retired On]                  = src.[Retired On]
            , tgt.[Source System Code]          = @SourceSystemCode
            , tgt.[Row Hash Type 1]             = HASHBYTES(N'SHA2_256',
                    CONCAT_WS(N'|', ISNULL(src.[Sales Channel], N''),
                              ISNULL(src.[Channel Type Code], N''),
                              ISNULL(src.[Returns Policy Code], N''),
                              ISNULL(CONVERT(NVARCHAR(12), src.[Marketplace Commission Pct]), N''),
                              ISNULL(CONVERT(NVARCHAR(6), src.[Reporting Lag Days]), N''),
                              ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N'')))
            , tgt.[Last Load Batch Id]          = @BatchId
        WHEN NOT MATCHED BY TARGET THEN INSERT
            ([WWI Sales Channel ID], [Sales Channel Code], [Sales Channel], [Channel Group Code],
             [Channel Type Code], [Region Code], [Operating Country Code], [Partner Name],
             [Is Own Channel], [Is Marketplace], [Marketplace Commission Pct],
             [Reports Sell Through], [Reporting Lag Days], [Order Capture System Code],
             [Order Number Prefix], [Default Payment Method Code], [Allows Backorder],
             [Allows Partial Shipment], [Returns Policy Code], [Price List Code],
             [Attribution Model Code], [Is Active], [Launched On], [Retired On],
             [Source System Code], [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (src.[WWI Sales Channel ID], src.[Sales Channel Code],
             ISNULL(src.[Sales Channel], src.[Sales Channel Code]), src.[Channel Group Code],
             src.[Channel Type Code], src.[Region Code], src.[Operating Country Code],
             src.[Partner Name], src.[Is Own Channel], src.[Is Marketplace],
             src.[Marketplace Commission Pct], src.[Reports Sell Through],
             src.[Reporting Lag Days], src.[Order Capture System Code], src.[Order Number Prefix],
             src.[Default Payment Method Code], src.[Allows Backorder],
             src.[Allows Partial Shipment], src.[Returns Policy Code], src.[Price List Code],
             src.[Attribution Model Code], src.[Is Active], src.[Launched On], src.[Retired On],
             @SourceSystemCode,
             HASHBYTES(N'SHA2_256',
                 CONCAT_WS(N'|', ISNULL(src.[Sales Channel], N''),
                           ISNULL(src.[Channel Type Code], N''),
                           ISNULL(src.[Returns Policy Code], N''),
                           ISNULL(CONVERT(NVARCHAR(12), src.[Marketplace Commission Pct]), N''),
                           ISNULL(CONVERT(NVARCHAR(6), src.[Reporting Lag Days]), N''),
                           ISNULL(CONVERT(NVARCHAR(1), src.[Is Active]), N''))),
             @Now, @HighDate, @LineageKey, @BatchId);

        SET @UpdatedCount  = (SELECT COUNT_BIG(*) FROM [Dimension].[Sales Channel]
                              WHERE [Last Load Batch Id] = @BatchId AND [Valid From] < @Now);
        SET @InsertedCount = (SELECT COUNT_BIG(*) FROM [Dimension].[Sales Channel]
                              WHERE [Last Load Batch Id] = @BatchId AND [Valid From] = @Now);

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Sales Channel', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1Merge', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Sales Channel',
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
             @SourceComponent    = N'Dimension.Sales Channel',
             @ProcedureName      = N'Integration.usp_MigrateStagedSalesChannelData',
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
