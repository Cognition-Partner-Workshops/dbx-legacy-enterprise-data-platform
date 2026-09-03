/*
    Object        : [Integration].[usp_MigrateStagedCustomerSegmentData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.CustomerSegment, Dimension.Customer Segment,
                    Dimension.Customer, the etl control framework
    Called by     : DIM_Load_CustomerSegment

    Type 2 segment definitions, plus the reassignment of customers to segments.
    The definitions change when marketing re-cuts the model; the assignment is a
    Type 1 attribute on the customer row and is overwritten in place, which is why
    a sale made in 2019 reports under the customer's segment today rather than the
    segment they were in at the time. Marketing knows and prefers it.

    The regional divergence here is consent, not scoring. EU segments that require
    profiling consent may only be assigned to customers who granted it; customers
    who did not are pushed to the segment family default. APAC marketplace
    customers are excluded from modelling entirely because the marketplace does
    not pass through the identifiers. NA has no gate at all.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCustomerSegmentData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @PackageName        NVARCHAR(200) = N'DIM_Load_CustomerSegment',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_SLS',
    @AssignCustomers    BIT           = 1,
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
    DECLARE @AssignedCount      BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#SegmentSource') IS NOT NULL
            DROP TABLE #SegmentSource;

        SELECT
              g.[SegmentCode]               AS [Segment Code]
            , g.[WWICustomerSegmentID]      AS [WWI Customer Segment ID]
            , g.[SegmentName]               AS [Customer Segment]
            , g.[SegmentFamilyCode]         AS [Segment Family Code]
            , g.[ScoringModelCode]          AS [Scoring Model Code]
            , g.[ScoringModelVersion]       AS [Scoring Model Version]
            , g.[ScoringFrequencyCode]      AS [Scoring Frequency Code]
            , g.[MinimumScore]              AS [Minimum Score]
            , g.[MaximumScore]              AS [Maximum Score]
            , g.[RecencyBand]               AS [Recency Band]
            , g.[FrequencyBand]             AS [Frequency Band]
            , g.[MonetaryBand]              AS [Monetary Band]
            , g.[ChurnRiskBand]             AS [Churn Risk Band]
            , g.[LifetimeValueBand]         AS [Lifetime Value Band]
            , g.[TargetContactFrequency]    AS [Target Contact Frequency]
            , g.[RequiresProfilingConsent]  AS [Requires Profiling Consent]
            , g.[MarketplaceOnly]           AS [Marketplace Only]
            , g.[LastScoredOn]              AS [Last Scored On]
            , g.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(BIT, 0)               AS [Excluded From Modelling]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
        INTO #SegmentSource
        FROM [stg].[CustomerSegment] AS g
        WHERE g.[RegionCode] = @RegionCode;

        SET @SourceRowCount = @@ROWCOUNT;

        /* Overlapping score ranges inside a model family assign customers twice. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Customer Segment',
               a.[Segment Code], N'OVERLAPPING_SCORE_RANGE',
               N'Score range overlaps another segment in the same model; assignment is not deterministic.',
               N'Dimension', CONCAT(N'Overlaps=', b.[Segment Code])
        FROM #SegmentSource AS a
        INNER JOIN #SegmentSource AS b
            ON  b.[Scoring Model Code] = a.[Scoring Model Code]
            AND b.[Segment Code]      <> a.[Segment Code]
            AND a.[Minimum Score]     <= b.[Maximum Score]
            AND b.[Minimum Score]     <= a.[Maximum Score]
        WHERE a.[Segment Code] < b.[Segment Code];

        SET @RejectCount = @@ROWCOUNT;

        IF @RegionCode = N'EU'
            UPDATE #SegmentSource
            SET [Requires Profiling Consent] = 1
            WHERE [Scoring Model Code] LIKE N'BEHAV%';

        IF @RegionCode = N'APAC'
            UPDATE #SegmentSource
            SET [Excluded From Modelling] = 1
            WHERE [Marketplace Only] = 1;

        UPDATE #SegmentSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Customer Segment], N''), ISNULL([Scoring Model Code], N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Scoring Model Version]), N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Minimum Score]), N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Maximum Score]), N''),
                          ISNULL([Recency Band], N''), ISNULL([Frequency Band], N''),
                          ISNULL([Monetary Band], N''), ISNULL([Churn Risk Band], N'')));

        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Customer Segment] AS d
        INNER JOIN #SegmentSource AS s
            ON  s.[Segment Code] = d.[Segment Code]
            AND d.[Region Code]  = @RegionCode
        WHERE d.[Is Current Row]      = 1
          AND d.[Customer Segment Key] > 0
          AND d.[Row Hash Type 2]    <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Customer Segment]
            ([WWI Customer Segment ID], [Customer Segment], [Segment Code], [Segment Family Code],
             [Region Code], [Scoring Model Code], [Scoring Model Version], [Scoring Frequency Code],
             [Minimum Score], [Maximum Score], [Recency Band], [Frequency Band], [Monetary Band],
             [Churn Risk Band], [Lifetime Value Band], [Target Contact Frequency],
             [Requires Profiling Consent], [Marketplace Only], [Excluded From Modelling],
             [Last Scored On], [Source System Code], [Effective From], [Effective To],
             [Effective From Date], [Effective Sequence], [Is Current Row], [Version Number],
             [Row Hash Type 2], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Customer Segment ID]
            , ISNULL(s.[Customer Segment], s.[Segment Code])
            , s.[Segment Code]
            , s.[Segment Family Code]
            , @RegionCode
            , s.[Scoring Model Code]
            , s.[Scoring Model Version]
            , s.[Scoring Frequency Code]
            , s.[Minimum Score]
            , s.[Maximum Score]
            , s.[Recency Band]
            , s.[Frequency Band]
            , s.[Monetary Band]
            , s.[Churn Risk Band]
            , s.[Lifetime Value Band]
            , s.[Target Contact Frequency]
            , ISNULL(s.[Requires Profiling Consent], 0)
            , ISNULL(s.[Marketplace Only], 0)
            , s.[Excluded From Modelling]
            , s.[Last Scored On]
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
            , 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #SegmentSource AS s
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Customer Segment] AS d
            WHERE d.[Segment Code]        = s.[Segment Code]
              AND d.[Region Code]         = @RegionCode
              AND d.[Customer Segment Key] > 0
        ) AS prior
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Customer Segment] AS cur
            WHERE cur.[Segment Code]         = s.[Segment Code]
              AND cur.[Region Code]          = @RegionCode
              AND cur.[Is Current Row]       = 1
              AND cur.[Customer Segment Key] > 0
        );

        SET @InsertedCount = @@ROWCOUNT;

        IF @AssignCustomers = 1
        BEGIN
            /*
                Assignment. Consent-gated segments fall back to the family default
                segment, which is by convention the segment whose code ends in
                '_DEF'. If the family has no default the customer keeps the
                not-applicable member (-2), never NULL.
            */
            UPDATE c
            SET c.[Customer Segment Key] =
                    CASE
                        WHEN seg.[Requires Profiling Consent] = 1
                             AND ISNULL(c.[Profiling Consent Flag], 0) = 0
                        THEN ISNULL(dflt.[Customer Segment Key], -2)
                        WHEN seg.[Excluded From Modelling] = 1 THEN -2
                        ELSE seg.[Customer Segment Key]
                    END,
                c.[Last Load Batch Id] = @BatchId
            FROM [Dimension].[Customer] AS c
            INNER JOIN [stg].[CustomerSegmentAssignment] AS a
                ON a.[WWICustomerID] = c.[WWI Customer ID]
            INNER JOIN [Dimension].[Customer Segment] AS seg
                ON  seg.[Segment Code]   = a.[SegmentCode]
                AND seg.[Region Code]    = @RegionCode
                AND seg.[Is Current Row] = 1
            LEFT OUTER JOIN [Dimension].[Customer Segment] AS dflt
                ON  dflt.[Segment Family Code] = seg.[Segment Family Code]
                AND dflt.[Region Code]         = @RegionCode
                AND dflt.[Is Current Row]      = 1
                AND dflt.[Segment Code] LIKE N'%[_]DEF'
            WHERE c.[Is Current Row] = 1
              AND c.[Region Code]    = @RegionCode
              AND c.[Customer Key]   > 0;

            SET @AssignedCount = @@ROWCOUNT;
        END;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Customer Segment', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             @AssignedCount, @ClosedCount, @InsertedCount, @RejectCount, N'Type2', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Customer Segment',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @AssignedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @AssignedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Customer Segment',
             @ProcedureName      = N'Integration.usp_MigrateStagedCustomerSegmentData',
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
