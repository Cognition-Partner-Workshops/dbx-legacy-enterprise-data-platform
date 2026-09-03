/*
    Object        : [Integration].[usp_MigrateStagedPaymentTermsData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.PaymentTerms, Dimension.Payment Terms,
                    the etl control framework
    Called by     : REF_Load_PaymentTerms (weekly reference load)

    Type 1 overwrite with hand-written regional post-processing.

    The three regions do not agree on what a payment term is:

      NA   - net-day terms with an early settlement discount (2/10 net 30 and its
             relatives). No statutory ceiling, so late interest is contractual.
      EU   - the late payment directive caps B2B terms at 60 days unless the
             contract says otherwise, and the late interest basis is the ECB
             reference rate plus a margin. Terms longer than the cap are loaded
             and flagged, never silently truncated.
      APAC - proximo and instalment terms dominate (end of month plus n days,
             and 30/60/90 instalment splits), and several markets deduct
             withholding tax at settlement.

    The legacy four-character terms code from the 1998 order entry system is
    carried through because two Access reports still key on it.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedPaymentTermsData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_PaymentTerms',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_AR',
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
        IF OBJECT_ID(N'tempdb..#PaymentTermsSource') IS NOT NULL
            DROP TABLE #PaymentTermsSource;

        SELECT
              UPPER(LTRIM(RTRIM(t.[PaymentTermsCode])))     AS [Payment Terms Code]
            , t.[PaymentTermsName]                          AS [Payment Terms]
            , UPPER(ISNULL(t.[DueDateRuleCode], N'NET'))    AS [Due Date Rule Code]
            , CONVERT(SMALLINT, t.[NetDays])                AS [Net Days]
            , CONVERT(SMALLINT, t.[ProximoDay])             AS [Proximo Day]
            , CONVERT(SMALLINT, t.[ProximoMonthOffset])     AS [Proximo Month Offset]
            , CONVERT(SMALLINT, ISNULL(t.[InstalmentCount], 1)) AS [Instalment Count]
            , CONVERT(SMALLINT, t.[InstalmentIntervalDays]) AS [Instalment Interval Days]
            , t.[DiscountPercentage]                        AS [Discount Percentage]
            , CONVERT(SMALLINT, t.[DiscountDays])           AS [Discount Days]
            , CONVERT(SMALLINT, ISNULL(t.[GraceDays], 0))   AS [Grace Days]
            , UPPER(ISNULL(t.[AppliesToCode], N'BOTH'))     AS [Applies To Code]
            , UPPER(ISNULL(t.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , LEFT(UPPER(ISNULL(t.[LegacyTermsCode], N'')), 4) AS [Legacy Terms Code]
            , CONVERT(BIT, 0)                               AS [Is Statutory Maximum]
            , CONVERT(SMALLINT, NULL)                       AS [Statutory Maximum Days]
            , CONVERT(NVARCHAR(15), NULL)                   AS [Late Interest Basis Code]
            , CONVERT(DECIMAL(9, 4), NULL)                  AS [Late Interest Rate]
            , CONVERT(BIT, 0)                               AS [Withholding Applies]
            , ISNULL(t.[IsActive], 1)                       AS [Is Active]
        INTO #PaymentTermsSource
        FROM [stg].[PaymentTerms] AS t
        WHERE NULLIF(LTRIM(RTRIM(t.[PaymentTermsCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           NA. No statutory cap. Anything with a discount is treated as an
           early settlement term and the discount days are defaulted to 10
           when the source leaves them blank, which it usually does.
           --------------------------------------------------------------- */
        UPDATE s
        SET s.[Discount Days]            = CASE
                                               WHEN s.[Discount Percentage] > 0
                                                    AND ISNULL(s.[Discount Days], 0) = 0 THEN 10
                                               ELSE s.[Discount Days]
                                           END,
            s.[Late Interest Basis Code] = N'CONTRACT',
            s.[Late Interest Rate]       = CONVERT(DECIMAL(9, 4), 0.0150),
            s.[Grace Days]               = ISNULL(s.[Grace Days], 5)
        FROM #PaymentTermsSource AS s
        WHERE s.[Region Code] = N'NA';

        /* ---------------------------------------------------------------
           EU. Late payment directive: 60 day B2B ceiling, interest at the
           ECB reference rate plus eight points. Terms beyond the ceiling are
           kept but marked, because sales has signed several of them.
           --------------------------------------------------------------- */
        UPDATE s
        SET s.[Is Statutory Maximum]     = CASE WHEN ISNULL(s.[Net Days], 0) > 60 THEN 1 ELSE 0 END,
            s.[Statutory Maximum Days]   = 60,
            s.[Late Interest Basis Code] = N'ECB_PLUS_8',
            s.[Late Interest Rate]       = CONVERT(DECIMAL(9, 4), 0.0800),
            s.[Grace Days]               = 0
        FROM #PaymentTermsSource AS s
        WHERE s.[Region Code] = N'EU';

        /* ---------------------------------------------------------------
           APAC. Proximo and instalment terms, withholding on services in the
           markets that operate it. The withholding flag is derived from the
           terms code prefix because the source has never carried it.
           --------------------------------------------------------------- */
        UPDATE s
        SET s.[Proximo Day]              = CASE
                                               WHEN s.[Due Date Rule Code] = N'PROX'
                                                    AND ISNULL(s.[Proximo Day], 0) = 0 THEN 25
                                               ELSE s.[Proximo Day]
                                           END,
            s.[Instalment Interval Days] = CASE
                                               WHEN s.[Instalment Count] > 1
                                                    AND ISNULL(s.[Instalment Interval Days], 0) = 0 THEN 30
                                               ELSE s.[Instalment Interval Days]
                                           END,
            s.[Withholding Applies]      = CASE
                                               WHEN LEFT(s.[Payment Terms Code], 3) IN (N'WHT', N'SVC') THEN 1
                                               ELSE 0
                                           END,
            s.[Late Interest Basis Code] = N'LOCAL_STAT',
            s.[Late Interest Rate]       = CONVERT(DECIMAL(9, 4), 0.1000)
        FROM #PaymentTermsSource AS s
        WHERE s.[Region Code] = N'APAC';

        /* A discount window longer than the net term is a source data error and
           would produce a negative discount period in the AR aging. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Payment Terms',
               s.[Payment Terms Code], N'DISCOUNT_WINDOW_EXCEEDS_TERM',
               N'Discount days exceed net days; discount window ignored downstream.',
               N'Reference', CONCAT(N'NetDays=', s.[Net Days], N'|DiscountDays=', s.[Discount Days])
        FROM #PaymentTermsSource AS s
        WHERE ISNULL(s.[Discount Days], 0) > ISNULL(s.[Net Days], 0)
          AND s.[Due Date Rule Code] = N'NET';

        SET @RejectCount = @@ROWCOUNT;

        DELETE s
        FROM #PaymentTermsSource AS s
        WHERE ISNULL(s.[Discount Days], 0) > ISNULL(s.[Net Days], 0)
          AND s.[Due Date Rule Code] = N'NET';

        UPDATE d
        SET d.[Payment Terms]            = s.[Payment Terms],
            d.[Due Date Rule Code]       = s.[Due Date Rule Code],
            d.[Net Days]                 = s.[Net Days],
            d.[Proximo Day]              = s.[Proximo Day],
            d.[Proximo Month Offset]     = s.[Proximo Month Offset],
            d.[Instalment Count]         = s.[Instalment Count],
            d.[Instalment Interval Days] = s.[Instalment Interval Days],
            d.[Discount Percentage]      = s.[Discount Percentage],
            d.[Discount Days]            = s.[Discount Days],
            d.[Grace Days]               = s.[Grace Days],
            d.[Applies To Code]          = s.[Applies To Code],
            d.[Region Code]              = s.[Region Code],
            d.[Is Statutory Maximum]     = s.[Is Statutory Maximum],
            d.[Statutory Maximum Days]   = s.[Statutory Maximum Days],
            d.[Late Interest Basis Code] = s.[Late Interest Basis Code],
            d.[Late Interest Rate]       = s.[Late Interest Rate],
            d.[Withholding Applies]      = s.[Withholding Applies],
            d.[Legacy Terms Code]        = NULLIF(s.[Legacy Terms Code], N''),
            d.[Is Active]                = s.[Is Active],
            d.[Source System Code]       = @SourceSystemCode,
            d.[Row Hash Type 1]          = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Payment Terms], N''),
                            ISNULL(s.[Due Date Rule Code], N''),
                            ISNULL(CONVERT(NVARCHAR(6), s.[Net Days]), N''),
                            ISNULL(CONVERT(NVARCHAR(12), s.[Discount Percentage]), N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Withholding Applies]), N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))),
            d.[Last Load Batch Id]       = @BatchId
        FROM [Dimension].[Payment Terms] AS d
        INNER JOIN #PaymentTermsSource AS s
            ON s.[Payment Terms Code] = d.[Payment Terms Code]
        WHERE d.[Payment Terms Key] > 0
          AND (d.[Row Hash Type 1] IS NULL
               OR d.[Row Hash Type 1] <> HASHBYTES(N'SHA2_256',
                    CONCAT_WS(N'|', ISNULL(s.[Payment Terms], N''),
                              ISNULL(s.[Due Date Rule Code], N''),
                              ISNULL(CONVERT(NVARCHAR(6), s.[Net Days]), N''),
                              ISNULL(CONVERT(NVARCHAR(12), s.[Discount Percentage]), N''),
                              ISNULL(CONVERT(NVARCHAR(1), s.[Withholding Applies]), N''),
                              ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))));

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Payment Terms]
            ([Payment Terms Code], [Payment Terms], [Due Date Rule Code], [Net Days],
             [Proximo Day], [Proximo Month Offset], [Instalment Count],
             [Instalment Interval Days], [Discount Percentage], [Discount Days],
             [Grace Days], [Applies To Code], [Region Code], [Is Statutory Maximum],
             [Statutory Maximum Days], [Late Interest Basis Code], [Late Interest Rate],
             [Withholding Applies], [Legacy Terms Code], [Is Active], [Source System Code],
             [Row Hash Type 1], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[Payment Terms Code]
            , ISNULL(s.[Payment Terms], s.[Payment Terms Code])
            , s.[Due Date Rule Code]
            , s.[Net Days]
            , s.[Proximo Day]
            , s.[Proximo Month Offset]
            , s.[Instalment Count]
            , s.[Instalment Interval Days]
            , s.[Discount Percentage]
            , s.[Discount Days]
            , s.[Grace Days]
            , s.[Applies To Code]
            , s.[Region Code]
            , s.[Is Statutory Maximum]
            , s.[Statutory Maximum Days]
            , s.[Late Interest Basis Code]
            , s.[Late Interest Rate]
            , s.[Withholding Applies]
            , NULLIF(s.[Legacy Terms Code], N'')
            , s.[Is Active]
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Payment Terms], N''),
                          ISNULL(s.[Due Date Rule Code], N''),
                          ISNULL(CONVERT(NVARCHAR(6), s.[Net Days]), N''),
                          ISNULL(CONVERT(NVARCHAR(12), s.[Discount Percentage]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Withholding Applies]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #PaymentTermsSource AS s
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Payment Terms] AS d
                          WHERE d.[Payment Terms Code] = s.[Payment Terms Code]
                            AND d.[Payment Terms Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Payment Terms', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1UpdateInsert', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Payment Terms',
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
             @SourceComponent    = N'Dimension.Payment Terms',
             @ProcedureName      = N'Integration.usp_MigrateStagedPaymentTermsData',
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
