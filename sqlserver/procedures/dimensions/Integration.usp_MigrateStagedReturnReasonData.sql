/*
    Object        : [Integration].[usp_MigrateStagedReturnReasonData]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.ReturnReason, Dimension.Return Reason,
                    the etl control framework
    Called by     : REF_Load_ReturnReason

    Type 1, and the only load in the dimensional layer that is genuinely
    row-by-row. The mapping from a source reason to the common reason group is
    done in a cursor with a long CASE because the APAC marketplace feeds send
    free text rather than codes, and the 2016 author wanted each unmapped text
    written to the reject table individually with the text in the payload so the
    returns team could work the list. Set-based rewrites have been proposed
    twice; the reject payload requirement killed both.

      NA_RMA        - two-digit agent-selected codes from the 1999 RMA screen.
      EU_STATUTORY  - consumer-rights withdrawal codes; a 14-day withdrawal is
                      its own reason and carries a mandatory full refund and no
                      restocking fee, whatever the source says.
      APAC_MKTPL    - per-marketplace text, banded here or marked UNMAPPED.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedReturnReasonData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_ReturnReason',
    @SourceSystemCode   NVARCHAR(20)  = N'FILE_RMA',
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

    /* Cursor variables. Named the way the 2016 author named them. */
    DECLARE @vReasonCode    NVARCHAR(15);
    DECLARE @vSourceSet     NVARCHAR(15);
    DECLARE @vReasonText    NVARCHAR(200);
    DECLARE @vRegion        NVARCHAR(10);
    DECLARE @vMarketplace   NVARCHAR(20);
    DECLARE @vGroup         NVARCHAR(15);
    DECLARE @vDisposition   NVARCHAR(10);
    DECLARE @vRefundObl     NVARCHAR(10);
    DECLARE @vRestockPct    DECIMAL(9, 4);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#ReturnReasonSource') IS NOT NULL DROP TABLE #ReturnReasonSource;

        SELECT
              UPPER(LTRIM(RTRIM(r.[ReturnReasonCode])))     AS [Return Reason Code]
            , UPPER(ISNULL(r.[ReasonSourceSet], N'NA_RMA')) AS [Reason Source Set]
            , r.[WWIReturnReasonID]                         AS [WWI Return Reason ID]
            , r.[ReturnReasonName]                          AS [Return Reason]
            , r.[SourceReasonText]                          AS [Source Reason Text]
            , UPPER(ISNULL(r.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , UPPER(r.[MarketplaceCode])                    AS [Marketplace Code]
            , ISNULL(r.[IsCustomerFault], 0)                AS [Is Customer Fault]
            , ISNULL(r.[IsSupplierChargeable], 0)           AS [Is Supplier Chargeable]
            , ISNULL(r.[IsCarrierChargeable], 0)            AS [Is Carrier Chargeable]
            , ISNULL(r.[IsQualityDefect], 0)                AS [Is Quality Defect]
            , ISNULL(r.[RequiresInspection], 0)             AS [Requires Inspection]
            , ISNULL(r.[RequiresPhotoEvidence], 0)          AS [Requires Photo Evidence]
            , CONVERT(SMALLINT, r.[ReturnWindowDays])       AS [Return Window Days]
            , r.[RestockingFeePercentage]                   AS [Restocking Fee Percentage]
            , ISNULL(r.[IsActive], 1)                       AS [Is Active]
            , CONVERT(NVARCHAR(15), NULL)                   AS [Reason Group Code]
            , CONVERT(NVARCHAR(10), NULL)                   AS [Disposition Code]
            , CONVERT(NVARCHAR(10), NULL)                   AS [Refund Obligation Code]
            , CONVERT(BIT, 0)                               AS [Is Statutory Withdrawal]
            , CONVERT(BIT, 0)                               AS [Quality Notification Required]
            , CONVERT(NVARCHAR(15), NULL)                   AS [Cost Allocation Code]
        INTO #ReturnReasonSource
        FROM [ref].[ReturnReason] AS r
        WHERE NULLIF(LTRIM(RTRIM(r.[ReturnReasonCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        DECLARE ReasonCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT [Return Reason Code], [Reason Source Set], [Source Reason Text],
                   [Region Code], [Marketplace Code], [Restocking Fee Percentage]
            FROM #ReturnReasonSource;

        OPEN ReasonCursor;
        FETCH NEXT FROM ReasonCursor
            INTO @vReasonCode, @vSourceSet, @vReasonText, @vRegion, @vMarketplace, @vRestockPct;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @vGroup       = NULL;
            SET @vDisposition = NULL;
            SET @vRefundObl   = NULL;

            IF @vSourceSet = N'NA_RMA'
            BEGIN
                SELECT @vGroup = CASE @vReasonCode
                                     WHEN N'01' THEN N'DAMAGE'
                                     WHEN N'02' THEN N'DAMAGE'
                                     WHEN N'03' THEN N'QUALITY'
                                     WHEN N'04' THEN N'WRONGITEM'
                                     WHEN N'05' THEN N'WRONGITEM'
                                     WHEN N'06' THEN N'LATE'
                                     WHEN N'07' THEN N'CHANGEDMIND'
                                     WHEN N'08' THEN N'CHANGEDMIND'
                                     WHEN N'12' THEN N'FRAUD'
                                     WHEN N'99' THEN N'UNMAPPED'   -- 'Other', used for a third of NA returns
                                     ELSE N'UNMAPPED'
                                 END;

                SELECT @vDisposition = CASE @vGroup
                                           WHEN N'DAMAGE'      THEN N'SCRAP'
                                           WHEN N'QUALITY'     THEN N'RTV'
                                           WHEN N'WRONGITEM'   THEN N'RESTOCK'
                                           WHEN N'LATE'        THEN N'RESTOCK'
                                           WHEN N'CHANGEDMIND' THEN N'RESTOCK'
                                           WHEN N'FRAUD'       THEN N'REWORK'
                                           ELSE N'RESTOCK'
                                       END;

                SET @vRefundObl = CASE WHEN @vGroup IN (N'DAMAGE', N'QUALITY', N'WRONGITEM')
                                       THEN N'FULL' ELSE N'PARTIAL' END;

                /* NA charges a restocking fee on change-of-mind returns unless
                   the source already set one. */
                IF @vGroup = N'CHANGEDMIND' AND ISNULL(@vRestockPct, 0) = 0
                    SET @vRestockPct = CONVERT(DECIMAL(9, 4), 15.0000);
            END
            ELSE IF @vSourceSet = N'EU_STATUTORY'
            BEGIN
                SELECT @vGroup = CASE
                                     WHEN @vReasonCode LIKE N'WD%'  THEN N'CHANGEDMIND'
                                     WHEN @vReasonCode LIKE N'DEF%' THEN N'QUALITY'
                                     WHEN @vReasonCode LIKE N'DMG%' THEN N'DAMAGE'
                                     WHEN @vReasonCode LIKE N'MIS%' THEN N'WRONGITEM'
                                     WHEN @vReasonCode LIKE N'DEL%' THEN N'LATE'
                                     ELSE N'UNMAPPED'
                                 END;

                /* Statutory withdrawal: full refund including outbound shipping,
                   no restocking fee, whatever the source file says. */
                SET @vRefundObl   = N'FULL';
                SET @vRestockPct  = CONVERT(DECIMAL(9, 4), 0);
                SET @vDisposition = CASE WHEN @vGroup IN (N'DAMAGE', N'QUALITY') THEN N'RTV' ELSE N'RESTOCK' END;
            END
            ELSE
            BEGIN
                /* APAC marketplaces. Free text, matched against the phrases the
                   returns team compiled by hand in 2018 and never revisited. */
                SELECT @vGroup = CASE
                                     WHEN @vReasonText LIKE N'%damag%'        THEN N'DAMAGE'
                                     WHEN @vReasonText LIKE N'%broken%'       THEN N'DAMAGE'
                                     WHEN @vReasonText LIKE N'%not as descr%' THEN N'WRONGITEM'
                                     WHEN @vReasonText LIKE N'%wrong%'        THEN N'WRONGITEM'
                                     WHEN @vReasonText LIKE N'%defect%'       THEN N'QUALITY'
                                     WHEN @vReasonText LIKE N'%quality%'      THEN N'QUALITY'
                                     WHEN @vReasonText LIKE N'%late%'         THEN N'LATE'
                                     WHEN @vReasonText LIKE N'%did not arriv%' THEN N'LATE'
                                     WHEN @vReasonText LIKE N'%change%mind%'  THEN N'CHANGEDMIND'
                                     WHEN @vReasonText LIKE N'%no longer%'    THEN N'CHANGEDMIND'
                                     ELSE N'UNMAPPED'
                                 END;

                SET @vDisposition = CASE WHEN @vGroup = N'DAMAGE' THEN N'SCRAP' ELSE N'RESTOCK' END;
                SET @vRefundObl   = CASE WHEN @vGroup = N'UNMAPPED' THEN N'CREDIT' ELSE N'FULL' END;

                IF @vGroup = N'UNMAPPED'
                BEGIN
                    INSERT INTO [etl].[RejectedRecord]
                        (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
                         RejectReasonCode, RejectReason, RejectStage, RecordPayload)
                    VALUES
                        (@PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Return Reason',
                         @vReasonCode, N'REASON_TEXT_UNMAPPED',
                         N'Marketplace return reason text does not match any known phrase; banded as UNMAPPED.',
                         N'Reference',
                         CONCAT(N'Marketplace=', @vMarketplace, N'|Text=', @vReasonText));

                    SET @RejectCount = @RejectCount + 1;
                END;
            END;

            UPDATE #ReturnReasonSource
            SET [Reason Group Code]        = @vGroup,
                [Disposition Code]         = @vDisposition,
                [Refund Obligation Code]   = @vRefundObl,
                [Restocking Fee Percentage] = @vRestockPct,
                [Is Statutory Withdrawal]  = CASE WHEN @vSourceSet = N'EU_STATUTORY'
                                                   AND @vReasonCode LIKE N'WD%' THEN 1 ELSE 0 END,
                [Quality Notification Required] = CASE WHEN @vGroup = N'QUALITY' THEN 1 ELSE 0 END,
                [Cost Allocation Code]     = CASE
                                                 WHEN @vGroup = N'QUALITY' THEN N'SUPPLIER'
                                                 WHEN @vGroup = N'DAMAGE'  THEN N'CARRIER'
                                                 WHEN @vGroup = N'CHANGEDMIND' THEN N'MARKETING'
                                                 ELSE N'COGS'
                                             END
            WHERE [Return Reason Code] = @vReasonCode
              AND [Reason Source Set]  = @vSourceSet;

            FETCH NEXT FROM ReasonCursor
                INTO @vReasonCode, @vSourceSet, @vReasonText, @vRegion, @vMarketplace, @vRestockPct;
        END;

        CLOSE ReasonCursor;
        DEALLOCATE ReasonCursor;

        UPDATE d
        SET d.[Return Reason]                 = s.[Return Reason],
            d.[Reason Group Code]             = s.[Reason Group Code],
            d.[Region Code]                   = s.[Region Code],
            d.[Marketplace Code]              = s.[Marketplace Code],
            d.[Source Reason Text]            = s.[Source Reason Text],
            d.[Is Customer Fault]             = s.[Is Customer Fault],
            d.[Is Supplier Chargeable]        = s.[Is Supplier Chargeable],
            d.[Is Carrier Chargeable]         = s.[Is Carrier Chargeable],
            d.[Is Quality Defect]             = s.[Is Quality Defect],
            d.[Is Statutory Withdrawal]       = s.[Is Statutory Withdrawal],
            d.[Refund Obligation Code]        = s.[Refund Obligation Code],
            d.[Restocking Fee Percentage]     = s.[Restocking Fee Percentage],
            d.[Disposition Code]              = s.[Disposition Code],
            d.[Requires Inspection]           = s.[Requires Inspection],
            d.[Requires Photo Evidence]       = s.[Requires Photo Evidence],
            d.[Quality Notification Required] = s.[Quality Notification Required],
            d.[Return Window Days]            = s.[Return Window Days],
            d.[Cost Allocation Code]          = s.[Cost Allocation Code],
            d.[Is Active]                     = s.[Is Active],
            d.[Source System Code]            = @SourceSystemCode,
            d.[Row Hash Type 1]               = HASHBYTES(N'SHA2_256',
                  CONCAT_WS(N'|', ISNULL(s.[Return Reason], N''), ISNULL(s.[Reason Group Code], N''),
                            ISNULL(s.[Disposition Code], N''), ISNULL(s.[Refund Obligation Code], N''),
                            ISNULL(CONVERT(NVARCHAR(12), s.[Restocking Fee Percentage]), N''),
                            ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N''))),
            d.[Last Load Batch Id]            = @BatchId
        FROM [Dimension].[Return Reason] AS d
        INNER JOIN #ReturnReasonSource AS s
            ON  s.[Return Reason Code] = d.[Return Reason Code]
            AND s.[Reason Source Set]  = d.[Reason Source Set]
        WHERE d.[Return Reason Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Return Reason]
            ([WWI Return Reason ID], [Return Reason Code], [Return Reason], [Reason Group Code],
             [Reason Source Set], [Region Code], [Marketplace Code], [Source Reason Text],
             [Is Customer Fault], [Is Supplier Chargeable], [Is Carrier Chargeable],
             [Is Quality Defect], [Is Statutory Withdrawal], [Refund Obligation Code],
             [Restocking Fee Percentage], [Disposition Code], [Requires Inspection],
             [Requires Photo Evidence], [Quality Notification Required], [Return Window Days],
             [Cost Allocation Code], [Is Active], [Source System Code], [Row Hash Type 1],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI Return Reason ID]
            , s.[Return Reason Code]
            , ISNULL(s.[Return Reason], s.[Return Reason Code])
            , s.[Reason Group Code]
            , s.[Reason Source Set]
            , s.[Region Code]
            , s.[Marketplace Code]
            , s.[Source Reason Text]
            , s.[Is Customer Fault]
            , s.[Is Supplier Chargeable]
            , s.[Is Carrier Chargeable]
            , s.[Is Quality Defect]
            , s.[Is Statutory Withdrawal]
            , s.[Refund Obligation Code]
            , s.[Restocking Fee Percentage]
            , s.[Disposition Code]
            , s.[Requires Inspection]
            , s.[Requires Photo Evidence]
            , s.[Quality Notification Required]
            , ISNULL(s.[Return Window Days],
                     CASE s.[Region Code] WHEN N'NA' THEN 30 WHEN N'EU' THEN 14 ELSE 7 END)
            , s.[Cost Allocation Code]
            , s.[Is Active]
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Return Reason], N''), ISNULL(s.[Reason Group Code], N''),
                          ISNULL(s.[Disposition Code], N''), ISNULL(s.[Refund Obligation Code], N''),
                          ISNULL(CONVERT(NVARCHAR(12), s.[Restocking Fee Percentage]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Active]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #ReturnReasonSource AS s
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Return Reason] AS d
                          WHERE d.[Return Reason Code] = s.[Return Reason Code]
                            AND d.[Reason Source Set]  = s.[Reason Source Set]
                            AND d.[Return Reason Key]  > 0);

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Return Reason', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, 0, @InsertedCount, @RejectCount, N'Type1RowByRow', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Return Reason',
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

        IF CURSOR_STATUS(N'local', N'ReasonCursor') >= 0
        BEGIN
            CLOSE ReasonCursor;
            DEALLOCATE ReasonCursor;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Return Reason',
             @ProcedureName      = N'Integration.usp_MigrateStagedReturnReasonData',
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
