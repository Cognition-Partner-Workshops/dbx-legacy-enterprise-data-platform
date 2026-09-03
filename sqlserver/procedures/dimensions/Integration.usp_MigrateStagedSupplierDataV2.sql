/*
    Object        : [Integration].[usp_MigrateStagedSupplierDataV2]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Supplier, Dimension.Supplier, Dimension.Supplier Category,
                    Dimension.Payment Terms, Dimension.Geography,
                    Integration.DimensionLoadAudit, the etl control framework
    Called by     : DIM_Load_Supplier

    Type 2 on the commercial attributes, Type 1 on contact and sanction screening.

    Unlike the customer load this one uses a single MERGE for the close-out and a
    follow-up INSERT for the new version - the "two-step MERGE" pattern the 2009
    supplier project introduced and which was never applied anywhere else. It works
    because the supplier extract is a full snapshot with one row per supplier, so
    the same target row can never be touched twice.

    Supplier identity is the awkward part: the Microsoft sample keys on an integer
    [WWI Supplier ID] but the Oracle purchasing system keys on a six-character
    mnemonic. Both are carried and the match is on the mnemonic when it is present,
    which means a supplier that exists only in the sample data matches on the
    integer and a supplier that exists in both matches on the mnemonic. The
    resulting duplicate rows from the 2010 conversion are still in the dimension
    and are excluded by [Is Superseded Duplicate].
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedSupplierDataV2]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Load_Supplier',
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
    DECLARE @ClosedCount        BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @Type1Count         BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @WatermarkFrom      NVARCHAR(50);
    DECLARE @WatermarkTo        NVARCHAR(50);
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'DimensionLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        EXEC [etl].[usp_GetWatermark]
             @SourceSystemCode = @SourceSystemCode,
             @ObjectName       = N'Dimension.Supplier',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#SupplierSource') IS NOT NULL
            DROP TABLE #SupplierSource;

        SELECT
              s.[WWISupplierID]                     AS [WWI Supplier ID]
            , UPPER(LTRIM(RTRIM(s.[SupplierMnemonic]))) AS [Source Supplier Reference]
            , LTRIM(RTRIM(s.[SupplierName]))        AS [Supplier]
            , s.[CategoryCode]                      AS [Category Code]
            , s.[PrimaryContactName]                AS [Primary Contact]
            , s.[PaymentTermsCode]                  AS [Payment Terms Code]
            , s.[PaymentDays]                       AS [Payment Days]
            , s.[PostalCode]                        AS [Postal Code]
            , s.[CountryCode]                       AS [Country Code]
            , s.[RegionCode]                        AS [Region Code]
            , s.[BankCountryCode]                   AS [Bank Country Code]
            , s.[ApprovalStatusCode]                AS [Approval Status Code]
            , s.[RiskRatingCode]                    AS [Risk Rating Code]
            , s.[IsPreferred]                       AS [Is Preferred Supplier]
            , s.[IsSingleSource]                    AS [Is Single Source]
            , s.[ContractLeadTimeDays]              AS [Contract Lead Time Days]
            , s.[QualityRating]                     AS [Quality Rating]
            , s.[SanctionScreeningStatus]           AS [Sanction Screening Status]
            , s.[SanctionScreenedOn]                AS [Sanction Screened On]
            , s.[TaxpayerIdType]                    AS [Taxpayer Identification Type]
            , s.[VatRegistrationNumber]             AS [EU VAT Number]
            , s.[EoriNumber]                        AS [EORI Number]
            , s.[WithholdingTaxRate]                AS [Withholding Tax Rate]
            , s.[SourceChangedOn]                   AS [Source Changed On]
            , CONVERT(VARBINARY(32), NULL)          AS [Row Hash Type 2]
            , CONVERT(VARBINARY(32), NULL)          AS [Row Hash Type 1]
        INTO #SupplierSource
        FROM [stg].[Supplier] AS s
        WHERE (@RegionCode = N'GLOBAL' OR s.[RegionCode] = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* A supplier with no name and no mnemonic cannot be matched to anything. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT
              @PackageExecutionId
            , @BatchId
            , @SourceSystemCode
            , N'Dimension.Supplier'
            , CONVERT(NVARCHAR(200), s.[WWI Supplier ID])
            , N'UNIDENTIFIABLE_SUPPLIER'
            , N'Supplier has neither a name nor a purchasing mnemonic.'
            , N'Dimension'
            , CONCAT(N'Country=', s.[Country Code], N'; Category=', s.[Category Code])
        FROM #SupplierSource AS s
        WHERE NULLIF(LTRIM(RTRIM(s.[Supplier])), N'') IS NULL
          AND NULLIF(s.[Source Supplier Reference], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        DELETE FROM #SupplierSource
        WHERE NULLIF(LTRIM(RTRIM([Supplier])), N'') IS NULL
          AND NULLIF([Source Supplier Reference], N'') IS NULL;

        UPDATE #SupplierSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Supplier], N''), ISNULL([Category Code], N''),
                          ISNULL([Payment Terms Code], N''), ISNULL(CONVERT(NVARCHAR(10), [Payment Days]), N''),
                          ISNULL([Bank Country Code], N''), ISNULL([Approval Status Code], N''),
                          ISNULL([Risk Rating Code], N''), ISNULL(CONVERT(NVARCHAR(1), [Is Preferred Supplier]), N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Contract Lead Time Days]), N''))),
            [Row Hash Type 1] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Primary Contact], N''), ISNULL([Sanction Screening Status], N''),
                          ISNULL(CONVERT(NVARCHAR(30), [Sanction Screened On], 126), N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Quality Rating]), N'')));

        /* Step one of the two-step MERGE: close the superseded current rows. */
        MERGE [Dimension].[Supplier] AS tgt
        USING
        (
            SELECT s.*
            FROM #SupplierSource AS s
        ) AS src
            ON  (
                    (src.[Source Supplier Reference] IS NOT NULL AND tgt.[Source Supplier Reference] = src.[Source Supplier Reference])
                 OR (src.[Source Supplier Reference] IS NULL     AND tgt.[WWI Supplier ID]   = src.[WWI Supplier ID])
                )
            AND tgt.[Is Current Row] = 1
            AND tgt.[Supplier Key]   > 0
            AND ISNULL(tgt.[Is Superseded Duplicate], 0) = 0
        WHEN MATCHED AND tgt.[Row Hash Type 2] <> src.[Row Hash Type 2]
            THEN UPDATE SET
                  tgt.[Is Current Row]     = 0
                , tgt.[Effective To]       = ISNULL(src.[Source Changed On], @Now)
                , tgt.[Valid To]           = ISNULL(src.[Source Changed On], @Now)
                , tgt.[Last Load Batch Id] = @BatchId
        WHEN MATCHED AND tgt.[Row Hash Type 1] <> src.[Row Hash Type 1]
            THEN UPDATE SET
                  tgt.[Primary Contact]          = src.[Primary Contact]
                , tgt.[Sanction Screening Status] = src.[Sanction Screening Status]
                , tgt.[Sanction Screened On]     = src.[Sanction Screened On]
                , tgt.[Quality Rating]           = src.[Quality Rating]
                , tgt.[Row Hash Type 1]          = src.[Row Hash Type 1]
                , tgt.[Last Load Batch Id]       = @BatchId;

        SET @ClosedCount = @@ROWCOUNT;

        /* Step two: insert the new version for everything that is now uncovered. */
        INSERT INTO [Dimension].[Supplier]
            ([WWI Supplier ID], [Source Supplier Reference], [Supplier], [Category], [Supplier Category Key],
             [Primary Contact], [Payment Days], [Payment Terms Code], [Postal Code], [Country Code],
             [Region Code], [Bank Country Code], [Approval Status Code], [Risk Rating Code],
             [Is Preferred Supplier], [Is Single Source], [Contract Lead Time Days], [Quality Rating],
             [Sanction Screening Status], [Sanction Screened On], [Taxpayer Identification Type],
             [EU VAT Number], [EORI Number], [Withholding Tax Rate],
             [Source System Code], [Effective From], [Effective To], [Effective Sequence],
             [Is Current Row], [Version Number], [Row Hash Type 2], [Row Hash Type 1],
             [Is Inferred Member], [Is Superseded Duplicate], [Valid From], [Valid To],
             [Lineage Key], [Last Load Batch Id], [Last Load Package Execution Id])
        SELECT
              s.[WWI Supplier ID]
            , s.[Source Supplier Reference]
            , ISNULL(s.[Supplier], s.[Source Supplier Reference])
            , ISNULL(s.[Category Code], N'Unknown')
            , ISNULL(cat.[Supplier Category Key], -1)
            , ISNULL(s.[Primary Contact], N'Unknown')
            , ISNULL(s.[Payment Days], 0)
            , s.[Payment Terms Code]
            , ISNULL(s.[Postal Code], N'N/A')
            , s.[Country Code]
            , ISNULL(s.[Region Code], @RegionCode)
            , s.[Bank Country Code]
            , s.[Approval Status Code]
            , s.[Risk Rating Code]
            , s.[Is Preferred Supplier]
            , s.[Is Single Source]
            , s.[Contract Lead Time Days]
            , s.[Quality Rating]
            , s.[Sanction Screening Status]
            , s.[Sanction Screened On]
            , s.[Taxpayer Identification Type]
            , s.[EU VAT Number]
            , s.[EORI Number]
            , s.[Withholding Tax Rate]
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , s.[Row Hash Type 1]
            , 0
            , 0
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
            , @PackageExecutionId
        FROM #SupplierSource AS s
        LEFT OUTER JOIN [Dimension].[Supplier Category] AS cat
            ON cat.[Supplier Category Code] = s.[Category Code]
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Supplier] AS d
            WHERE d.[Supplier Key] > 0
              AND (
                    (s.[Source Supplier Reference] IS NOT NULL AND d.[Source Supplier Reference] = s.[Source Supplier Reference])
                 OR (s.[Source Supplier Reference] IS NULL     AND d.[WWI Supplier ID]   = s.[WWI Supplier ID])
                  )
        ) AS prior
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Supplier] AS cur
            WHERE cur.[Is Current Row] = 1
              AND cur.[Supplier Key]   > 0
              AND ISNULL(cur.[Is Superseded Duplicate], 0) = 0
              AND (
                    (s.[Source Supplier Reference] IS NOT NULL AND cur.[Source Supplier Reference] = s.[Source Supplier Reference])
                 OR (s.[Source Supplier Reference] IS NULL     AND cur.[WWI Supplier ID]   = s.[WWI Supplier ID])
                  )
        );

        SET @InsertedCount = @@ROWCOUNT;

        SELECT @Type1Count = COUNT_BIG(*)
        FROM [Dimension].[Supplier]
        WHERE [Last Load Batch Id] = @BatchId
          AND [Is Current Row]     = 1;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Supplier', @RegionCode, @BatchId, @PackageExecutionId,
             @SourceRowCount, @Type1Count, @ClosedCount, @InsertedCount, @RejectCount,
             N'HybridSCD', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Supplier',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @ClosedCount,
             @RejectRowCount     = @RejectCount;

        SELECT @WatermarkTo = CONVERT(NVARCHAR(50), MAX([Source Changed On]), 126)
        FROM #SupplierSource;

        IF @WatermarkTo IS NOT NULL
            EXEC [etl].[usp_SetWatermark]
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Supplier',
                 @WatermarkTo        = @WatermarkTo,
                 @PackageExecutionId = @PackageExecutionId;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @ClosedCount,
             @RowsRejected       = @RejectCount,
             @WatermarkFrom      = @WatermarkFrom,
             @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Supplier',
             @ProcedureName      = N'Integration.usp_MigrateStagedSupplierDataV2',
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
