/*
    Object        : [Integration].[usp_MigrateStagedVendorContractData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.VendorContract, Dimension.Vendor Contract, Dimension.Supplier,
                    the etl control framework
    Called by     : DIM_Load_VendorContract

    Type 2 at amendment grain. Procurement issues amendments, not new contracts,
    and several amendments can be signed on the same day - typically a price
    schedule and a term extension raised by two buyers. The same-day ordering is
    therefore taken from the amendment number, not from the timestamp, because
    the Oracle extract stamps every row of a day's batch with the batch time.

    [Effective Sequence] is the amendment number within the day. Two amendments
    with the same number on the same day is a genuine procurement error and is
    rejected rather than guessed at.

    Regional divergence is contractual: NA carries a liquidated damages cap, EU
    carries statutory late-payment interest and a termination notice period, APAC
    carries local content percentage and stamp duty. The price protection code and
    the FX collar apply everywhere but are only populated where the contract
    currency differs from the supplier's settlement currency.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedVendorContractData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @PackageName        NVARCHAR(200) = N'DIM_Load_VendorContract',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_PROC',
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
             @ObjectName       = N'Dimension.Vendor Contract',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#ContractSource') IS NOT NULL
            DROP TABLE #ContractSource;

        SELECT
              c.[ContractNumber]            AS [Contract Number]
            , c.[AmendmentNumber]           AS [Amendment Number]
            , c.[AmendmentReasonCode]       AS [Amendment Reason Code]
            , c.[ContractTitle]             AS [Contract Title]
            , c.[ContractTypeCode]          AS [Contract Type Code]
            , c.[ContractStatusCode]        AS [Contract Status Code]
            , c.[SupplierMnemonic]          AS [Source Supplier Reference]
            , c.[ContractStartDate]         AS [Contract Start Date]
            , c.[ContractEndDate]           AS [Contract End Date]
            , c.[SignedOn]                  AS [Signed On]
            , c.[SignedByEmployeeNumber]    AS [Signed By Employee Number]
            , c.[ContractCurrencyCode]      AS [Contract Currency Code]
            , c.[CommittedSpendAmount]      AS [Committed Spend Amount]
            , c.[MinimumOrderValue]         AS [Minimum Order Value]
            , c.[PaymentTermsCode]          AS [Payment Terms Code]
            , c.[PriceProtectionCode]       AS [Price Protection Code]
            , c.[PriceIndexReference]       AS [Price Index Reference]
            , c.[FxCollarLowerRate]         AS [FX Collar Lower Rate]
            , c.[FxCollarUpperRate]         AS [FX Collar Upper Rate]
            , c.[RebateTier1Threshold]      AS [Rebate Tier 1 Threshold]
            , c.[RebateTier1Percentage]     AS [Rebate Tier 1 Percentage]
            , c.[RebateTier2Threshold]      AS [Rebate Tier 2 Threshold]
            , c.[RebateTier2Percentage]     AS [Rebate Tier 2 Percentage]
            , c.[ServiceLevelTargetPct]     AS [Service Level Target Pct]
            , c.[PenaltyClauseCode]         AS [Penalty Clause Code]
            , c.[PenaltyRate]               AS [Penalty Rate]
            , c.[AutoRenewFlag]             AS [Auto Renew Flag]
            , c.[RenewalNoticeDays]         AS [Renewal Notice Days]
            , c.[GoverningLawCountryCode]   AS [Governing Law Country Code]
            , c.[DocumentReference]         AS [Document Reference]
            , c.[LiquidatedDamagesCap]      AS [NA Liquidated Damages Cap]
            , c.[StatutoryInterestApplies]  AS [EU Statutory Interest Applies]
            , c.[TerminationNoticeDays]     AS [EU Termination Notice Days]
            , c.[LocalContentPercentage]    AS [APAC Local Content Percentage]
            , c.[StampDutyAmount]           AS [APAC Stamp Duty Amount]
            , c.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
        INTO #ContractSource
        FROM [stg].[VendorContract] AS c
        WHERE c.[RegionCode] = @RegionCode
          AND c.[SourceChangedOn] > CONVERT(DATETIME2(7), @WatermarkFrom);

        SET @SourceRowCount = @@ROWCOUNT;

        /* Two amendments with the same number on one contract cannot be ordered. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Vendor Contract',
               CONCAT(s.[Contract Number], N'/', CONVERT(NVARCHAR(10), s.[Amendment Number])),
               N'DUPLICATE_AMENDMENT_NUMBER',
               N'Contract has more than one amendment with the same number; ordering is ambiguous.',
               N'Dimension', CONCAT(N'Occurrences=', CONVERT(NVARCHAR(10), COUNT_BIG(*)))
        FROM #ContractSource AS s
        GROUP BY s.[Contract Number], s.[Amendment Number]
        HAVING COUNT_BIG(*) > 1;

        SET @RejectCount = @@ROWCOUNT;

        DELETE s
        FROM #ContractSource AS s
        WHERE EXISTS
        (
            SELECT 1
            FROM #ContractSource AS d
            WHERE d.[Contract Number]  = s.[Contract Number]
              AND d.[Amendment Number] = s.[Amendment Number]
            GROUP BY d.[Contract Number], d.[Amendment Number]
            HAVING COUNT_BIG(*) > 1
        );

        /* Regional clause conditioning. */
        IF @RegionCode = N'EU'
            UPDATE #ContractSource
            SET [EU Statutory Interest Applies] = 1,
                -- the late payment directive overrides anything longer than 60 days
                [EU Termination Notice Days]    = ISNULL([EU Termination Notice Days], 90),
                [NA Liquidated Damages Cap]     = NULL,
                [APAC Local Content Percentage] = NULL,
                [APAC Stamp Duty Amount]        = NULL;

        IF @RegionCode = N'NA'
            UPDATE #ContractSource
            SET [EU Statutory Interest Applies] = NULL,
                [EU Termination Notice Days]    = NULL,
                [APAC Local Content Percentage] = NULL,
                [APAC Stamp Duty Amount]        = NULL;

        IF @RegionCode = N'APAC'
            UPDATE #ContractSource
            SET [EU Statutory Interest Applies] = NULL,
                [EU Termination Notice Days]    = NULL,
                [NA Liquidated Damages Cap]     = NULL,
                [APAC Local Content Percentage] = ISNULL([APAC Local Content Percentage], 0);

        /* The FX collar only means anything on a cross-currency contract. */
        UPDATE c
        SET c.[FX Collar Lower Rate] = NULL,
            c.[FX Collar Upper Rate] = NULL
        FROM #ContractSource AS c
        INNER JOIN [Dimension].[Supplier] AS s
            ON  s.[Source Supplier Reference] = c.[Source Supplier Reference]
            AND s.[Is Current Row]            = 1
        WHERE ISNULL(s.[Settlement Currency Code], N'') = ISNULL(c.[Contract Currency Code], N'');

        UPDATE #ContractSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Contract Title], N''), ISNULL([Contract Status Code], N''),
                          ISNULL(CONVERT(NVARCHAR(30), [Contract End Date], 126), N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Committed Spend Amount]), N''),
                          ISNULL([Payment Terms Code], N''), ISNULL([Price Protection Code], N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Rebate Tier 1 Percentage]), N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Rebate Tier 2 Percentage]), N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Service Level Target Pct]), N''),
                          ISNULL(CONVERT(NVARCHAR(10), [Amendment Number]), N'')));

        /* Close the current version of any contract that has a newer amendment. */
        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Signed On], ISNULL(s.[Source Changed On], @Now)),
            d.[Valid To]           = ISNULL(s.[Signed On], ISNULL(s.[Source Changed On], @Now)),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Vendor Contract] AS d
        INNER JOIN #ContractSource AS s
            ON s.[Contract Number] = d.[Contract Number]
        WHERE d.[Is Current Row]     = 1
          AND d.[Vendor Contract Key] > 0
          AND s.[Amendment Number]   > ISNULL(d.[Amendment Number], -1);

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Vendor Contract]
            ([Contract Number], [Amendment Number], [Amendment Reason Code], [Contract Title],
             [Contract Type Code], [Contract Status Code], [Supplier Key], [Source Supplier Reference],
             [Region Code], [Contract Start Date], [Contract End Date], [Signed On],
             [Signed By Employee Number], [Contract Currency Code], [Committed Spend Amount],
             [Minimum Order Value], [Payment Terms Code], [Price Protection Code],
             [Price Index Reference], [FX Collar Lower Rate], [FX Collar Upper Rate],
             [Rebate Tier 1 Threshold], [Rebate Tier 1 Percentage], [Rebate Tier 2 Threshold],
             [Rebate Tier 2 Percentage], [Service Level Target Pct], [Penalty Clause Code],
             [Penalty Rate], [Auto Renew Flag], [Renewal Notice Days], [Governing Law Country Code],
             [Document Reference], [NA Liquidated Damages Cap], [EU Statutory Interest Applies],
             [EU Termination Notice Days], [APAC Local Content Percentage], [APAC Stamp Duty Amount],
             [Source System Code], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[Contract Number]
            , s.[Amendment Number]
            , s.[Amendment Reason Code]
            , s.[Contract Title]
            , s.[Contract Type Code]
            , s.[Contract Status Code]
            , ISNULL(sup.[Supplier Key], -1)
            , s.[Source Supplier Reference]
            , @RegionCode
            , s.[Contract Start Date]
            , s.[Contract End Date]
            , s.[Signed On]
            , s.[Signed By Employee Number]
            , s.[Contract Currency Code]
            , s.[Committed Spend Amount]
            , s.[Minimum Order Value]
            , s.[Payment Terms Code]
            , s.[Price Protection Code]
            , s.[Price Index Reference]
            , s.[FX Collar Lower Rate]
            , s.[FX Collar Upper Rate]
            , s.[Rebate Tier 1 Threshold]
            , s.[Rebate Tier 1 Percentage]
            , s.[Rebate Tier 2 Threshold]
            , s.[Rebate Tier 2 Percentage]
            , s.[Service Level Target Pct]
            , s.[Penalty Clause Code]
            , s.[Penalty Rate]
            , s.[Auto Renew Flag]
            , s.[Renewal Notice Days]
            , s.[Governing Law Country Code]
            , s.[Document Reference]
            , s.[NA Liquidated Damages Cap]
            , s.[EU Statutory Interest Applies]
            , s.[EU Termination Notice Days]
            , s.[APAC Local Content Percentage]
            , s.[APAC Stamp Duty Amount]
            , @SourceSystemCode
            , ISNULL(s.[Signed On], ISNULL(s.[Source Changed On], @Now))
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Signed On], ISNULL(s.[Source Changed On], @Now)))
            , s.[Amendment Number]
            , 1
            , s.[Amendment Number] + 1
            , s.[Row Hash Type 2]
            , ISNULL(s.[Signed On], ISNULL(s.[Source Changed On], @Now))
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #ContractSource AS s
        LEFT OUTER JOIN [Dimension].[Supplier] AS sup
            ON  sup.[Source Supplier Reference] = s.[Source Supplier Reference]
            AND sup.[Is Current Row]            = 1
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Vendor Contract] AS d
            WHERE d.[Contract Number]  = s.[Contract Number]
              AND d.[Amendment Number] = s.[Amendment Number]
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Same Day Change Count],
             [Load Pattern], [Started On], [Completed On])
        SELECT N'Vendor Contract', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
               0, @ClosedCount, @InsertedCount, @RejectCount,
               (SELECT COUNT_BIG(*) FROM #ContractSource AS x
                WHERE EXISTS (SELECT 1 FROM #ContractSource AS y
                              WHERE y.[Contract Number] = x.[Contract Number]
                                AND y.[Amendment Number] <> x.[Amendment Number])),
               N'Type2Amendment', @Now, SYSDATETIME();

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Vendor Contract',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @ClosedCount,
             @RejectRowCount     = @RejectCount;

        SELECT @WatermarkTo = CONVERT(NVARCHAR(50), MAX([Source Changed On]), 126)
        FROM #ContractSource;

        IF @WatermarkTo IS NOT NULL
            EXEC [etl].[usp_SetWatermark]
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Vendor Contract',
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
             @SourceComponent    = N'Dimension.Vendor Contract',
             @ProcedureName      = N'Integration.usp_MigrateStagedVendorContractData',
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
