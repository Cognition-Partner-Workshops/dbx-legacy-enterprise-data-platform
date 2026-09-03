/*
    Object        : [Integration].[usp_MigrateStagedSalespersonData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.SalesRepresentative, Dimension.Employee, Dimension.Salesperson,
                    Dimension.Sales Territory, the etl control framework
    Called by     : DIM_Load_Salesperson

    Salesperson is a second Type 2 dimension over the same people as Employee and
    it is allowed to disagree with it. It is sourced from the commissions system,
    not from HR, and the commissions system keeps a salesperson row alive for the
    whole of the commission run-off period after the person leaves. A salesperson
    who left in March is inactive in Employee and current in Salesperson until the
    final payout, and the sales facts point at the Salesperson key.

    Only the commission-relevant attributes drive a new version: plan, rate,
    quota, territory and manager. A change of job title alone does not, because
    the 2011 title harmonisation created 4,000 versions overnight and the
    commission reports could not be reconciled for a quarter.

    Regional divergence sits in the payout mechanics: NA has an accelerator
    threshold above quota, EU caps commission as a percentage of base under the
    collective agreement, APAC pays on a local-currency budget rate fixed at the
    start of the fiscal year and settles monthly or quarterly by country.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedSalespersonData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @QuotaFiscalYear    INT,
    @PackageName        NVARCHAR(200) = N'DIM_Load_Salesperson',
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
             @ObjectName       = N'Dimension.Salesperson',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#SalespersonSource') IS NOT NULL
            DROP TABLE #SalespersonSource;

        SELECT
              r.[SalespersonCode]           AS [Salesperson Code]
            , r.[WWIEmployeeID]             AS [WWI Employee ID]
            , r.[SalespersonName]           AS [Salesperson]
            , r.[PreferredName]             AS [Preferred Name]
            , r.[SalesRoleCode]             AS [Sales Role Code]
            , r.[SalesOfficeCode]           AS [Sales Office Code]
            , r.[SalesTerritoryCode]        AS [Sales Territory Code]
            , r.[ChannelResponsibilityCode] AS [Channel Responsibility Code]
            , r.[CommissionPlanCode]        AS [Commission Plan Code]
            , r.[CommissionRate]            AS [Commission Rate]
            , r.[CommissionCurrencyCode]    AS [Commission Currency Code]
            , r.[QuotaBasisCode]            AS [Quota Basis Code]
            , r.[AnnualQuotaAmount]         AS [Annual Quota Amount]
            , r.[QuotaCurrencyCode]         AS [Quota Currency Code]
            , r.[ManagerEmployeeNumber]     AS [Manager Employee Number]
            , r.[SalesStartDate]            AS [Sales Start Date]
            , r.[SalesEndDate]              AS [Sales End Date]
            , r.[CountryCode]               AS [Country Code]
            , r.[AcceleratorThresholdPct]   AS [NA Accelerator Threshold Pct]
            , r.[CommissionCapPct]          AS [EU Commission Cap Pct]
            , r.[CollectiveAgreementCode]   AS [EU Collective Agreement Code]
            , r.[BudgetFxRate]              AS [APAC Budget FX Rate]
            , r.[PayoutFrequencyCode]       AS [APAC Payout Frequency Code]
            , r.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
        INTO #SalespersonSource
        FROM [stg].[SalesRepresentative] AS r
        WHERE r.[RegionCode] = @RegionCode;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A quota with no currency cannot be converted for group reporting. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Salesperson',
               s.[Salesperson Code], N'QUOTA_NO_CURRENCY',
               N'Annual quota supplied without a quota currency code.',
               N'Dimension', CONCAT(N'Quota=', CONVERT(NVARCHAR(30), s.[Annual Quota Amount]))
        FROM #SalespersonSource AS s
        WHERE s.[Annual Quota Amount] IS NOT NULL
          AND NULLIF(s.[Quota Currency Code], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* Region-specific payout conditioning, applied before hashing. */
        IF @RegionCode = N'NA'
            UPDATE #SalespersonSource
            SET [NA Accelerator Threshold Pct] = ISNULL([NA Accelerator Threshold Pct], 100.00),
                [EU Commission Cap Pct]        = NULL,
                [EU Collective Agreement Code] = NULL,
                [APAC Budget FX Rate]          = NULL,
                [APAC Payout Frequency Code]   = NULL;

        IF @RegionCode = N'EU'
            UPDATE #SalespersonSource
            SET [NA Accelerator Threshold Pct] = NULL,
                -- the works agreement caps variable pay; 40% where the plan is silent
                [EU Commission Cap Pct]        = ISNULL([EU Commission Cap Pct], 40.00),
                [APAC Budget FX Rate]          = NULL,
                [APAC Payout Frequency Code]   = NULL;

        IF @RegionCode = N'APAC'
            UPDATE #SalespersonSource
            SET [NA Accelerator Threshold Pct] = NULL,
                [EU Commission Cap Pct]        = NULL,
                [EU Collective Agreement Code] = NULL,
                [APAC Budget FX Rate]          = ISNULL([APAC Budget FX Rate], 1.000000),
                [APAC Payout Frequency Code]   =
                    ISNULL([APAC Payout Frequency Code],
                           CASE WHEN [Country Code] IN (N'JPN', N'KOR') THEN N'QTR' ELSE N'MTH' END);

        /* Commission-relevant attributes only. Job title is not in the hash. */
        UPDATE #SalespersonSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Sales Role Code], N''), ISNULL([Sales Territory Code], N''),
                          ISNULL([Commission Plan Code], N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Commission Rate]), N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Annual Quota Amount]), N''),
                          ISNULL([Quota Currency Code], N''), ISNULL([Manager Employee Number], N''),
                          ISNULL([Channel Responsibility Code], N''),
                          ISNULL(CONVERT(NVARCHAR(30), [Sales End Date], 126), N'')));

        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Salesperson] AS d
        INNER JOIN #SalespersonSource AS s
            ON s.[Salesperson Code] = d.[Salesperson Code]
        WHERE d.[Is Current Row]   = 1
          AND d.[Salesperson Key]  > 0
          AND d.[Row Hash Type 2] <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Salesperson]
            ([Salesperson Code], [WWI Employee ID], [Salesperson], [Preferred Name], [Employee Key],
             [Sales Role Code], [Sales Office Code], [Sales Territory Key], [Sales Territory],
             [Channel Responsibility Code], [Commission Plan Code], [Commission Rate],
             [Commission Currency Code], [Quota Basis Code], [Annual Quota Amount],
             [Quota Currency Code], [Quota Fiscal Year], [Sales Manager Employee Key],
             [Sales Start Date], [Sales End Date], [Hire Date], [Country Code], [Region Code],
             [Is Active], [NA Accelerator Threshold Pct], [EU Commission Cap Pct],
             [EU Collective Agreement Code], [APAC Budget FX Rate], [APAC Payout Frequency Code],
             [Source System Code], [Effective From], [Effective To], [Effective From Date],
             [Effective Sequence], [Is Current Row], [Version Number], [Row Hash Type 2],
             [Is Inferred Member], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[Salesperson Code]
            , s.[WWI Employee ID]
            , s.[Salesperson]
            , s.[Preferred Name]
            , ISNULL(e.[Employee Key], -1)
            , s.[Sales Role Code]
            , s.[Sales Office Code]
            , ISNULL(t.[Sales Territory Key], -1)
            , ISNULL(t.[Sales Territory], N'Unknown')
            , s.[Channel Responsibility Code]
            , s.[Commission Plan Code]
            , s.[Commission Rate]
            , ISNULL(s.[Commission Currency Code], s.[Quota Currency Code])
            , s.[Quota Basis Code]
            , s.[Annual Quota Amount]
            , s.[Quota Currency Code]
            , @QuotaFiscalYear
            , ISNULL(m.[Employee Key], -1)
            , s.[Sales Start Date]
            , s.[Sales End Date]
            , e.[Hire Date]
            , s.[Country Code]
            , @RegionCode
            , CASE WHEN s.[Sales End Date] IS NULL OR s.[Sales End Date] > CONVERT(DATE, @Now)
                   THEN 1 ELSE 0 END
            , s.[NA Accelerator Threshold Pct]
            , s.[EU Commission Cap Pct]
            , s.[EU Collective Agreement Code]
            , s.[APAC Budget FX Rate]
            , s.[APAC Payout Frequency Code]
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
            , ISNULL(sameday.[Sequence], 0) + 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , 0
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #SalespersonSource AS s
        LEFT OUTER JOIN [Dimension].[Employee] AS e
            ON  e.[WWI Employee ID] = s.[WWI Employee ID]
            AND e.[Is Current Row]  = 1
        LEFT OUTER JOIN [Dimension].[Employee] AS m
            ON  m.[Employee Number] = s.[Manager Employee Number]
            AND m.[Is Current Row]  = 1
        LEFT OUTER JOIN [Dimension].[Sales Territory] AS t
            ON  t.[Sales Territory Code] = s.[Sales Territory Code]
            AND t.[Is Active]            = 1
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Salesperson] AS d
            WHERE d.[Salesperson Code] = s.[Salesperson Code]
              AND d.[Salesperson Key]  > 0
        ) AS prior
        OUTER APPLY
        (
            SELECT MAX(d.[Effective Sequence]) AS [Sequence]
            FROM [Dimension].[Salesperson] AS d
            WHERE d.[Salesperson Code]    = s.[Salesperson Code]
              AND d.[Effective From Date] = CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
        ) AS sameday
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Salesperson] AS cur
            WHERE cur.[Salesperson Code] = s.[Salesperson Code]
              AND cur.[Is Current Row]   = 1
              AND cur.[Salesperson Key]  > 0
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Salesperson', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @RejectCount, N'Type2', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Salesperson',
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
             @SourceComponent    = N'Dimension.Salesperson',
             @ProcedureName      = N'Integration.usp_MigrateStagedSalespersonData',
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
