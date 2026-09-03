/*
    Object        : [Integration].[usp_LoadBridgeEmployeeTerritory]
    Deploy target : WideWorldImportersDW
    Depends on    : ref.TerritoryAlignment, stg.Salesperson, Dimension.Employee,
                    Dimension.Salesperson, Dimension.Sales Territory,
                    Dimension.Employee Territory Bridge, the etl control framework
    Called by     : DIM_Load_BridgeEmployeeTerritory, run after the employee,
                    salesperson and territory dimension loads

    Loads the salesperson-to-territory coverage bridge one alignment year at a
    time. Territory alignment is re-cut every January and the bridge is the only
    place the historic alignment survives, because [Dimension].[Sales Territory]
    is Type 1 and the January load overwrites it.

    Coverage is closed rather than deleted: a rep who loses a territory in the
    January re-cut keeps their row with [Coverage To] set to 31 December of the
    previous alignment year, so a commission restatement over a prior period still
    resolves.

    Regional rules, as they were written into the three commission plans:
      NA    a PRIMARY rep plus an INSIDE overlay at a fixed 0.20. The primary's
            factor is reduced to 0.80 to make room; if the source already did the
            reduction the load leaves it alone, which it detects by the factors
            already summing to one.
      EU    the works-council agreement forbids allocating a rep below 0.25, so a
            row below that is rejected rather than normalised. The national
            account manager comes through as an OVERLAY and is exempt.
      APAC  distributor-managed territories have no rep; they are allocated whole
            to the channel manager with [Is Distributor Managed] = 1 and no quota
            share, because the distributor carries the quota.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadBridgeEmployeeTerritory]
    @BatchId            BIGINT,
    @AlignmentYear      SMALLINT      = NULL,     -- NULL means the current calendar year
    @RegionCode         NVARCHAR(10)  = N'GLOBAL',
    @PackageName        NVARCHAR(200) = N'DIM_Load_BridgeEmployeeTerritory',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_SLS',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @ClosedCount        BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @CoverageFrom       DATE;
    DECLARE @CoverageTo         DATE;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    DECLARE @rBusinessKey       NVARCHAR(200);
    DECLARE @rReasonCode        NVARCHAR(50);
    DECLARE @rReason            NVARCHAR(500);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'Bridge',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        SET @AlignmentYear = ISNULL(@AlignmentYear, CONVERT(SMALLINT, YEAR(@Now)));
        SET @CoverageFrom  = DATEFROMPARTS(@AlignmentYear, 1, 1);
        SET @CoverageTo    = DATEFROMPARTS(@AlignmentYear, 12, 31);

        IF OBJECT_ID(N'tempdb..#Coverage') IS NOT NULL DROP TABLE #Coverage;

        CREATE TABLE #Coverage
        (
            [WWI Employee ID]           INT             NOT NULL,
            [Sales Territory Code]      NVARCHAR(20)    NOT NULL,
            [Coverage Role Code]        NVARCHAR(15)    NOT NULL,
            [Allocation Factor]         DECIMAL(9, 6)   NOT NULL,
            [Allocation Basis Code]     NVARCHAR(15)    NULL,
            [Quota Share Amount]        DECIMAL(18, 2)  NULL,
            [Quota Currency Code]       NVARCHAR(3)     NULL,
            [Commission Share Percentage] DECIMAL(9, 4) NULL,
            [Product Category Scope]    NVARCHAR(40)    NULL,
            [Is Distributor Managed]    BIT             NULL,
            [Region Code]               NVARCHAR(10)    NOT NULL,
            [Reject Reason Code]        NVARCHAR(50)    NULL,
            [Reject Reason]             NVARCHAR(500)   NULL
        );

        INSERT INTO #Coverage
            ([WWI Employee ID], [Sales Territory Code], [Coverage Role Code],
             [Allocation Factor], [Allocation Basis Code], [Quota Share Amount],
             [Quota Currency Code], [Commission Share Percentage], [Product Category Scope],
             [Is Distributor Managed], [Region Code])
        SELECT
              a.[WWIEmployeeID]
            , UPPER(LTRIM(RTRIM(a.[SalesTerritoryCode])))
            , UPPER(ISNULL(NULLIF(LTRIM(RTRIM(a.[CoverageRoleCode])), N''), N'PRIMARY'))
            , CONVERT(DECIMAL(9, 6), ISNULL(a.[AllocationFactor], 1.0))
            , UPPER(NULLIF(LTRIM(RTRIM(a.[AllocationBasisCode])), N''))
            , a.[QuotaShareAmount]
            , UPPER(NULLIF(LTRIM(RTRIM(a.[QuotaCurrencyCode])), N''))
            , a.[CommissionSharePercentage]
            , NULLIF(LTRIM(RTRIM(a.[ProductCategoryScope])), N'')
            , CONVERT(BIT, ISNULL(a.[IsDistributorManaged], 0))
            , UPPER(ISNULL(a.[RegionCode], N'NA'))
        FROM [ref].[TerritoryAlignment] AS a
        WHERE a.[AlignmentYear] = @AlignmentYear
          AND (@RegionCode = N'GLOBAL' OR UPPER(ISNULL(a.[RegionCode], N'NA')) = @RegionCode);

        SET @SourceRowCount = @@ROWCOUNT;

        /* ------------------------------------------------ NA inside overlay */
        /* The inside-sales overlay is not in the alignment file; it is derived
           from the salesperson master, and has been since 2010. */
        INSERT INTO #Coverage
            ([WWI Employee ID], [Sales Territory Code], [Coverage Role Code],
             [Allocation Factor], [Allocation Basis Code], [Region Code])
        SELECT
              s.[WWIEmployeeID]
            , UPPER(LTRIM(RTRIM(s.[SalesTerritoryCode])))
            , N'INSIDE'
            , 0.20
            , N'MANUAL'
            , N'NA'
        FROM [stg].[Salesperson] AS s
        WHERE UPPER(ISNULL(s.[RegionCode], N'NA')) = N'NA'
          AND UPPER(ISNULL(s.[SalespersonTypeCode], N'')) = N'INSIDE'
          AND NULLIF(LTRIM(RTRIM(s.[SalesTerritoryCode])), N'') IS NOT NULL
          AND (@RegionCode = N'GLOBAL' OR @RegionCode = N'NA')
          AND NOT EXISTS (SELECT 1
                          FROM #Coverage AS c
                          WHERE c.[WWI Employee ID]      = s.[WWIEmployeeID]
                            AND c.[Sales Territory Code] = UPPER(LTRIM(RTRIM(s.[SalesTerritoryCode]))));

        /* Make room for the overlay only where the territory now over-allocates. */
        UPDATE c
        SET c.[Allocation Factor] = 0.80
        FROM #Coverage AS c
        INNER JOIN (
            SELECT [Sales Territory Code]
            FROM #Coverage
            WHERE [Region Code] = N'NA'
            GROUP BY [Sales Territory Code]
            HAVING SUM([Allocation Factor]) > 1.000001
        ) AS o
            ON o.[Sales Territory Code] = c.[Sales Territory Code]
        WHERE c.[Region Code]        = N'NA'
          AND c.[Coverage Role Code] = N'PRIMARY';

        /* ------------------------------------------------ APAC distributor */
        UPDATE #Coverage
        SET [Coverage Role Code]     = N'CHANNEL',
            [Is Distributor Managed] = 1,
            [Quota Share Amount]     = NULL,
            [Allocation Factor]      = 1.0
        WHERE [Region Code] = N'APAC'
          AND ISNULL([Is Distributor Managed], 0) = 1;

        /* ------------------------------------------------ screens */
        UPDATE #Coverage
        SET [Reject Reason Code] = N'ALLOCATION_RANGE',
            [Reject Reason]      = N'Allocation factor must be greater than zero and no more than one.'
        WHERE [Allocation Factor] <= 0 OR [Allocation Factor] > 1;

        UPDATE #Coverage
        SET [Reject Reason Code] = N'EU_MIN_ALLOCATION',
            [Reject Reason]      = N'EU works-council agreement forbids allocating a covering rep below 0.25.'
        WHERE [Reject Reason Code] IS NULL
          AND [Region Code]        = N'EU'
          AND [Coverage Role Code] <> N'OVERLAY'
          AND [Allocation Factor]  < 0.25;

        UPDATE c
        SET c.[Reject Reason Code] = N'TERRITORY_UNKNOWN',
            c.[Reject Reason]      = N'Sales territory code is not present in Dimension.Sales Territory.'
        FROM #Coverage AS c
        WHERE c.[Reject Reason Code] IS NULL
          AND NOT EXISTS (SELECT 1
                          FROM [Dimension].[Sales Territory] AS t
                          WHERE t.[Sales Territory Code] = c.[Sales Territory Code]
                            AND t.[Sales Territory Key]  > 0);

        DECLARE curReject CURSOR LOCAL FAST_FORWARD FOR
            SELECT CONCAT(CONVERT(NVARCHAR(20), [WWI Employee ID]), N'|', [Sales Territory Code]),
                   [Reject Reason Code], [Reject Reason]
            FROM #Coverage
            WHERE [Reject Reason Code] IS NOT NULL;

        OPEN curReject;
        FETCH NEXT FROM curReject INTO @rBusinessKey, @rReasonCode, @rReason;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [etl].[usp_LogRejectedRecord]
                 @PackageExecutionId = @PackageExecutionId,
                 @BatchId            = @BatchId,
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Employee Territory Bridge',
                 @BusinessKey        = @rBusinessKey,
                 @RejectReasonCode   = @rReasonCode,
                 @RejectReason       = @rReason,
                 @RejectStage        = N'Dimension';

            SET @RejectCount = @RejectCount + 1;

            FETCH NEXT FROM curReject INTO @rBusinessKey, @rReasonCode, @rReason;
        END;

        CLOSE curReject;
        DEALLOCATE curReject;

        DELETE FROM #Coverage WHERE [Reject Reason Code] IS NOT NULL;

        /* ------------------------------------------------ close prior alignment */
        UPDATE b
        SET b.[Is Current Coverage] = 0,
            b.[Coverage To]         = DATEADD(DAY, -1, @CoverageFrom),
            b.[Last Load Batch Id]  = @BatchId,
            b.[Last Load Package Execution Id] = @PackageExecutionId
        FROM [Dimension].[Employee Territory Bridge] AS b
        WHERE b.[Is Current Coverage] = 1
          AND b.[Alignment Year]      < @AlignmentYear
          AND (@RegionCode = N'GLOBAL' OR b.[Region Code] = @RegionCode);

        SET @ClosedCount = @@ROWCOUNT;

        /* A re-run of the same alignment year replaces its own rows. */
        DELETE FROM [Dimension].[Employee Territory Bridge]
        WHERE [Alignment Year] = @AlignmentYear
          AND (@RegionCode = N'GLOBAL' OR [Region Code] = @RegionCode);

        INSERT INTO [Dimension].[Employee Territory Bridge]
            ([WWI Employee ID], [Sales Territory Code], [Employee Key], [Salesperson Key],
             [Sales Territory Key], [Alignment Year], [Coverage From], [Coverage To],
             [Is Current Coverage], [Coverage Role Code], [Allocation Factor],
             [Allocation Basis Code], [Quota Share Amount], [Quota Currency Code],
             [Commission Share Percentage], [Product Category Scope], [Is Distributor Managed],
             [Region Code], [Source System Code], [Last Load Batch Id],
             [Last Load Package Execution Id])
        SELECT
              c.[WWI Employee ID]
            , c.[Sales Territory Code]
            , e.[Employee Key]
            , sp.[Salesperson Key]
            , t.[Sales Territory Key]
            , @AlignmentYear
            , @CoverageFrom
            , CASE WHEN @AlignmentYear = YEAR(@Now) THEN CONVERT(DATE, N'9999-12-31')
                   ELSE @CoverageTo END
            , CASE WHEN @AlignmentYear = YEAR(@Now) THEN 1 ELSE 0 END
            , c.[Coverage Role Code]
            , c.[Allocation Factor]
            , ISNULL(c.[Allocation Basis Code], N'MANUAL')
            , c.[Quota Share Amount]
            /* Quota currency defaults per region; the EU plan is denominated in
               EUR even for the non-euro member states, which the FX team has
               objected to since 2016. */
            , ISNULL(c.[Quota Currency Code],
                     CASE c.[Region Code] WHEN N'EU' THEN N'EUR'
                                          WHEN N'APAC' THEN N'USD'
                                          ELSE N'USD' END)
            , c.[Commission Share Percentage]
            , c.[Product Category Scope]
            , ISNULL(c.[Is Distributor Managed], 0)
            , c.[Region Code]
            , @SourceSystemCode
            , @BatchId
            , @PackageExecutionId
        FROM #Coverage AS c
        LEFT OUTER JOIN [Dimension].[Employee] AS e
            ON  e.[WWI Employee ID] = c.[WWI Employee ID]
            AND e.[Is Current Row]  = 1
        LEFT OUTER JOIN [Dimension].[Salesperson] AS sp
            ON  sp.[WWI Employee ID] = c.[WWI Employee ID]
            AND sp.[Is Current Row]  = 1
        LEFT OUTER JOIN [Dimension].[Sales Territory] AS t
            ON t.[Sales Territory Code] = c.[Sales Territory Code];

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Employee Territory Bridge', @RegionCode, @BatchId, @PackageExecutionId,
             @SourceRowCount, @ClosedCount, @InsertedCount, @RejectCount,
             N'BridgeDeleteInsert', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Employee Territory Bridge',
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

        IF CURSOR_STATUS(N'local', N'curReject') >= 0
        BEGIN
            CLOSE curReject;
            DEALLOCATE curReject;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Employee Territory Bridge',
             @ProcedureName      = N'Integration.usp_LoadBridgeEmployeeTerritory',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount;

        THROW;
    END CATCH;
END;
GO
