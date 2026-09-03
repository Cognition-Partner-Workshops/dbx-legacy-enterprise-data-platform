/*
    Object        : [Integration].[usp_MigrateStagedEmployeeData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Employee, Dimension.Employee, Dimension.Cost Center,
                    Dimension.Sales Territory, the etl control framework
    Called by     : DIM_Load_Employee

    Pure Type 2. Every attribute change opens a new version, which is why the
    dimension has eleven versions of some 2003 hires: the HR extract sends the
    whole record whenever anybody touches it and the [Photo] column was excluded
    from the hash only in 2012.

    The organisation hierarchy is recursive and ragged - a country manager reports
    to a regional VP in NA and EU but directly to the group MD in APAC, so the
    depth is not uniform. The path and the flattened levels are rebuilt in a
    second pass over the current rows after the versions are written, because the
    manager row may itself be new in the same batch and its key is not known until
    then. The rebuild walks four levels only; anything deeper is folded into level
    four and marked with a trailing '+' in the path.

    Regional divergence is in the employment attributes: NA carries FLSA
    classification and EEO job category, EU carries works-council membership,
    collective agreement, notice period and the working-time opt out, APAC carries
    work-permit type and expiry and the provident-fund number type. The retention
    expiry for terminated employees is seven years in NA, six in EU and five in
    APAC, and only the EU rows are pseudonymised on expiry (by the retention job,
    not here).
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedEmployeeData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @PackageName        NVARCHAR(200) = N'DIM_Load_Employee',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_APP',
    @RebuildHierarchy   BIT           = 1,
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
    DECLARE @RetentionYears     INT;
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
        SET @RetentionYears = CASE @RegionCode WHEN N'NA' THEN 7
                                               WHEN N'EU' THEN 6
                                               WHEN N'APAC' THEN 5
                                               ELSE 7 END;

        EXEC [etl].[usp_GetWatermark]
             @SourceSystemCode = @SourceSystemCode,
             @ObjectName       = N'Dimension.Employee',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#EmployeeSource') IS NOT NULL
            DROP TABLE #EmployeeSource;

        SELECT
              e.[WWIEmployeeID]         AS [WWI Employee ID]
            , e.[EmployeeNumber]        AS [Employee Number]
            , e.[FullName]              AS [Employee]
            , e.[PreferredName]         AS [Preferred Name]
            , e.[LocalScriptName]       AS [Local Script Name]
            , e.[JobTitle]              AS [Job Title]
            , e.[JobGradeCode]          AS [Job Grade Code]
            , e.[DepartmentCode]        AS [Department Code]
            , e.[DepartmentName]        AS [Department Name]
            , e.[CostCenterCode]        AS [Cost Center Code]
            , e.[ManagerEmployeeNumber] AS [Manager Employee Number]
            , e.[HireDate]              AS [Hire Date]
            , e.[TerminationDate]       AS [Termination Date]
            , e.[EmploymentStatusCode]  AS [Employment Status Code]
            , e.[EmploymentTypeCode]    AS [Employment Type Code]
            , e.[WorkLocationCode]      AS [Work Location Code]
            , e.[CountryCode]           AS [Country Code]
            , e.[IsSalesperson]         AS [Is Salesperson]
            , e.[IsManager]             AS [Is Manager]
            , e.[SalesTerritoryCode]    AS [Sales Territory Code]
            , e.[FlsaClassification]    AS [FLSA Classification]
            , e.[EeoJobCategory]        AS [EEO Job Category]
            , e.[UnionLocalCode]        AS [Union Local Code]
            , e.[CollectiveAgreementCode] AS [Collective Agreement Code]
            , e.[IsWorksCouncilMember]  AS [Is Works Council Member]
            , e.[NoticePeriodMonths]    AS [Notice Period Months]
            , e.[WorkingTimeOptOut]     AS [Working Time Opt Out]
            , e.[WorkPermitTypeCode]    AS [Work Permit Type Code]
            , e.[WorkPermitExpiryDate]  AS [Work Permit Expiry Date]
            , e.[ProvidentFundNumberType] AS [Provident Fund Number Type]
            , e.[SourceChangedOn]       AS [Source Changed On]
            , CONVERT(VARBINARY(32), NULL) AS [Row Hash Type 2]
        INTO #EmployeeSource
        FROM [stg].[Employee] AS e
        WHERE e.[RegionCode] = @RegionCode
          AND e.[SourceChangedOn] > CONVERT(DATETIME2(7), @WatermarkFrom);

        SET @SourceRowCount = @@ROWCOUNT;

        /*
            An employee whose manager number is their own number loops the
            hierarchy rebuild forever. Rather than reject the row the manager is
            blanked and the row is logged as a reject for the HR team to fix; the
            employee still lands in the dimension because payroll facts need a key.
        */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Employee',
               s.[Employee Number], N'SELF_MANAGER',
               N'Employee reports to themselves; manager cleared to protect the hierarchy rebuild.',
               N'Dimension', CONCAT(N'Title=', s.[Job Title], N'; Dept=', s.[Department Code])
        FROM #EmployeeSource AS s
        WHERE s.[Manager Employee Number] = s.[Employee Number];

        SET @RejectCount = @@ROWCOUNT;

        UPDATE #EmployeeSource
        SET [Manager Employee Number] = NULL
        WHERE [Manager Employee Number] = [Employee Number];

        /* Photo is deliberately outside the hash. See the header. */
        UPDATE #EmployeeSource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([Employee], N''), ISNULL([Preferred Name], N''),
                          ISNULL([Job Title], N''), ISNULL([Job Grade Code], N''),
                          ISNULL([Department Code], N''), ISNULL([Cost Center Code], N''),
                          ISNULL([Manager Employee Number], N''), ISNULL([Employment Status Code], N''),
                          ISNULL([Employment Type Code], N''), ISNULL([Work Location Code], N''),
                          ISNULL(CONVERT(NVARCHAR(30), [Termination Date], 126), N''),
                          ISNULL(CONVERT(NVARCHAR(1), [Is Salesperson]), N''),
                          ISNULL([Collective Agreement Code], N''), ISNULL([Work Permit Type Code], N'')));

        /* Close-out. Same-day changes are ordered by the source change timestamp. */
        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[Employee] AS d
        INNER JOIN #EmployeeSource AS s
            ON s.[WWI Employee ID] = d.[WWI Employee ID]
        WHERE d.[Is Current Row]   = 1
          AND d.[Employee Key]     > 0
          AND d.[Row Hash Type 2] <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Employee]
            ([WWI Employee ID], [Employee Number], [Employee], [Preferred Name], [Local Script Name],
             [Job Title], [Job Grade Code], [Department Code], [Department Name], [Cost Center Key],
             [Manager Employee Number], [Hire Date], [Termination Date], [Employment Status Code],
             [Employment Type Code], [Work Location Code], [Country Code], [Region Code],
             [Is Salesperson], [Is Manager], [Is Active], [Primary Sales Territory Key],
             [FLSA Classification], [EEO Job Category], [Union Local Code], [Collective Agreement Code],
             [Is Works Council Member], [Notice Period Months], [Working Time Opt Out],
             [Work Permit Type Code], [Work Permit Expiry Date], [Provident Fund Number Type],
             [Data Retention Expiry Date], [Source System Code], [Effective From], [Effective To],
             [Effective From Date], [Effective Sequence], [Is Current Row], [Version Number],
             [Row Hash Type 2], [Is Inferred Member], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        SELECT
              s.[WWI Employee ID]
            , s.[Employee Number]
            , s.[Employee]
            , s.[Preferred Name]
            , s.[Local Script Name]
            , ISNULL(s.[Job Title], N'Unknown')
            , s.[Job Grade Code]
            , s.[Department Code]
            , s.[Department Name]
            , ISNULL(cc.[Cost Center Key], -1)
            , s.[Manager Employee Number]
            , s.[Hire Date]
            , s.[Termination Date]
            , s.[Employment Status Code]
            , s.[Employment Type Code]
            , s.[Work Location Code]
            , s.[Country Code]
            , @RegionCode
            , ISNULL(s.[Is Salesperson], 0)
            , ISNULL(s.[Is Manager], 0)
            , CASE WHEN s.[Termination Date] IS NULL OR s.[Termination Date] > CONVERT(DATE, @Now)
                   THEN 1 ELSE 0 END
            , ISNULL(t.[Sales Territory Key], -2)
            , CASE WHEN @RegionCode = N'NA' THEN s.[FLSA Classification] END
            , CASE WHEN @RegionCode = N'NA' THEN s.[EEO Job Category] END
            , CASE WHEN @RegionCode = N'NA' THEN s.[Union Local Code] END
            , CASE WHEN @RegionCode = N'EU' THEN s.[Collective Agreement Code] END
            , CASE WHEN @RegionCode = N'EU' THEN s.[Is Works Council Member] END
            , CASE WHEN @RegionCode = N'EU' THEN s.[Notice Period Months] END
            , CASE WHEN @RegionCode = N'EU' THEN s.[Working Time Opt Out] END
            , CASE WHEN @RegionCode = N'APAC' THEN s.[Work Permit Type Code] END
            , CASE WHEN @RegionCode = N'APAC' THEN s.[Work Permit Expiry Date] END
            , CASE WHEN @RegionCode = N'APAC' THEN s.[Provident Fund Number Type] END
            , CASE WHEN s.[Termination Date] IS NULL THEN NULL
                   ELSE DATEADD(YEAR, @RetentionYears, s.[Termination Date]) END
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
        FROM #EmployeeSource AS s
        LEFT OUTER JOIN [Dimension].[Cost Center] AS cc
            ON  cc.[Cost Center Code] = s.[Cost Center Code]
            AND cc.[Is Current Row]   = 1
        LEFT OUTER JOIN [Dimension].[Sales Territory] AS t
            ON  t.[Sales Territory Code] = s.[Sales Territory Code]
            AND t.[Is Active]            = 1
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[Employee] AS d
            WHERE d.[WWI Employee ID] = s.[WWI Employee ID]
              AND d.[Employee Key]    > 0
        ) AS prior
        OUTER APPLY
        (
            SELECT MAX(d.[Effective Sequence]) AS [Sequence]
            FROM [Dimension].[Employee] AS d
            WHERE d.[WWI Employee ID]     = s.[WWI Employee ID]
              AND d.[Effective From Date] = CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
        ) AS sameday
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[Employee] AS cur
            WHERE cur.[WWI Employee ID] = s.[WWI Employee ID]
              AND cur.[Is Current Row]  = 1
              AND cur.[Employee Key]    > 0
        );

        SET @InsertedCount = @@ROWCOUNT;

        IF @RebuildHierarchy = 1
        BEGIN
            /*
                Second pass. Manager keys first, then the path, then the flattened
                levels. The recursion is capped at four levels; the APAC branch is
                three deep and the NA branch occasionally five, and the fifth level
                is folded into level four with a '+' marker.
            */
            UPDATE d
            SET d.[Manager Employee Key] = ISNULL(m.[Employee Key], -1)
            FROM [Dimension].[Employee] AS d
            LEFT OUTER JOIN [Dimension].[Employee] AS m
                ON  m.[Employee Number] = d.[Manager Employee Number]
                AND m.[Is Current Row]  = 1
                AND m.[Employee Key]    > 0
            WHERE d.[Is Current Row] = 1
              AND d.[Region Code]    = @RegionCode
              AND d.[Employee Key]   > 0;

            WITH OrgTree AS
            (
                SELECT  e.[Employee Key],
                        e.[Employee Number],
                        e.[Manager Employee Number],
                        CONVERT(NVARCHAR(400), N'\' + e.[Employee Number]) AS [Path],
                        1 AS [Level]
                FROM [Dimension].[Employee] AS e
                WHERE e.[Is Current Row] = 1
                  AND e.[Region Code]    = @RegionCode
                  AND e.[Employee Key]   > 0
                  AND e.[Manager Employee Number] IS NULL

                UNION ALL

                SELECT  c.[Employee Key],
                        c.[Employee Number],
                        c.[Manager Employee Number],
                        CONVERT(NVARCHAR(400), p.[Path] + N'\' + c.[Employee Number]),
                        p.[Level] + 1
                FROM [Dimension].[Employee] AS c
                INNER JOIN OrgTree AS p
                    ON p.[Employee Number] = c.[Manager Employee Number]
                WHERE c.[Is Current Row] = 1
                  AND c.[Region Code]    = @RegionCode
                  AND c.[Employee Key]   > 0
                  AND p.[Level] < 12
            )
            UPDATE d
            SET d.[Organisation Path]  = t.[Path],
                d.[Organisation Level] = t.[Level],
                d.[Is Leaf Node]       = CASE WHEN EXISTS (SELECT 1 FROM [Dimension].[Employee] AS c
                                                           WHERE c.[Manager Employee Number] = d.[Employee Number]
                                                             AND c.[Is Current Row] = 1)
                                              THEN 0 ELSE 1 END
            FROM [Dimension].[Employee] AS d
            INNER JOIN OrgTree AS t
                ON t.[Employee Key] = d.[Employee Key]
            OPTION (MAXRECURSION 20);

            UPDATE [Dimension].[Employee]
            SET [Organisation Unit Level 1] = PARSENAME(REPLACE(STUFF([Organisation Path], 1, 1, N''), N'\', N'.'), 4),
                [Organisation Unit Level 2] = PARSENAME(REPLACE(STUFF([Organisation Path], 1, 1, N''), N'\', N'.'), 3),
                [Organisation Unit Level 3] = PARSENAME(REPLACE(STUFF([Organisation Path], 1, 1, N''), N'\', N'.'), 2),
                [Organisation Unit Level 4] =
                    CASE WHEN [Organisation Level] > 4
                         THEN PARSENAME(REPLACE(STUFF([Organisation Path], 1, 1, N''), N'\', N'.'), 1) + N'+'
                         ELSE PARSENAME(REPLACE(STUFF([Organisation Path], 1, 1, N''), N'\', N'.'), 1) END
            WHERE [Is Current Row]  = 1
              AND [Region Code]     = @RegionCode
              AND [Organisation Path] IS NOT NULL
              AND LEN([Organisation Path]) - LEN(REPLACE([Organisation Path], N'\', N'')) <= 4;
        END;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Employee', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @RejectCount, N'Type2', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Employee',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @ClosedCount,
             @RejectRowCount     = @RejectCount;

        SELECT @WatermarkTo = CONVERT(NVARCHAR(50), MAX([Source Changed On]), 126)
        FROM #EmployeeSource;

        IF @WatermarkTo IS NOT NULL
            EXEC [etl].[usp_SetWatermark]
                 @SourceSystemCode   = @SourceSystemCode,
                 @ObjectName         = N'Dimension.Employee',
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
             @SourceComponent    = N'Dimension.Employee',
             @ProcedureName      = N'Integration.usp_MigrateStagedEmployeeData',
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
