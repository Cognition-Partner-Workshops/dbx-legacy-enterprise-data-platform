/*
    Object        : [Integration].[usp_MigrateStagedGeographyData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Geography, Dimension.Geography, Dimension.Country,
                    Dimension.Region, the etl control framework
    Called by     : REF_Load_Geography (weekly reference load)

    Full refresh of the country / subdivision outrigger, plus the two outriggers
    above it. Region is seeded here rather than loaded - there have only ever
    been three operating regions and they are written out by hand at the top of
    the procedure, which is why there is no stg.Region anywhere in the estate.

    Country is refreshed from the distinct countries in the geography extract,
    so a country with no subdivisions in the source simply does not appear. This
    has bitten the estate twice (Monaco, Liechtenstein) and both are hard-coded
    into the seed block below as a result.

    Regional divergence lives in the tax regime, the fiscal year end, the week
    start day, the retention period and the consent model. All four differ per
    region and the EU rows additionally differ per member state.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedGeographyData]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'REF_Load_Geography',
    @SourceSystemCode   NVARCHAR(20)  = N'ORA_MDM',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @HighDate           DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @CountryRowCount    BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @DeletedCount       BIGINT = 0;
    DECLARE @RejectCount        BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReferenceLoad',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        /* ---------------------------------------------------------------
           Region. Three rows, written by hand since 2006.
           --------------------------------------------------------------- */
        IF OBJECT_ID(N'tempdb..#RegionSeed') IS NOT NULL DROP TABLE #RegionSeed;

        SELECT *
        INTO #RegionSeed
        FROM (VALUES
            (N'NA',   N'North America', N'Wide World Importers Inc.',     N'Seattle',
             N'USD', N'SALESTAX', N'NA_445',  12, N'FY_END_YEAR',  1, N'CURRENT_RATE', N'FED_RESERVE',
             N'OPT_OUT',  N'STATE_PRIVACY', 7,  N'CASS'),
            (N'EU',   N'Europe',        N'Wide World Importers GmbH',     N'Frankfurt',
             N'EUR', N'VAT',      N'EU_CAL',  12, N'FY_END_YEAR',  2, N'CLOSING_RATE', N'ECB',
             N'OPT_IN',   N'GDPR',          6,  N'UPU'),
            (N'APAC', N'Asia Pacific',  N'Wide World Importers Pte. Ltd.', N'Singapore',
             N'SGD', N'GST',      N'APAC_AM',  3, N'FY_START_YEAR', 1, N'AVERAGE_RATE', N'LOCAL_CB',
             N'MIXED',    N'PDPA',          5,  N'LOCAL')
        ) AS v ([Region Code], [Region Name], [Operating Company Name], [Head Office City],
                [Reporting Currency Code], [Tax Regime Code], [Fiscal Calendar Code],
                [Fiscal Year End Month], [Fiscal Year Naming Rule], [Week Start Day],
                [FX Translation Method Code], [FX Rate Source Code], [Consent Model Code],
                [Data Protection Regime Code], [Default Retention Years], [Address Standard Code]);

        MERGE [Dimension].[Region] WITH (HOLDLOCK) AS tgt
        USING #RegionSeed AS src
            ON tgt.[Region Code] = src.[Region Code]
           AND tgt.[Region Key]  > 0
        WHEN MATCHED THEN UPDATE SET
              tgt.[Region Name]                  = src.[Region Name]
            , tgt.[Operating Company Name]       = src.[Operating Company Name]
            , tgt.[Head Office City]             = src.[Head Office City]
            , tgt.[Reporting Currency Code]      = src.[Reporting Currency Code]
            , tgt.[Tax Regime Code]              = src.[Tax Regime Code]
            , tgt.[Fiscal Calendar Code]         = src.[Fiscal Calendar Code]
            , tgt.[Fiscal Year End Month]        = src.[Fiscal Year End Month]
            , tgt.[Fiscal Year Naming Rule]      = src.[Fiscal Year Naming Rule]
            , tgt.[Week Start Day]               = src.[Week Start Day]
            , tgt.[FX Translation Method Code]   = src.[FX Translation Method Code]
            , tgt.[FX Rate Source Code]          = src.[FX Rate Source Code]
            , tgt.[Consent Model Code]           = src.[Consent Model Code]
            , tgt.[Data Protection Regime Code]  = src.[Data Protection Regime Code]
            , tgt.[Default Retention Years]      = src.[Default Retention Years]
            , tgt.[Address Standard Code]        = src.[Address Standard Code]
            , tgt.[Is Active]                    = 1
            , tgt.[Source System Code]           = @SourceSystemCode
            , tgt.[Last Load Batch Id]           = @BatchId
        WHEN NOT MATCHED BY TARGET THEN INSERT
            ([Region Code], [Region Name], [Operating Company Name], [Head Office City],
             [Reporting Currency Code], [Tax Regime Code], [Fiscal Calendar Code],
             [Fiscal Year End Month], [Fiscal Year Naming Rule], [Week Start Day],
             [FX Translation Method Code], [FX Rate Source Code], [Consent Model Code],
             [Data Protection Regime Code], [Default Retention Years], [Address Standard Code],
             [Is Active], [Source System Code], [Valid From], [Valid To], [Lineage Key],
             [Last Load Batch Id])
        VALUES
            (src.[Region Code], src.[Region Name], src.[Operating Company Name],
             src.[Head Office City], src.[Reporting Currency Code], src.[Tax Regime Code],
             src.[Fiscal Calendar Code], src.[Fiscal Year End Month], src.[Fiscal Year Naming Rule],
             src.[Week Start Day], src.[FX Translation Method Code], src.[FX Rate Source Code],
             src.[Consent Model Code], src.[Data Protection Regime Code],
             src.[Default Retention Years], src.[Address Standard Code], 1, @SourceSystemCode,
             @Now, @HighDate, @LineageKey, @BatchId);

        /* ---------------------------------------------------------------
           Geography extract.
           --------------------------------------------------------------- */
        IF OBJECT_ID(N'tempdb..#GeographySource') IS NOT NULL DROP TABLE #GeographySource;

        SELECT
              UPPER(LTRIM(RTRIM(g.[CountryCode])))          AS [Country Code]
            , g.[CountryName]                               AS [Country Name]
            , g.[FormalCountryName]                         AS [Formal Country Name]
            , UPPER(LTRIM(RTRIM(g.[SubdivisionCode])))      AS [Subdivision Code]
            , g.[SubdivisionName]                           AS [Subdivision Name]
            , g.[SubdivisionType]                           AS [Subdivision Type]
            , UPPER(ISNULL(g.[RegionCode], N'GLOBAL'))      AS [Region Code]
            , g.[SubregionName]                             AS [Subregion Name]
            , g.[ContinentName]                             AS [Continent Name]
            , UPPER(g.[LocalCurrencyCode])                  AS [Local Currency Code]
            , UPPER(g.[CountryAlpha2Code])                  AS [Country Alpha2 Code]
            , g.[CountryNumericCode]                        AS [Country Numeric Code]
            , g.[CallingCode]                               AS [Calling Code]
            , g.[PrimaryLanguageCode]                       AS [Primary Language Code]
            , ISNULL(g.[IsEuMemberState], 0)                AS [Is EU Member State]
            , g.[EuAccessionDate]                           AS [EU Accession Date]
            , g.[EuExitDate]                                AS [EU Exit Date]
            , ISNULL(g.[IsSanctioned], 0)                   AS [Is Sanctioned]
            , g.[SanctionProgrammeCode]                     AS [Sanction Programme Code]
            , ISNULL(g.[IsTradingCountry], 1)               AS [Is Trading Country]
            , g.[StandardTaxRate]                           AS [Standard Tax Rate]
            , g.[ReducedTaxRate]                            AS [Reduced Tax Rate]
            , g.[TaxAuthorityName]                          AS [Tax Authority Name]
            , g.[TaxRegistrationFormat]                     AS [Tax Registration Format]
        INTO #GeographySource
        FROM [stg].[Geography] AS g
        WHERE LEN(LTRIM(RTRIM(g.[CountryCode]))) = 3
          AND NULLIF(LTRIM(RTRIM(g.[SubdivisionCode])), N'') IS NOT NULL;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A trading country with no tax rate produces zero-rated invoices. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT DISTINCT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.Geography',
               s.[Country Code], N'TRADING_COUNTRY_NO_TAX_RATE',
               N'Trading country has no standard tax rate; invoices will be raised zero-rated.',
               N'Reference', CONCAT(N'Region=', s.[Region Code], N'|Country=', s.[Country Name])
        FROM #GeographySource AS s
        WHERE s.[Is Trading Country] = 1
          AND s.[Standard Tax Rate] IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           Country outrigger, distinct from the subdivision grain.
           --------------------------------------------------------------- */
        MERGE [Dimension].[Country] WITH (HOLDLOCK) AS tgt
        USING (
            SELECT
                  s.[Country Code]
                , MAX(s.[Country Name])           AS [Country Name]
                , MAX(s.[Formal Country Name])    AS [Formal Country Name]
                , MAX(s.[Country Alpha2 Code])    AS [Country Alpha2 Code]
                , MAX(s.[Country Numeric Code])   AS [Country Numeric Code]
                , MAX(s.[Region Code])            AS [Region Code]
                , MAX(s.[Subregion Name])         AS [Subregion Name]
                , MAX(s.[Continent Name])         AS [Continent Name]
                , MAX(s.[Local Currency Code])    AS [Local Currency Code]
                , MAX(s.[Calling Code])           AS [Calling Code]
                , MAX(s.[Primary Language Code])  AS [Primary Language Code]
                , MAX(CONVERT(TINYINT, s.[Is EU Member State])) AS [Is EU Member State]
                , MAX(s.[EU Accession Date])      AS [EU Accession Date]
                , MAX(s.[EU Exit Date])           AS [EU Exit Date]
                , MAX(CONVERT(TINYINT, s.[Is Sanctioned]))      AS [Is Sanctioned]
                , MAX(s.[Sanction Programme Code]) AS [Sanction Programme Code]
                , MAX(CONVERT(TINYINT, s.[Is Trading Country])) AS [Is Trading Country]
            FROM #GeographySource AS s
            GROUP BY s.[Country Code]
        ) AS src
            ON tgt.[Country Code] = src.[Country Code]
           AND tgt.[Country Key]  > 0
        WHEN MATCHED THEN UPDATE SET
              tgt.[Country Name]            = src.[Country Name]
            , tgt.[Formal Country Name]     = src.[Formal Country Name]
            , tgt.[Country Alpha2 Code]     = src.[Country Alpha2 Code]
            , tgt.[Country Numeric Code]    = src.[Country Numeric Code]
            , tgt.[Region Code]             = src.[Region Code]
            , tgt.[Region Key]              = (SELECT r.[Region Key] FROM [Dimension].[Region] AS r
                                               WHERE r.[Region Code] = src.[Region Code])
            , tgt.[Subregion Name]          = src.[Subregion Name]
            , tgt.[Continent Name]          = src.[Continent Name]
            , tgt.[Local Currency Code]     = src.[Local Currency Code]
            , tgt.[Calling Code]            = src.[Calling Code]
            , tgt.[Primary Language Code]   = src.[Primary Language Code]
            , tgt.[Is EU Member State]      = src.[Is EU Member State]
            , tgt.[EU Accession Date]       = src.[EU Accession Date]
            , tgt.[EU Exit Date]            = src.[EU Exit Date]
            , tgt.[Is Sanctioned]           = src.[Is Sanctioned]
            , tgt.[Sanction Programme Code] = src.[Sanction Programme Code]
            , tgt.[Is Trading Country]      = src.[Is Trading Country]
            , tgt.[Tax Regime Code]         = CASE src.[Region Code]
                                                  WHEN N'NA'   THEN N'SALESTAX'
                                                  WHEN N'EU'   THEN N'VAT'
                                                  WHEN N'APAC' THEN N'GST'
                                                  ELSE N'NONE'
                                              END
            , tgt.[Source System Code]      = @SourceSystemCode
            , tgt.[Last Load Batch Id]      = @BatchId
        WHEN NOT MATCHED BY TARGET THEN INSERT
            ([Country Code], [Country Name], [Formal Country Name], [Country Alpha2 Code],
             [Country Numeric Code], [Region Code], [Region Key], [Subregion Name],
             [Continent Name], [Local Currency Code], [Calling Code], [Primary Language Code],
             [Is EU Member State], [EU Accession Date], [EU Exit Date], [Is Sanctioned],
             [Sanction Programme Code], [Is Trading Country], [Tax Regime Code],
             [Source System Code], [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        VALUES
            (src.[Country Code], src.[Country Name], src.[Formal Country Name],
             src.[Country Alpha2 Code], src.[Country Numeric Code], src.[Region Code],
             (SELECT r.[Region Key] FROM [Dimension].[Region] AS r WHERE r.[Region Code] = src.[Region Code]),
             src.[Subregion Name], src.[Continent Name], src.[Local Currency Code],
             src.[Calling Code], src.[Primary Language Code], src.[Is EU Member State],
             src.[EU Accession Date], src.[EU Exit Date], src.[Is Sanctioned],
             src.[Sanction Programme Code], src.[Is Trading Country],
             CASE src.[Region Code]
                 WHEN N'NA'   THEN N'SALESTAX'
                 WHEN N'EU'   THEN N'VAT'
                 WHEN N'APAC' THEN N'GST'
                 ELSE N'NONE'
             END,
             @SourceSystemCode, @Now, @HighDate, @LineageKey, @BatchId);

        SET @CountryRowCount = @@ROWCOUNT;

        /* ---------------------------------------------------------------
           Geography grain: country plus subdivision. Delete and reinsert the
           codes present in the extract; anything absent is left alone so the
           facts that already point at it keep resolving.
           --------------------------------------------------------------- */
        DELETE d
        FROM [Dimension].[Geography] AS d
        INNER JOIN #GeographySource AS s
            ON  s.[Country Code]     = d.[Country Code]
            AND s.[Subdivision Code] = d.[Subdivision Code]
        WHERE d.[Geography Key] > 0;

        SET @DeletedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[Geography]
            ([Country Key], [Region Key], [Country Code], [Country Name], [Subdivision Code],
             [Subdivision Name], [Subdivision Type], [Region Code], [Subregion Name],
             [Continent Name], [Local Currency Code], [Reporting Currency Code],
             [FX Translation Method Code], [Tax Regime Code], [Standard Tax Rate],
             [Reduced Tax Rate], [Tax Authority Name], [Tax Registration Format],
             [Fiscal Year End Month], [Fiscal Calendar Code], [Week Start Day],
             [Date Format Pattern], [Decimal Separator], [Address Format Code],
             [Data Protection Regime Code], [Consent Model Code], [Retention Years],
             [Cross Border Transfer Allowed], [Is Trading Country], [Is Sanctioned],
             [Trade Block Code], [Source System Code], [Row Hash Type 1], [Valid From],
             [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              c.[Country Key]
            , r.[Region Key]
            , s.[Country Code]
            , s.[Country Name]
            , s.[Subdivision Code]
            , s.[Subdivision Name]
            , s.[Subdivision Type]
            , s.[Region Code]
            , s.[Subregion Name]
            , s.[Continent Name]
            , s.[Local Currency Code]
            , r.[Reporting Currency Code]
            , r.[FX Translation Method Code]
            , CASE s.[Region Code]
                  WHEN N'NA'   THEN N'SALESTAX'
                  WHEN N'EU'   THEN N'VAT'
                  WHEN N'APAC' THEN N'GST'
                  ELSE N'NONE'
              END
            /* NA rates are held per jurisdiction, not per subdivision, so the
               subdivision rate is the state rate only and the county and city
               components are added by the fact load. */
            , CASE
                  WHEN s.[Region Code] = N'NA' THEN ISNULL(s.[Standard Tax Rate], CONVERT(DECIMAL(9, 4), 0))
                  ELSE s.[Standard Tax Rate]
              END
            , s.[Reduced Tax Rate]
            , s.[Tax Authority Name]
            , CASE
                  WHEN s.[Is EU Member State] = 1 THEN ISNULL(s.[Tax Registration Format], N'{CC}999999999')
                  ELSE s.[Tax Registration Format]
              END
            , r.[Fiscal Year End Month]
            , r.[Fiscal Calendar Code]
            , r.[Week Start Day]
            , CASE s.[Region Code]
                  WHEN N'NA' THEN N'MM/dd/yyyy'
                  WHEN N'EU' THEN N'dd.MM.yyyy'
                  ELSE N'yyyy-MM-dd'
              END
            , CASE WHEN s.[Region Code] = N'EU' THEN N',' ELSE N'.' END
            , CASE s.[Region Code]
                  WHEN N'NA'   THEN N'US'
                  WHEN N'EU'   THEN N'UPU'
                  WHEN N'APAC' THEN N'LOCAL'
                  ELSE N'UPU'
              END
            , r.[Data Protection Regime Code]
            , r.[Consent Model Code]
            , r.[Default Retention Years]
            /* Transfers out of the EEA need a transfer mechanism; the flag is
               conservative and set to 0 for every EU member state. */
            , CASE WHEN s.[Is EU Member State] = 1 THEN 0 ELSE 1 END
            , s.[Is Trading Country]
            , s.[Is Sanctioned]
            , CASE
                  WHEN s.[Is EU Member State] = 1 THEN N'EU'
                  WHEN s.[Country Code] IN (N'USA', N'CAN', N'MEX') THEN N'USMCA'
                  WHEN s.[Region Code] = N'APAC' THEN N'RCEP'
                  ELSE NULL
              END
            , @SourceSystemCode
            , HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL(s.[Subdivision Name], N''),
                          ISNULL(CONVERT(NVARCHAR(12), s.[Standard Tax Rate]), N''),
                          ISNULL(s.[Tax Authority Name], N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Trading Country]), N''),
                          ISNULL(CONVERT(NVARCHAR(1), s.[Is Sanctioned]), N'')))
            , @Now
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #GeographySource AS s
        LEFT OUTER JOIN [Dimension].[Country] AS c
            ON c.[Country Code] = s.[Country Code]
        LEFT OUTER JOIN [Dimension].[Region] AS r
            ON r.[Region Code] = s.[Region Code];

        SET @InsertedCount = @@ROWCOUNT;
        SET @UpdatedCount  = @CountryRowCount;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Geography', N'GLOBAL', @BatchId, @PackageExecutionId, @SourceRowCount,
             @UpdatedCount, @DeletedCount, @InsertedCount, @RejectCount, N'FullRefresh',
             @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Geography',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @DeleteRowCount     = @DeletedCount,
             @RejectRowCount     = @RejectCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsDeleted        = @DeletedCount,
             @RowsRejected       = @RejectCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Geography',
             @ProcedureName      = N'Integration.usp_MigrateStagedGeographyData',
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
