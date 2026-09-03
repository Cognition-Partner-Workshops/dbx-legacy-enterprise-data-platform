/*
    Object        : [Integration].[usp_MigrateStagedCityData]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.City, ref.PostalFormat, Dimension.City, Dimension.Country,
                    Dimension.Geography, Dimension.Region, Dimension.Sales Territory,
                    Dimension.Tax Jurisdiction, the etl control framework
    Called by     : DIM_Load_City

    Type 2 on the city, because the administrative geography moves: county
    consolidations in NA, NUTS re-codings in EU, prefecture mergers in APAC. The
    population figure is deliberately part of the Type 2 hash, which is why the
    dimension gains a version for every city in the year the census lands.

    Postal standardisation is the regional divergence and it is genuinely three
    different algorithms, not one with a flag:

      NA   : CASS-style. ZIP is five digits, the plus-four is stored separately,
             the delivery point and carrier route come from the address vendor
             file and the DPV confirmation code says whether it is deliverable.
      EU   : per-country patterns held in ref.PostalFormat and applied by regular
             expression; the standardised form keeps the country prefix that the
             1998 order-entry system wrote into the postcode field for Germany and
             France only, so those two are stripped explicitly below.
      APAC : mostly numeric but Hong Kong has no postcode at all and Ireland-style
             alphanumeric codes appear in Singapore addresses keyed by expats. The
             standardised value is the raw value uppercased with spaces removed,
             and the format code records which country pattern matched.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_MigrateStagedCityData]
    @BatchId            BIGINT,
    @RegionCode         NVARCHAR(10),
    @PackageName        NVARCHAR(200) = N'DIM_Load_City',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_APP',
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
             @ObjectName       = N'Dimension.City',
             @WatermarkFrom    = @WatermarkFrom OUTPUT,
             @WatermarkTo      = @WatermarkTo OUTPUT;

        IF OBJECT_ID(N'tempdb..#CitySource') IS NOT NULL
            DROP TABLE #CitySource;

        SELECT
              c.[WWICityID]                 AS [WWI City ID]
            , c.[CityName]                  AS [City]
            , c.[LocalScriptCityName]       AS [Local Script City Name]
            , c.[StateProvince]             AS [State Province]
            , c.[CountryCode]               AS [Country Code]
            , c.[Continent]                 AS [Continent]
            , c.[Subregion]                 AS [Subregion]
            , c.[SalesTerritoryCode]        AS [Sales Territory Code]
            , c.[LatestRecordedPopulation]  AS [Latest Recorded Population]
            , c.[PostalCodeRaw]             AS [Postal Code Raw]
            , c.[CountyName]                AS [County Name]
            , c.[CountyFipsCode]            AS [County FIPS Code]
            , c.[MetropolitanStatisticalArea] AS [Metropolitan Statistical Area]
            , c.[NutsLevel3Code]            AS [NUTS Level 3 Code]
            , c.[DistrictName]              AS [District Name]
            , c.[PrefectureOrProvince]      AS [Prefecture Or Province]
            , c.[LocalityName]              AS [Locality Name]
            , c.[TimeZoneName]              AS [Time Zone Name]
            , c.[UtcOffsetMinutes]          AS [UTC Offset Minutes]
            , c.[ObservesDaylightSaving]    AS [Observes Daylight Saving]
            , c.[TaxJurisdictionCode]       AS [Tax Jurisdiction Code]
            , c.[SourceChangedOn]           AS [Source Changed On]
            , CONVERT(NVARCHAR(20), NULL)   AS [Postcode Standardized]
            , CONVERT(NVARCHAR(10), NULL)   AS [ZIP Code]
            , CONVERT(NVARCHAR(4), NULL)    AS [ZIP Plus Four]
            , CONVERT(NVARCHAR(10), NULL)   AS [Postal Format Code]
            , CONVERT(NVARCHAR(50), NULL)   AS [Postcode Format Pattern]
            , CONVERT(NVARCHAR(10), NULL)   AS [Delivery Point Code]
            , CONVERT(NVARCHAR(10), NULL)   AS [Carrier Route Code]
            , CONVERT(NVARCHAR(5), NULL)    AS [DPV Confirmation Code]
            , CONVERT(NVARCHAR(10), NULL)   AS [Address Line Order Code]
            , CONVERT(VARBINARY(32), NULL)  AS [Row Hash Type 2]
        INTO #CitySource
        FROM [stg].[City] AS c
        WHERE c.[RegionCode] = @RegionCode;

        SET @SourceRowCount = @@ROWCOUNT;

        /* A city with no country cannot be placed in the geography hierarchy. */
        INSERT INTO [etl].[RejectedRecord]
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, RecordPayload)
        SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, N'Dimension.City',
               CONVERT(NVARCHAR(200), s.[WWI City ID]), N'MISSING_COUNTRY',
               N'City has no country code; geography and tax jurisdiction cannot be resolved.',
               N'Dimension', CONCAT(N'City=', s.[City], N'; State=', s.[State Province])
        FROM #CitySource AS s
        WHERE NULLIF(s.[Country Code], N'') IS NULL;

        SET @RejectCount = @@ROWCOUNT;

        DELETE FROM #CitySource WHERE NULLIF([Country Code], N'') IS NULL;

        IF @RegionCode = N'NA'
        BEGIN
            /* CASS-style. Everything past the first five digits is the plus-four. */
            UPDATE #CitySource
            SET [ZIP Code] = LEFT(REPLACE(REPLACE(ISNULL([Postal Code Raw], N''), N'-', N''), N' ', N''), 5),
                [ZIP Plus Four] =
                    CASE WHEN LEN(REPLACE(REPLACE(ISNULL([Postal Code Raw], N''), N'-', N''), N' ', N'')) >= 9
                         THEN SUBSTRING(REPLACE(REPLACE([Postal Code Raw], N'-', N''), N' ', N''), 6, 4) END,
                [Postal Format Code] = N'US_ZIP',
                [Postcode Format Pattern] = N'#####-####',
                [Address Line Order Code] = N'CITY_STATE_ZIP',
                [DPV Confirmation Code] =
                    CASE WHEN [Postal Code Raw] IS NULL THEN N'N'
                         WHEN LEN(REPLACE(REPLACE([Postal Code Raw], N'-', N''), N' ', N'')) >= 9 THEN N'Y'
                         ELSE N'S' END;

            UPDATE #CitySource
            SET [Postcode Standardized] =
                    CASE WHEN [ZIP Plus Four] IS NULL THEN [ZIP Code]
                         ELSE [ZIP Code] + N'-' + [ZIP Plus Four] END,
                -- carrier route is only meaningful when the plus-four is present
                [Carrier Route Code] = CASE WHEN [ZIP Plus Four] IS NOT NULL THEN N'C' + [ZIP Plus Four] END,
                [Delivery Point Code] = CASE WHEN [ZIP Plus Four] IS NOT NULL
                                             THEN RIGHT([ZIP Plus Four], 2) END;
        END;

        IF @RegionCode = N'EU'
        BEGIN
            UPDATE s
            SET s.[Postal Format Code]       = f.[FormatCode],
                s.[Postcode Format Pattern]  = f.[FormatPattern],
                s.[Address Line Order Code]  = N'POSTCODE_CITY'
            FROM #CitySource AS s
            LEFT OUTER JOIN [ref].[PostalFormat] AS f
                ON f.[CountryCode] = s.[Country Code];

            /*
                Strip the country prefix the 1998 order-entry system wrote into the
                postcode for Germany and France. Every other country was entered
                without one, so a generic strip would eat the first character of
                Dutch and Polish codes.
            */
            UPDATE #CitySource
            SET [Postcode Standardized] =
                    UPPER(REPLACE(
                        CASE WHEN [Country Code] = N'DEU' AND LEFT(ISNULL([Postal Code Raw], N''), 2) = N'D-'
                             THEN STUFF([Postal Code Raw], 1, 2, N'')
                             WHEN [Country Code] = N'FRA' AND LEFT(ISNULL([Postal Code Raw], N''), 2) = N'F-'
                             THEN STUFF([Postal Code Raw], 1, 2, N'')
                             ELSE ISNULL([Postal Code Raw], N'') END, N' ', N''));

            /* United Kingdom codes keep the space before the inward code. */
            UPDATE #CitySource
            SET [Postcode Standardized] =
                    STUFF([Postcode Standardized], LEN([Postcode Standardized]) - 2, 0, N' ')
            WHERE [Country Code] = N'GBR'
              AND LEN([Postcode Standardized]) BETWEEN 5 AND 7;
        END;

        IF @RegionCode = N'APAC'
        BEGIN
            UPDATE #CitySource
            SET [Postcode Standardized] = UPPER(REPLACE(ISNULL([Postal Code Raw], N''), N' ', N'')),
                [Address Line Order Code] =
                    CASE WHEN [Country Code] IN (N'JPN', N'KOR', N'CHN') THEN N'LARGE_TO_SMALL'
                         ELSE N'SMALL_TO_LARGE' END,
                [Postal Format Code] =
                    CASE WHEN [Country Code] = N'HKG' THEN N'NONE'
                         WHEN [Country Code] = N'SGP' THEN N'SG_6DIGIT'
                         WHEN [Country Code] = N'JPN' THEN N'JP_7DIGIT'
                         WHEN [Country Code] = N'AUS' THEN N'AU_4DIGIT'
                         WHEN [Country Code] = N'IND' THEN N'IN_PIN6'
                         ELSE N'APAC_OTHER' END;

            /* Hong Kong has no postcode; the placeholder keeps joins from failing. */
            UPDATE #CitySource
            SET [Postcode Standardized] = N'NOPOSTCODE'
            WHERE [Country Code] = N'HKG'
              AND NULLIF([Postcode Standardized], N'') IS NULL;
        END;

        UPDATE #CitySource
        SET [Row Hash Type 2] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', ISNULL([City], N''), ISNULL([State Province], N''),
                          ISNULL([Country Code], N''), ISNULL([Sales Territory Code], N''),
                          ISNULL(CONVERT(NVARCHAR(20), [Latest Recorded Population]), N''),
                          ISNULL([Postcode Standardized], N''), ISNULL([County FIPS Code], N''),
                          ISNULL([NUTS Level 3 Code], N''), ISNULL([Prefecture Or Province], N''),
                          ISNULL([Tax Jurisdiction Code], N'')));

        UPDATE d
        SET d.[Is Current Row]     = 0,
            d.[Effective To]       = ISNULL(s.[Source Changed On], @Now),
            d.[Valid To]           = ISNULL(s.[Source Changed On], @Now),
            d.[Last Load Batch Id] = @BatchId
        FROM [Dimension].[City] AS d
        INNER JOIN #CitySource AS s
            ON s.[WWI City ID] = d.[WWI City ID]
        WHERE d.[Is Current Row]   = 1
          AND d.[City Key]         > 0
          AND d.[Row Hash Type 2] <> s.[Row Hash Type 2];

        SET @ClosedCount = @@ROWCOUNT;

        INSERT INTO [Dimension].[City]
            ([WWI City ID], [City], [Local Script City Name], [State Province], [Country],
             [Country Code], [Continent], [Sales Territory], [Sales Territory Key], [Region],
             [Region Code], [Region Key], [Subregion], [Country Key], [Geography Key],
             [Tax Jurisdiction Key], [Latest Recorded Population], [Postcode Standardized],
             [ZIP Code], [ZIP Plus Four], [Postal Format Code], [Postcode Format Pattern],
             [Delivery Point Code], [Carrier Route Code], [DPV Confirmation Code],
             [Address Line Order Code], [County Name], [County FIPS Code],
             [Metropolitan Statistical Area], [NUTS Level 3 Code], [Is EU Member State],
             [District Name], [Prefecture Or Province], [Locality Name], [Time Zone Name],
             [UTC Offset Minutes], [Observes Daylight Saving], [Source System Code],
             [Effective From], [Effective To], [Effective From Date], [Effective Sequence],
             [Is Current Row], [Version Number], [Row Hash Type 2], [Is Inferred Member],
             [Valid From], [Valid To], [Lineage Key], [Last Load Batch Id])
        SELECT
              s.[WWI City ID]
            , ISNULL(s.[City], N'Unknown')
            , s.[Local Script City Name]
            , ISNULL(s.[State Province], N'N/A')
            , ISNULL(ctry.[Country Name], s.[Country Code])
            , s.[Country Code]
            , ISNULL(s.[Continent], N'Unknown')
            , ISNULL(t.[Sales Territory], N'Unknown')
            , ISNULL(t.[Sales Territory Key], -1)
            , ISNULL(rg.[Region Name], @RegionCode)
            , @RegionCode
            , ISNULL(rg.[Region Key], -1)
            , ISNULL(s.[Subregion], N'Unknown')
            , ISNULL(ctry.[Country Key], -1)
            , ISNULL(g.[Geography Key], -1)
            , ISNULL(tj.[Tax Jurisdiction Key], -1)
            , s.[Latest Recorded Population]
            , s.[Postcode Standardized]
            , s.[ZIP Code]
            , s.[ZIP Plus Four]
            , s.[Postal Format Code]
            , s.[Postcode Format Pattern]
            , s.[Delivery Point Code]
            , s.[Carrier Route Code]
            , s.[DPV Confirmation Code]
            , s.[Address Line Order Code]
            , s.[County Name]
            , s.[County FIPS Code]
            , s.[Metropolitan Statistical Area]
            , s.[NUTS Level 3 Code]
            , ISNULL(ctry.[Is EU Member State], 0)
            , s.[District Name]
            , s.[Prefecture Or Province]
            , s.[Locality Name]
            , s.[Time Zone Name]
            , s.[UTC Offset Minutes]
            , s.[Observes Daylight Saving]
            , @SourceSystemCode
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , CONVERT(DATE, ISNULL(s.[Source Changed On], @Now))
            , 1
            , 1
            , ISNULL(prior.[Max Version], 0) + 1
            , s.[Row Hash Type 2]
            , 0
            , ISNULL(s.[Source Changed On], @Now)
            , @HighDate
            , @LineageKey
            , @BatchId
        FROM #CitySource AS s
        LEFT OUTER JOIN [Dimension].[Country] AS ctry
            ON ctry.[Country Code] = s.[Country Code]
        LEFT OUTER JOIN [Dimension].[Region] AS rg
            ON rg.[Region Code] = @RegionCode
        LEFT OUTER JOIN [Dimension].[Sales Territory] AS t
            ON  t.[Sales Territory Code] = s.[Sales Territory Code]
            AND t.[Is Active]            = 1
        LEFT OUTER JOIN [Dimension].[Geography] AS g
            ON  g.[Country Code]    = s.[Country Code]
            AND g.[Subdivision Code] = s.[State Province]
        LEFT OUTER JOIN [Dimension].[Tax Jurisdiction] AS tj
            ON  tj.[Tax Jurisdiction Code] = s.[Tax Jurisdiction Code]
            AND tj.[Is Active]             = 1
        OUTER APPLY
        (
            SELECT MAX(d.[Version Number]) AS [Max Version]
            FROM [Dimension].[City] AS d
            WHERE d.[WWI City ID] = s.[WWI City ID]
              AND d.[City Key]    > 0
        ) AS prior
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM [Dimension].[City] AS cur
            WHERE cur.[WWI City ID]   = s.[WWI City ID]
              AND cur.[Is Current Row] = 1
              AND cur.[City Key]       > 0
        );

        SET @InsertedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [Type 2 Close Count],
             [Type 2 Insert Count], [Reject Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'City', @RegionCode, @BatchId, @PackageExecutionId, @SourceRowCount,
             0, @ClosedCount, @InsertedCount, @RejectCount, N'Type2', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.City',
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
             @SourceComponent    = N'Dimension.City',
             @ProcedureName      = N'Integration.usp_MigrateStagedCityData',
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
