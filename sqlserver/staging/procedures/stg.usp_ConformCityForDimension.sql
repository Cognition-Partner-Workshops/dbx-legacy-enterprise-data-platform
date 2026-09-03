/*
    stg.usp_ConformCityForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : DIM_Load_City (SSIS), before Integration.usp_MigrateStagedCityData
    Reads         : raw.OracleGeography, ref.Country, ref.Region, ref.TaxJurisdiction,
                    ref.PostalFormatRule, stg.Geography
    Writes        : stg.City, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogRejectedRecordSet, etl.usp_LogError

    stg.Geography carries the country and subdivision grain that the address
    loads join to; the city dimension needs one row per city, which the ERP
    geography extract also holds but at a lower grain. Rather than a second
    extract this load collapses the geography rows to the city grain and adds
    the attributes only the city dimension versions.

    Postal codes are left as landed. The dimension procedure runs the CASS-like
    NA rules, the ref.PostalFormatRule EU rules and the APAC local rules itself,
    and standardising twice was found to produce two different answers for the
    same Canadian city during the 2016 rollout. All this load does is decide
    which rule set applies and record it.

    Regional attributes are populated for their own region only: county and MSA
    in NA, NUTS level 3 in EU, prefecture and district in APAC. Hong Kong has no
    postcode at all, so the row is not rejected for an empty one.
*/

IF OBJECT_ID(N'stg.usp_ConformCityForDimension', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformCityForDimension;
GO

CREATE PROCEDURE stg.usp_ConformCityForDimension
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.City';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleGeography AS g
        WHERE g.BatchId = @BatchId;

        DELETE FROM stg.City
        WHERE BatchId = @BatchId;

        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            @ObjectName,
            stg.ufn_SourceSystemKey(@SourceSystemCode, g.GEOGRAPHY_ID, 1),
            N'Country',
            N'COUNTRY_CD',
            g.COUNTRY_CD,
            @SourceSystemCode,
            N'LOOKUP_MISS',
            N'Geography row has no city name or no conformed country; city dimension row not built.',
            N'Transform',
            0,
            CONCAT(g.GEOGRAPHY_ID, N'|', g.COUNTRY_CD, N'|', g.CITY_NAME, N'|', g.POSTAL_CD)
        FROM raw.OracleGeography AS g
        LEFT JOIN ref.Country AS c
            ON c.CountryCode = UPPER(LTRIM(RTRIM(g.COUNTRY_CD)))
        WHERE g.BatchId = @BatchId
          AND (
                  NULLIF(LTRIM(RTRIM(g.CITY_NAME)), N'') IS NULL
               OR c.CountryCode IS NULL
              );

        SET @RejectedRows = @@ROWCOUNT;

        BEGIN TRANSACTION;

        WITH RankedGeography AS
        (
            SELECT
                g.GEOGRAPHY_ID,
                g.COUNTRY_CD,
                g.ISO3_CD,
                g.REGION_CD,
                g.SUB_REGION_NAME,
                g.STATE_PROVINCE_CD,
                g.STATE_PROVINCE_NAME,
                g.CITY_NAME,
                g.POSTAL_CD,
                g.TIMEZONE_NAME,
                g.TAX_JURISDICTION_CD,
                g.POPULATION_NUM,
                g.LAST_UPDATE_DT,
                RowRank = ROW_NUMBER() OVER
                (
                    PARTITION BY UPPER(LTRIM(RTRIM(g.COUNTRY_CD))),
                                 UPPER(LTRIM(RTRIM(ISNULL(g.STATE_PROVINCE_CD, N'')))),
                                 UPPER(LTRIM(RTRIM(g.CITY_NAME)))
                    ORDER BY     stg.ufn_SafeDate(g.LAST_UPDATE_DT, N'NA') DESC,
                                 TRY_CONVERT(BIGINT, g.POPULATION_NUM) DESC,
                                 g.SourceRowNumber DESC
                )
            FROM raw.OracleGeography AS g
            WHERE g.BatchId = @BatchId
              AND NULLIF(LTRIM(RTRIM(g.CITY_NAME)), N'') IS NOT NULL
        )
        INSERT INTO stg.City
        (
            CityBusinessKey, SourceSystemCode, WWICityID, CityName, LocalScriptCityName,
            StateProvince, CountryCode, Continent, Subregion, SalesTerritoryCode,
            LatestRecordedPopulation, PostalCodeRaw, PostalRuleSetCode, CountyName, CountyFipsCode,
            MetropolitanStatisticalArea, NutsLevel3Code, DistrictName, PrefectureOrProvince,
            LocalityName, TimeZoneName, UtcOffsetMinutes, ObservesDaylightSaving,
            TaxJurisdictionCode, RegionCode, SourceChangedOn, DqStatusCode, RowHash,
            BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(@SourceSystemCode, N'|', UPPER(LTRIM(RTRIM(rg.COUNTRY_CD))), N'|',
                   UPPER(LTRIM(RTRIM(ISNULL(rg.STATE_PROVINCE_CD, N'')))), N'|',
                   UPPER(LTRIM(RTRIM(rg.CITY_NAME)))),
            @SourceSystemCode,
            TRY_CONVERT(BIGINT, sg.GeographyBusinessKey),
            stg.ufn_CleanString(rg.CITY_NAME, 0),
            CASE WHEN r.RegionCode = N'APAC' THEN stg.ufn_CleanString(rg.CITY_NAME, 0) END,
            stg.ufn_CleanString(COALESCE(rg.STATE_PROVINCE_NAME, rg.STATE_PROVINCE_CD), 0),
            c.CountryCodeIso3,
            CASE r.RegionCode
                WHEN N'NA'   THEN N'North America'
                WHEN N'EU'   THEN N'Europe'
                WHEN N'APAC' THEN N'Asia Pacific'
                ELSE N'Unknown'
            END,
            stg.ufn_CleanString(COALESCE(rg.SUB_REGION_NAME, c.SubRegionName), 0),
            CASE r.RegionCode
                WHEN N'NA'   THEN CONCAT(N'NA-', LEFT(UPPER(LTRIM(RTRIM(ISNULL(rg.STATE_PROVINCE_CD, N'XX')))), 2))
                WHEN N'EU'   THEN CONCAT(N'EU-', UPPER(LTRIM(RTRIM(rg.COUNTRY_CD))))
                WHEN N'APAC' THEN CONCAT(N'AP-', UPPER(LTRIM(RTRIM(rg.COUNTRY_CD))))
                ELSE N'UNASSIGNED'
            END,
            TRY_CONVERT(BIGINT, rg.POPULATION_NUM),
            NULLIF(LTRIM(RTRIM(rg.POSTAL_CD)), N''),
            CASE r.RegionCode
                WHEN N'NA'   THEN N'NA_USPS'
                WHEN N'EU'   THEN N'EU_COUNTRY'
                WHEN N'APAC' THEN N'APAC_LOCAL'
                ELSE N'PASSTHROUGH'
            END,
            -- NA only: the county and the FIPS code come off the tax jurisdiction row.
            CASE WHEN r.RegionCode = N'NA' THEN stg.ufn_CleanString(tj.CountyOrDistrictName, 0) END,
            CASE WHEN r.RegionCode = N'NA' THEN RIGHT(N'00000' + ISNULL(tj.TaxJurisdictionCode, N''), 5) END,
            CASE WHEN r.RegionCode = N'NA' AND TRY_CONVERT(BIGINT, rg.POPULATION_NUM) >= 250000
                 THEN CONCAT(stg.ufn_CleanString(rg.CITY_NAME, 0), N' MSA') END,
            -- EU only: NUTS 3 is approximated from the country and the first postal segment.
            CASE WHEN r.RegionCode = N'EU'
                 THEN CONCAT(UPPER(LTRIM(RTRIM(rg.COUNTRY_CD))),
                             LEFT(REPLACE(ISNULL(rg.POSTAL_CD, N''), N' ', N'') + N'000', 3)) END,
            CASE WHEN r.RegionCode = N'APAC' THEN stg.ufn_CleanString(rg.SUB_REGION_NAME, 0) END,
            CASE WHEN r.RegionCode = N'APAC' THEN stg.ufn_CleanString(rg.STATE_PROVINCE_NAME, 0) END,
            stg.ufn_CleanString(rg.CITY_NAME, 0),
            NULLIF(LTRIM(RTRIM(rg.TIMEZONE_NAME)), N''),
            CASE
                WHEN rg.TIMEZONE_NAME LIKE N'America/%'  THEN -300
                WHEN rg.TIMEZONE_NAME LIKE N'Europe/%'   THEN 60
                WHEN rg.TIMEZONE_NAME LIKE N'Asia/%'     THEN 480
                WHEN rg.TIMEZONE_NAME LIKE N'Australia/%' THEN 600
                ELSE 0
            END,
            CASE WHEN r.RegionCode IN (N'NA', N'EU') THEN 1 ELSE 0 END,
            COALESCE(tj.TaxJurisdictionCode, NULLIF(LTRIM(RTRIM(rg.TAX_JURISDICTION_CD)), N'')),
            ISNULL(r.RegionCode, N'NA'),
            stg.ufn_SafeDate(rg.LAST_UPDATE_DT, ISNULL(r.RegionCode, N'NA')),
            CASE
                WHEN c.CountryCode IS NULL                                          THEN N'FAIL'
                WHEN NULLIF(LTRIM(RTRIM(rg.POSTAL_CD)), N'') IS NULL
                     AND c.PostalCodeRequiredFlag = 1                               THEN N'WARN'
                WHEN c.StateProvinceRequiredFlag = 1
                     AND NULLIF(LTRIM(RTRIM(rg.STATE_PROVINCE_CD)), N'') IS NULL    THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(rg.CITY_NAME, N'|', rg.STATE_PROVINCE_CD, N'|', rg.COUNTRY_CD, N'|',
                       rg.POSTAL_CD, N'|', rg.POPULATION_NUM, N'|', rg.TIMEZONE_NAME)),
            @BatchId,
            @PackageExecutionId
        FROM RankedGeography AS rg
        INNER JOIN ref.Country AS c
            ON c.CountryCode = UPPER(LTRIM(RTRIM(rg.COUNTRY_CD)))
        LEFT JOIN ref.Region AS r
            ON r.RegionCode = c.RegionCode
        LEFT JOIN ref.TaxJurisdiction AS tj
            ON  tj.CountryCode        = c.CountryCode
            AND ISNULL(tj.StateProvinceCode, N'') = ISNULL(UPPER(LTRIM(RTRIM(rg.STATE_PROVINCE_CD))), N'')
            AND ISNULL(tj.CityName, N'')          = ISNULL(UPPER(LTRIM(RTRIM(rg.CITY_NAME))), N'')
        LEFT JOIN stg.Geography AS sg
            ON  sg.BatchId     = @BatchId
            AND sg.CountryCode = c.CountryCode
            AND sg.CityName    = stg.ufn_CleanString(rg.CITY_NAME, 0)
        WHERE rg.RowRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        IF @RejectedRows > 0
            EXEC etl.usp_LogRejectedRecordSet
                @ObjectName         = @ObjectName,
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @SourceSystemCode   = @SourceSystemCode,
                @RejectStage        = N'Transform',
                @RejectReasonCode   = N'LOOKUP_MISS',
                @SourceTable        = N'err.RejectedLookupFailure',
                @SourceFilter       = N'SourceObjectName = N''stg.City''';

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'DIM_Load_City',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformCityForDimension';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
