/*
    stg.usp_TruncateAndReload_Geography

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_GEOGRAPHY (SSIS)
    Reads         : raw.OracleGeography, ref.Country, ref.Region, ref.TaxJurisdiction
    Writes        : stg.Geography, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The ERP geography table is the closest thing the estate has to a conformed
    location list, but it is dirty in three predictable ways:

      1. rows created before the 2011 ISO clean-up carry an ISO3 code in
         COUNTRY_CD, so the country has to be resolved from either column;
      2. postal codes are unformatted, and the mask that should describe them is
         only populated for the countries someone bothered with;
      3. the same city appears with and without a postal code, which is why the
         business key includes the postal code and the load deduplicates on it.

    Tax jurisdiction resolution is genuinely different per region and is the main
    reason this procedure is not a straight copy of the other reference loads:
        NA   resolved by ZIP range (PostalCodeLow/PostalCodeHigh).
        EU   resolved by country only; VAT is a country-level regime.
        APAC resolved by country plus state/prefecture where a state exists.
*/

IF OBJECT_ID(N'stg.usp_TruncateAndReload_Geography', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateAndReload_Geography;
GO

CREATE PROCEDURE stg.usp_TruncateAndReload_Geography
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Geography';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @DeletedRows  BIGINT = 0;
    DECLARE @LookupMisses BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleGeography AS r
        WHERE r.BatchId = @BatchId;

        BEGIN TRANSACTION;

        DELETE FROM stg.Geography
        WHERE BatchId = @BatchId;

        SET @DeletedRows = @@ROWCOUNT;

        SELECT
            ResolvedCountryCode = c.CountryCode,
            CountryName         = c.CountryName,
            CountryCodeIso3     = c.CountryCodeIso3,
            RegionCode          = c.RegionCode,
            SubRegionName       = COALESCE(NULLIF(LTRIM(RTRIM(r.SUB_REGION_NAME)), N''), c.SubRegionName),
            StateProvinceCode   = NULLIF(UPPER(LTRIM(RTRIM(r.STATE_PROVINCE_CD))), N''),
            StateProvinceName   = LEFT(stg.ufn_CleanString(r.STATE_PROVINCE_NAME, 0), 100),
            CityName            = LEFT(stg.ufn_CleanString(r.CITY_NAME, 0), 100),
            PostalCode          = stg.ufn_StandardizePostalCode(r.POSTAL_CD, c.CountryCode),
            PostalFormatMask    = COALESCE(NULLIF(LTRIM(RTRIM(r.POSTAL_FORMAT_MASK)), N''), c.PostalFormatMask),
            TimeZoneName        = NULLIF(LTRIM(RTRIM(r.TIMEZONE_NAME)), N''),
            LocalCurrencyCode   = COALESCE(LEFT(NULLIF(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), N''), 3), c.LocalCurrencyCode),
            SourceJurisdiction  = NULLIF(UPPER(LTRIM(RTRIM(r.TAX_JURISDICTION_CD))), N''),
            Population          = CONVERT(BIGINT, stg.ufn_SafeDecimal(r.POPULATION_NUM, N'.')),
            Latitude            = CONVERT(DECIMAL(9,6), stg.ufn_SafeDecimal(r.LATITUDE, N'.')),
            Longitude           = CONVERT(DECIMAL(9,6), stg.ufn_SafeDecimal(r.LONGITUDE, N'.')),
            SourceGeographyId   = LTRIM(RTRIM(r.GEOGRAPHY_ID)),
            SourceCountryValue  = LTRIM(RTRIM(r.COUNTRY_CD))
        INTO #TypedGeography
        FROM raw.OracleGeography AS r
        LEFT JOIN ref.Country AS c
            ON  c.CountryCode    = LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2)
            OR  c.CountryCodeIso3 = LEFT(UPPER(LTRIM(RTRIM(COALESCE(NULLIF(r.ISO3_CD, N''), r.COUNTRY_CD)))), 3)
        WHERE r.BatchId = @BatchId;

        --  Unresolvable countries are a lookup failure rather than a hard reject:
        --  the row is dropped from the dimension feed but the value is recorded so
        --  the steward can add the country.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleGeography', MIN(t.SourceGeographyId), N'Country',
            N'COUNTRY_CD', t.SourceCountryValue, @SourceSystemCode, N'LOOKUP_MISS',
            N'COUNTRY_CD does not resolve to ref.Country by ISO2 or ISO3',
            N'Transform', 1, 0, COUNT_BIG(*),
            CONCAT(N'{"COUNTRY_CD":"', t.SourceCountryValue, N'"}')
        FROM #TypedGeography AS t
        WHERE t.ResolvedCountryCode IS NULL
        GROUP BY t.SourceCountryValue;

        SET @LookupMisses = @@ROWCOUNT;

        --  Deduplicate: the same country/state/city/postal can arrive more than
        --  once because the ERP holds one row per site as well as one per city.
        INSERT INTO stg.Geography
        (
            GeographyBusinessKey, SourceSystemCode, CountryCode, CountryCodeIso3, CountryName,
            RegionCode, SubRegionName, StateProvinceCode, StateProvinceName, CityName, PostalCode,
            PostalFormatMask, TimeZoneName, LocalCurrencyCode, TaxJurisdictionCode, TaxRegimeCode,
            Population, Latitude, Longitude, DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            CONCAT(d.ResolvedCountryCode, N'|', ISNULL(d.StateProvinceCode, N'-'), N'|',
                   ISNULL(d.CityName, N'-'), N'|', ISNULL(d.PostalCode, N'-')),
            @SourceSystemCode,
            d.ResolvedCountryCode,
            d.CountryCodeIso3,
            d.CountryName,
            d.RegionCode,
            d.SubRegionName,
            d.StateProvinceCode,
            d.StateProvinceName,
            d.CityName,
            d.PostalCode,
            d.PostalFormatMask,
            d.TimeZoneName,
            d.LocalCurrencyCode,
            COALESCE(d.SourceJurisdiction, tj.TaxJurisdictionCode),
            COALESCE(tj.TaxRegimeCode, rg.TaxRegimeCode),
            d.Population,
            d.Latitude,
            d.Longitude,
            CASE
                WHEN d.PostalCode IS NULL AND d.PostalCodeRequiredFlag = 1 THEN N'WARN'
                WHEN tj.TaxJurisdictionCode IS NULL THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(d.CountryName, N'|', d.StateProvinceName, N'|', d.CityName, N'|',
                       d.PostalCode, N'|', d.LocalCurrencyCode, N'|', d.TimeZoneName)),
            @BatchId,
            @PackageExecutionId
        FROM
        (
            SELECT
                t.*,
                c.PostalCodeRequiredFlag,
                ROW_NUMBER() OVER
                (
                    PARTITION BY t.ResolvedCountryCode, t.StateProvinceCode, t.CityName, t.PostalCode
                    ORDER BY CASE WHEN t.Latitude IS NOT NULL THEN 0 ELSE 1 END,
                             CASE WHEN t.Population IS NOT NULL THEN 0 ELSE 1 END,
                             t.SourceGeographyId
                ) AS DedupRank
            FROM #TypedGeography AS t
            INNER JOIN ref.Country AS c
                ON c.CountryCode = t.ResolvedCountryCode
        ) AS d
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = d.RegionCode
        OUTER APPLY
        (
            SELECT TOP (1) j.TaxJurisdictionCode, j.TaxRegimeCode
            FROM ref.TaxJurisdiction AS j
            WHERE j.CountryCode = d.ResolvedCountryCode
              AND j.EffectiveToDate IS NULL
              AND (
                      --  NA: ZIP range match, most specific range first.
                      (rg.RegionCode = N'NA'
                       AND d.PostalCode IS NOT NULL
                       AND j.PostalCodeLow IS NOT NULL
                       AND d.PostalCode BETWEEN j.PostalCodeLow AND j.PostalCodeHigh)
                      --  EU: VAT is national, so the country-level row wins.
                   OR (rg.RegionCode = N'EU' AND j.StateProvinceCode IS NULL)
                      --  APAC: state/prefecture where present, country otherwise.
                   OR (rg.RegionCode = N'APAC'
                       AND (j.StateProvinceCode = d.StateProvinceCode OR j.StateProvinceCode IS NULL))
                  )
            ORDER BY CASE WHEN j.PostalCodeLow IS NOT NULL THEN 0 ELSE 1 END,
                     CASE WHEN j.StateProvinceCode IS NOT NULL THEN 0 ELSE 1 END,
                     j.TaxJurisdictionCode
        ) AS tj
        WHERE d.DedupRank = 1;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @DeleteRowCount     = @DeletedRows,
            @RejectRowCount     = @LookupMisses;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_GEOGRAPHY',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TruncateAndReload_Geography';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
