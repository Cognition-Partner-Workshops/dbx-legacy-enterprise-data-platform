/*
    stg.usp_NormalizeAddress

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_NORMALIZE_ADDRESS (SSIS), after STG_NORMALIZE_CUSTOMER
    Reads/writes  : stg.CustomerAddress
    Writes        : work.CustomerAddressStandardized
    Reads         : ref.Country, ref.Region, ref.PostalFormatRule, stg.Geography
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Address standardisation, kept row-by-row on purpose.

    This is the oldest procedure in the staging database. It was written against
    the 2004 address-cleansing rules, converted from a DTS ActiveX script, and it
    still walks a cursor because the rule set is chosen per row and the postal
    mask is applied character by character. Rewriting it set-based has been on the
    backlog since 2013; the audit trail it writes into
    work.CustomerAddressStandardized is what the data-quality team reconciles
    against, so any rewrite has to reproduce it exactly.

    Rule sets:
        NA_USPS    upper case, street-type abbreviations, ZIP5 (the +4 is kept in
                   PostalCodeRaw only), state code must be two letters.
        EU_COUNTRY per-country mask from ref.PostalFormatRule; the country prefix
                   some source systems put in front of the postal code is
                   stripped; city is title-cased, not upper-cased.
        APAC_LOCAL address line 3 becomes DistrictName; postal code is digits
                   only; the state/prefecture column is optional.
*/

IF OBJECT_ID(N'stg.usp_NormalizeAddress', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_NormalizeAddress;
GO

CREATE PROCEDURE stg.usp_NormalizeAddress
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.CustomerAddress';
    DECLARE @TargetRows    BIGINT = 0;
    DECLARE @UpdatedRows   BIGINT = 0;
    DECLARE @UnparsedRows  BIGINT = 0;

    DECLARE @AddressKey    NVARCHAR(120);
    DECLARE @CustomerKey   NVARCHAR(100);
    DECLARE @Line1         NVARCHAR(200);
    DECLARE @Line2         NVARCHAR(200);
    DECLARE @City          NVARCHAR(100);
    DECLARE @StateCode     NVARCHAR(20);
    DECLARE @PostalRaw     NVARCHAR(40);
    DECLARE @CountryCode   NCHAR(2);
    DECLARE @RegionCode    NVARCHAR(10);
    DECLARE @RuleSetCode   NVARCHAR(30);

    DECLARE @OutLine1      NVARCHAR(200);
    DECLARE @OutCity       NVARCHAR(100);
    DECLARE @OutState      NVARCHAR(20);
    DECLARE @OutPostal     NVARCHAR(20);
    DECLARE @MaskApplied   NVARCHAR(30);
    DECLARE @StatusCode    NVARCHAR(20);
    DECLARE @ChangedList   NVARCHAR(200);
    DECLARE @GeographyKey  NVARCHAR(120);
    DECLARE @MatchLevel    NVARCHAR(20);
    DECLARE @TruncateTo    TINYINT;
    DECLARE @StripChars    NVARCHAR(30);

    BEGIN TRY
        SELECT @TargetRows = COUNT_BIG(*)
        FROM stg.CustomerAddress AS a
        WHERE a.BatchId = @BatchId;

        DECLARE AddressCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                a.AddressBusinessKey,
                a.CustomerBusinessKey,
                a.AddressLine1,
                a.AddressLine2,
                a.CityName,
                a.StateProvinceCode,
                a.PostalCodeRaw,
                a.CountryCode,
                ISNULL(cn.RegionCode, a.RegionCode),
                ISNULL(rg.AddressRuleSetCode, N'NA_USPS')
            FROM stg.CustomerAddress AS a
            LEFT JOIN ref.Country AS cn
                ON cn.CountryCode = a.CountryCode
            LEFT JOIN ref.Region AS rg
                ON rg.RegionCode = ISNULL(cn.RegionCode, a.RegionCode)
            WHERE a.BatchId = @BatchId;

        OPEN AddressCursor;
        FETCH NEXT FROM AddressCursor
            INTO @AddressKey, @CustomerKey, @Line1, @Line2, @City, @StateCode,
                 @PostalRaw, @CountryCode, @RegionCode, @RuleSetCode;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @OutLine1     = stg.ufn_CleanString(@Line1, 0);
            SET @OutCity      = stg.ufn_CleanString(@City, 0);
            SET @OutState     = NULLIF(UPPER(LTRIM(RTRIM(ISNULL(@StateCode, N'')))), N'');
            SET @OutPostal    = stg.ufn_StandardizePostalCode(@PostalRaw, @CountryCode);
            SET @MaskApplied  = NULL;
            SET @ChangedList  = N'';
            SET @GeographyKey = NULL;
            SET @MatchLevel   = N'NONE';
            SET @TruncateTo   = NULL;
            SET @StripChars   = NULL;

            IF @RuleSetCode = N'NA_USPS'
            BEGIN
                SET @OutLine1 = UPPER(@OutLine1);
                SET @OutLine1 = REPLACE(@OutLine1, N' STREET',    N' ST');
                SET @OutLine1 = REPLACE(@OutLine1, N' AVENUE',    N' AVE');
                SET @OutLine1 = REPLACE(@OutLine1, N' BOULEVARD', N' BLVD');
                SET @OutLine1 = REPLACE(@OutLine1, N' SUITE',     N' STE');
                SET @OutLine1 = REPLACE(@OutLine1, N' APARTMENT', N' APT');
                SET @OutCity  = UPPER(@OutCity);

                --  ZIP5 only; the +4 stays in PostalCodeRaw.
                IF @OutPostal IS NOT NULL AND CHARINDEX(N'-', @OutPostal) > 0
                    SET @OutPostal = LEFT(@OutPostal, CHARINDEX(N'-', @OutPostal) - 1);

                IF @OutPostal IS NOT NULL AND LEN(@OutPostal) > 5
                    SET @OutPostal = LEFT(@OutPostal, 5);

                IF @OutState IS NOT NULL AND LEN(@OutState) <> 2
                    SET @OutState = NULL;
            END
            ELSE IF @RuleSetCode = N'EU_COUNTRY'
            BEGIN
                SELECT TOP (1)
                    @MaskApplied = r.FormatMask,
                    @TruncateTo  = r.TruncateToLength,
                    @StripChars  = r.StripCharacters
                FROM ref.PostalFormatRule AS r
                WHERE r.CountryCode = @CountryCode
                  AND r.RuleSetCode = N'EU_COUNTRY'
                ORDER BY r.RulePriority;

                --  Some EU source systems prefix the postal code with the country.
                IF @OutPostal IS NOT NULL
                   AND LEFT(@OutPostal, 2) = @CountryCode
                   AND LEN(@OutPostal) > 4
                    SET @OutPostal = LTRIM(REPLACE(STUFF(@OutPostal, 1, 2, N''), N'-', N''));

                IF @StripChars IS NOT NULL AND @OutPostal IS NOT NULL
                    SET @OutPostal = REPLACE(@OutPostal, @StripChars, N'');

                IF @TruncateTo IS NOT NULL AND @OutPostal IS NOT NULL AND LEN(@OutPostal) > @TruncateTo
                    SET @OutPostal = LEFT(@OutPostal, @TruncateTo);

                --  Cities keep their casing in the EU: MÜNCHEN upper-cased loses
                --  the umlaut on the legacy collation used by two of the ledgers.
                SET @OutCity = @OutCity;
            END
            ELSE
            BEGIN
                --  APAC_LOCAL. Line 3 has already landed in DistrictName during
                --  the load; here only the postal code and the optional state.
                IF @OutPostal IS NOT NULL
                    SET @OutPostal = REPLACE(REPLACE(@OutPostal, N'-', N''), N' ', N'');

                IF @OutPostal IS NOT NULL AND @OutPostal LIKE N'%[^0-9]%'
                    SET @OutPostal = NULL;

                SET @MaskApplied = N'DIGITS_ONLY';
            END;

            IF ISNULL(@OutLine1, N'') <> ISNULL(@Line1, N'')
                SET @ChangedList = @ChangedList + N'AddressLine1;';
            IF ISNULL(@OutCity, N'') <> ISNULL(@City, N'')
                SET @ChangedList = @ChangedList + N'CityName;';
            IF ISNULL(@OutPostal, N'') <> ISNULL(@PostalRaw, N'')
                SET @ChangedList = @ChangedList + N'PostalCode;';
            IF ISNULL(@OutState, N'') <> ISNULL(@StateCode, N'')
                SET @ChangedList = @ChangedList + N'StateProvinceCode;';

            SET @StatusCode =
                CASE
                    WHEN @OutLine1 IS NULL OR @OutCity IS NULL THEN N'UNPARSED'
                    WHEN @ChangedList = N''                    THEN N'CLEAN'
                    ELSE N'CORRECTED'
                END;

            --  Geography resolution: exact postal first, then city.
            SELECT TOP (1)
                @GeographyKey = g.GeographyBusinessKey,
                @MatchLevel   = N'POSTAL'
            FROM stg.Geography AS g
            WHERE g.BatchId     = @BatchId
              AND g.CountryCode = @CountryCode
              AND g.PostalCode  = @OutPostal
              AND @OutPostal IS NOT NULL;

            IF @GeographyKey IS NULL
                SELECT TOP (1)
                    @GeographyKey = g.GeographyBusinessKey,
                    @MatchLevel   = N'CITY'
                FROM stg.Geography AS g
                WHERE g.BatchId     = @BatchId
                  AND g.CountryCode = @CountryCode
                  AND g.CityName    = @OutCity
                ORDER BY g.GeographyBusinessKey;

            INSERT INTO work.CustomerAddressStandardized
            (
                BatchId, PackageExecutionId, AddressBusinessKey, CustomerBusinessKey, RuleSetCode,
                InputAddressLine1, InputCityName, InputPostalCode, InputCountryCode,
                OutputAddressLine1, OutputCityName, OutputStateProvinceCode, OutputPostalCode,
                OutputCountryCode, PostalMaskApplied, StandardizationStatusCode, ChangedComponentList,
                GeographyBusinessKey, GeographyMatchLevel
            )
            VALUES
            (
                @BatchId, @PackageExecutionId, @AddressKey, @CustomerKey, @RuleSetCode,
                @Line1, @City, @PostalRaw, @CountryCode,
                @OutLine1, @OutCity, @OutState, @OutPostal,
                @CountryCode, @MaskApplied, @StatusCode, NULLIF(@ChangedList, N''),
                @GeographyKey, @MatchLevel
            );

            UPDATE stg.CustomerAddress
            SET AddressLine1           = @OutLine1,
                CityNameStandardized   = @OutCity,
                StateProvinceCode      = @OutState,
                PostalCodeStandardized = @OutPostal,
                PostalCodeValidFlag    = CASE WHEN @OutPostal IS NULL THEN 0 ELSE 1 END,
                GeographyBusinessKey   = @GeographyKey,
                GeocodeQualityCode     = @MatchLevel,
                StandardizationRuleSet = @RuleSetCode,
                DqStatusCode           = CASE
                                             WHEN @StatusCode = N'UNPARSED' THEN N'FAIL'
                                             WHEN @GeographyKey IS NULL     THEN N'WARN'
                                             ELSE DqStatusCode
                                         END,
                RowHash                = HASHBYTES('SHA2_256',
                                             CONCAT(@OutLine1, N'|', @OutCity, N'|', @OutState, N'|',
                                                    @OutPostal, N'|', @CountryCode))
            WHERE AddressBusinessKey = @AddressKey
              AND BatchId            = @BatchId;

            SET @UpdatedRows = @UpdatedRows + @@ROWCOUNT;

            IF @StatusCode = N'UNPARSED'
                SET @UnparsedRows = @UnparsedRows + 1;

            FETCH NEXT FROM AddressCursor
                INTO @AddressKey, @CustomerKey, @Line1, @Line2, @City, @StateCode,
                     @PostalRaw, @CountryCode, @RegionCode, @RuleSetCode;
        END;

        CLOSE AddressCursor;
        DEALLOCATE AddressCursor;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @TargetRows,
            @TargetRowCount     = @TargetRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @UnparsedRows;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'AddressCursor') >= 0
        BEGIN
            CLOSE AddressCursor;
            DEALLOCATE AddressCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_NORMALIZE_ADDRESS',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_NormalizeAddress';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
