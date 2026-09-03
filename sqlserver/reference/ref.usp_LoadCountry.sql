/*
    ref.usp_LoadCountry

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_Geography (SSIS)
    Reads         : raw.OracleGeography, ref.Region, ref.CodeCrosswalk
    Writes        : ref.Country, err.RejectedLookupFailure, err.RejectedConstraintViolation
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The ERP geography extract is the only country list either source system
    publishes, so the conformed country set is derived from it. Three things
    make this more than a distinct select:

      1. rows created before the 2011 ISO clean-up carry the ISO3 value in
         COUNTRY_CD, so the ISO2 code has to be recovered from either column
         through ref.CodeCrosswalk domain COUNTRY;
      2. the same country arrives once per site, with the descriptive columns
         populated to different degrees, so the row kept is the most complete
         one rather than the first one;
      3. EU membership drives the VAT treatment and has to survive history -
         accession and exit dates are held so a 2015 invoice is still valued
         the way it was valued in 2015.

    Countries that cannot be resolved to a two-character code are rejected to
    err.RejectedConstraintViolation rather than dropped, because the geography
    load will otherwise attribute their addresses to the unknown member and the
    stewards will never see them.
*/

IF OBJECT_ID(N'ref.usp_LoadCountry', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadCountry;
GO

CREATE PROCEDURE ref.usp_LoadCountry
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'ref.Country';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;
    DECLARE @MergeAction TABLE (ActionName NVARCHAR(10) NOT NULL);

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleGeography AS r
        WHERE r.BatchId = @BatchId;

        SELECT
            SourceCountryValue = UPPER(LTRIM(RTRIM(r.COUNTRY_CD))),
            --  Either column can hold either code shape; the crosswalk wins when
            --  it has an opinion, otherwise the two-character value is taken.
            CountryCode        = COALESCE
                                 (
                                     x.ConformedCodeValue,
                                     NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2), N'')
                                 ),
            CountryCodeIso3    = NULLIF(LEFT(UPPER(LTRIM(RTRIM(COALESCE(NULLIF(r.ISO3_CD, N''), r.COUNTRY_CD)))), 3), N''),
            CountryName        = LEFT(stg.ufn_CleanString(r.COUNTRY_NAME, 0), 100),
            RegionCode         = UPPER(LTRIM(RTRIM(r.REGION_CD))),
            SubRegionName      = NULLIF(LEFT(stg.ufn_CleanString(r.SUB_REGION_NAME, 0), 100), N''),
            LocalCurrencyCode  = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.CURRENCY_CD))), 3), N''),
            PostalFormatMask   = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.POSTAL_FORMAT_MASK))), 30), N''),
            PostalCodePresent  = CASE WHEN NULLIF(LTRIM(RTRIM(r.POSTAL_CD)), N'') IS NULL THEN 0 ELSE 1 END,
            StatePresent       = CASE WHEN NULLIF(LTRIM(RTRIM(r.STATE_PROVINCE_CD)), N'') IS NULL THEN 0 ELSE 1 END,
            SourceGeographyId  = LTRIM(RTRIM(r.GEOGRAPHY_ID))
        INTO #CountryCandidate
        FROM raw.OracleGeography AS r
        LEFT JOIN ref.CodeCrosswalk AS x
            ON  x.CodeDomainCode   = N'COUNTRY'
            AND x.SourceSystemCode = @SourceSystemCode
            AND x.SourceCodeValue  = UPPER(LTRIM(RTRIM(r.COUNTRY_CD)))
            AND x.EffectiveToDate IS NULL
        WHERE r.BatchId = @BatchId;

        --  Unresolvable country codes never reach ref.Country.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.Country', N'PK_refCountry', N'PK',
            MIN(c.SourceGeographyId), N'CountryCode', c.SourceCountryValue, N'CONSTRAINT',
            N'COUNTRY_CD does not resolve to a two-character conformed country code',
            N'Reference', CONCAT(N'{"COUNTRY_CD":"', c.SourceCountryValue, N'"}')
        FROM #CountryCandidate AS c
        WHERE c.CountryCode IS NULL
           OR LEN(c.CountryCode) < 2
        GROUP BY c.SourceCountryValue;

        SET @RejectedRows = @@ROWCOUNT;

        --  One row per country: the site rows disagree about how much of the
        --  descriptive detail they carry, so the most populated value wins.
        SELECT
            CountryCode       = c.CountryCode,
            CountryCodeIso3   = MAX(c.CountryCodeIso3),
            CountryName       = MAX(c.CountryName),
            RegionCode        = MAX(c.RegionCode),
            SubRegionName     = MAX(c.SubRegionName),
            LocalCurrencyCode = MAX(c.LocalCurrencyCode),
            PostalFormatMask  = MAX(c.PostalFormatMask),
            PostalRequired    = MAX(c.PostalCodePresent),
            StateRequired     = MAX(c.StatePresent)
        INTO #CountryConformed
        FROM #CountryCandidate AS c
        WHERE c.CountryCode IS NOT NULL
          AND LEN(c.CountryCode) = 2
        GROUP BY c.CountryCode;

        BEGIN TRANSACTION;

        MERGE ref.Country AS tgt
        USING
        (
            SELECT
                c.CountryCode,
                c.CountryCodeIso3,
                CountryName            = ISNULL(NULLIF(c.CountryName, N''), c.CountryCode),
                --  An unrecognised region falls back to NA, which is what the
                --  original 2009 load did and what the fact loads still expect.
                RegionCode             = ISNULL(g.RegionCode, N'NA'),
                c.SubRegionName,
                c.LocalCurrencyCode,
                c.PostalFormatMask,
                PostalCodeRequiredFlag = c.PostalRequired,
                StateProvinceRequiredFlag = c.StateRequired,
                --  Address line order: the EU and NA print the street first,
                --  the CJK countries print the largest unit first.
                AddressLineOrderCode   = CASE
                                             WHEN c.CountryCode IN (N'JP', N'CN', N'KR', N'TW')
                                                 THEN N'EASTERN'
                                             ELSE N'WESTERN'
                                         END,
                VatRegistrationMask    = CASE
                                             WHEN e.CountryCode IS NOT NULL
                                                 THEN CONCAT(c.CountryCode, N'999999999')
                                             ELSE NULL
                                         END,
                IsEuMemberState        = CASE WHEN e.CountryCode IS NOT NULL THEN 1 ELSE 0 END,
                e.EuAccessionDate,
                e.EuExitDate
            FROM #CountryConformed AS c
            LEFT JOIN ref.Region AS g
                ON g.RegionCode = c.RegionCode
            LEFT JOIN
            (
                --  Membership, accession and exit are legal facts, held here so
                --  historical VAT treatment survives a reload.
                SELECT *
                FROM
                (
                    VALUES
                        (N'AT', CONVERT(DATE, N'1995-01-01'), CONVERT(DATE, NULL)),
                        (N'BE', CONVERT(DATE, N'1958-01-01'), CONVERT(DATE, NULL)),
                        (N'DE', CONVERT(DATE, N'1958-01-01'), CONVERT(DATE, NULL)),
                        (N'DK', CONVERT(DATE, N'1973-01-01'), CONVERT(DATE, NULL)),
                        (N'ES', CONVERT(DATE, N'1986-01-01'), CONVERT(DATE, NULL)),
                        (N'FI', CONVERT(DATE, N'1995-01-01'), CONVERT(DATE, NULL)),
                        (N'FR', CONVERT(DATE, N'1958-01-01'), CONVERT(DATE, NULL)),
                        (N'IE', CONVERT(DATE, N'1973-01-01'), CONVERT(DATE, NULL)),
                        (N'IT', CONVERT(DATE, N'1958-01-01'), CONVERT(DATE, NULL)),
                        (N'NL', CONVERT(DATE, N'1958-01-01'), CONVERT(DATE, NULL)),
                        (N'PL', CONVERT(DATE, N'2004-05-01'), CONVERT(DATE, NULL)),
                        (N'PT', CONVERT(DATE, N'1986-01-01'), CONVERT(DATE, NULL)),
                        (N'SE', CONVERT(DATE, N'1995-01-01'), CONVERT(DATE, NULL)),
                        (N'GB', CONVERT(DATE, N'1973-01-01'), CONVERT(DATE, N'2020-01-31'))
                ) AS m (CountryCode, EuAccessionDate, EuExitDate)
            ) AS e
                ON e.CountryCode = c.CountryCode
        ) AS src
            ON tgt.CountryCode = src.CountryCode
        WHEN MATCHED THEN
            UPDATE SET
                tgt.CountryCodeIso3           = COALESCE(src.CountryCodeIso3, tgt.CountryCodeIso3),
                tgt.CountryName               = src.CountryName,
                tgt.RegionCode                = src.RegionCode,
                tgt.SubRegionName             = COALESCE(src.SubRegionName, tgt.SubRegionName),
                tgt.LocalCurrencyCode         = COALESCE(src.LocalCurrencyCode, tgt.LocalCurrencyCode),
                tgt.PostalFormatMask          = COALESCE(src.PostalFormatMask, tgt.PostalFormatMask),
                tgt.PostalCodeRequiredFlag    = src.PostalCodeRequiredFlag,
                tgt.StateProvinceRequiredFlag = src.StateProvinceRequiredFlag,
                tgt.AddressLineOrderCode      = src.AddressLineOrderCode,
                tgt.VatRegistrationMask       = COALESCE(src.VatRegistrationMask, tgt.VatRegistrationMask),
                tgt.IsEuMemberState           = src.IsEuMemberState,
                tgt.EuAccessionDate           = COALESCE(src.EuAccessionDate, tgt.EuAccessionDate),
                tgt.EuExitDate                = COALESCE(src.EuExitDate, tgt.EuExitDate),
                tgt.IsActive                  = 1
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                CountryCode, CountryCodeIso3, CountryName, RegionCode, SubRegionName,
                LocalCurrencyCode, PostalFormatMask, PostalCodeRequiredFlag,
                StateProvinceRequiredFlag, AddressLineOrderCode, VatRegistrationMask,
                IsEuMemberState, EuAccessionDate, EuExitDate, IsActive
            )
            VALUES
            (
                src.CountryCode, src.CountryCodeIso3, src.CountryName, src.RegionCode,
                src.SubRegionName, src.LocalCurrencyCode, src.PostalFormatMask,
                src.PostalCodeRequiredFlag, src.StateProvinceRequiredFlag,
                src.AddressLineOrderCode, src.VatRegistrationMask, src.IsEuMemberState,
                src.EuAccessionDate, src.EuExitDate, 1
            )
        OUTPUT $action INTO @MergeAction (ActionName);

        SELECT
            @InsertedRows = COUNT_BIG(CASE WHEN a.ActionName = N'INSERT' THEN 1 END),
            @UpdatedRows  = COUNT_BIG(CASE WHEN a.ActionName = N'UPDATE' THEN 1 END)
        FROM @MergeAction AS a;

        COMMIT TRANSACTION;

        DECLARE @TargetRowCountValue BIGINT = @InsertedRows + @UpdatedRows;
        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @TargetRowCountValue,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @RejectedRows;

        DROP TABLE #CountryCandidate;
        DROP TABLE #CountryConformed;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_Geography',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadCountry';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
