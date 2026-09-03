/*
    ref.usp_LoadTaxJurisdiction

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_Geography (SSIS)
    Reads         : raw.OracleTaxRate, raw.OracleGeography, ref.Country, ref.Region
    Writes        : ref.TaxJurisdiction, err.RejectedConstraintViolation,
                    err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Three regimes, three genuinely different shapes. This procedure is long
    because the shapes are not variations of one another and every attempt to
    fold them into a single pass has produced wrong tax on one continent:

    NA  sales tax is a stack of separately governed rates resolved by ZIP range.
        State, county, city and special district each publish their own rate and
        the combined rate is their sum - except in Canada, where PST compounds
        on GST and the combined rate is (1+gst)*(1+pst)-1. The jurisdiction key
        is COUNTRY|STATE|COUNTY|CITY and PostalCodeLow/PostalCodeHigh carry the
        ZIP range the address lookup uses. Nothing here is reverse charged and
        registration is per state.

    EU  VAT is a single country-level rate with a recovery percentage and a
        reverse-charge flag. There is no state, county or city component at all,
        so those columns stay NULL rather than being filled with zeros: a NULL
        county rate means "this regime has no county rate", a zero would mean
        "this county charges nothing". The jurisdiction key is the country code.
        Reverse charge applies to cross-border B2B supplies between member
        states and is carried per jurisdiction because the UK rows have to keep
        their pre-exit treatment.

    APAC GST is a single national rate with no recovery and no reverse charge,
        but the jurisdiction is registered per state in AU and per prefecture in
        JP, so the key is COUNTRY|STATE where a state exists and COUNTRY where it
        does not. Registration is mandatory above the local turnover threshold,
        which is why RegistrationRequiredFlag is set for every APAC row.

    Every row is effective-dated. A rate change closes the previous row on the
    day before the new EFFECTIVE_FROM_DT rather than updating it in place, so
    that a restatement of an old invoice still values at the old rate.
*/

IF OBJECT_ID(N'ref.usp_LoadTaxJurisdiction', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadTaxJurisdiction;
GO

CREATE PROCEDURE ref.usp_LoadTaxJurisdiction
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'ref.TaxJurisdiction';
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @NaRows        BIGINT = 0;
    DECLARE @EuRows        BIGINT = 0;
    DECLARE @ApacRows      BIGINT = 0;
    DECLARE @ClosedRows    BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;
    DECLARE @LookupMisses  BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleTaxRate AS r
        WHERE r.BatchId = @BatchId;

        SELECT
            TaxRateId        = LTRIM(RTRIM(r.TAX_RATE_ID)),
            TaxCode          = UPPER(LTRIM(RTRIM(r.TAX_CD))),
            TaxRegimeCode    = UPPER(LTRIM(RTRIM(r.TAX_REGIME_CD))),
            SourceJurisdiction = NULLIF(UPPER(LTRIM(RTRIM(r.TAX_JURISDICTION_CD))), N''),
            CountryCode      = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2), N''),
            StateProvinceCode = NULLIF(LEFT(UPPER(LTRIM(RTRIM(r.STATE_PROVINCE_CD))), 20), N''),
            TaxClassCode     = NULLIF(UPPER(LTRIM(RTRIM(r.TAX_CLASS_CD))), N''),
            RatePercent      = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(r.RATE_PCT, N'.')),
            CompoundFlag     = CASE WHEN UPPER(LTRIM(RTRIM(r.COMPOUND_FLG))) IN (N'Y', N'YES', N'1') THEN 1 ELSE 0 END,
            RecoverablePct   = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(r.RECOVERABLE_PCT, N'.')),
            ReverseChargeFlag = CASE WHEN UPPER(LTRIM(RTRIM(r.REVERSE_CHARGE_FLG))) IN (N'Y', N'YES', N'1') THEN 1 ELSE 0 END,
            EffectiveFromDate = CONVERT(DATE, ISNULL(stg.ufn_SafeDate(r.EFFECTIVE_FROM_DT, N'NA'), N'1900-01-01')),
            EffectiveToDate  = CONVERT(DATE, stg.ufn_SafeDate(r.EFFECTIVE_TO_DT, N'NA'))
        INTO #TaxTyped
        FROM raw.OracleTaxRate AS r
        WHERE r.BatchId = @BatchId;

        --  A rate with no country cannot be resolved by any of the three
        --  resolution paths and never reaches the conformed set.
        INSERT INTO err.RejectedConstraintViolation
        (
            BatchId, PackageExecutionId, TargetObjectName, ConstraintName, ConstraintTypeCode,
            ViolatingBusinessKey, ViolatingColumnName, ViolatingValue, RejectReasonCode,
            RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'ref.TaxJurisdiction', N'PK_refTaxJurisdiction', N'PK',
            t.TaxRateId, N'CountryCode', ISNULL(t.CountryCode, N''), N'CONSTRAINT',
            CASE
                WHEN t.CountryCode IS NULL THEN N'COUNTRY_CD is empty; the jurisdiction cannot be keyed'
                ELSE N'RATE_PCT is not a usable percentage'
            END,
            N'Reference',
            CONCAT(N'{"TAX_RATE_ID":"', t.TaxRateId, N'","TAX_CD":"', t.TaxCode, N'"}')
        FROM #TaxTyped AS t
        WHERE t.CountryCode IS NULL
           OR t.RatePercent IS NULL
           OR t.RatePercent < 0;

        SET @RejectedRows = @@ROWCOUNT;

        --  Countries the conformed list has never heard of are reported, and
        --  their rates are held back rather than keyed against nothing.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleTaxRate', MIN(t.TaxRateId), N'Country',
            N'COUNTRY_CD', t.CountryCode, @SourceSystemCode, N'LOOKUP_MISS',
            N'tax rate references a country that is not in ref.Country',
            N'Reference', 0, 0, COUNT_BIG(*), NULL
        FROM #TaxTyped AS t
        WHERE t.CountryCode IS NOT NULL
          AND t.RatePercent >= 0
          AND NOT EXISTS (SELECT 1 FROM ref.Country AS c WHERE c.CountryCode = t.CountryCode)
        GROUP BY t.CountryCode;

        SET @LookupMisses = @@ROWCOUNT;

        SELECT
            t.*,
            RegionCode = g.RegionCode
        INTO #TaxUsable
        FROM #TaxTyped AS t
        INNER JOIN ref.Country AS c
            ON c.CountryCode = t.CountryCode
        INNER JOIN ref.Region AS g
            ON g.RegionCode = c.RegionCode
        WHERE t.RatePercent >= 0;

        BEGIN TRANSACTION;

        --  A rate that has been superseded closes the day before its successor
        --  starts. Effective dating is done here, not in the fact loads.
        UPDATE j
        SET j.EffectiveToDate = DATEADD(day, -1, s.EffectiveFromDate)
        FROM ref.TaxJurisdiction AS j
        INNER JOIN
        (
            SELECT
                CountryCode,
                StateProvinceCode,
                EffectiveFromDate = MIN(EffectiveFromDate)
            FROM #TaxUsable
            GROUP BY CountryCode, StateProvinceCode
        ) AS s
            ON  j.CountryCode = s.CountryCode
            AND ISNULL(j.StateProvinceCode, N'-') = ISNULL(s.StateProvinceCode, N'-')
        WHERE j.EffectiveToDate IS NULL
          AND j.EffectiveFromDate < s.EffectiveFromDate;

        SET @ClosedRows = @@ROWCOUNT;

        --  ------------------------------------------------------------------
        --  NA: a stack of separately governed rates, resolved by ZIP range.
        --  ------------------------------------------------------------------
        WITH NaStack AS
        (
            SELECT
                t.CountryCode,
                t.StateProvinceCode,
                CountyOrDistrictName = MAX(CASE WHEN t.TaxClassCode = N'COUNTY' THEN t.SourceJurisdiction END),
                CityName             = MAX(CASE WHEN t.TaxClassCode = N'CITY' THEN t.SourceJurisdiction END),
                StateRate            = MAX(CASE WHEN t.TaxClassCode IN (N'STATE', N'GST') THEN t.RatePercent END),
                CountyRate           = MAX(CASE WHEN t.TaxClassCode = N'COUNTY' THEN t.RatePercent END),
                CityRate             = MAX(CASE WHEN t.TaxClassCode = N'CITY' THEN t.RatePercent END),
                DistrictRate         = MAX(CASE WHEN t.TaxClassCode IN (N'DISTRICT', N'SPECIAL') THEN t.RatePercent END),
                ProvincialRate       = MAX(CASE WHEN t.TaxClassCode IN (N'PST', N'QST', N'HST') THEN t.RatePercent END),
                CompoundFlag         = MAX(t.CompoundFlag),
                EffectiveFromDate    = MIN(t.EffectiveFromDate),
                EffectiveToDate      = MAX(t.EffectiveToDate),
                FallbackRate         = MAX(t.RatePercent)
            FROM #TaxUsable AS t
            WHERE t.RegionCode = N'NA'
            GROUP BY t.CountryCode, t.StateProvinceCode
        )
        INSERT INTO ref.TaxJurisdiction
        (
            TaxJurisdictionCode, TaxJurisdictionName, TaxRegimeCode, CountryCode, StateProvinceCode,
            CountyOrDistrictName, CityName, PostalCodeLow, PostalCodeHigh, CombinedRatePercent,
            StateRatePercent, CountyRatePercent, CityRatePercent, SpecialDistrictRatePercent,
            ReverseChargeEligible, RegistrationRequiredFlag, EffectiveFromDate, EffectiveToDate
        )
        SELECT
            LEFT(CONCAT(n.CountryCode, N'|', ISNULL(n.StateProvinceCode, N'XX'), N'|',
                        ISNULL(n.CountyOrDistrictName, N'-'), N'|', ISNULL(n.CityName, N'-')), 30),
            CONCAT(n.CountryCode, N' ', ISNULL(n.StateProvinceCode, N'state-wide'), N' sales tax'),
            N'SALESTAX',
            n.CountryCode,
            n.StateProvinceCode,
            n.CountyOrDistrictName,
            n.CityName,
            p.PostalCodeLow,
            p.PostalCodeHigh,
            --  Canada compounds PST on top of GST; the United States sums.
            CASE
                WHEN n.CountryCode = N'CA' AND n.CompoundFlag = 1
                    THEN CONVERT(DECIMAL(9,4),
                                 (((1 + ISNULL(n.StateRate, 0) / 100.0)
                                   * (1 + ISNULL(n.ProvincialRate, 0) / 100.0)) - 1) * 100.0)
                ELSE ISNULL(n.StateRate, n.FallbackRate) + ISNULL(n.CountyRate, 0)
                     + ISNULL(n.CityRate, 0) + ISNULL(n.DistrictRate, 0)
            END,
            ISNULL(n.StateRate, n.FallbackRate),
            n.CountyRate,
            n.CityRate,
            n.DistrictRate,
            0,                                  -- reverse charge does not exist under sales tax
            CASE WHEN n.StateProvinceCode IS NULL THEN 0 ELSE 1 END,
            n.EffectiveFromDate,
            n.EffectiveToDate
        FROM NaStack AS n
        OUTER APPLY
        (
            --  The ZIP range comes from the geography extract, which is the only
            --  place the estate holds one.
            SELECT
                PostalCodeLow  = MIN(NULLIF(LTRIM(RTRIM(g.POSTAL_CD)), N'')),
                PostalCodeHigh = MAX(NULLIF(LTRIM(RTRIM(g.POSTAL_CD)), N''))
            FROM raw.OracleGeography AS g
            WHERE g.BatchId = @BatchId
              AND LEFT(UPPER(LTRIM(RTRIM(g.COUNTRY_CD))), 2) = n.CountryCode
              AND UPPER(LTRIM(RTRIM(g.STATE_PROVINCE_CD))) = ISNULL(n.StateProvinceCode, N'')
        ) AS p
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.TaxJurisdiction AS j
                  WHERE j.TaxJurisdictionCode =
                        LEFT(CONCAT(n.CountryCode, N'|', ISNULL(n.StateProvinceCode, N'XX'), N'|',
                                    ISNULL(n.CountyOrDistrictName, N'-'), N'|',
                                    ISNULL(n.CityName, N'-')), 30)
              );

        SET @NaRows = @@ROWCOUNT;

        --  ------------------------------------------------------------------
        --  EU: one country-level VAT rate, recovery percentage, reverse charge.
        --  The sub-national rate columns are deliberately left NULL.
        --  ------------------------------------------------------------------
        WITH EuVat AS
        (
            SELECT
                t.CountryCode,
                StandardRate      = MAX(CASE WHEN t.TaxClassCode IN (N'STANDARD', N'STD') THEN t.RatePercent END),
                AnyRate           = MAX(t.RatePercent),
                RecoverablePct    = MAX(t.RecoverablePct),
                ReverseChargeFlag = MAX(t.ReverseChargeFlag),
                EffectiveFromDate = MIN(t.EffectiveFromDate),
                EffectiveToDate   = MAX(t.EffectiveToDate)
            FROM #TaxUsable AS t
            WHERE t.RegionCode = N'EU'
            GROUP BY t.CountryCode
        )
        INSERT INTO ref.TaxJurisdiction
        (
            TaxJurisdictionCode, TaxJurisdictionName, TaxRegimeCode, CountryCode, StateProvinceCode,
            CountyOrDistrictName, CityName, PostalCodeLow, PostalCodeHigh, CombinedRatePercent,
            StateRatePercent, CountyRatePercent, CityRatePercent, SpecialDistrictRatePercent,
            ReverseChargeEligible, RegistrationRequiredFlag, EffectiveFromDate, EffectiveToDate
        )
        SELECT
            LEFT(CONCAT(N'EU|', e.CountryCode), 30),
            CONCAT(c.CountryName, N' VAT'),
            N'VAT',
            e.CountryCode,
            NULL,                               -- VAT has no state component
            NULL,                               -- and no county component
            NULL,                               -- and no city component
            NULL,                               -- VAT is never resolved by postal range
            NULL,
            ISNULL(e.StandardRate, e.AnyRate),
            NULL,
            NULL,
            NULL,
            NULL,
            --  Reverse charge applies to cross-border B2B supplies between
            --  member states; a country that has left keeps its historical rows
            --  but is no longer eligible on new supplies.
            CASE
                WHEN c.IsEuMemberState = 1 AND c.EuExitDate IS NULL THEN e.ReverseChargeFlag
                ELSE 0
            END,
            1,                                  -- VAT registration is always required
            e.EffectiveFromDate,
            COALESCE(e.EffectiveToDate, DATEADD(day, -1, c.EuExitDate))
        FROM EuVat AS e
        INNER JOIN ref.Country AS c
            ON c.CountryCode = e.CountryCode
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.TaxJurisdiction AS j
                  WHERE j.TaxJurisdictionCode = LEFT(CONCAT(N'EU|', e.CountryCode), 30)
              );

        SET @EuRows = @@ROWCOUNT;

        --  ------------------------------------------------------------------
        --  APAC: a national GST rate, registered per state or prefecture.
        --  ------------------------------------------------------------------
        WITH ApacGst AS
        (
            SELECT
                t.CountryCode,
                t.StateProvinceCode,
                GstRate           = MAX(t.RatePercent),
                EffectiveFromDate = MIN(t.EffectiveFromDate),
                EffectiveToDate   = MAX(t.EffectiveToDate)
            FROM #TaxUsable AS t
            WHERE t.RegionCode = N'APAC'
            GROUP BY t.CountryCode, t.StateProvinceCode
        )
        INSERT INTO ref.TaxJurisdiction
        (
            TaxJurisdictionCode, TaxJurisdictionName, TaxRegimeCode, CountryCode, StateProvinceCode,
            CountyOrDistrictName, CityName, PostalCodeLow, PostalCodeHigh, CombinedRatePercent,
            StateRatePercent, CountyRatePercent, CityRatePercent, SpecialDistrictRatePercent,
            ReverseChargeEligible, RegistrationRequiredFlag, EffectiveFromDate, EffectiveToDate
        )
        SELECT
            LEFT(CONCAT(N'GST|', a.CountryCode,
                        CASE WHEN a.StateProvinceCode IS NULL THEN N'' ELSE CONCAT(N'|', a.StateProvinceCode) END), 30),
            CONCAT(c.CountryName, N' GST',
                   CASE WHEN a.StateProvinceCode IS NULL THEN N'' ELSE CONCAT(N' (', a.StateProvinceCode, N')') END),
            N'GST',
            a.CountryCode,
            a.StateProvinceCode,
            NULL,
            NULL,
            NULL,
            NULL,
            a.GstRate,
            a.GstRate,                          -- the national rate is the only component
            NULL,
            NULL,
            NULL,
            0,                                  -- no reverse charge under GST in this estate
            1,                                  -- registration is required per state / prefecture
            a.EffectiveFromDate,
            a.EffectiveToDate
        FROM ApacGst AS a
        INNER JOIN ref.Country AS c
            ON c.CountryCode = a.CountryCode
        WHERE NOT EXISTS
              (
                  SELECT 1
                  FROM ref.TaxJurisdiction AS j
                  WHERE j.TaxJurisdictionCode =
                        LEFT(CONCAT(N'GST|', a.CountryCode,
                                    CASE WHEN a.StateProvinceCode IS NULL THEN N''
                                         ELSE CONCAT(N'|', a.StateProvinceCode) END), 30)
              );

        SET @ApacRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @NaRows + @EuRows + @ApacRows,
            @InsertRowCount     = @NaRows + @EuRows + @ApacRows,
            @UpdateRowCount     = @ClosedRows,
            @RejectRowCount     = @RejectedRows + @LookupMisses;

        DROP TABLE #TaxTyped;
        DROP TABLE #TaxUsable;
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
            @ProcedureName      = N'ref.usp_LoadTaxJurisdiction';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
