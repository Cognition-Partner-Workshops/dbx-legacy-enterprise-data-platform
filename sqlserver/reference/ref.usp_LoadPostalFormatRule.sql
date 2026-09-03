/*
    ref.usp_LoadPostalFormatRule

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : REF_Load_WarehouseSite (SSIS)
    Reads         : ref.Country, ref.Region, raw.OracleGeography
    Writes        : ref.PostalFormatRule, err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    stg.ufn_StandardizePostalCode reads these rules, so they have to exist
    before any address is normalised. The rules are per country because postal
    codes are per country - there is no universal shape and every attempt to
    invent one has broken either Canada or Japan:

        US  five digits; the +4 is stripped rather than kept, because the OLTP
            database only ever populated ZIP5 and the two systems have to agree.
        CA  A9A 9A9 with a single space; the source sends it with, without and
            occasionally with a hyphen.
        GB  the outward/inward split is variable length, so the rule is a length
            range with a space inserted before the last three characters.
        NL  four digits plus two letters, upper-cased, no space.
        JP  seven digits with a hyphen after the third.
        AU / NZ  four digits, no separator.
        DE / FR / IT / ES  five digits, no separator.

    Countries with no explicit rule fall back to their region's rule set at
    priority 900, which strips nothing and only upper-cases; that is what the
    original 2009 address routine did and the fact loads still depend on it.
*/

IF OBJECT_ID(N'ref.usp_LoadPostalFormatRule', N'P') IS NOT NULL
    DROP PROCEDURE ref.usp_LoadPostalFormatRule;
GO

CREATE PROCEDURE ref.usp_LoadPostalFormatRule
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'ref.PostalFormatRule';
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @UpdatedRows   BIGINT = 0;
    DECLARE @FallbackRows  BIGINT = 0;
    DECLARE @LookupMisses  BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM ref.Country AS c
        WHERE c.IsActive = 1;

        --  The per-country grid. Maintained here rather than sourced, because
        --  neither source system publishes a postal rule - the ERP only carries
        --  a mask, and only for the countries someone bothered with.
        SELECT *
        INTO #PostalRule
        FROM
        (
            VALUES
                (N'US', N'NA_USPS',    100, N' -',   1, N'99999',    5,  10, CONVERT(TINYINT, 5),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'ZIP5 only; the +4 extension is dropped so the ERP and the OLTP database agree.'),
                (N'CA', N'NA_USPS',    110, N'- ',   1, N'A9A 9A9',  6,  7,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, 3), N' ',
                 N'Six characters with one space inserted after the forward sortation area.'),
                (N'GB', N'EU_COUNTRY', 200, N' ',    1, N'AA9A 9AA', 5,  8,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), N' ',
                 N'Variable outward code; the inward code is always the last three characters.'),
                (N'IE', N'EU_COUNTRY', 210, N' ',    1, N'A99 AAAA', 7,  8,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, 3), N' ',
                 N'Eircode; rows created before 2015 have no postal code at all and are allowed through.'),
                (N'NL', N'EU_COUNTRY', 220, N' ',    1, N'9999AA',   6,  7,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'Four digits and two letters, upper-cased, no separator.'),
                (N'DE', N'EU_COUNTRY', 230, N' -',   0, N'99999',    5,  5,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Five digits.'),
                (N'FR', N'EU_COUNTRY', 240, N' -',   0, N'99999',    5,  5,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'Five digits; the leading zero is significant and must not be trimmed.'),
                (N'IT', N'EU_COUNTRY', 250, N' -',   0, N'99999',    5,  5,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Five digits.'),
                (N'ES', N'EU_COUNTRY', 260, N' -',   0, N'99999',    5,  5,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'Five digits; the first two identify the province.'),
                (N'PL', N'EU_COUNTRY', 270, N' ',    0, N'99-999',   5,  6,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, 2), N'-', N'Two digits, a hyphen, three digits.'),
                (N'JP', N'APAC_LOCAL', 300, N' ',    0, N'999-9999', 7,  8,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, 3), N'-', N'Seven digits with a hyphen after the third.'),
                (N'AU', N'APAC_LOCAL', 310, N' -',   0, N'9999',     4,  4,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Four digits.'),
                (N'NZ', N'APAC_LOCAL', 320, N' -',   0, N'9999',     4,  4,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Four digits.'),
                (N'SG', N'APAC_LOCAL', 330, N' -',   0, N'999999',   6,  6,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Six digits.'),
                (N'CN', N'APAC_LOCAL', 340, N' -',   0, N'999999',   6,  6,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL), N'Six digits.'),
                (N'IN', N'APAC_LOCAL', 350, N' -',   0, N'999999',   6,  6,  CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'Six digit PIN; older rows carry the pre-1972 six digit code unchanged.'),
                (N'HK', N'APAC_LOCAL', 360, N' -',   1, CONVERT(NVARCHAR(30), NULL),
                 CONVERT(TINYINT, NULL), CONVERT(TINYINT, NULL), CONVERT(TINYINT, NULL),
                 CONVERT(TINYINT, NULL), CONVERT(NVARCHAR(2), NULL),
                 N'Hong Kong has no postal code; the column is expected to be empty.')
        ) AS v (CountryCode, RuleSetCode, RulePriority, StripCharacters, UpperCaseFlag, FormatMask,
                MinimumLength, MaximumLength, TruncateToLength, InsertSeparatorAt, SeparatorCharacter,
                RuleNote);

        BEGIN TRANSACTION;

        MERGE ref.PostalFormatRule AS tgt
        USING #PostalRule AS src
            ON  tgt.CountryCode = src.CountryCode
            AND tgt.RuleSetCode = src.RuleSetCode
            AND tgt.RulePriority = src.RulePriority
        WHEN MATCHED THEN
            UPDATE SET
                tgt.StripCharacters    = src.StripCharacters,
                tgt.UpperCaseFlag      = src.UpperCaseFlag,
                tgt.FormatMask         = src.FormatMask,
                tgt.MinimumLength      = src.MinimumLength,
                tgt.MaximumLength      = src.MaximumLength,
                tgt.TruncateToLength   = src.TruncateToLength,
                tgt.InsertSeparatorAt  = src.InsertSeparatorAt,
                tgt.SeparatorCharacter = src.SeparatorCharacter,
                tgt.RuleNote           = src.RuleNote
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                CountryCode, RuleSetCode, RulePriority, StripCharacters, UpperCaseFlag, FormatMask,
                MinimumLength, MaximumLength, TruncateToLength, InsertSeparatorAt,
                SeparatorCharacter, RuleNote
            )
            VALUES
            (
                src.CountryCode, src.RuleSetCode, src.RulePriority, src.StripCharacters,
                src.UpperCaseFlag, src.FormatMask, src.MinimumLength, src.MaximumLength,
                src.TruncateToLength, src.InsertSeparatorAt, src.SeparatorCharacter, src.RuleNote
            );

        SET @UpdatedRows = @@ROWCOUNT;

        --  Every remaining active country gets its region's fallback rule, so
        --  the address routine always finds something to apply.
        INSERT INTO ref.PostalFormatRule
        (
            CountryCode, RuleSetCode, RulePriority, StripCharacters, UpperCaseFlag, FormatMask,
            MinimumLength, MaximumLength, TruncateToLength, InsertSeparatorAt, SeparatorCharacter,
            RuleNote
        )
        SELECT
            c.CountryCode,
            g.AddressRuleSetCode,
            900,
            NULL,
            1,
            NULLIF(c.PostalFormatMask, N''),
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            N'Region fallback: no country rule has been written, so the value is only trimmed and upper-cased.'
        FROM ref.Country AS c
        INNER JOIN ref.Region AS g
            ON g.RegionCode = c.RegionCode
        WHERE c.IsActive = 1
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.PostalFormatRule AS p
                  WHERE p.CountryCode = c.CountryCode
              );

        SET @FallbackRows = @@ROWCOUNT;
        SET @InsertedRows = @FallbackRows;

        --  Countries the ERP sends addresses for that the conformed list does
        --  not carry cannot be given a rule at all.
        INSERT INTO err.RejectedLookupFailure
        (
            BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
            LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
            RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, N'raw.OracleGeography', MIN(LTRIM(RTRIM(r.GEOGRAPHY_ID))),
            N'PostalFormatRule', N'COUNTRY_CD', LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2),
            @SourceSystemCode, N'LOOKUP_MISS',
            N'addresses exist for a country with no conformed country row, so no postal rule applies',
            N'Reference', 1, 0, COUNT_BIG(*), NULL
        FROM raw.OracleGeography AS r
        WHERE r.BatchId = @BatchId
          AND NULLIF(LTRIM(RTRIM(r.POSTAL_CD)), N'') IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM ref.Country AS c
                  WHERE c.CountryCode = LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2)
              )
        GROUP BY LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_CD))), 2);

        SET @LookupMisses = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows + @UpdatedRows,
            @InsertRowCount     = @InsertedRows,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @LookupMisses;

        DROP TABLE #PostalRule;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'REF_Load_WarehouseSite',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'ref.usp_LoadPostalFormatRule';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
