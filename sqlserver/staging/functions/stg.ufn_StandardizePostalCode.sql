/*
    stg.ufn_StandardizePostalCode

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : stg.usp_NormalizeAddress, stg.usp_TruncateAndReload_Geography

    Region rules, which is why this is not a single REPLACE:
      NA    US ZIP is truncated to five digits and the +4 is dropped, because the
            warehouse geography grain is ZIP5; CA postal codes become A9A 9A9.
      EU    Per-country masks: GB keeps the outward/inward split with one space,
            DE/FR/ES/IT are five or five-with-country-prefix digits, NL is
            '9999 AA', and the pre-2007 six-digit PL values are re-hyphenated.
      APAC  Digits only; JP gets the three-four hyphen back, AU/NZ are four
            digits, SG is six, and anything alphabetic is left alone because the
            HK and MO rows have no postal code at all.

    Returns NULL when nothing usable survives, so the caller can decide whether a
    missing postal code is a reject or just a warning for that country.

    The catalog name for this object is stg.fn_StandardizePostalCode; a synonym
    is created at the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_StandardizePostalCode', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_StandardizePostalCode;
GO

CREATE FUNCTION stg.ufn_StandardizePostalCode
(
    @PostalCode  NVARCHAR(40),
    @CountryCode NCHAR(2)
)
RETURNS NVARCHAR(20)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Text   NVARCHAR(40) = UPPER(LTRIM(RTRIM(ISNULL(@PostalCode, N''))));
    DECLARE @Digits NVARCHAR(40) = N'';
    DECLARE @Index  INT = 1;
    DECLARE @Char   NCHAR(1);

    IF @Text = N'' OR @Text IN (N'NULL', N'N/A', N'00000', N'0', N'-')
        RETURN NULL;

    SET @Text = REPLACE(REPLACE(REPLACE(@Text, N'.', N''), N'  ', N' '), NCHAR(160), N'');

    -- Hand-rolled digit extraction: still here from the SQL Server 2000 version.
    WHILE @Index <= LEN(@Text)
    BEGIN
        SET @Char = SUBSTRING(@Text, @Index, 1);
        IF @Char BETWEEN N'0' AND N'9'
            SET @Digits = @Digits + @Char;
        SET @Index = @Index + 1;
    END;

    IF @CountryCode = N'US'
    BEGIN
        IF LEN(@Digits) >= 5
            RETURN LEFT(@Digits, 5);
        RETURN NULL;
    END;

    IF @CountryCode = N'CA'
    BEGIN
        SET @Text = REPLACE(@Text, N' ', N'');
        IF LEN(@Text) = 6
            RETURN LEFT(@Text, 3) + N' ' + RIGHT(@Text, 3);
        RETURN NULL;
    END;

    IF @CountryCode = N'GB'
    BEGIN
        SET @Text = REPLACE(@Text, N' ', N'');
        IF LEN(@Text) BETWEEN 5 AND 7
            RETURN LEFT(@Text, LEN(@Text) - 3) + N' ' + RIGHT(@Text, 3);
        RETURN NULL;
    END;

    IF @CountryCode = N'NL'
    BEGIN
        SET @Text = REPLACE(@Text, N' ', N'');
        IF LEN(@Text) = 6
            RETURN LEFT(@Text, 4) + N' ' + RIGHT(@Text, 2);
        RETURN NULL;
    END;

    IF @CountryCode = N'PL'
    BEGIN
        IF LEN(@Digits) = 5
            RETURN LEFT(@Digits, 2) + N'-' + RIGHT(@Digits, 3);
        RETURN NULL;
    END;

    IF @CountryCode IN (N'DE', N'FR', N'ES', N'IT', N'FI')
    BEGIN
        IF LEN(@Digits) = 5
            RETURN @Digits;
        IF LEN(@Digits) = 4                          -- leading zero lost in a spreadsheet round-trip
            RETURN N'0' + @Digits;
        RETURN NULL;
    END;

    IF @CountryCode = N'JP'
    BEGIN
        IF LEN(@Digits) = 7
            RETURN LEFT(@Digits, 3) + N'-' + RIGHT(@Digits, 4);
        RETURN NULL;
    END;

    IF @CountryCode = N'SG'
    BEGIN
        IF LEN(@Digits) = 6
            RETURN @Digits;
        IF LEN(@Digits) = 5
            RETURN N'0' + @Digits;
        RETURN NULL;
    END;

    IF @CountryCode IN (N'AU', N'NZ')
    BEGIN
        IF LEN(@Digits) = 4
            RETURN @Digits;
        IF LEN(@Digits) = 3
            RETURN N'0' + @Digits;
        RETURN NULL;
    END;

    IF @CountryCode IN (N'HK', N'MO', N'AE')         -- no postal code system in use
        RETURN NULL;

    -- Fallback: keep whatever the source sent, single-spaced.
    IF LEN(@Text) > 20
        SET @Text = LEFT(@Text, 20);

    RETURN NULLIF(LTRIM(RTRIM(@Text)), N'');
END;
GO

IF OBJECT_ID(N'stg.fn_StandardizePostalCode', N'SN') IS NULL
    CREATE SYNONYM stg.fn_StandardizePostalCode FOR stg.ufn_StandardizePostalCode;
GO
