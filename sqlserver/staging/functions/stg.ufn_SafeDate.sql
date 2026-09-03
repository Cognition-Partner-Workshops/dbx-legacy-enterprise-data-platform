/*
    stg.ufn_SafeDate

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : every raw-to-stg typing procedure, and the file feeds in
                    particular

    Converts a raw date string to DATETIME2 without ever raising. The region hint
    decides how an ambiguous DD/MM versus MM/DD value is read: the NA partner
    files send 03/04/2019 meaning 4 March, the EU files send the same characters
    meaning 3 April, and the APAC files send 2019-03-04. Getting this wrong is
    silent and expensive, so the caller is required to pass a region.

    Unparseable values return NULL and the caller routes the row to err.*; the
    two sentinel dates the ERP uses for "not set" are also treated as NULL.

    The catalog name for this object is stg.fn_SafeDate; a synonym is created at
    the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_SafeDate', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_SafeDate;
GO

CREATE FUNCTION stg.ufn_SafeDate
(
    @Value      NVARCHAR(100),
    @RegionCode NVARCHAR(10)
)
RETURNS DATETIME2(3)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Text   NVARCHAR(100) = LTRIM(RTRIM(ISNULL(@Value, N'')));
    DECLARE @Result DATETIME2(3) = NULL;
    DECLARE @Style  INT;

    IF @Text = N'' OR UPPER(@Text) IN (N'NULL', N'N/A', N'0', N'00000000')
        RETURN NULL;

    -- Sentinels the ERP writes instead of leaving the column empty.
    IF @Text IN (N'4712-12-31', N'31-DEC-4712', N'9999-12-31', N'1900-01-01')
        RETURN NULL;

    SET @Text = REPLACE(REPLACE(@Text, N'.', N'/'), N'-', N'/');

    SET @Style = CASE
                     WHEN @RegionCode = N'NA'   THEN 101   -- mm/dd/yyyy
                     WHEN @RegionCode = N'EU'   THEN 103   -- dd/mm/yyyy
                     WHEN @RegionCode = N'APAC' THEN 111   -- yyyy/mm/dd
                     ELSE 120
                 END;

    -- An 8 digit run is always YYYYMMDD regardless of region.
    IF LEN(@Text) = 8 AND @Text NOT LIKE N'%[^0-9]%'
        SET @Result = TRY_CONVERT(DATETIME2(3), LEFT(@Text, 4) + N'-' + SUBSTRING(@Text, 5, 2) + N'-' + RIGHT(@Text, 2), 120);

    IF @Result IS NULL
        SET @Result = TRY_CONVERT(DATETIME2(3), @Text, @Style);

    -- ISO first, then the Oracle DD-MON-YYYY shape, then a last permissive try.
    IF @Result IS NULL
        SET @Result = TRY_CONVERT(DATETIME2(3), @Value, 120);

    IF @Result IS NULL
        SET @Result = TRY_CONVERT(DATETIME2(3), @Value, 106);

    IF @Result IS NULL
        SET @Result = TRY_CONVERT(DATETIME2(3), @Value);

    -- Anything outside the plausible trading window is a parsing accident.
    IF @Result IS NOT NULL AND (@Result < CONVERT(DATETIME2(3), N'1980-01-01') OR @Result > DATEADD(YEAR, 5, SYSUTCDATETIME()))
        SET @Result = NULL;

    RETURN @Result;
END;
GO

IF OBJECT_ID(N'stg.fn_SafeDate', N'SN') IS NULL
    CREATE SYNONYM stg.fn_SafeDate FOR stg.ufn_SafeDate;
GO
