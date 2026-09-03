/*
    stg.ufn_StandardizePhone

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : stg.usp_NormalizeCustomer, stg.usp_NormalizeSupplier

    Produces an E.164-ish '+<country><subscriber>' string. The estate has never
    had a phone validation service, so this is a formatting exercise only: the
    number is not checked for existence and the extension is preserved separately
    with an 'x' because the credit control team relies on it.

    Trunk-prefix handling differs by country and is the reason this function is
    not three lines: NA numbers have no trunk prefix, most EU countries drop a
    leading 0, Italy keeps it, and Japan drops the leading 0 as well.

    The catalog name for this object is stg.fn_StandardizePhone; a synonym is
    created at the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_StandardizePhone', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_StandardizePhone;
GO

CREATE FUNCTION stg.ufn_StandardizePhone
(
    @PhoneNumber NVARCHAR(60),
    @CountryCode NCHAR(2)
)
RETURNS NVARCHAR(40)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Text      NVARCHAR(60) = UPPER(LTRIM(RTRIM(ISNULL(@PhoneNumber, N''))));
    DECLARE @Extension NVARCHAR(20) = NULL;
    DECLARE @Digits    NVARCHAR(60) = N'';
    DECLARE @Dialling  NVARCHAR(5);
    DECLARE @Index     INT = 1;
    DECLARE @Char      NCHAR(1);
    DECLARE @Position  INT;

    IF @Text = N'' OR @Text IN (N'NULL', N'N/A', N'NONE', N'0')
        RETURN NULL;

    -- Split the extension off before the digits are harvested.
    SET @Position = NULLIF(CHARINDEX(N'EXT', @Text), 0);
    IF @Position IS NULL
        SET @Position = NULLIF(CHARINDEX(N' X', @Text), 0);
    IF @Position IS NOT NULL
    BEGIN
        SET @Extension = LTRIM(RTRIM(SUBSTRING(@Text, @Position, 20)));
        SET @Extension = REPLACE(REPLACE(REPLACE(@Extension, N'EXT', N''), N'.', N''), N':', N'');
        SET @Extension = LTRIM(RTRIM(REPLACE(@Extension, N'X', N'')));
        SET @Text = LEFT(@Text, @Position - 1);
    END;

    WHILE @Index <= LEN(@Text)
    BEGIN
        SET @Char = SUBSTRING(@Text, @Index, 1);
        IF @Char BETWEEN N'0' AND N'9'
            SET @Digits = @Digits + @Char;
        SET @Index = @Index + 1;
    END;

    IF LEN(@Digits) < 6
        RETURN NULL;

    SET @Dialling = CASE @CountryCode
                        WHEN N'US' THEN N'1'
                        WHEN N'CA' THEN N'1'
                        WHEN N'MX' THEN N'52'
                        WHEN N'GB' THEN N'44'
                        WHEN N'DE' THEN N'49'
                        WHEN N'FR' THEN N'33'
                        WHEN N'IT' THEN N'39'
                        WHEN N'ES' THEN N'34'
                        WHEN N'NL' THEN N'31'
                        WHEN N'PL' THEN N'48'
                        WHEN N'JP' THEN N'81'
                        WHEN N'AU' THEN N'61'
                        WHEN N'NZ' THEN N'64'
                        WHEN N'SG' THEN N'65'
                        WHEN N'HK' THEN N'852'
                        ELSE NULL
                    END;

    IF @Dialling IS NULL
        RETURN N'+' + @Digits + ISNULL(N' x' + @Extension, N'');

    -- Strip a duplicated country code, then the national trunk prefix.
    IF LEFT(@Digits, LEN(@Dialling)) = @Dialling AND LEN(@Digits) > LEN(@Dialling) + 6
        SET @Digits = SUBSTRING(@Digits, LEN(@Dialling) + 1, LEN(@Digits));

    IF LEFT(@Digits, 1) = N'0' AND @CountryCode NOT IN (N'US', N'CA', N'IT')
        SET @Digits = SUBSTRING(@Digits, 2, LEN(@Digits));

    RETURN N'+' + @Dialling + @Digits + ISNULL(N' x' + @Extension, N'');
END;
GO

IF OBJECT_ID(N'stg.fn_StandardizePhone', N'SN') IS NULL
    CREATE SYNONYM stg.fn_StandardizePhone FOR stg.ufn_StandardizePhone;
GO
