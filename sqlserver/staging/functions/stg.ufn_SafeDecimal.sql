/*
    stg.ufn_SafeDecimal

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : the raw-to-stg typing procedures and the partner file loads

    Numeric strings arrive in at least five shapes across the estate:
      1 234,56    EU partner extracts (space group separator, decimal comma)
      1,234.56    NA partner extracts and the OLTP CSV unloads
      1234.56-    trailing sign from the ERP fixed-width unload
      (1234.56)   accounting negative from the spreadsheet uploads
      1234.56CR   credit suffix from the oldest AP feed

    Everything is normalised to a plain DECIMAL(19,6) here so that the staging
    tables can keep an honest data type. Unparseable values return NULL and the
    caller rejects the row rather than silently loading a zero, which is what the
    1990s version of this logic did and what the 2011 restatement was caused by.

    The catalog name for this object is stg.fn_SafeDecimal; a synonym is created
    at the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_SafeDecimal', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_SafeDecimal;
GO

CREATE FUNCTION stg.ufn_SafeDecimal
(
    @Value            NVARCHAR(100),
    @DecimalSeparator NCHAR(1) = N'.'
)
RETURNS DECIMAL(19,6)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Text       NVARCHAR(100) = UPPER(LTRIM(RTRIM(ISNULL(@Value, N''))));
    DECLARE @IsNegative BIT = 0;

    IF @Text = N'' OR @Text IN (N'NULL', N'N/A', N'-', N'.')
        RETURN NULL;

    IF LEFT(@Text, 1) = N'(' AND RIGHT(@Text, 1) = N')'
    BEGIN
        SET @IsNegative = 1;
        SET @Text = SUBSTRING(@Text, 2, LEN(@Text) - 2);
    END;

    IF RIGHT(@Text, 2) = N'CR'
    BEGIN
        SET @IsNegative = 1;
        SET @Text = LEFT(@Text, LEN(@Text) - 2);
    END
    ELSE IF RIGHT(@Text, 2) = N'DR'
        SET @Text = LEFT(@Text, LEN(@Text) - 2);

    IF RIGHT(@Text, 1) = N'-'
    BEGIN
        SET @IsNegative = 1;
        SET @Text = LEFT(@Text, LEN(@Text) - 1);
    END;

    IF LEFT(@Text, 1) = N'-'
    BEGIN
        SET @IsNegative = 1;
        SET @Text = SUBSTRING(@Text, 2, LEN(@Text) - 1);
    END;

    SET @Text = REPLACE(REPLACE(REPLACE(@Text, N' ', N''), NCHAR(160), N''), N'+', N'');
    SET @Text = REPLACE(REPLACE(@Text, N'$', N''), N'%', N'');

    IF @DecimalSeparator = N','
    BEGIN
        SET @Text = REPLACE(@Text, N'.', N'');      -- '.' is the group separator in this shape
        SET @Text = REPLACE(@Text, N',', N'.');
    END
    ELSE
        SET @Text = REPLACE(@Text, N',', N'');

    IF @Text = N'' OR @Text LIKE N'%[^0-9.]%'
        RETURN NULL;

    DECLARE @Result DECIMAL(19,6) = TRY_CONVERT(DECIMAL(19,6), @Text);

    IF @Result IS NULL
        RETURN NULL;

    RETURN CASE WHEN @IsNegative = 1 THEN -@Result ELSE @Result END;
END;
GO

IF OBJECT_ID(N'stg.fn_SafeDecimal', N'SN') IS NULL
    CREATE SYNONYM stg.fn_SafeDecimal FOR stg.ufn_SafeDecimal;
GO
