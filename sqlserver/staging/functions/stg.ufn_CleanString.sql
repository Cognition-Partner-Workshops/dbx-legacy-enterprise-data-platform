/*
    stg.ufn_CleanString

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : stg.usp_NormalizeCustomer, stg.usp_NormalizeSupplier,
                    stg.usp_NormalizeAddress, the raw-to-stg typing procedures

    Trims, collapses internal whitespace and strips the control characters the
    mainframe-era extracts still put in text columns. Empty and whitespace-only
    strings come back as NULL, which is what every downstream NOT NULL check
    expects; a literal 'NULL', 'N/A' or '?' from the source is treated the same
    way, because two of the three feeds have used all of them over the years.

    The catalog name for this object is stg.fn_CleanString; the estate naming
    standard requires the ufn_ prefix, so the catalog name exists as a synonym
    created at the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_CleanString', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_CleanString;
GO

CREATE FUNCTION stg.ufn_CleanString
(
    @Value          NVARCHAR(4000),
    @UpperCaseFlag  BIT = 0
)
RETURNS NVARCHAR(4000)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Result NVARCHAR(4000) = @Value;

    IF @Result IS NULL
        RETURN NULL;

    -- Control characters that survive the flat-file extracts.
    SET @Result = REPLACE(@Result, NCHAR(9),  N' ');
    SET @Result = REPLACE(@Result, NCHAR(10), N' ');
    SET @Result = REPLACE(@Result, NCHAR(13), N' ');
    SET @Result = REPLACE(@Result, NCHAR(0),  N'');
    SET @Result = REPLACE(@Result, NCHAR(160), N' ');   -- non-breaking space from the web feed

    -- Collapse runs of spaces without a loop: the classic three-replace trick.
    SET @Result = REPLACE(REPLACE(REPLACE(@Result, N' ', N'<~>'), N'><', N''), N'<~>', N' ');
    SET @Result = LTRIM(RTRIM(@Result));

    IF @Result = N''
        RETURN NULL;

    IF UPPER(@Result) IN (N'NULL', N'N/A', N'NA', N'?', N'-', N'UNKNOWN', N'.')
        RETURN NULL;

    IF @UpperCaseFlag = 1
        SET @Result = UPPER(@Result);

    RETURN @Result;
END;
GO

IF OBJECT_ID(N'stg.fn_CleanString', N'SN') IS NULL
    CREATE SYNONYM stg.fn_CleanString FOR stg.ufn_CleanString;
GO
