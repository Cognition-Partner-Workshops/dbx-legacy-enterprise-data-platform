/*
    stg.ufn_SourceSystemKey

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : every raw-to-stg load procedure

    Builds the conformed business key '<SourceSystemCode>|<natural key>' that the
    whole estate matches on. Two wrinkles that must not be tidied away:

      1. The three regional Oracle instances (ORA_ERP_NA / ORA_ERP_EU /
         ORA_ERP_AP) share one customer and supplier numbering series inherited
         from the 2004 consolidation, so they collapse to ORA_ERP for key
         purposes. They do NOT collapse for GL or cost-centre keys, which are
         genuinely per-ledger; the caller passes CollapseRegionalInstances = 0
         for those.
      2. The web channel (WWI_WEB) writes the same customer numbers as the OLTP
         database, so it collapses to WWI_OLTP as well.

    The catalog name for this object is stg.fn_SourceSystemKey; a synonym is
    created at the bottom of this file.
*/

IF OBJECT_ID(N'stg.ufn_SourceSystemKey', N'FN') IS NOT NULL
    DROP FUNCTION stg.ufn_SourceSystemKey;
GO

CREATE FUNCTION stg.ufn_SourceSystemKey
(
    @SourceSystemCode           NVARCHAR(20),
    @NaturalKey                 NVARCHAR(100),
    @CollapseRegionalInstances  BIT = 1
)
RETURNS NVARCHAR(140)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @System NVARCHAR(20) = UPPER(LTRIM(RTRIM(ISNULL(@SourceSystemCode, N''))));
    DECLARE @Key    NVARCHAR(100) = UPPER(LTRIM(RTRIM(ISNULL(@NaturalKey, N''))));

    IF @Key = N'' OR @System = N''
        RETURN NULL;

    IF @CollapseRegionalInstances = 1
    BEGIN
        IF @System IN (N'ORA_ERP_NA', N'ORA_ERP_EU', N'ORA_ERP_AP')
            SET @System = N'ORA_ERP';
        IF @System = N'WWI_WEB'
            SET @System = N'WWI_OLTP';
    END;

    -- Oracle pads its numeric identifiers; the OLTP database does not.
    IF @System = N'ORA_ERP' AND @Key NOT LIKE N'%[^0-9]%'
        SET @Key = RIGHT(N'0000000000' + @Key, 10);

    -- The partner feeds have sent keys with embedded pipes since the 2013 format
    -- change; a pipe would break every downstream split, so it is escaped.
    SET @Key = REPLACE(@Key, N'|', N'/');

    RETURN @System + N'|' + @Key;
END;
GO

IF OBJECT_ID(N'stg.fn_SourceSystemKey', N'SN') IS NULL
    CREATE SYNONYM stg.fn_SourceSystemKey FOR stg.ufn_SourceSystemKey;
GO
