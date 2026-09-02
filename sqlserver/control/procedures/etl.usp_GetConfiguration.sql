/*
    etl.usp_GetConfiguration / etl.ufn_GetConfigurationValue

    Deploy target : WideWorldImportersStaging
    Called by     : maintenance packages, staging procedures, MNT_Validate_Configuration
    Depends on    : etl.Configuration

    Environment-specific values win over the N'ALL' default. No secret is ever
    stored here - rows flagged IsSensitive carry the *name* of the credential the
    deployment supplies, never its value.
*/
IF OBJECT_ID(N'etl.ufn_GetConfigurationValue', N'FN') IS NOT NULL
    DROP FUNCTION etl.ufn_GetConfigurationValue;
GO

CREATE FUNCTION etl.ufn_GetConfigurationValue
(
    @ConfigurationKey NVARCHAR(100),
    @EnvironmentCode  NVARCHAR(10)
)
RETURNS NVARCHAR(500)
AS
BEGIN
    DECLARE @Value NVARCHAR(500);

    SELECT TOP (1) @Value = c.ConfigurationValue
    FROM etl.Configuration AS c
    WHERE c.ConfigurationKey = @ConfigurationKey
      AND c.EnvironmentCode IN (@EnvironmentCode, N'ALL')
      AND c.IsSensitive = 0
    ORDER BY CASE WHEN c.EnvironmentCode = @EnvironmentCode THEN 0 ELSE 1 END;

    RETURN @Value;
END;
GO

IF OBJECT_ID(N'etl.usp_GetConfiguration', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_GetConfiguration;
GO

CREATE PROCEDURE etl.usp_GetConfiguration
(
    @ConfigurationKey   NVARCHAR(100),
    @EnvironmentCode    NVARCHAR(10) = NULL,
    @ConfigurationValue NVARCHAR(500) OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;

    SET @EnvironmentCode = ISNULL(@EnvironmentCode,
                                  etl.ufn_GetConfigurationValue(N'EnvironmentCode', N'ALL'));

    SET @ConfigurationValue = etl.ufn_GetConfigurationValue(@ConfigurationKey, ISNULL(@EnvironmentCode, N'DEV'));

    IF @ConfigurationValue IS NULL
    BEGIN
        DECLARE @msg NVARCHAR(400) =
            CONCAT(N'Configuration key ', @ConfigurationKey, N' is not defined for environment ',
                   ISNULL(@EnvironmentCode, N'(unknown)'), N'.');
        THROW 51020, @msg, 1;
    END;

    SELECT @ConfigurationValue AS ConfigurationValue;

    RETURN 0;
END;
GO
