/*
    Object          : SSIS catalogue environment WWI_TEST
    Deploy target   : SSISDB on the SSIS catalogue instance
    Deploy order    : after deployment/ssis/Deploy-SsisCatalog.ps1
    Called by       : deployment/ssis/Deploy-SsisEnvironment.ps1
    Notes           : GENERATED FILE - do not edit by hand. Rendered from
                      config/environments/test.env.yaml by
                      deployment/ssis/render_environment_sql.py.

                      Creates the environment, its variables and the project
                      parameter references, then binds every reference. Values
                      marked sensitive are passed in as sqlcmd variables and
                      are never stored in this repository.

                      Idempotent. Not executed against any catalogue.

                      Every value arrives as a sqlcmd variable, supplied by the
                      deploy driver from the environment variable named in the
                      comment above it, falling back to the YAML default. No
                      host, account or credential value is baked into this file.

    sqlcmd variables required: ORACLE_PASSWORD, SQLSERVER_PASSWORD
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE SSISDB;
GO

DECLARE @FolderName      NVARCHAR(128) = N'WWI_TEST';
DECLARE @EnvironmentName NVARCHAR(128) = N'WWI_TEST';

IF NOT EXISTS (SELECT 1 FROM catalog.folders WHERE name = @FolderName)
    EXEC catalog.create_folder @folder_name = @FolderName;

IF NOT EXISTS (SELECT 1
               FROM catalog.environments AS e
               INNER JOIN catalog.folders AS f ON f.folder_id = e.folder_id
               WHERE e.name = @EnvironmentName AND f.name = @FolderName)
BEGIN
    EXEC catalog.create_environment
         @folder_name      = @FolderName,
         @environment_name = @EnvironmentName,
         @environment_description = N'User acceptance and regression environment. Loaded from a pseudonymised PROD extract each quarter. EU personal data is pseudonymised at extract time, so the EU consent job runs against surrogate contact rows here.';
END
GO

/* OracleHost (String) <- ORACLE_HOST */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OracleHost' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OracleHost';
END

DECLARE @Value NVARCHAR(4000) = N'$(OracleHost)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleHost',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter OracleHost. Source: ORACLE_HOST.';
GO

/* OraclePort (Int32) <- ORACLE_PORT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OraclePort' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OraclePort';
END

DECLARE @Value INT = N'$(OraclePort)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OraclePort',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter OraclePort. Source: ORACLE_PORT.';
GO

/* OracleService (String) <- ORACLE_SERVICE */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OracleService' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OracleService';
END

DECLARE @Value NVARCHAR(4000) = N'$(OracleService)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleService',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter OracleService. Source: ORACLE_SERVICE.';
GO

/* OracleUser (String) <- ORACLE_USER */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OracleUser' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OracleUser';
END

DECLARE @Value NVARCHAR(4000) = N'$(OracleUser)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleUser',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter OracleUser. Source: ORACLE_USER.';
GO

/* OracleProvider (String) <- ORACLE_PROVIDER */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OracleProvider' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OracleProvider';
END

DECLARE @Value NVARCHAR(4000) = N'$(OracleProvider)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleProvider',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter OracleProvider. Source: ORACLE_PROVIDER.';
GO

/* OraclePassword (String, sensitive) <- ORACLE_PASSWORD */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'OraclePassword' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'OraclePassword';
END

DECLARE @Value NVARCHAR(4000) = N'$(OraclePassword)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OraclePassword',
     @data_type        = N'String',
     @sensitive        = 1,
     @value            = @Value,
     @description      = N'Bound to project parameter OraclePassword. Source: ORACLE_PASSWORD.';
GO

/* SqlServerHost (String) <- SQLSERVER_HOST */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerHost' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerHost';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerHost)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerHost',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerHost. Source: SQLSERVER_HOST.';
GO

/* SqlServerPort (Int32) <- SQLSERVER_PORT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerPort' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerPort';
END

DECLARE @Value INT = N'$(SqlServerPort)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerPort',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerPort. Source: SQLSERVER_PORT.';
GO

/* SqlServerUser (String) <- SQLSERVER_USER */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerUser' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerUser';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerUser)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerUser',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerUser. Source: SQLSERVER_USER.';
GO

/* SqlServerProvider (String) <- SQLSERVER_PROVIDER */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerProvider' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerProvider';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerProvider)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerProvider',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerProvider. Source: SQLSERVER_PROVIDER.';
GO

/* SqlServerTrustServerCertificate (Boolean) <- SQLSERVER_TRUST_SERVER_CERTIFICATE */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerTrustServerCertificate' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerTrustServerCertificate';
END

DECLARE @Value BIT = N'$(SqlServerTrustServerCertificate)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerTrustServerCertificate',
     @data_type        = N'Boolean',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerTrustServerCertificate. Source: SQLSERVER_TRUST_SERVER_CERTIFICATE.';
GO

/* SqlServerPassword (String, sensitive) <- SQLSERVER_PASSWORD */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerPassword' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerPassword';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerPassword)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerPassword',
     @data_type        = N'String',
     @sensitive        = 1,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerPassword. Source: SQLSERVER_PASSWORD.';
GO

/* SqlServerOltpDb (String) <- SQLSERVER_OLTP_DB */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerOltpDb' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerOltpDb';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerOltpDb)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerOltpDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerOltpDb. Source: SQLSERVER_OLTP_DB.';
GO

/* SqlServerStagingDb (String) <- SQLSERVER_STAGING_DB */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerStagingDb' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerStagingDb';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerStagingDb)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerStagingDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerStagingDb. Source: SQLSERVER_STAGING_DB.';
GO

/* SqlServerDwDb (String) <- SQLSERVER_DW_DB */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SqlServerDwDb' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SqlServerDwDb';
END

DECLARE @Value NVARCHAR(4000) = N'$(SqlServerDwDb)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerDwDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SqlServerDwDb. Source: SQLSERVER_DW_DB.';
GO

/* InboundFileRoot (String) <- ETL_INBOUND_FILE_ROOT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'InboundFileRoot' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'InboundFileRoot';
END

DECLARE @Value NVARCHAR(4000) = N'$(InboundFileRoot)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'InboundFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter InboundFileRoot. Source: ETL_INBOUND_FILE_ROOT.';
GO

/* ArchiveFileRoot (String) <- ETL_ARCHIVE_FILE_ROOT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'ArchiveFileRoot' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'ArchiveFileRoot';
END

DECLARE @Value NVARCHAR(4000) = N'$(ArchiveFileRoot)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'ArchiveFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter ArchiveFileRoot. Source: ETL_ARCHIVE_FILE_ROOT.';
GO

/* QuarantineFileRoot (String) <- ETL_QUARANTINE_FILE_ROOT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'QuarantineFileRoot' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'QuarantineFileRoot';
END

DECLARE @Value NVARCHAR(4000) = N'$(QuarantineFileRoot)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'QuarantineFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter QuarantineFileRoot. Source: ETL_QUARANTINE_FILE_ROOT.';
GO

/* DefaultBatchSize (Int32) <- ETL_DEFAULT_BATCH_SIZE */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'DefaultBatchSize' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'DefaultBatchSize';
END

DECLARE @Value INT = N'$(DefaultBatchSize)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'DefaultBatchSize',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter DefaultBatchSize. Source: ETL_DEFAULT_BATCH_SIZE.';
GO

/* SourceQueryTimeoutSeconds (Int32) <- ETL_SOURCE_QUERY_TIMEOUT_SECONDS */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'SourceQueryTimeoutSeconds' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'SourceQueryTimeoutSeconds';
END

DECLARE @Value INT = N'$(SourceQueryTimeoutSeconds)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SourceQueryTimeoutSeconds',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter SourceQueryTimeoutSeconds. Source: ETL_SOURCE_QUERY_TIMEOUT_SECONDS.';
GO

/* MaxRejectPercent (Int32) <- ETL_MAX_REJECT_PERCENT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'MaxRejectPercent' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'MaxRejectPercent';
END

DECLARE @Value INT = N'$(MaxRejectPercent)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'MaxRejectPercent',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter MaxRejectPercent. Source: ETL_MAX_REJECT_PERCENT.';
GO

/* EnvironmentCode (String) <- ETL_ENVIRONMENT_CODE */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'EnvironmentCode' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'EnvironmentCode';
END

DECLARE @Value NVARCHAR(4000) = N'$(EnvironmentCode)';
EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'EnvironmentCode',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = @Value,
     @description      = N'Bound to project parameter EnvironmentCode. Source: ETL_ENVIRONMENT_CODE.';
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Orchestration' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Orchestration',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Orchestration'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Orchestration',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Extract_Oracle' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Extract_Oracle',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_Oracle'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_Oracle',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Extract_SqlServer' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Extract_SqlServer',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Extract_SqlServer'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Extract_SqlServer',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Ingest_Files' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Ingest_Files',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Ingest_Files'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Ingest_Files',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Staging' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Staging',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Staging'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Staging',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_DataQuality' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_DataQuality',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_DataQuality'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_DataQuality',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_ReferenceData' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_ReferenceData',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ReferenceData'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ReferenceData',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Dimensions' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Dimensions',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Dimensions'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Dimensions',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Facts' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Facts',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Facts'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Facts',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Aggregates' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Aggregates',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Aggregates'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Aggregates',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Finance' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Finance',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Finance'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Finance',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Sales' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Sales',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Sales'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Sales',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Inventory' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Inventory',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Inventory'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Inventory',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Procurement' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Procurement',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Procurement'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Procurement',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Customer360' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Customer360',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Customer360'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Customer360',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_ErrorHandling' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_ErrorHandling',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_ErrorHandling'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_ErrorHandling',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* Project reference and parameter bindings. */
IF NOT EXISTS (SELECT 1
               FROM SSISDB.catalog.environment_references AS r
               INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
               INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
               WHERE p.name = N'WWI_Maintenance' AND f.name = N'WWI_TEST'
                 AND r.environment_name = N'WWI_TEST')
BEGIN
    DECLARE @ReferenceId BIGINT;
    EXEC SSISDB.catalog.create_environment_reference
         @folder_name       = N'WWI_TEST',
         @project_name      = N'WWI_Maintenance',
         @environment_name  = N'WWI_TEST',
         @reference_type    = 'R',             /* relative: environment in this folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

/* OracleHost <- environment variable OracleHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'OracleHost',
         @parameter_value = N'OracleHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OraclePort <- environment variable OraclePort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'OraclePort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'OraclePort',
         @parameter_value = N'OraclePort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleService <- environment variable OracleService */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleService')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'OracleService',
         @parameter_value = N'OracleService',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleUser <- environment variable OracleUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'OracleUser',
         @parameter_value = N'OracleUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* OracleProvider <- environment variable OracleProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'OracleProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'OracleProvider',
         @parameter_value = N'OracleProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Oracle_ERP.Password <- environment variable OraclePassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Oracle_ERP.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'CM.WWI_Oracle_ERP.Password',
         @parameter_value = N'OraclePassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerHost <- environment variable SqlServerHost */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerHost')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerHost',
         @parameter_value = N'SqlServerHost',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerPort <- environment variable SqlServerPort */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerPort')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerPort',
         @parameter_value = N'SqlServerPort',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerUser <- environment variable SqlServerUser */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerUser')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerUser',
         @parameter_value = N'SqlServerUser',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerProvider <- environment variable SqlServerProvider */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerProvider')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerProvider',
         @parameter_value = N'SqlServerProvider',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerTrustServerCertificate <- environment variable SqlServerTrustServerCertificate */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerTrustServerCertificate')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerTrustServerCertificate',
         @parameter_value = N'SqlServerTrustServerCertificate',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Source_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Source_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'CM.WWI_Source_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_Staging_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_Staging_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'CM.WWI_Staging_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* CM.WWI_DW_Destination_DB.Password <- environment variable SqlServerPassword */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'CM.WWI_DW_Destination_DB.Password')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'CM.WWI_DW_Destination_DB.Password',
         @parameter_value = N'SqlServerPassword',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerOltpDb <- environment variable SqlServerOltpDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerOltpDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerOltpDb',
         @parameter_value = N'SqlServerOltpDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerStagingDb <- environment variable SqlServerStagingDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerStagingDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerStagingDb',
         @parameter_value = N'SqlServerStagingDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SqlServerDwDb <- environment variable SqlServerDwDb */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SqlServerDwDb')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SqlServerDwDb',
         @parameter_value = N'SqlServerDwDb',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* InboundFileRoot <- environment variable InboundFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'InboundFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'InboundFileRoot',
         @parameter_value = N'InboundFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ArchiveFileRoot <- environment variable ArchiveFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'ArchiveFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'ArchiveFileRoot',
         @parameter_value = N'ArchiveFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* QuarantineFileRoot <- environment variable QuarantineFileRoot */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'QuarantineFileRoot')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'QuarantineFileRoot',
         @parameter_value = N'QuarantineFileRoot',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* DefaultBatchSize <- environment variable DefaultBatchSize */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'DefaultBatchSize')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'DefaultBatchSize',
         @parameter_value = N'DefaultBatchSize',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* SourceQueryTimeoutSeconds <- environment variable SourceQueryTimeoutSeconds */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'SourceQueryTimeoutSeconds')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'SourceQueryTimeoutSeconds',
         @parameter_value = N'SourceQueryTimeoutSeconds',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* MaxRejectPercent <- environment variable MaxRejectPercent */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'MaxRejectPercent')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'MaxRejectPercent',
         @parameter_value = N'MaxRejectPercent',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* EnvironmentCode <- environment variable EnvironmentCode */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
           WHERE f.name = N'WWI_TEST' AND pr.name = N'WWI_Maintenance'
             AND op.object_type = 20 AND op.parameter_name = N'EnvironmentCode')
BEGIN
    EXEC SSISDB.catalog.set_object_parameter_value
         @object_type    = 20,                     /* project parameter */
         @folder_name    = N'WWI_TEST',
         @project_name   = N'WWI_Maintenance',
         @parameter_name = N'EnvironmentCode',
         @parameter_value = N'EnvironmentCode',
         @value_type     = 'R';                    /* referenced environment variable */
END
GO

/* ---------------------------------------------------------------------------
   Post-conditions. These are the reason this script is worth running twice:
   a binding that silently did not take looks exactly like a working one until
   an execution fails with an empty password.
   --------------------------------------------------------------------------- */

/* 1. Every reference resolves inside this folder - that is what local means,
      whatever letter the catalog chose to record for reference_type. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_references AS r
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = r.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'WWI_TEST'
             AND ISNULL(r.environment_folder_name, N'WWI_TEST') <> N'WWI_TEST')
    THROW 50001, 'An environment reference in this folder points outside it.', 1;

/* 2. Every project in the folder has exactly one reference to the environment. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.projects AS p
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = p.folder_id
           LEFT JOIN SSISDB.catalog.environment_references AS r
                  ON r.project_id = p.project_id
                 AND r.environment_name = N'WWI_TEST'
           WHERE f.name = N'WWI_TEST'
           GROUP BY p.project_id
           HAVING COUNT(r.reference_id) <> 1)
    THROW 50002, 'A project in this folder does not have exactly one reference to the environment.', 1;

/* 3. No sensitive parameter carries a literal value. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'WWI_TEST' AND op.sensitive = 1 AND op.value_type = 'V')
    THROW 50003, 'A sensitive parameter holds a literal value instead of an environment reference.', 1;

/* 4. Every referenced variable actually exists in the environment. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'WWI_TEST' AND op.value_type = 'R'
             AND NOT EXISTS (SELECT 1
                             FROM SSISDB.catalog.environment_variables AS v
                             INNER JOIN SSISDB.catalog.environments AS e
                                     ON e.environment_id = v.environment_id
                             INNER JOIN SSISDB.catalog.folders AS ef
                                     ON ef.folder_id = e.folder_id
                             WHERE ef.name = N'WWI_TEST'
                               AND e.name = N'WWI_TEST'
                               AND v.name = op.referenced_variable_name))
    THROW 50004, 'A parameter references an environment variable that does not exist.', 1;

/* 5. Every CM.<connection>.Password in the folder is bound by reference. */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.object_parameters AS op
           INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
           INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
           WHERE f.name = N'WWI_TEST' AND op.object_type = 20
             AND op.parameter_name LIKE 'CM.%.Password'
             AND op.value_type <> 'R')
    THROW 50005, 'A connection manager password parameter is not bound to the environment.', 1;

SELECT p.name          AS project_name,
       op.parameter_name,
       op.value_type,
       op.sensitive,
       op.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS op
INNER JOIN SSISDB.catalog.projects AS p ON p.project_id = op.project_id
INNER JOIN SSISDB.catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = N'WWI_TEST'
  AND op.object_type = 20
ORDER BY p.name, op.parameter_name;
GO
