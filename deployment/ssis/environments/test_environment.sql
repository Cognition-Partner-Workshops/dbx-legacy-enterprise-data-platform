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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleHost',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'oracle-erp-test.internal.example',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OraclePort',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = 1521,
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleService',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WWIGERPT',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OracleUser',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WWI_ETL_TEST',
     @description      = N'Bound to project parameter OracleUser. Source: ORACLE_USER.';
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'OraclePassword',
     @data_type        = N'String',
     @sensitive        = 1,
     @value            = N'$(OraclePassword)',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerHost',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'sqltest01.internal.example',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerPort',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = 1433,
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerUser',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WWI_ETL',
     @description      = N'Bound to project parameter SqlServerUser. Source: SQLSERVER_USER.';
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerPassword',
     @data_type        = N'String',
     @sensitive        = 1,
     @value            = N'$(SqlServerPassword)',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerOltpDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WideWorldImporters',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerStagingDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WideWorldImporters_Staging',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SqlServerDwDb',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'WideWorldImportersDW',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'InboundFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'\\wwi-files-test\landing\inbound',
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'ArchiveFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'\\wwi-files-test\landing\archive',
     @description      = N'Bound to project parameter ArchiveFileRoot. Source: ETL_ARCHIVE_FILE_ROOT.';
GO

/* RejectFileRoot (String) <- ETL_REJECT_FILE_ROOT */
IF EXISTS (SELECT 1
           FROM SSISDB.catalog.environment_variables AS v
           INNER JOIN SSISDB.catalog.environments AS e ON e.environment_id = v.environment_id
           INNER JOIN SSISDB.catalog.folders AS f ON f.folder_id = e.folder_id
           WHERE v.name = N'RejectFileRoot' AND e.name = N'WWI_TEST' AND f.name = N'WWI_TEST')
BEGIN
    EXEC SSISDB.catalog.delete_environment_variable
         @folder_name = N'WWI_TEST', @environment_name = N'WWI_TEST',
         @variable_name = N'RejectFileRoot';
END

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'RejectFileRoot',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'\\wwi-files-test\landing\quarantine',
     @description      = N'Bound to project parameter RejectFileRoot. Source: ETL_REJECT_FILE_ROOT.';
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'DefaultBatchSize',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = 50000,
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'SourceQueryTimeoutSeconds',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = 1800,
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'MaxRejectPercent',
     @data_type        = N'Int32',
     @sensitive        = 0,
     @value            = 5,
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

EXEC SSISDB.catalog.create_environment_variable
     @folder_name      = N'WWI_TEST',
     @environment_name = N'WWI_TEST',
     @variable_name    = N'EnvironmentCode',
     @data_type        = N'String',
     @sensitive        = 0,
     @value            = N'TEST',
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Orchestration',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Orchestration'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_Oracle',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Extract_Oracle'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Extract_SqlServer',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Extract_SqlServer'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Ingest_Files',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Ingest_Files'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Staging',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Staging'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_DataQuality',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_DataQuality'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ReferenceData',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_ReferenceData'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Dimensions',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Dimensions'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Facts',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Facts'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Aggregates',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Aggregates'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Finance',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Finance'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Sales',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Sales'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Inventory',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Inventory'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Procurement',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Procurement'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Customer360',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Customer360'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_ErrorHandling',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_ErrorHandling'
  AND p.object_type = 20
ORDER BY p.parameter_name;
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
         @reference_location = 'L',            /* local: same folder */
         @reference_id      = @ReferenceId OUTPUT;
END
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'OracleHost',
     @parameter_value = N'OracleHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'OraclePort',
     @parameter_value = N'OraclePort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'OracleService',
     @parameter_value = N'OracleService',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'OracleUser',
     @parameter_value = N'OracleUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'OraclePassword',
     @parameter_value = N'OraclePassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerHost',
     @parameter_value = N'SqlServerHost',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerPort',
     @parameter_value = N'SqlServerPort',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerUser',
     @parameter_value = N'SqlServerUser',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerPassword',
     @parameter_value = N'SqlServerPassword',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerOltpDb',
     @parameter_value = N'SqlServerOltpDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerStagingDb',
     @parameter_value = N'SqlServerStagingDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SqlServerDwDb',
     @parameter_value = N'SqlServerDwDb',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'InboundFileRoot',
     @parameter_value = N'InboundFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'ArchiveFileRoot',
     @parameter_value = N'ArchiveFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'RejectFileRoot',
     @parameter_value = N'RejectFileRoot',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'DefaultBatchSize',
     @parameter_value = N'DefaultBatchSize',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'SourceQueryTimeoutSeconds',
     @parameter_value = N'SourceQueryTimeoutSeconds',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'MaxRejectPercent',
     @parameter_value = N'MaxRejectPercent',
     @value_type     = 'R';                    /* referenced environment variable */
GO

EXEC SSISDB.catalog.set_object_parameter_value
     @object_type    = 20,                     /* project parameter */
     @folder_name    = N'WWI_TEST',
     @project_name   = N'WWI_Maintenance',
     @parameter_name = N'EnvironmentCode',
     @parameter_value = N'EnvironmentCode',
     @value_type     = 'R';                    /* referenced environment variable */
GO

/* Post-condition: every project parameter resolves to an environment variable. */
SELECT p.parameter_name,
       p.value_type,
       p.design_default_value,
       p.referenced_variable_name
FROM SSISDB.catalog.object_parameters AS p
INNER JOIN SSISDB.catalog.projects AS pr ON pr.project_id = p.project_id
INNER JOIN SSISDB.catalog.folders  AS f  ON f.folder_id  = pr.folder_id
WHERE f.name = N'WWI_TEST'
  AND pr.name = N'WWI_Maintenance'
  AND p.object_type = 20
ORDER BY p.parameter_name;
GO
