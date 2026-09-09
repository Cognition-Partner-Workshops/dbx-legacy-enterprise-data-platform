# GENERATED FILE - do not edit by hand. Rendered from
# config/environments/dev.env.yaml by deployment/ssis/render_environment_sql.py.
#
# sqlcmd variable -> (environment variable, default). A Secret entry has no
# default: the deploy fails if its environment variable is unset.
@{
    'OracleHost' = @{ EnvVar = 'ORACLE_HOST'; Default = 'oracle-erp-dev.internal.example'; Secret = $false }
    'OraclePort' = @{ EnvVar = 'ORACLE_PORT'; Default = '1521'; Secret = $false }
    'OracleService' = @{ EnvVar = 'ORACLE_SERVICE'; Default = 'WWIGERPD'; Secret = $false }
    'OracleUser' = @{ EnvVar = 'ORACLE_USER'; Default = 'WWI_ETL_DEV'; Secret = $false }
    'OracleProvider' = @{ EnvVar = 'ORACLE_PROVIDER'; Default = 'OraOLEDB.Oracle.1'; Secret = $false }
    'OraclePassword' = @{ EnvVar = 'ORACLE_PASSWORD'; Default = $null; Secret = $true }
    'SqlServerHost' = @{ EnvVar = 'SQLSERVER_HOST'; Default = 'sqldev01.internal.example'; Secret = $false }
    'SqlServerPort' = @{ EnvVar = 'SQLSERVER_PORT'; Default = '1433'; Secret = $false }
    'SqlServerUser' = @{ EnvVar = 'SQLSERVER_USER'; Default = 'WWI_ETL'; Secret = $false }
    'SqlServerProvider' = @{ EnvVar = 'SQLSERVER_PROVIDER'; Default = 'MSOLEDBSQL19.1'; Secret = $false }
    'SqlServerTrustServerCertificate' = @{ EnvVar = 'SQLSERVER_TRUST_SERVER_CERTIFICATE'; Default = '1'; Secret = $false }
    'SqlServerPassword' = @{ EnvVar = 'SQLSERVER_PASSWORD'; Default = $null; Secret = $true }
    'SqlServerOltpDb' = @{ EnvVar = 'SQLSERVER_OLTP_DB'; Default = 'WideWorldImporters'; Secret = $false }
    'SqlServerStagingDb' = @{ EnvVar = 'SQLSERVER_STAGING_DB'; Default = 'WideWorldImporters_Staging'; Secret = $false }
    'SqlServerDwDb' = @{ EnvVar = 'SQLSERVER_DW_DB'; Default = 'WideWorldImportersDW'; Secret = $false }
    'InboundFileRoot' = @{ EnvVar = 'ETL_INBOUND_FILE_ROOT'; Default = 'C:\WWI\DEV\inbound'; Secret = $false }
    'ArchiveFileRoot' = @{ EnvVar = 'ETL_ARCHIVE_FILE_ROOT'; Default = 'C:\WWI\DEV\archive'; Secret = $false }
    'QuarantineFileRoot' = @{ EnvVar = 'ETL_QUARANTINE_FILE_ROOT'; Default = 'C:\WWI\DEV\quarantine'; Secret = $false }
    'DefaultBatchSize' = @{ EnvVar = 'ETL_DEFAULT_BATCH_SIZE'; Default = '10000'; Secret = $false }
    'SourceQueryTimeoutSeconds' = @{ EnvVar = 'ETL_SOURCE_QUERY_TIMEOUT_SECONDS'; Default = '600'; Secret = $false }
    'MaxRejectPercent' = @{ EnvVar = 'ETL_MAX_REJECT_PERCENT'; Default = '25'; Secret = $false }
    'EnvironmentCode' = @{ EnvVar = 'ETL_ENVIRONMENT_CODE'; Default = 'DEV'; Secret = $false }
}
