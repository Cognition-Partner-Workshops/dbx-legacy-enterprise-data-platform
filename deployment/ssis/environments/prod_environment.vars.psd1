# GENERATED FILE - do not edit by hand. Rendered from
# config/environments/prod.env.yaml by deployment/ssis/render_environment_sql.py.
#
# sqlcmd variable -> (environment variable, default). A Secret entry has no
# default: the deploy fails if its environment variable is unset.
@{
    'OracleHost' = @{ EnvVar = 'ORACLE_HOST'; Default = 'oracle-erp-prod.internal.example'; Secret = $false }
    'OraclePort' = @{ EnvVar = 'ORACLE_PORT'; Default = '1521'; Secret = $false }
    'OracleService' = @{ EnvVar = 'ORACLE_SERVICE'; Default = 'WWIGERP'; Secret = $false }
    'OracleUser' = @{ EnvVar = 'ORACLE_USER'; Default = 'WWI_ETL_READER'; Secret = $false }
    'OraclePassword' = @{ EnvVar = 'ORACLE_PASSWORD'; Default = $null; Secret = $true }
    'SqlServerHost' = @{ EnvVar = 'SQLSERVER_HOST'; Default = 'sqlprod-oltp.internal.example'; Secret = $false }
    'SqlServerPort' = @{ EnvVar = 'SQLSERVER_PORT'; Default = '1433'; Secret = $false }
    'SqlServerUser' = @{ EnvVar = 'SQLSERVER_USER'; Default = 'WWI_ETL'; Secret = $false }
    'SqlServerPassword' = @{ EnvVar = 'SQLSERVER_PASSWORD'; Default = $null; Secret = $true }
    'SqlServerOltpDb' = @{ EnvVar = 'SQLSERVER_OLTP_DB'; Default = 'WideWorldImporters'; Secret = $false }
    'SqlServerStagingDb' = @{ EnvVar = 'SQLSERVER_STAGING_DB'; Default = 'WideWorldImporters_Staging'; Secret = $false }
    'SqlServerDwDb' = @{ EnvVar = 'SQLSERVER_DW_DB'; Default = 'WideWorldImportersDW'; Secret = $false }
    'InboundFileRoot' = @{ EnvVar = 'ETL_INBOUND_FILE_ROOT'; Default = '\\wwi-files\landing\inbound'; Secret = $false }
    'ArchiveFileRoot' = @{ EnvVar = 'ETL_ARCHIVE_FILE_ROOT'; Default = '\\wwi-files\landing\archive'; Secret = $false }
    'QuarantineFileRoot' = @{ EnvVar = 'ETL_QUARANTINE_FILE_ROOT'; Default = '\\wwi-files\landing\quarantine'; Secret = $false }
    'DefaultBatchSize' = @{ EnvVar = 'ETL_DEFAULT_BATCH_SIZE'; Default = '100000'; Secret = $false }
    'SourceQueryTimeoutSeconds' = @{ EnvVar = 'ETL_SOURCE_QUERY_TIMEOUT_SECONDS'; Default = '3600'; Secret = $false }
    'MaxRejectPercent' = @{ EnvVar = 'ETL_MAX_REJECT_PERCENT'; Default = '2'; Secret = $false }
    'EnvironmentCode' = @{ EnvVar = 'ETL_ENVIRONMENT_CODE'; Default = 'PROD'; Secret = $false }
}
