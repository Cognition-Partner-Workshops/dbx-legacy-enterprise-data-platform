/*
    Object          : SQL Agent credential and proxy *references*
    Deploy target   : master (credential) and msdb (proxy)
    Deploy order    : sqlserver/agent step 3 - after 01_create_operators.sql
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Notes           : This script creates the credential and proxy NAMES the job
                      scripts bind to. It deliberately does not contain, and
                      must never contain, a secret. The identity secret is
                      injected by the deployment driver from the environment
                      variable named in the comment beside each credential, and
                      the script aborts if the driver did not supply it.
                      Idempotent. Not executed against any server.

    sqlcmd variables (supplied by the deployment driver):
        $(SsisProxyAccount)   - domain account the SSIS steps run as
        $(FileProxyAccount)   - domain account with rights on the landing zone
        $(SsisProxySecret)    - injected at deploy time from SQLSERVER_PASSWORD
        $(FileProxySecret)    - injected at deploy time from SQLSERVER_PASSWORD
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE master;
GO

IF N'$(SsisProxyAccount)' = N'' OR N'$(FileProxyAccount)' = N''
BEGIN
    RAISERROR (N'Proxy account names were not supplied by the deployment driver. Set SQLSERVER_SSIS_PROXY_ACCOUNT and SQLSERVER_FILE_PROXY_ACCOUNT before running the agent stage.', 16, 1);
END
GO

/* Credential for the SSIS execution identity (secret injected at deploy time). */
IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'WWI_SSIS_Execution_Credential')
BEGIN
    EXEC sys.sp_executesql
        N'CREATE CREDENTIAL WWI_SSIS_Execution_Credential
              WITH IDENTITY = N''$(SsisProxyAccount)'',
                   SECRET   = N''$(SsisProxySecret)'';';
END
ELSE
BEGIN
    EXEC sys.sp_executesql
        N'ALTER CREDENTIAL WWI_SSIS_Execution_Credential
              WITH IDENTITY = N''$(SsisProxyAccount)'',
                   SECRET   = N''$(SsisProxySecret)'';';
END
GO

/* Credential for file-system work on the landing zone (archive, quarantine). */
IF NOT EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'WWI_FileShare_Credential')
BEGIN
    EXEC sys.sp_executesql
        N'CREATE CREDENTIAL WWI_FileShare_Credential
              WITH IDENTITY = N''$(FileProxyAccount)'',
                   SECRET   = N''$(FileProxySecret)'';';
END
GO

USE msdb;
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysproxies WHERE name = N'WWI_SSIS_Proxy')
BEGIN
    EXEC msdb.dbo.sp_add_proxy
        @proxy_name      = N'WWI_SSIS_Proxy',
        @credential_name = N'WWI_SSIS_Execution_Credential',
        @enabled         = 1,
        @description     = N'Runs SSIS package steps for the WWI estate.';

    /* subsystem 11 = SSIS, 3 = CmdExec, 12 = PowerShell */
    EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'WWI_SSIS_Proxy', @subsystem_id = 11;
    EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'WWI_SSIS_Proxy', @subsystem_id = 3;
END
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysproxies WHERE name = N'WWI_FileOps_Proxy')
BEGIN
    EXEC msdb.dbo.sp_add_proxy
        @proxy_name      = N'WWI_FileOps_Proxy',
        @credential_name = N'WWI_FileShare_Credential',
        @enabled         = 1,
        @description     = N'Archive, quarantine and reject-file movement on the landing zone.';

    EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'WWI_FileOps_Proxy', @subsystem_id = 3;
    EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name = N'WWI_FileOps_Proxy', @subsystem_id = 12;
END
GO

/* Only the ETL operator role may use the proxies; see sqlserver/security. */
IF EXISTS (SELECT 1 FROM msdb.sys.database_principals WHERE name = N'WWI_ETL_JobOperator')
BEGIN
    IF NOT EXISTS (SELECT 1
                   FROM msdb.dbo.sysproxylogin AS pl
                   INNER JOIN msdb.dbo.sysproxies AS p ON p.proxy_id = pl.proxy_id
                   WHERE p.name = N'WWI_SSIS_Proxy')
    BEGIN
        EXEC msdb.dbo.sp_grant_login_to_proxy
            @msdb_role     = N'WWI_ETL_JobOperator',
            @proxy_name    = N'WWI_SSIS_Proxy';
    END
END
GO
