/*
    Object          : Linked-server and Oracle-side principal definitions
    Deploy target   : master (SQL Server side) - the Oracle side is documented only
    Deploy order    : sqlserver/security step 6 - last in the security stage
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : The GL feed from WWIGERP still arrives through a linked
                      server for two reconciliation queries that were never
                      migrated into SSIS. The link is defined here so the estate
                      is complete; the Oracle grants it depends on are listed in
                      comments because this repository does not deploy to
                      Oracle from SQL Server. No secrets appear here - the
                      remote password is supplied by the deployment driver from
                      ORACLE_PASSWORD. Idempotent; not executed against any
                      server.

    sqlcmd variables: $(OracleHost) $(OraclePort) $(OracleService)
                      $(OracleUser) $(OracleLinkSecret) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE master;
GO

/*
    Oracle-side principals this link expects. They are created by
    oracle/security in the ERP work package; repeated here as the SQL Server
    team's copy of the contract:

        WWI_ETL_READER   - SELECT on GL_JOURNAL_HEADER, GL_JOURNAL_LINE,
                           AP_INVOICE_HEADER, AR_INVOICE_HEADER, and the
                           FX_RATE_DAILY table
        WWI_RECON_READER - SELECT on the GL_PERIOD_BALANCE materialised view
        WWI_ETL_WRITER   - INSERT on ETL_EXTRACT_LOG only

    The linked server logs in as WWI_RECON_READER. It is deliberately not the
    same principal the SSIS extract uses, so a runaway reconciliation query can
    be killed without stopping the nightly extract.
*/

IF EXISTS (SELECT 1 FROM sys.servers WHERE name = N'WWIGERP_LINK')
BEGIN
    EXEC sys.sp_dropserver @server = N'WWIGERP_LINK', @droplogins = N'droplogins';
END
GO

EXEC sys.sp_addlinkedserver
    @server     = N'WWIGERP_LINK',
    @srvproduct = N'Oracle',
    @provider   = N'OraOLEDB.Oracle',
    @datasrc    = N'$(OracleHost):$(OraclePort)/$(OracleService)';
GO

IF N'$(OracleLinkSecret)' = N''
BEGIN
    RAISERROR (N'No secret was supplied for the Oracle linked server. Set ORACLE_PASSWORD before running the security stage.', 16, 1);
END
GO

EXEC sys.sp_addlinkedsrvlogin
    @rmtsrvname  = N'WWIGERP_LINK',
    @useself     = N'FALSE',
    @locallogin  = NULL,
    @rmtuser     = N'$(OracleUser)',
    @rmtpassword = N'$(OracleLinkSecret)';
GO

/* Distributed transactions are off: the GL feed is read-only and MSDTC across
   the Oracle gateway has never been supported by the platform team. */
EXEC sys.sp_serveroption @server = N'WWIGERP_LINK', @optname = N'rpc out',                @optvalue = N'true';
EXEC sys.sp_serveroption @server = N'WWIGERP_LINK', @optname = N'remote proc transaction promotion', @optvalue = N'false';
EXEC sys.sp_serveroption @server = N'WWIGERP_LINK', @optname = N'query timeout',          @optvalue = N'1800';
EXEC sys.sp_serveroption @server = N'WWIGERP_LINK', @optname = N'connect timeout',        @optvalue = N'60';
GO

/* Only the ETL principal may traverse the link. */
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'WWI_LinkedServerUser' AND type = 'R')
    CREATE SERVER ROLE WWI_LinkedServerUser;
GO

ALTER SERVER ROLE WWI_LinkedServerUser ADD MEMBER [WWI_ETL];
ALTER SERVER ROLE WWI_LinkedServerUser ADD MEMBER WWI_ETL_ServerRole;
GO
