/*
    Object          : Server-level principal definitions for the WWI estate
    Deploy target   : master (on the SQL Server instance hosting the estate)
    Deploy order    : sqlserver/security step 1 - before any database role script
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : Names and memberships only. Windows logins are the norm in
                      this estate; the two SQL logins that survive exist because
                      the SSIS proxy and the legacy reporting gateway predate the
                      domain trust. Their secrets are injected at deploy time
                      from SQLSERVER_PASSWORD and are never stored here.
                      Idempotent. Not executed against any server.

    sqlcmd variables:
        $(DomainPrefix)        - AD domain short name, e.g. CONTOSO
        $(EnvironmentCode)     - DEV / TEST / PROD
        $(EtlServiceAccount)   - AD account running the SSIS/Agent workload
        $(AppServiceAccount)   - AD account running the WWI application pool
        $(ReportServiceAccount)- AD account running the reporting gateway
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE master;
GO

/*
    Windows service accounts. One per workload, never shared, because the audit
    trail in etl.PackageExecution is only meaningful if the executing principal
    identifies the workload.
*/
DECLARE @Logins TABLE (LoginName SYSNAME NOT NULL);

INSERT INTO @Logins (LoginName)
VALUES (N'$(DomainPrefix)\$(EtlServiceAccount)'),
       (N'$(DomainPrefix)\$(AppServiceAccount)'),
       (N'$(DomainPrefix)\$(ReportServiceAccount)'),
       (N'$(DomainPrefix)\WWI-DBA-$(EnvironmentCode)'),
       (N'$(DomainPrefix)\WWI-DataStewards'),
       (N'$(DomainPrefix)\WWI-FinanceAnalysts'),
       (N'$(DomainPrefix)\WWI-ETLOperators');

DECLARE @LoginName SYSNAME;
DECLARE @Sql NVARCHAR(MAX);

DECLARE login_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT LoginName FROM @Logins;

OPEN login_cursor;
FETCH NEXT FROM login_cursor INTO @LoginName;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = @LoginName)
    BEGIN
        SET @Sql = N'CREATE LOGIN ' + QUOTENAME(@LoginName) + N' FROM WINDOWS WITH DEFAULT_DATABASE = [master];';
        EXEC sys.sp_executesql @Sql;
    END

    FETCH NEXT FROM login_cursor INTO @LoginName;
END

CLOSE login_cursor;
DEALLOCATE login_cursor;
GO

/*
    Server roles. The estate never grants sysadmin to a workload account; the
    DBA group holds it and everything else is scoped.
*/
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'WWI_ETL_ServerRole' AND type = 'R')
    CREATE SERVER ROLE WWI_ETL_ServerRole;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'WWI_Reporting_ServerRole' AND type = 'R')
    CREATE SERVER ROLE WWI_Reporting_ServerRole;
GO

/* The ETL role needs to read wait stats and session state to diagnose its own runs. */
GRANT VIEW SERVER STATE TO WWI_ETL_ServerRole;
GRANT VIEW ANY DEFINITION TO WWI_ETL_ServerRole;
GRANT CONNECT SQL TO WWI_ETL_ServerRole;

/* Reporting is read-only and does not get VIEW ANY DEFINITION. */
GRANT VIEW SERVER STATE TO WWI_Reporting_ServerRole;
GRANT CONNECT SQL TO WWI_Reporting_ServerRole;
GO

ALTER SERVER ROLE WWI_ETL_ServerRole       ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER SERVER ROLE WWI_ETL_ServerRole       ADD MEMBER [$(DomainPrefix)\WWI-ETLOperators];
ALTER SERVER ROLE WWI_Reporting_ServerRole ADD MEMBER [$(DomainPrefix)\$(ReportServiceAccount)];
ALTER SERVER ROLE WWI_Reporting_ServerRole ADD MEMBER [$(DomainPrefix)\WWI-FinanceAnalysts];
ALTER SERVER ROLE [sysadmin]               ADD MEMBER [$(DomainPrefix)\WWI-DBA-$(EnvironmentCode)];
GO

/*
    Credential-backed SQL logins used by the SSIS proxy and the reporting
    gateway. The deploy driver supplies the secret; if it did not, stop rather
    than create a login with a guessable secret.
*/
IF N'$(SqlLoginSecret)' = N''
BEGIN
    RAISERROR (N'No secret was supplied for the SQL logins. Set SQLSERVER_PASSWORD before running the security stage.', 16, 1);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'WWI_ETL')
BEGIN
    EXEC sys.sp_executesql
        N'CREATE LOGIN [WWI_ETL] WITH PASSWORD = N''$(SqlLoginSecret)'',
              CHECK_POLICY = ON, CHECK_EXPIRATION = OFF, DEFAULT_DATABASE = [master];';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'WWI_ReportGateway')
BEGIN
    EXEC sys.sp_executesql
        N'CREATE LOGIN [WWI_ReportGateway] WITH PASSWORD = N''$(SqlLoginSecret)'',
              CHECK_POLICY = ON, CHECK_EXPIRATION = OFF, DEFAULT_DATABASE = [master];';
END
GO

ALTER SERVER ROLE WWI_ETL_ServerRole       ADD MEMBER [WWI_ETL];
ALTER SERVER ROLE WWI_Reporting_ServerRole ADD MEMBER [WWI_ReportGateway];
GO
