/*
    Object          : msdb and SSISDB permissions for the ETL workload
    Deploy target   : msdb and SSISDB
    Deploy order    : sqlserver/security step 5 - after the database role scripts
                      and before the agent stage, which binds proxies to
                      WWI_ETL_JobOperator.
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : Operators may start, stop and read the WWI jobs; they may
                      not create or alter them. SSIS catalogue rights are given
                      at folder level so a DEV folder cannot be executed by a
                      PROD principal or the reverse. Idempotent; not executed.

    sqlcmd variables: $(DomainPrefix) $(EtlServiceAccount) $(SsisFolder)
                      $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\$(EtlServiceAccount)')
    CREATE USER [$(DomainPrefix)\$(EtlServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(EtlServiceAccount)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\WWI-ETLOperators')
    CREATE USER [$(DomainPrefix)\WWI-ETLOperators] FOR LOGIN [$(DomainPrefix)\WWI-ETLOperators];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ETL_JobOperator' AND type = 'R')
    CREATE ROLE WWI_ETL_JobOperator;
GO

ALTER ROLE [SQLAgentOperatorRole] ADD MEMBER [$(DomainPrefix)\WWI-ETLOperators];
ALTER ROLE [SQLAgentUserRole]     ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER ROLE WWI_ETL_JobOperator    ADD MEMBER [$(DomainPrefix)\WWI-ETLOperators];
ALTER ROLE WWI_ETL_JobOperator    ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
GO

/* Read job history and step detail, start and stop, but never alter. */
GRANT EXECUTE ON OBJECT::msdb.dbo.sp_start_job  TO WWI_ETL_JobOperator;
GRANT EXECUTE ON OBJECT::msdb.dbo.sp_stop_job   TO WWI_ETL_JobOperator;
GRANT EXECUTE ON OBJECT::msdb.dbo.sp_help_job   TO WWI_ETL_JobOperator;
GRANT SELECT  ON OBJECT::msdb.dbo.sysjobhistory TO WWI_ETL_JobOperator;
GRANT SELECT  ON OBJECT::msdb.dbo.sysjobsteps   TO WWI_ETL_JobOperator;
DENY  EXECUTE ON OBJECT::msdb.dbo.sp_add_job      TO WWI_ETL_JobOperator;
DENY  EXECUTE ON OBJECT::msdb.dbo.sp_update_job   TO WWI_ETL_JobOperator;
DENY  EXECUTE ON OBJECT::msdb.dbo.sp_delete_job   TO WWI_ETL_JobOperator;
GO

/*
    SSIS catalogue. Folder-level rights are applied only when the catalogue and
    the environment's folder already exist; the SSIS deployment stage creates
    them, and the security stage is safe to run before or after it.
*/
IF DB_ID(N'SSISDB') IS NOT NULL
BEGIN
    EXEC sys.sp_executesql N'
        USE SSISDB;
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''$(DomainPrefix)\$(EtlServiceAccount)'')
            CREATE USER [$(DomainPrefix)\$(EtlServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(EtlServiceAccount)];
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''$(DomainPrefix)\WWI-ETLOperators'')
            CREATE USER [$(DomainPrefix)\WWI-ETLOperators] FOR LOGIN [$(DomainPrefix)\WWI-ETLOperators];
        ALTER ROLE [ssis_admin] ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];

        DECLARE @FolderId BIGINT;
        SELECT @FolderId = folder_id FROM catalog.folders WHERE name = N''$(SsisFolder)'';
        IF @FolderId IS NOT NULL
        BEGIN
            EXEC catalog.grant_permission @object_type = 1, @object_id = @FolderId,
                 @principal = N''$(DomainPrefix)\WWI-ETLOperators'', @permission_type = 1;  /* READ */
            EXEC catalog.grant_permission @object_type = 1, @object_id = @FolderId,
                 @principal = N''$(DomainPrefix)\WWI-ETLOperators'', @permission_type = 100; /* EXECUTE */
        END;';
END
ELSE
BEGIN
    PRINT N'SSISDB is not present on this instance; catalogue grants skipped.';
END
GO
