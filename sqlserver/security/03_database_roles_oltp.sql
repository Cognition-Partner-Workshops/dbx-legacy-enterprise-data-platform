/*
    Object          : Database roles and grants for the OLTP database
    Deploy target   : $(OltpDatabase) (WideWorldImporters)
    Deploy order    : sqlserver/security step 4 - after 02_database_roles_warehouse.sql
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : The OLTP database belongs to the application. The estate
                      only reads from it, and it reads through the extract
                      views, never the base tables - a rule introduced after an
                      extract took a schema lock during trading hours. The
                      Microsoft sample's own roles are left exactly as shipped.
                      Idempotent; not executed against any server.

    sqlcmd variables: $(DomainPrefix) $(EtlServiceAccount) $(AppServiceAccount)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE [$(OltpDatabase)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\$(EtlServiceAccount)')
    CREATE USER [$(DomainPrefix)\$(EtlServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(EtlServiceAccount)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ETL')
    CREATE USER [WWI_ETL] FOR LOGIN [WWI_ETL];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\$(AppServiceAccount)')
    CREATE USER [$(DomainPrefix)\$(AppServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(AppServiceAccount)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ExtractReader' AND type = 'R')
    CREATE ROLE WWI_ExtractReader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ChangeTrackingReader' AND type = 'R')
    CREATE ROLE WWI_ChangeTrackingReader;
GO

/*
    Extract surface only. The extract schema is created by the OLTP extension
    work package; if it is absent the grants are skipped rather than failing the
    deployment, because the security stage can legitimately run before it.
*/
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'extract')
BEGIN
    EXEC sys.sp_executesql N'GRANT SELECT ON SCHEMA::[extract] TO WWI_ExtractReader;';
    EXEC sys.sp_executesql N'GRANT EXECUTE ON SCHEMA::[extract] TO WWI_ExtractReader;';
END
ELSE
BEGIN
    PRINT N'Schema [extract] not present yet; extract grants skipped. Re-run the security stage after the OLTP extension stage.';
END
GO

/* Change tracking underpins the hourly incremental load. */
GRANT VIEW CHANGE TRACKING ON SCHEMA::Sales       TO WWI_ChangeTrackingReader;
GRANT VIEW CHANGE TRACKING ON SCHEMA::Purchasing  TO WWI_ChangeTrackingReader;
GRANT VIEW CHANGE TRACKING ON SCHEMA::Warehouse   TO WWI_ChangeTrackingReader;
GRANT VIEW DATABASE STATE TO WWI_ChangeTrackingReader;
GO

/* No direct base-table access for the ETL principal - the views are the contract. */
DENY SELECT ON SCHEMA::Application TO WWI_ExtractReader;
DENY INSERT, UPDATE, DELETE ON SCHEMA::Sales      TO WWI_ExtractReader;
DENY INSERT, UPDATE, DELETE ON SCHEMA::Purchasing TO WWI_ExtractReader;
DENY INSERT, UPDATE, DELETE ON SCHEMA::Warehouse  TO WWI_ExtractReader;
GO

ALTER ROLE WWI_ExtractReader        ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER ROLE WWI_ExtractReader        ADD MEMBER [WWI_ETL];
ALTER ROLE WWI_ChangeTrackingReader ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER ROLE WWI_ChangeTrackingReader ADD MEMBER [WWI_ETL];
GO
