/*
    Object          : Database roles and grants for the staging database
    Deploy target   : $(StagingDatabase) (WideWorldImporters_Staging)
    Deploy order    : sqlserver/security step 2 - after 00_server_principals.sql
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : Staging is the only database where the ETL principal has
                      DDL rights, because the raw and work schemas are truncated
                      and re-created by the loads. Everyone else is read-only,
                      and nobody but the stewards can read err.* - reject rows
                      carry unmasked customer detail. Idempotent; not executed.

    sqlcmd variables: $(DomainPrefix) $(EtlServiceAccount) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE [$(StagingDatabase)];
GO

/* --- users ------------------------------------------------------------- */

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\$(EtlServiceAccount)')
    CREATE USER [$(DomainPrefix)\$(EtlServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(EtlServiceAccount)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ETL')
    CREATE USER [WWI_ETL] FOR LOGIN [WWI_ETL];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\WWI-DataStewards')
    CREATE USER [$(DomainPrefix)\WWI-DataStewards] FOR LOGIN [$(DomainPrefix)\WWI-DataStewards];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\WWI-ETLOperators')
    CREATE USER [$(DomainPrefix)\WWI-ETLOperators] FOR LOGIN [$(DomainPrefix)\WWI-ETLOperators];
GO

/* --- roles ------------------------------------------------------------- */

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_StagingLoader' AND type = 'R')
    CREATE ROLE WWI_StagingLoader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_StagingReader' AND type = 'R')
    CREATE ROLE WWI_StagingReader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_DataSteward' AND type = 'R')
    CREATE ROLE WWI_DataSteward;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ControlReader' AND type = 'R')
    CREATE ROLE WWI_ControlReader;
GO

/* --- schema-level grants ----------------------------------------------- */

/* The loader owns the volatile schemas outright. */
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, EXECUTE ON SCHEMA::raw  TO WWI_StagingLoader;
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, EXECUTE ON SCHEMA::work TO WWI_StagingLoader;
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE         ON SCHEMA::stg  TO WWI_StagingLoader;
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE         ON SCHEMA::err  TO WWI_StagingLoader;
GRANT SELECT, INSERT, UPDATE, EXECUTE                 ON SCHEMA::etl  TO WWI_StagingLoader;
GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE         ON SCHEMA::ref  TO WWI_StagingLoader;
GO

/* Readers see cleansed staging and reference data, never raw and never rejects. */
GRANT SELECT ON SCHEMA::stg TO WWI_StagingReader;
GRANT SELECT ON SCHEMA::ref TO WWI_StagingReader;
DENY  SELECT ON SCHEMA::raw TO WWI_StagingReader;
DENY  SELECT ON SCHEMA::err TO WWI_StagingReader;
GO

/* Stewards work rejects by hand, which means write access to err and read to raw. */
GRANT SELECT, UPDATE ON SCHEMA::err TO WWI_DataSteward;
GRANT SELECT          ON SCHEMA::raw TO WWI_DataSteward;
GRANT SELECT          ON SCHEMA::stg TO WWI_DataSteward;
GRANT EXECUTE ON OBJECT::etl.usp_LogRejectedRecord TO WWI_DataSteward;
GO

/* Operators read the control framework and nothing else. */
GRANT SELECT ON SCHEMA::etl TO WWI_ControlReader;
GRANT EXECUTE ON OBJECT::etl.ufn_GetConfigurationValue TO WWI_ControlReader;
DENY  SELECT ON SCHEMA::raw TO WWI_ControlReader;
DENY  SELECT ON SCHEMA::err TO WWI_ControlReader;
GO

/* --- memberships -------------------------------------------------------- */

ALTER ROLE WWI_StagingLoader ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER ROLE WWI_StagingLoader ADD MEMBER [WWI_ETL];
ALTER ROLE WWI_ControlReader ADD MEMBER [$(DomainPrefix)\WWI-ETLOperators];
ALTER ROLE WWI_DataSteward   ADD MEMBER [$(DomainPrefix)\WWI-DataStewards];
ALTER ROLE WWI_StagingReader ADD MEMBER [$(DomainPrefix)\WWI-DataStewards];
GO

/*
    DEV only: stewards are also given db_datareader so they can chase a data
    problem without raising a ticket. This grant is deliberately not made in
    TEST or PROD.
*/
IF N'$(EnvironmentCode)' = N'DEV'
BEGIN
    ALTER ROLE [db_datareader] ADD MEMBER [$(DomainPrefix)\WWI-DataStewards];
END
GO
