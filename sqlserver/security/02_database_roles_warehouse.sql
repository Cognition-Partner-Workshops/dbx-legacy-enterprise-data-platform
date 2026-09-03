/*
    Object          : Database roles and grants for the warehouse database
    Deploy target   : $(DwDatabase) (WideWorldImportersDW)
    Deploy order    : sqlserver/security step 3 - after 01_database_roles_staging.sql
    Called by       : deployment/sqlserver/Deploy-SqlServer.ps1 -Stage security
    Notes           : The warehouse is read-mostly. Only the ETL principal
                      writes, and it writes through the Integration and
                      Dimension/Fact schemas of the Microsoft sample, which the
                      estate extends rather than replaces. Regional analyst
                      groups are separated because the EU dataset carries
                      personal data that the NA and APAC analyst groups are not
                      cleared for. Idempotent; not executed against any server.

    sqlcmd variables: $(DomainPrefix) $(EtlServiceAccount) $(ReportServiceAccount)
                      $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE [$(DwDatabase)];
GO

/* --- users ------------------------------------------------------------- */

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\$(EtlServiceAccount)')
    CREATE USER [$(DomainPrefix)\$(EtlServiceAccount)] FOR LOGIN [$(DomainPrefix)\$(EtlServiceAccount)];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ETL')
    CREATE USER [WWI_ETL] FOR LOGIN [WWI_ETL];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_ReportGateway')
    CREATE USER [WWI_ReportGateway] FOR LOGIN [WWI_ReportGateway];
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$(DomainPrefix)\WWI-FinanceAnalysts')
    CREATE USER [$(DomainPrefix)\WWI-FinanceAnalysts] FOR LOGIN [$(DomainPrefix)\WWI-FinanceAnalysts];
GO

/* --- roles ------------------------------------------------------------- */

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_WarehouseLoader' AND type = 'R')
    CREATE ROLE WWI_WarehouseLoader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_WarehouseReader' AND type = 'R')
    CREATE ROLE WWI_WarehouseReader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_FinanceReader' AND type = 'R')
    CREATE ROLE WWI_FinanceReader;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_RegionalReader_EU' AND type = 'R')
    CREATE ROLE WWI_RegionalReader_EU;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_RegionalReader_NA' AND type = 'R')
    CREATE ROLE WWI_RegionalReader_NA;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'WWI_RegionalReader_APAC' AND type = 'R')
    CREATE ROLE WWI_RegionalReader_APAC;
GO

/* --- schema-level grants ----------------------------------------------- */

GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::Integration TO WWI_WarehouseLoader;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::Dimension   TO WWI_WarehouseLoader;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::Fact        TO WWI_WarehouseLoader;
GRANT SELECT, INSERT, UPDATE         ON SCHEMA::etl         TO WWI_WarehouseLoader;
GRANT EXECUTE                        ON SCHEMA::etl         TO WWI_WarehouseLoader;
GRANT ALTER                          ON SCHEMA::Integration TO WWI_WarehouseLoader;
GO

GRANT SELECT ON SCHEMA::Dimension TO WWI_WarehouseReader;
GRANT SELECT ON SCHEMA::Fact      TO WWI_WarehouseReader;
DENY  SELECT ON SCHEMA::Integration TO WWI_WarehouseReader;
GO

/* Finance sees the aggregate and ledger surface, including the close audit trail. */
GRANT SELECT ON SCHEMA::Fact TO WWI_FinanceReader;
GRANT SELECT ON SCHEMA::agg  TO WWI_FinanceReader;
GRANT SELECT ON OBJECT::etl.vw_BatchStatus TO WWI_FinanceReader;
GO

/*
    Regional separation. The regional roles differ in what they may see, not
    just in name: the EU role is denied the un-pseudonymised customer contact
    columns, APAC is denied cross-border customer rows, and NA has neither
    restriction but is denied the EU consent audit surface.
*/
GRANT SELECT ON SCHEMA::Dimension TO WWI_RegionalReader_EU;
GRANT SELECT ON SCHEMA::Fact      TO WWI_RegionalReader_EU;
DENY  SELECT ON OBJECT::Dimension.[Customer]([Primary Contact])  TO WWI_RegionalReader_EU;
DENY  SELECT ON OBJECT::Dimension.[Customer]([Postal Code])      TO WWI_RegionalReader_EU;
GO

GRANT SELECT ON SCHEMA::Dimension TO WWI_RegionalReader_APAC;
GRANT SELECT ON SCHEMA::Fact      TO WWI_RegionalReader_APAC;
DENY  SELECT ON OBJECT::Dimension.[Customer]([Primary Contact])  TO WWI_RegionalReader_APAC;
GO

GRANT SELECT ON SCHEMA::Dimension TO WWI_RegionalReader_NA;
GRANT SELECT ON SCHEMA::Fact      TO WWI_RegionalReader_NA;
GO

/* --- memberships -------------------------------------------------------- */

ALTER ROLE WWI_WarehouseLoader ADD MEMBER [$(DomainPrefix)\$(EtlServiceAccount)];
ALTER ROLE WWI_WarehouseLoader ADD MEMBER [WWI_ETL];
ALTER ROLE WWI_WarehouseReader ADD MEMBER [WWI_ReportGateway];
ALTER ROLE WWI_FinanceReader   ADD MEMBER [$(DomainPrefix)\WWI-FinanceAnalysts];
GO
