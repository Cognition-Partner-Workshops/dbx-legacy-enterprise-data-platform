<#
    Creates or verifies the SSISDB catalog on the estate's SQL Server instance.

    Idempotent: with a working catalog already present this reports its schema
    version and does nothing. Only the two things a catalog cannot exist without
    are ever changed - 'clr enabled', and the catalog itself.

    Creation path, in order:
      1. The managed Integration Services API
         (Microsoft.SqlServer.Management.IntegrationServices.Catalog.Create()),
         which is the code path SSMS uses. It is a client-side call: no file has
         to reach the SQL Server host.
      2. If that assembly is not available, the documented T-SQL fallback -
         restore SSISDB from the SSISDBBackup.bak that ships on the instance,
         create the asymmetric-key login the catalog assemblies are signed with,
         re-encrypt the database master key by the service master key, enable
         Service Broker and mark sp_ssis_startup as a startup procedure. Pass
         -AllowRestoreFallback to permit it; it is off by default because it
         needs a path on the server.

    The catalog password encrypts SSISDB's database master key. It is read from
    SSIS_CATALOG_PASSWORD only, is never rendered into SQL text, never written
    to disk and never logged.

    Environment: SSIS_SERVER, SSIS_CATALOG_PASSWORD, and SQLSERVER_USER /
    SQLSERVER_PASSWORD when the instance is reached with SQL authentication.

    Usage:
        .\deployment\ssis\New-SsisCatalog.ps1 -WhatIf
        .\deployment\ssis\New-SsisCatalog.ps1
        .\deployment\ssis\New-SsisCatalog.ps1 -VerifyOnly
#>

[CmdletBinding()]
param(
    [switch] $VerifyOnly,
    [switch] $AllowRestoreFallback,
    [string] $BackupPath = 'C:\Program Files\Microsoft SQL Server\160\DTS\Binn\SSISDBBackup.bak',
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'SsisCatalog.ps1')
$script:WwiLogPrefix = 'wwi-ssis-catalog'

Assert-WwiEnvironmentVariable @('SSIS_SERVER')
Confirm-WwiProduction

$state = Test-WwiSsisCatalog

Write-WwiLog ("instance {0}: SSISDB database {1}, catalog {2}, clr enabled {3}, sysadmin {4}" -f `
    $env:SSIS_SERVER,
    $(if ($state.DatabaseExists) { 'present' } else { 'absent' }),
    $(if ($state.CatalogUsable) { "usable (schema $($state.SchemaVersion))" } else { 'not usable' }),
    [int] $state.ClrEnabled,
    [int] $state.IsSysadmin)

if ($state.CatalogUsable) {
    Write-WwiLog 'SSISDB already exists and answers catalog.catalog_properties; nothing to do.'
    return
}

if ($VerifyOnly) {
    Stop-WwiWithError 'there is no usable SSISDB catalog on this instance (-VerifyOnly, so nothing was created).'
}

if ($state.DatabaseExists -and -not $state.CatalogUsable) {
    Stop-WwiWithError ('a database named SSISDB exists but does not answer catalog.catalog_properties. ' +
                       'That is not a catalog this script can adopt; resolve it by hand before re-running.')
}

if (-not $state.IsSysadmin) {
    Stop-WwiWithError 'creating the catalog needs sysadmin on the instance; the current login does not have it.'
}

Assert-WwiEnvironmentVariable @('SSIS_CATALOG_PASSWORD')

if ($DryRun) {
    Write-WwiLog "WHATIF enable 'clr enabled' on $($env:SSIS_SERVER) (currently $([int] $state.ClrEnabled))"
    Write-WwiLog 'WHATIF create the SSISDB catalog with the password from SSIS_CATALOG_PASSWORD'
    return
}

if (-not $state.ClrEnabled) {
    # The catalog's stored procedures are CLR. 'clr strict security' can stay on:
    # the assemblies are Microsoft-signed and Create() provisions the
    # MS_SQLEnableSystemAssemblyLoadingUser login that lets them load.
    Write-WwiLog "enabling 'clr enabled'"
    Invoke-WwiSqlNonQuery -Query @'
EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
EXEC sp_configure 'clr enabled', 1; RECONFIGURE;
'@ | Out-Null
}

$created = $false

if (Import-WwiIntegrationServicesApi) {
    Write-WwiLog 'creating the catalog through the Integration Services managed API'
    $connectionString = Get-WwiSsisConnectionString -Database 'master'
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection $connectionString
    try {
        $serverConnection = New-Object Microsoft.SqlServer.Management.Common.ServerConnection $sqlConnection
        $integrationServices = New-Object Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices $serverConnection
        $catalog = New-Object Microsoft.SqlServer.Management.IntegrationServices.Catalog `
            $integrationServices, 'SSISDB', $env:SSIS_CATALOG_PASSWORD
        $catalog.Create()
        $created = $true
    }
    catch {
        # The message can quote the failing statement, so it is reported without
        # the exception's data, which is where a parameter value could surface.
        Write-WwiLog "the managed API could not create the catalog: $($_.Exception.Message)" 'WARN'
    }
    finally {
        $sqlConnection.Dispose()
    }
}
else {
    Write-WwiLog 'the Integration Services managed API is not installed on this host.' 'WARN'
}

if (-not $created -and $AllowRestoreFallback) {
    Write-WwiLog "falling back to restoring SSISDB from $BackupPath on the server"

    # The password is a parameter, so it is never part of the batch text; the
    # dynamic statements below are the two places T-SQL has no parameter for.
    Invoke-WwiSqlNonQuery -Parameters @{
        backup   = $BackupPath
        password = $env:SSIS_CATALOG_PASSWORD
    } -Query @'
SET XACT_ABORT ON;

RESTORE DATABASE SSISDB FROM DISK = @backup WITH RECOVERY;

IF NOT EXISTS (SELECT 1 FROM sys.asymmetric_keys WHERE name = 'MS_SQLEnableSystemAssemblyLoadingKey')
BEGIN
    CREATE ASYMMETRIC KEY MS_SQLEnableSystemAssemblyLoadingKey
        FROM EXECUTABLE FILE =
        'C:\Program Files\Microsoft SQL Server\160\DTS\Binn\Microsoft.SqlServer.IntegrationServices.Server.dll';
    CREATE LOGIN MS_SQLEnableSystemAssemblyLoadingUser FROM ASYMMETRIC KEY MS_SQLEnableSystemAssemblyLoadingKey;
    GRANT UNSAFE ASSEMBLY TO MS_SQLEnableSystemAssemblyLoadingUser;
END

DECLARE @sql NVARCHAR(MAX) =
    N'USE SSISDB; OPEN MASTER KEY DECRYPTION BY PASSWORD = ' + QUOTENAME(@password, '''') + N';'
  + N' ALTER MASTER KEY ADD ENCRYPTION BY SERVICE MASTER KEY;';
EXEC sys.sp_executesql @sql;

ALTER DATABASE SSISDB SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;

USE SSISDB;
EXEC sp_procoption N'sp_ssis_startup', 'startup', 'on';
'@ | Out-Null

    $created = $true
}

if (-not $created) {
    Stop-WwiWithError ('the catalog could not be created. Install the Integration Services managed API ' +
                       '(the standalone-ssis-devops-tools package carries it) or re-run with ' +
                       '-AllowRestoreFallback so SSISDB can be restored from SSISDBBackup.bak on the server.')
}

$state = Test-WwiSsisCatalog
if (-not $state.CatalogUsable) {
    Stop-WwiWithError 'the create call returned, but catalog.catalog_properties still does not answer.'
}

Write-WwiLog "SSISDB created; catalog schema version $($state.SchemaVersion)"
