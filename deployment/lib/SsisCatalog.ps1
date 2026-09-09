<#
    Shared SSISDB helpers: the managed Integration Services client, and the
    read-only catalog queries the deployment drivers verify themselves with.

    Everything here is client-side. Nothing needs a file to exist on the SQL
    Server host, and nothing writes a credential to disk or to the log.

    The managed assemblies are .NET Framework, so this must be dot-sourced from
    Windows PowerShell 5.1, not from pwsh.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:WwiSsisAssemblyRoots = @(
    'C:\ProgramData\chocolatey\lib\standalone-ssis-devops-tools\tools',
    'C:\Program Files (x86)\Microsoft SQL Server\160\SDK\Assemblies',
    'C:\Program Files (x86)\Microsoft SQL Server\150\SDK\Assemblies',
    'C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies'
)

function Import-WwiIntegrationServicesApi {
    <#
        Loads Microsoft.SqlServer.Management.IntegrationServices and its SMO
        dependency. Returns $true when the managed API is usable, $false when it
        is not - callers then take their documented fallback rather than dying,
        because the estate has deploy hosts with only the command-line tools.
    #>
    param([switch] $Quiet)

    if ('Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices' -as [type]) {
        return $true
    }

    foreach ($root in $script:WwiSsisAssemblyRoots) {
        $is  = Join-Path $root 'Microsoft.SqlServer.Management.IntegrationServices.dll'
        $smo = Join-Path $root 'Microsoft.SqlServer.Smo.dll'
        if (-not (Test-Path $is)) { continue }
        try {
            # Catalog.Create() reaches the native Microsoft.SqlServer.BatchParser
            # library, which is resolved by the loader search path rather than by
            # the managed assembly's location, so the root has to be on PATH.
            if (($env:PATH -split ';') -notcontains $root) { $env:PATH = "$root;$env:PATH" }
            if (Test-Path $smo) { Add-Type -Path $smo -ErrorAction Stop }
            Add-Type -Path $is -ErrorAction Stop
            if (-not $Quiet) { Write-WwiLog "loaded the Integration Services managed API from $root" }
            return $true
        }
        catch {
            if (-not $Quiet) {
                Write-WwiLog "could not load the managed API from ${root}: $($_.Exception.Message)" 'WARN'
            }
        }
    }
    return $false
}

function Get-WwiSsisConnectionString {
    <#
        The connection string for the catalog instance. SQLSERVER_PASSWORD is
        read straight out of the environment into the string and never logged;
        callers must not echo the return value.

        -ForceIntegrated connects as the Windows principal even when
        SQLSERVER_USER is set: on the catalog's own host that variable carries
        the account the catalog binds, not the login to connect with, and the
        catalog procedures refuse a SQL-authenticated caller outright.
    #>
    param(
        [string] $Database = 'master',
        [switch] $ForceIntegrated
    )

    if ([string]::IsNullOrWhiteSpace($env:SSIS_SERVER)) {
        Stop-WwiWithError 'SSIS_SERVER is not set.'
    }

    $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
    $builder['Data Source']     = $env:SSIS_SERVER
    $builder['Initial Catalog'] = $Database
    $builder['Application Name'] = 'wwi-ssis-deploy'
    $builder['TrustServerCertificate'] = $true
    $builder['Connect Timeout'] = 30

    if ($ForceIntegrated -or [string]::IsNullOrWhiteSpace($env:SQLSERVER_USER)) {
        $builder['Integrated Security'] = $true
    } else {
        Assert-WwiEnvironmentVariable @('SQLSERVER_PASSWORD')
        $builder['User ID']  = $env:SQLSERVER_USER
        $builder['Password'] = $env:SQLSERVER_PASSWORD
    }
    return $builder.ConnectionString
}

function Invoke-WwiSqlQuery {
    <#
        Runs one query against the catalog instance and returns rows as objects.

        This exists so the drivers can verify their own work against
        catalog.projects / catalog.packages rather than trusting an exit code.
        Parameters are passed as SqlParameters, never string-formatted in.
    #>
    param(
        [Parameter(Mandatory)][string] $Query,
        [hashtable] $Parameters = @{},
        [string] $Database = 'master',
        [int] $TimeoutSeconds = 120
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection (Get-WwiSsisConnectionString -Database $Database)
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText    = $Query
        $command.CommandTimeout = $TimeoutSeconds
        foreach ($key in $Parameters.Keys) {
            $value = $Parameters[$key]
            if ($null -eq $value) { $value = [DBNull]::Value }
            [void] $command.Parameters.AddWithValue("@$key", $value)
        }

        $table = New-Object System.Data.DataTable
        $reader = $command.ExecuteReader()
        try { $table.Load($reader) } finally { $reader.Dispose() }

        $rows = @()
        foreach ($row in $table.Rows) {
            $item = [ordered] @{}
            foreach ($column in $table.Columns) {
                $value = $row[$column]
                $item[$column.ColumnName] = if ($value -is [DBNull]) { $null } else { $value }
            }
            $rows += [pscustomobject] $item
        }
        return , $rows
    }
    finally {
        $connection.Dispose()
    }
}

function Invoke-WwiSqlNonQuery {
    param(
        [Parameter(Mandatory)][string] $Query,
        [hashtable] $Parameters = @{},
        [string] $Database = 'master',
        [int] $TimeoutSeconds = 600
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection (Get-WwiSsisConnectionString -Database $Database)
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText    = $Query
        $command.CommandTimeout = $TimeoutSeconds
        foreach ($key in $Parameters.Keys) {
            $value = $Parameters[$key]
            if ($null -eq $value) { $value = [DBNull]::Value }
            [void] $command.Parameters.AddWithValue("@$key", $value)
        }
        return $command.ExecuteNonQuery()
    }
    finally {
        $connection.Dispose()
    }
}

function Test-WwiSsisCatalog {
    <#
        Answers the only question that matters before creating a catalog: is
        there a working SSISDB on this instance already? A database called
        SSISDB whose catalog schema does not answer is not a catalog, so both
        halves are checked.
    #>
    $rows = Invoke-WwiSqlQuery -Query @'
SELECT database_exists = CASE WHEN DB_ID('SSISDB') IS NULL THEN 0 ELSE 1 END,
       clr_enabled     = CONVERT(INT, (SELECT value_in_use FROM sys.configurations
                                       WHERE name = 'clr enabled')),
       is_sysadmin     = IS_SRVROLEMEMBER('sysadmin');
'@

    $state = [ordered] @{
        DatabaseExists = [bool] $rows[0].database_exists
        CatalogUsable  = $false
        SchemaVersion  = $null
        ClrEnabled     = [bool] $rows[0].clr_enabled
        IsSysadmin     = ($rows[0].is_sysadmin -eq 1)
    }

    if ($state.DatabaseExists) {
        try {
            $properties = Invoke-WwiSqlQuery -Database 'SSISDB' -Query @'
SELECT property_name, property_value
FROM catalog.catalog_properties;
'@
            $version = $properties | Where-Object { $_.property_name -eq 'SCHEMA_VERSION' }
            $state.SchemaVersion = if ($version) { [string] $version.property_value } else { $null }
            $state.CatalogUsable = $true
        }
        catch {
            $state.CatalogUsable = $false
        }
    }

    return [pscustomobject] $state
}

function Get-WwiCatalogInventory {
    <#
        What is actually deployed in a folder, from the catalog's own views.
        This is the evidence a deployment is verified against; an exit code is
        not evidence.
    #>
    param([Parameter(Mandatory)][string] $Folder)

    $projects = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ folder = $Folder } -Query @'
SELECT p.name AS project_name,
       p.project_id,
       p.last_deployed_time,
       package_count = (SELECT COUNT(*) FROM catalog.packages AS k WHERE k.project_id = p.project_id)
FROM catalog.projects AS p
INNER JOIN catalog.folders AS f ON f.folder_id = p.folder_id
WHERE f.name = @folder
ORDER BY p.name;
'@

    $packages = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ folder = $Folder } -Query @'
SELECT p.name AS project_name, k.name AS package_name
FROM catalog.packages AS k
INNER JOIN catalog.projects AS p ON p.project_id = k.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @folder
ORDER BY p.name, k.name;
'@

    return [pscustomobject] @{
        Folder   = $Folder
        Projects = $projects
        Packages = $packages
    }
}
