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

        -Integrated connects as the Windows principal. Every catalog procedure
        that writes - create_execution, start_execution, deploy_project, the
        environment procedures - rejects a SQL-authenticated caller, so the
        catalog write path must set it and must therefore run on a host whose
        Windows identity the instance knows.
    #>
    param(
        [Parameter(Mandatory)][string] $Query,
        [hashtable] $Parameters = @{},
        [string] $Database = 'master',
        [int] $TimeoutSeconds = 120,
        [switch] $Integrated
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection (Get-WwiSsisConnectionString -Database $Database -ForceIntegrated:$Integrated)
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
        [int] $TimeoutSeconds = 600,
        [switch] $Integrated
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection (Get-WwiSsisConnectionString -Database $Database -ForceIntegrated:$Integrated)
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

function Get-WwiConnectionAuthentication {
    <#
        How the instance sees this connection: 'SQL' for a SQL login, 'NTLM' or
        'KERBEROS' for a Windows principal. Read from the instance rather than
        inferred from the connection string, because that is the value
        catalog.create_execution itself gates on.
    #>
    param([string] $Database = 'master', [switch] $Integrated)

    $rows = Invoke-WwiSqlQuery -Database $Database -Integrated:$Integrated -Query @'
SELECT auth_scheme = c.auth_scheme,
       login_name  = s.login_name,
       is_sysadmin = IS_SRVROLEMEMBER('sysadmin')
FROM sys.dm_exec_connections AS c
INNER JOIN sys.dm_exec_sessions AS s ON s.session_id = c.session_id
WHERE c.session_id = @@SPID;
'@
    return $rows[0]
}

function Assert-WwiCatalogWindowsAuthentication {
    <#
        catalog.create_execution and catalog.start_execution refuse a caller
        that authenticated with a SQL login: "The operation cannot be started by
        an account that uses SQL Server Authentication." The refusal happens
        inside the procedure, after any surrounding control-framework work has
        already been committed, so the runner asks the instance up front and
        stops before it opens anything it would have to unwind.
    #>
    param([string] $Database = 'SSISDB')

    $context = Get-WwiConnectionAuthentication -Database $Database -Integrated
    if ($context.auth_scheme -eq 'SQL') {
        Stop-WwiWithError ("the SSISDB connection authenticated as '$($context.login_name)' with " +
            "auth_scheme=SQL. catalog.create_execution only accepts a Windows principal, so the " +
            "runner must execute on the catalog host (deployment/ssis/Invoke-EstateOrchestrationRemote.ps1) " +
            'rather than against it with SQLSERVER_USER.')
    }
    Write-WwiLog ("SSISDB connection is $($context.auth_scheme) as $($context.login_name)" +
                  " (sysadmin=$($context.is_sysadmin))")
    return $context
}

$script:WwiSsisParameterClrType = [ordered] @{
    'Boolean'  = [bool]
    'Byte'     = [byte]
    'SByte'    = [sbyte]
    'Int16'    = [int16]
    'Int32'    = [int32]
    'Int64'    = [int64]
    'UInt32'   = [uint32]
    'UInt64'   = [uint64]
    'Single'   = [single]
    'Double'   = [double]
    'Decimal'  = [decimal]
    'DateTime' = [datetime]
    'String'   = [string]
}

function Get-WwiPackageParameterDeclaration {
    <#
        The parameters a deployed package declares, from catalog.object_parameters:
        parameter name -> its declared CLR data type and sensitivity.

        object_type 30 is a package parameter and 20 a project parameter; only
        the package's own are settable per execution with object_type 30.
    #>
    param(
        [Parameter(Mandatory)][string] $Folder,
        [Parameter(Mandatory)][string] $Project,
        [Parameter(Mandatory)][string] $Package,
        [switch] $Integrated
    )

    $rows = Invoke-WwiSqlQuery -Database 'SSISDB' -Integrated:$Integrated -Parameters @{
        folder = $Folder; project = $Project; package = $Package
    } -Query @'
SELECT o.parameter_name, o.data_type, o.sensitive, o.required
FROM catalog.object_parameters AS o
INNER JOIN catalog.projects AS p ON p.project_id = o.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @folder AND p.name = @project
  AND o.object_type = 30 AND o.object_name = @package;
'@

    $declared = @{}
    foreach ($row in $rows) {
        $declared[[string] $row.parameter_name] = [pscustomobject] @{
            DataType  = [string] $row.data_type
            Sensitive = [bool] $row.sensitive
        }
    }
    return $declared
}

function ConvertTo-WwiSsisParameterValue {
    <#
        One execution parameter value in the CLR type the package declared.

        catalog.set_execution_parameter_value takes a sql_variant, and the
        catalog compares the base type of what it is handed against the
        parameter's declared type rather than converting it. A value sent as a
        string against an Int32 parameter is refused with "The data type of the
        input value is not compatible with the data type of the 'Int32'", and
        because create_execution has already committed by then the execution is
        left stranded in status 1 with no operation message to explain it. So
        the client converts first, and a value that cannot be converted is an
        error before an execution exists at all.
    #>
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][AllowNull()] $Value,
        [Parameter(Mandatory)][string] $DataType
    )

    $key = ($DataType -replace '^System\.', '')
    if (-not $script:WwiSsisParameterClrType.Contains($key)) {
        Stop-WwiWithError ("package parameter $Name is declared as '$DataType', which the runner has no " +
            'binding for; add it to $script:WwiSsisParameterClrType in deployment/lib/SsisCatalog.ps1.')
    }
    if ($null -eq $Value) { return [DBNull]::Value }

    $target = $script:WwiSsisParameterClrType[$key]
    if ($Value.GetType() -eq $target) { return $Value }
    try {
        return [System.Convert]::ChangeType($Value, $target, [System.Globalization.CultureInfo]::InvariantCulture)
    }
    catch {
        Stop-WwiWithError ("package parameter $Name is declared as $key but was given " +
            "'$Value' ($($Value.GetType().Name)), which does not convert: $($_.Exception.Message)")
    }
}

function ConvertTo-WwiExecutionParameter {
    <#
        Every supplied value converted to its declared type, checked against the
        deployed package rather than against what the caller believes it takes.
        A name the package does not declare is an error too: the catalog would
        refuse it after create_execution had already committed. A sensitive
        parameter is never set from here: those are bound through the SSISDB
        environment, and a runner argument would put the value in plain sight.
    #>
    param(
        [Parameter(Mandatory)][hashtable] $Declared,
        [Parameter(Mandatory)] $Parameters,
        [Parameter(Mandatory)][string] $Package
    )

    $bound = [ordered] @{}
    foreach ($name in $Parameters.Keys) {
        if (-not $Declared.ContainsKey($name)) {
            Stop-WwiWithError ("$Package does not declare a package parameter named $name; the deployed " +
                'project declares: ' + (($Declared.Keys | Sort-Object) -join ', '))
        }
        if ($Declared[$name].Sensitive) {
            Stop-WwiWithError ("$Package declares $name as a sensitive parameter; it must come from the " +
                'SSISDB environment binding, not from a runner argument.')
        }
        $bound[$name] = ConvertTo-WwiSsisParameterValue -Name $name -Value $Parameters[$name] `
                                                        -DataType $Declared[$name].DataType
    }
    return $bound
}

function Get-WwiLandingZoneDirectory {
    <#
        Every directory the landing zone contract promises, resolved against a
        root. The manifest is generated from config/landing-zone.yaml, so this
        stays in step with the feeds without a second list to maintain.
    #>
    param(
        [Parameter(Mandatory)][string] $LandingRoot,
        [string] $ManifestPath
    )

    if ([string]::IsNullOrWhiteSpace($ManifestPath)) {
        $ManifestPath = Join-Path (Get-WwiRepositoryRoot) 'deployment\preflight\feed-manifest.json'
    }
    if (-not (Test-Path $ManifestPath)) {
        Stop-WwiWithError "the feed manifest is missing: $ManifestPath"
    }

    $manifest = Get-Content -Raw -Path $ManifestPath | ConvertFrom-Json
    $relative = @()
    $relative += $manifest.top_level_directories
    $relative += $manifest.working_directories | ForEach-Object { $_.relative_path }
    $relative += $manifest.feeds | ForEach-Object { $_.relative_path }

    $directories = @($LandingRoot)
    foreach ($path in ($relative | Sort-Object -Unique)) {
        $directories += (Join-Path $LandingRoot ($path -replace '/', '\'))
    }
    return , $directories
}

function Assert-WwiExecutionHostLandingZone {
    <#
        The file ingestion packages enumerate a directory on the catalog host.
        A missing directory is not an error there - a Foreach loop over nothing
        succeeds, loads nothing, and the batch reconciles to zero rows - so the
        landing zone is proved to exist before a batch is opened, from the
        instance's own view of its file system rather than from the client's.
    #>
    param(
        [Parameter(Mandatory)][string] $LandingRoot,
        [string] $ManifestPath,
        [switch] $Integrated
    )

    $directories = Get-WwiLandingZoneDirectory -LandingRoot $LandingRoot -ManifestPath $ManifestPath

    $batch = New-Object System.Text.StringBuilder
    [void] $batch.AppendLine('SET NOCOUNT ON;')
    [void] $batch.AppendLine('DECLARE @probe TABLE (path NVARCHAR(400), dir_exists INT);')
    [void] $batch.AppendLine('DECLARE @hit TABLE (file_exists INT, dir_exists INT, parent_exists INT);')
    foreach ($directory in $directories) {
        $literal = $directory.Replace("'", "''")
        [void] $batch.AppendLine('DELETE FROM @hit;')
        [void] $batch.AppendLine("INSERT INTO @hit EXEC master.dbo.xp_fileexist N'$literal';")
        [void] $batch.AppendLine("INSERT INTO @probe SELECT N'$literal', dir_exists FROM @hit;")
    }
    [void] $batch.AppendLine('SELECT path, dir_exists FROM @probe ORDER BY path;')

    $rows = Invoke-WwiSqlQuery -Database 'master' -Integrated:$Integrated -Query $batch.ToString()
    $missing = @($rows | Where-Object { $_.dir_exists -ne 1 } | ForEach-Object { $_.path })
    if ($missing.Count -gt 0) {
        Stop-WwiWithError ("the catalog host cannot see $($missing.Count) of $($directories.Count) " +
            "landing zone directories under $LandingRoot - the file feeds would enumerate nothing and " +
            "report success. Stage the landing zone first " +
            "(deployment/files/Stage-LandingZoneRemote.ps1). Missing: " + ($missing -join ', '))
    }
    Write-WwiLog "landing zone verified on the catalog host: $($directories.Count) directories under $LandingRoot"
    return , $directories
}
