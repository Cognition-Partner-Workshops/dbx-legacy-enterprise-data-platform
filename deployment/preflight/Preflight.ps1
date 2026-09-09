<#
    Preflight checks for a WWI estate deployment (Windows deploy host).

    The Windows variant is the one the platform team actually uses, because the
    SSIS stage can only run from a host with the Integration Services tooling
    installed. It checks tooling, environment variables, repository layout, the
    per-environment configuration file and the landing-zone directories.

    -Connectivity additionally proves, from the host it runs on, that Oracle and
    SQL Server answer, that WWI_EXTRACT authenticates, and that each feed in
    deployment/preflight/feed-manifest.json is present under the landing root.
    Those checks are read-only: they SELECT, they count files, and they never
    write, create or move anything. Run them on the execution host - the host
    the SSIS catalogue spawns ISServerExec on - because a deploy workstation
    passing them says nothing about what the catalogue can reach.

    -ExecutionHost drops the checks that describe a deploy workstation - the
    repository layout and the authoring tools - and keeps the ones that describe
    the machine the packages run on: the providers, the two databases, the
    WWI_EXTRACT login, the landing root and the seven feeds. The execution host
    holds no checkout, so asking it for one only produces noise.

    Usage: .\deployment\preflight\Preflight.ps1 [-Stage oracle|sqlserver|ssis|all] [-Connectivity]
#>

[CmdletBinding()]
param(
    [ValidateSet('oracle', 'sqlserver', 'ssis', 'all')]
    [string] $Stage = 'all',
    [switch] $Connectivity,
    [switch] $ExecutionHost
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-preflight'

$repoRoot = if ($ExecutionHost) { $null } else { Get-WwiRepositoryRoot }
$results  = [System.Collections.Generic.List[object]]::new()

function Add-Check {
    param([string] $Description, [scriptblock] $Test)
    $ok = $false
    try { $ok = [bool] (& $Test) } catch { $ok = $false }
    $results.Add([pscustomobject]@{ Check = $Description; Passed = $ok })
    $label = if ($ok) { 'PASS' } else { 'FAIL' }
    Write-Host ("  {0}  {1}" -f $label, $Description)
}

function Test-EnvVar { param([string] $Name) -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name)) }
function Test-RepoPath { param([string] $Relative) Test-Path (Join-Path $repoRoot $Relative) }

$environmentCode = Get-WwiEnvironmentCode
Write-WwiLog "environment $environmentCode, stage $Stage"

if ($ExecutionHost) {
    Write-Host 'Execution host mode: repository layout and authoring tool checks are skipped'
}

if (-not $ExecutionHost) {
    Write-Host 'Repository layout'
    Add-Check 'docs/ESTATE_BUILD_CONTRACT.md present' { Test-RepoPath 'docs/ESTATE_BUILD_CONTRACT.md' }
    Add-Check 'config/estate-catalog.yaml present'    { Test-RepoPath 'config/estate-catalog.yaml' }
    Add-Check 'sqlserver/control present'             { Test-RepoPath 'sqlserver/control' }
    Add-Check 'sqlserver/agent present'               { Test-RepoPath 'sqlserver/agent' }
    Add-Check 'config/.env.example present'           { Test-RepoPath 'config/.env.example' }
    Add-Check "config/environments/$($environmentCode.ToLower()).env.yaml present" {
        Test-RepoPath ("config/environments/{0}.env.yaml" -f $environmentCode.ToLower())
    }
}

if ($Stage -in @('all', 'oracle')) {
    Write-Host 'Oracle stage'
    if (-not $ExecutionHost) {
        Add-Check 'sqlplus or sql (sqlcl) on PATH' {
            (Get-Command sqlplus -ErrorAction SilentlyContinue) -or (Get-Command sql -ErrorAction SilentlyContinue)
        }
    }
    foreach ($v in @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_USER', 'ORACLE_PASSWORD')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    # oracle/ddl/03_create_schemas.sql substitutes one per schema account.
    if (-not $ExecutionHost) {
        foreach ($v in @('WWI_MDM_SECRET', 'WWI_PROC_SECRET', 'WWI_FIN_SECRET',
                         'WWI_REF_SECRET', 'WWI_AUDIT_SECRET', 'WWI_EXTRACT_SECRET')) {
            Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
        }
        Add-Check 'oracle/ source tree present' { Test-RepoPath 'oracle' }
    }
}

if ($Stage -in @('all', 'sqlserver')) {
    Write-Host 'SQL Server stage'
    Add-Check 'sqlcmd on PATH' { [bool] (Get-Command sqlcmd -ErrorAction SilentlyContinue) }
    foreach ($v in @('SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
                     'SQLSERVER_OLTP_DB', 'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    if (-not $ExecutionHost) {
        Add-Check 'sqlserver/control/ scripts present'  { Test-RepoPath 'sqlserver/control' }
        Add-Check 'sqlserver/security/ scripts present' { Test-RepoPath 'sqlserver/security' }
    }
}

if ($Stage -in @('all', 'ssis')) {
    Write-Host 'SSIS stage'
    if ($ExecutionHost) {
        # What matters here is that the catalogue's own runtime is installed,
        # not that the authoring tools are.
        Add-Check 'ISServerExec.exe present (Integration Services runtime)' {
            [bool] (Get-ChildItem 'C:\Program Files\Microsoft SQL Server' -Filter 'ISServerExec.exe' `
                        -Recurse -File -ErrorAction SilentlyContinue | Select-Object -First 1)
        }
    }
    else {
        Add-Check 'ISDeploymentWizard.exe or dtutil.exe on PATH' {
            (Get-Command ISDeploymentWizard.exe -ErrorAction SilentlyContinue) -or
            (Get-Command dtutil.exe -ErrorAction SilentlyContinue) -or
            ($env:WWI_ALLOW_MISSING_SSIS_TOOLS -eq '1')
        }
        Add-Check 'SqlServer PowerShell module available' {
            [bool] (Get-Module -ListAvailable -Name SqlServer)
        }
    }
    foreach ($v in @('SSIS_SERVER', 'SSIS_FOLDER')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    if (-not $ExecutionHost) {
        Add-Check 'ssis/ project tree present' { Test-RepoPath 'ssis' }
        Add-Check 'ssis/ contains at least one .dtproj' {
            [bool] (Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtproj' -Recurse -File |
                    Where-Object { $_.FullName -notmatch '\\obj\\' } | Select-Object -First 1)
        }
    }
}

Write-Host 'Landing zone'
if (-not [string]::IsNullOrWhiteSpace($env:WWI_LANDING_ROOT)) {
    foreach ($sub in @('inbound', 'archive', 'quarantine', 'work', 'outbound')) {
        Add-Check "landing zone $sub directory exists" ([scriptblock]::Create("Test-Path (Join-Path '$($env:WWI_LANDING_ROOT)' '$sub')"))
    }
} else {
    Write-Host '  SKIP  landing zone checks (WWI_LANDING_ROOT not set)'
}

if ($Connectivity) {
    Write-Host 'Connectivity (read-only, from this host)'

    if ($Stage -in @('all', 'sqlserver', 'ssis')) {
        Add-Check 'SQL Server answers and the login authenticates' {
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
            $builder['Data Source']     = '{0},{1}' -f $env:SQLSERVER_HOST, $env:SQLSERVER_PORT
            $builder['Initial Catalog'] = 'master'
            $builder['User ID']         = $env:SQLSERVER_USER
            $builder['Password']        = $env:SQLSERVER_PASSWORD
            $builder['TrustServerCertificate'] = $true
            $builder['Connect Timeout'] = 15
            $connection = New-Object System.Data.SqlClient.SqlConnection $builder.ConnectionString
            try {
                $connection.Open()
                $command = $connection.CreateCommand()
                $command.CommandText = 'SELECT SERVERPROPERTY(''ProductVersion'');'
                $version = [string] $command.ExecuteScalar()
                Write-Host "        SQL Server $version"
                -not [string]::IsNullOrWhiteSpace($version)
            }
            finally { $connection.Dispose() }
        }

        Add-Check 'MSOLEDBSQL 19 OLE DB provider registered' {
            [bool] (Get-ChildItem 'HKLM:\SOFTWARE\Classes\MSOLEDBSQL19' -ErrorAction SilentlyContinue)
        }
    }

    if ($Stage -in @('all', 'oracle', 'ssis')) {
        Add-Check 'Oracle OLE DB provider registered' {
            [bool] (Get-ChildItem 'HKLM:\SOFTWARE\Classes\OraOLEDB.Oracle' -ErrorAction SilentlyContinue)
        }

        Add-Check 'Oracle answers and WWI_EXTRACT authenticates' {
            # Easy Connect through the same provider the packages use, so a pass
            # here is evidence for the catalogue, not just for the network.
            $source = '{0}:{1}/{2}' -f $env:ORACLE_HOST, $env:ORACLE_PORT, $env:ORACLE_SERVICE
            $connectionString = 'Provider=OraOLEDB.Oracle;Data Source={0};User Id={1};Password={2};' -f
                $source, $env:ORACLE_USER, $env:ORACLE_PASSWORD
            $connection = New-Object System.Data.OleDb.OleDbConnection $connectionString
            try {
                $connection.Open()
                $command = $connection.CreateCommand()
                $command.CommandText = 'SELECT USER FROM DUAL'
                $who = [string] $command.ExecuteScalar()
                Write-Host "        Oracle session user $who"
                $who -eq $env:ORACLE_USER.ToUpperInvariant()
            }
            finally { $connection.Dispose() }
        }
    }

    $manifestPath = Join-Path $PSScriptRoot 'feed-manifest.json'
    if (-not (Test-Path $manifestPath)) {
        Add-Check 'feed-manifest.json present' { $false }
    }
    elseif ([string]::IsNullOrWhiteSpace($env:WWI_LANDING_ROOT)) {
        Add-Check 'WWI_LANDING_ROOT is set (required for the feed checks)' { $false }
    }
    else {
        $manifest = Get-Content -Raw $manifestPath | ConvertFrom-Json
        foreach ($feed in $manifest.feeds) {
            $directory = Join-Path $env:WWI_LANDING_ROOT ($feed.relative_path -replace '/', '\')
            $spec = $feed.file_spec
            $name = $feed.feed
            Add-Check "feed $name present in $($feed.relative_path)" ([scriptblock]::Create(@"
`$files = @(Get-ChildItem -Path '$directory' -Filter '$spec' -File -ErrorAction SilentlyContinue)
if (`$files.Count -eq 0) { `$false }
else {
    `$rows = 0
    foreach (`$file in `$files) { `$rows += @(Get-Content -LiteralPath `$file.FullName).Count }
    Write-Host ('        {0}: {1} file(s), {2} line(s)' -f '$name', `$files.Count, `$rows)
    `$true
}
"@))
        }
    }
}

$failed = @($results | Where-Object { -not $_.Passed })
Write-Host ''
Write-WwiLog ("{0} checks, {1} failures" -f $results.Count, $failed.Count)
if ($failed.Count -gt 0) {
    Stop-WwiWithError 'preflight failed. Fix the items above before running deploy-all.'
}
Write-WwiLog 'preflight passed. Connectivity itself is not checked here.'
