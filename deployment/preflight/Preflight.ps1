<#
    Preflight checks for a WWI estate deployment (Windows deploy host).

    The Windows variant is the one the platform team actually uses, because the
    SSIS stage can only run from a host with the Integration Services tooling
    installed. It checks tooling, environment variables, repository layout, the
    per-environment configuration file and the landing-zone directories.

    It does not open a connection to Oracle, SQL Server or the SSIS catalogue.
    Nothing in deployment/ has been executed against a server.

    Usage: .\deployment\preflight\Preflight.ps1 [-Stage oracle|sqlserver|ssis|all]
#>

[CmdletBinding()]
param(
    [ValidateSet('oracle', 'sqlserver', 'ssis', 'all')]
    [string] $Stage = 'all'
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-preflight'

$repoRoot = Get-WwiRepositoryRoot
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

Write-Host 'Repository layout'
Add-Check 'docs/ESTATE_BUILD_CONTRACT.md present' { Test-RepoPath 'docs/ESTATE_BUILD_CONTRACT.md' }
Add-Check 'config/estate-catalog.yaml present'    { Test-RepoPath 'config/estate-catalog.yaml' }
Add-Check 'sqlserver/control present'             { Test-RepoPath 'sqlserver/control' }
Add-Check 'sqlserver/agent present'               { Test-RepoPath 'sqlserver/agent' }
Add-Check 'config/.env.example present'           { Test-RepoPath 'config/.env.example' }
Add-Check "config/environments/$($environmentCode.ToLower()).env.yaml present" {
    Test-RepoPath ("config/environments/{0}.env.yaml" -f $environmentCode.ToLower())
}

if ($Stage -in @('all', 'oracle')) {
    Write-Host 'Oracle stage'
    Add-Check 'sqlplus or sql (sqlcl) on PATH' {
        (Get-Command sqlplus -ErrorAction SilentlyContinue) -or (Get-Command sql -ErrorAction SilentlyContinue)
    }
    foreach ($v in @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_USER', 'ORACLE_PASSWORD')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    # oracle/ddl/03_create_schemas.sql substitutes one per schema account.
    foreach ($v in @('WWI_MDM_SECRET', 'WWI_PROC_SECRET', 'WWI_FIN_SECRET',
                     'WWI_REF_SECRET', 'WWI_AUDIT_SECRET', 'WWI_EXTRACT_SECRET')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    Add-Check 'oracle/ source tree present' { Test-RepoPath 'oracle' }
}

if ($Stage -in @('all', 'sqlserver')) {
    Write-Host 'SQL Server stage'
    Add-Check 'sqlcmd on PATH' { [bool] (Get-Command sqlcmd -ErrorAction SilentlyContinue) }
    foreach ($v in @('SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
                     'SQLSERVER_OLTP_DB', 'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    Add-Check 'sqlserver/control/ scripts present'  { Test-RepoPath 'sqlserver/control' }
    Add-Check 'sqlserver/security/ scripts present' { Test-RepoPath 'sqlserver/security' }
}

if ($Stage -in @('all', 'ssis')) {
    Write-Host 'SSIS stage'
    Add-Check 'ISDeploymentWizard.exe or dtutil.exe on PATH' {
        (Get-Command ISDeploymentWizard.exe -ErrorAction SilentlyContinue) -or
        (Get-Command dtutil.exe -ErrorAction SilentlyContinue) -or
        ($env:WWI_ALLOW_MISSING_SSIS_TOOLS -eq '1')
    }
    Add-Check 'SqlServer PowerShell module available' {
        [bool] (Get-Module -ListAvailable -Name SqlServer)
    }
    foreach ($v in @('SSIS_SERVER', 'SSIS_FOLDER')) {
        Add-Check "$v is set" ([scriptblock]::Create("Test-EnvVar '$v'"))
    }
    Add-Check 'ssis/ project tree present' { Test-RepoPath 'ssis' }
    Add-Check 'ssis/ contains at least one .dtproj' {
        [bool] (Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtproj' -Recurse -File |
                Where-Object { $_.FullName -notmatch '\\obj\\' } | Select-Object -First 1)
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

$failed = @($results | Where-Object { -not $_.Passed })
Write-Host ''
Write-WwiLog ("{0} checks, {1} failures" -f $results.Count, $failed.Count)
if ($failed.Count -gt 0) {
    Stop-WwiWithError 'preflight failed. Fix the items above before running deploy-all.'
}
Write-WwiLog 'preflight passed. Connectivity itself is not checked here.'
