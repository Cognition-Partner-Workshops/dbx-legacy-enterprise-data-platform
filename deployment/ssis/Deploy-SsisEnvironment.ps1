<#
    Creates the SSIS catalogue environment for one estate environment and binds
    every project parameter to it.

    The SQL it runs is generated from config/environments/<env>.env.yaml by
    deployment/ssis/render_environment_sql.py and committed under
    deployment/ssis/environments/. This script does not regenerate it - the
    Windows deploy hosts do not all have Python - so if the YAML changed,
    re-render and commit before deploying. -Verify compares the two by
    timestamp and warns.

    Sensitive parameter values (OraclePassword, SqlServerPassword) are passed to
    sqlcmd as variables sourced from ORACLE_PASSWORD and SQLSERVER_PASSWORD.
    They are never written to disk by this script.

    This script has not been executed against any SSIS catalogue.

    Usage: .\deployment\ssis\Deploy-SsisEnvironment.ps1 [-DryRun] [-Verify]
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [switch] $Verify
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-ssis-env'

Assert-WwiEnvironmentVariable @('SSIS_SERVER', 'ORACLE_PASSWORD', 'SQLSERVER_PASSWORD')
Confirm-WwiProduction

$repoRoot        = Get-WwiRepositoryRoot
$environmentCode = Get-WwiEnvironmentCode
$lower           = $environmentCode.ToLower()

$scriptPath = Join-Path $repoRoot (Join-Path 'deployment/ssis/environments' "$($lower)_environment.sql")
$yamlPath   = Join-Path $repoRoot (Join-Path 'config/environments' "$lower.env.yaml")

if (-not (Test-Path $scriptPath)) {
    Stop-WwiWithError "$scriptPath is missing. Run: python3 deployment/ssis/render_environment_sql.py --all"
}

if ($Verify -and (Test-Path $yamlPath)) {
    $yamlStamp = (Get-Item $yamlPath).LastWriteTimeUtc
    $sqlStamp  = (Get-Item $scriptPath).LastWriteTimeUtc
    if ($yamlStamp -gt $sqlStamp) {
        Write-WwiLog "$yamlPath is newer than the rendered SQL; re-render before deploying." 'WARN'
    }
}

$arguments = @(
    '-S', $env:SSIS_SERVER, '-b', '-I', '-X1', '-j',
    '-v', "OraclePassword=$($env:ORACLE_PASSWORD)",
    '-v', "SqlServerPassword=$($env:SQLSERVER_PASSWORD)",
    '-i', $scriptPath
)

if (-not [string]::IsNullOrWhiteSpace($env:SQLSERVER_USER)) {
    $env:SQLCMDPASSWORD = $env:SQLSERVER_PASSWORD
    $arguments = @('-U', $env:SQLSERVER_USER) + $arguments
}

if ($DryRun) {
    # The secret-bearing -v arguments are deliberately not echoed.
    Write-WwiLog "WHATIF sqlcmd -S $($env:SSIS_SERVER) -i deployment/ssis/environments/$($lower)_environment.sql"
    Write-WwiLog 'WHATIF sensitive parameters would be supplied from ORACLE_PASSWORD and SQLSERVER_PASSWORD'
    return
}

Write-WwiLog "applying SSIS environment for $environmentCode"
& sqlcmd @arguments
if ($LASTEXITCODE -ne 0) {
    Stop-WwiWithError "the $environmentCode SSIS environment script failed (exit code $LASTEXITCODE)."
}

Write-WwiLog "SSIS environment step finished for $environmentCode"
