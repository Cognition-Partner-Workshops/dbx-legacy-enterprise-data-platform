<#
    Creates the SSIS catalogue environment for one estate environment and binds
    every project parameter to it.

    The SQL it runs is generated from config/environments/<env>.env.yaml by
    deployment/ssis/render_environment_sql.py and committed under
    deployment/ssis/environments/. This script does not regenerate it - the
    Windows deploy hosts do not all have Python - so if the YAML changed,
    re-render and commit before deploying. -Verify compares the two by
    timestamp and warns.

    Every environment value is substituted into the script in memory, so the
    committed SQL contains no host, account or credential value at all.
    Substitution is done here rather than by sqlcmd because the ODBC sqlcmd
    rejects a -v value containing a colon, which every Windows file root has.
    The mapping of token -> environment variable -> default is rendered alongside the
    SQL as <env>_environment.vars.psd1; this script reads it, takes the process
    environment where it is set and the default otherwise, and fails if a Secret
    entry has no environment variable behind it.

    The two secrets (OraclePassword, SqlServerPassword) are bound in the catalog
    to the four CM.<connection>.Password project parameters. They are never
    written to disk, echoed, or logged here.

    SQLSERVER_USER is the account the catalog binds, which is not necessarily
    the account sqlcmd connects as: the catalog environment procedures reject
    SQL authentication outright, so on the catalog's own host the connection is
    integrated and -IntegratedSecurity keeps the bound value out of the login.

    Usage: .\deployment\ssis\Deploy-SsisEnvironment.ps1 [-DryRun] [-Verify]
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [switch] $Verify,
    [switch] $IntegratedSecurity
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'SsisCatalog.ps1')
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

$varsPath = Join-Path $repoRoot (Join-Path 'deployment/ssis/environments' "$($lower)_environment.vars.psd1")
if (-not (Test-Path $varsPath)) {
    Stop-WwiWithError "$varsPath is missing. Run: python3 deployment/ssis/render_environment_sql.py --all"
}

$variableMap = Import-PowerShellDataFile -Path $varsPath
$values      = [ordered] @{}
$resolved    = [ordered] @{}

foreach ($name in $variableMap.Keys) {
    $entry  = $variableMap[$name]
    $value  = [Environment]::GetEnvironmentVariable($entry.EnvVar)
    $source = $entry.EnvVar
    if ([string]::IsNullOrWhiteSpace($value)) {
        if ($entry.Secret) {
            Stop-WwiWithError "$($entry.EnvVar) is not set; it supplies the sensitive environment variable $name."
        }
        $value  = [string] $entry.Default
        $source = "$($entry.EnvVar) (unset, using the rendered default)"
    }
    $values[$name] = $value
    # Only the source is recorded. A secret's value never reaches the log.
    $resolved[$name] = if ($entry.Secret) { $entry.EnvVar } else { $source }
}

if ($DryRun) {
    # Sources only: a secret's value is never echoed, not even under -DryRun.
    Write-WwiLog "WHATIF apply deployment/ssis/environments/$($lower)_environment.sql to $($env:SSIS_SERVER)"
    foreach ($name in $resolved.Keys) {
        Write-WwiLog "WHATIF $name <- $($resolved[$name])"
    }
    return
}

$sqlText = [System.IO.File]::ReadAllText($scriptPath)
foreach ($name in $values.Keys) {
    # Every token sits inside a SQL string literal, so a quote in a value is
    # doubled rather than left to break out of it.
    $sqlText = $sqlText.Replace("`$($name)", ([string] $values[$name]).Replace("'", "''"))
}

$unresolved = [regex]::Matches($sqlText, '\$\((?<name>[A-Za-z0-9_]+)\)') |
    ForEach-Object { $_.Groups['name'].Value } | Sort-Object -Unique
if ($unresolved) {
    Stop-WwiWithError ("the rendered SQL still references {0}; re-render the environment." -f ($unresolved -join ', '))
}

$batches = [regex]::Split($sqlText, '(?im)^\s*GO\s*$') | Where-Object { $_.Trim() }

Write-WwiLog "applying SSIS environment for $environmentCode ($($batches.Count) batch(es))"
$connectionString = Get-WwiSsisConnectionString -Database 'SSISDB' -ForceIntegrated:$IntegratedSecurity
$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
try {
    $connection.Open()
    $index = 0
    foreach ($batch in $batches) {
        $index++
        $command = $connection.CreateCommand()
        $command.CommandText = $batch
        $command.CommandTimeout = 300
        try { $command.ExecuteNonQuery() | Out-Null }
        catch {
            # The batch text can hold a substituted secret, so only its position
            # and the server's own message are reported.
            Stop-WwiWithError ("batch {0} of the {1} environment script failed: {2}" -f `
                $index, $environmentCode, $_.Exception.Message)
        }
    }
}
finally {
    $connection.Dispose()
}

Write-WwiLog "SSIS environment step finished for $environmentCode"
