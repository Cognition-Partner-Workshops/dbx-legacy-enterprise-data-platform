<#
    SQL Server stage driver (Windows deploy host).

    Deploys the SQL Server side of the estate in dependency order:

        1. control     etl.* framework, deployed into staging and the warehouse
        2. oltp        extract views and change-tracking extensions
        3. staging     raw/work/stg/err/ref schemas and load procedures
        4. reference   shared reference data loaded into staging
        5. warehouse   dimensions, facts, aggregates, load procedures, views
        6. security    roles, principals and grants
        7. agent       msdb job definitions

    Connection details come from SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER,
    SQLSERVER_PASSWORD, SQLSERVER_OLTP_DB, SQLSERVER_STAGING_DB and
    SQLSERVER_DW_DB. The secret is handed to sqlcmd through the SQLCMDPASSWORD
    variable so it never appears on a command line.

    This script has not been executed against any SQL Server instance.

    Usage: .\deployment\sqlserver\Deploy-SqlServer.ps1 [-DryRun] [-Stage staging] [-FromStage 3]
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [ValidateSet('control', 'oltp', 'staging', 'reference', 'warehouse', 'security', 'agent')]
    [string] $Stage,
    [int]    $FromStage = 1
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-sqlserver'

Assert-WwiEnvironmentVariable @('SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
                                'SQLSERVER_OLTP_DB', 'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB')
Confirm-WwiProduction
Assert-WwiTool 'sqlcmd' 'Install the SQL Server command-line utilities on the deploy host.'

$repoRoot        = Get-WwiRepositoryRoot
$environmentCode = Get-WwiEnvironmentCode
$serverInstance  = "$($env:SQLSERVER_HOST),$($env:SQLSERVER_PORT)"

# sqlcmd picks the secret up from here rather than from -P. Note that -X
# makes sqlcmd ignore SQLCMDPASSWORD, so it must not be passed below.
$env:SQLCMDPASSWORD = $env:SQLSERVER_PASSWORD

function Get-DefaultedEnv {
    param([string] $Name, [string] $Default = '')
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) { return $Default }
    return $value
}


$sqlcmdVariables = [ordered]@{
    OltpDatabase         = $env:SQLSERVER_OLTP_DB
    StagingDatabase      = $env:SQLSERVER_STAGING_DB
    DwDatabase           = $env:SQLSERVER_DW_DB
    EnvironmentCode      = $environmentCode
    DomainPrefix         = (Get-DefaultedEnv 'WWI_DOMAIN_PREFIX' 'CONTOSO')
    EtlServiceAccount    = (Get-DefaultedEnv 'WWI_ETL_SERVICE_ACCOUNT' 'svc-wwi-etl')
    AppServiceAccount    = (Get-DefaultedEnv 'WWI_APP_SERVICE_ACCOUNT' 'svc-wwi-app')
    ReportServiceAccount = (Get-DefaultedEnv 'WWI_REPORT_SERVICE_ACCOUNT' 'svc-wwi-report')
    SqlLoginSecret       = $env:SQLSERVER_PASSWORD
    OracleHost           = (Get-DefaultedEnv 'ORACLE_HOST')
    OraclePort           = (Get-DefaultedEnv 'ORACLE_PORT')
    OracleService        = (Get-DefaultedEnv 'ORACLE_SERVICE')
    OracleUser           = (Get-DefaultedEnv 'ORACLE_USER')
    OracleLinkSecret     = (Get-DefaultedEnv 'ORACLE_PASSWORD')
    SsisServer           = (Get-DefaultedEnv 'SSIS_SERVER' $env:SQLSERVER_HOST)
    SsisFolder           = (Get-DefaultedEnv 'SSIS_FOLDER' "WWI_$environmentCode")
    SsisProxyAccount     = (Get-DefaultedEnv 'WWI_SSIS_PROXY_ACCOUNT' 'svc-wwi-etl')
    FileProxyAccount     = (Get-DefaultedEnv 'WWI_FILE_PROXY_ACCOUNT' 'svc-wwi-files')
    SsisProxySecret      = (Get-DefaultedEnv 'WWI_SSIS_PROXY_PASSWORD')
    FileProxySecret      = (Get-DefaultedEnv 'WWI_FILE_PROXY_PASSWORD')
    AgentLogRoot         = (Get-DefaultedEnv 'WWI_AGENT_LOG_ROOT' 'D:\WWI\Logs\Agent')
    InboundFileRoot      = (Get-DefaultedEnv 'ETL_INBOUND_FILE_ROOT' 'D:\WWI\inbound')
    QuarantineFileRoot   = (Get-DefaultedEnv 'ETL_REJECT_FILE_ROOT' 'D:\WWI\reject')
    EtlOperatorEmail     = (Get-DefaultedEnv 'WWI_ETL_OPERATOR_EMAIL' 'wwi-etl-oncall@example.internal')
    DbaOperatorEmail     = (Get-DefaultedEnv 'WWI_DBA_OPERATOR_EMAIL' 'wwi-dba@example.internal')
    FinanceOperatorEmail = (Get-DefaultedEnv 'WWI_FINANCE_OPERATOR_EMAIL' 'wwi-finance-systems@example.internal')
}

function Get-SqlcmdVariableArguments {
    $args = @()
    foreach ($key in $sqlcmdVariables.Keys) {
        # sqlcmd rejects "-v Name=" outright, so a variable with no value has to
        # be left out; a script that needs it fails on the reference instead.
        if ([string]::IsNullOrWhiteSpace($sqlcmdVariables[$key])) { continue }
        $args += '-v'
        $args += ("{0}={1}" -f $key, $sqlcmdVariables[$key])
    }
    return $args
}

function Invoke-SqlScript {
    param([Parameter(Mandatory)][string] $Database, [Parameter(Mandatory)][string] $Path)

    $relative = $Path.Substring($repoRoot.Length + 1)
    if ($DryRun) {
        Write-WwiLog "WHATIF sqlcmd -S $serverInstance -d $Database -i $relative"
        return
    }

    Write-WwiLog "RUN    [$Database] $relative"
    $arguments = @('-S', $serverInstance, '-U', $env:SQLSERVER_USER, '-d', $Database,
                   '-b', '-I', '-j') + (Get-SqlcmdVariableArguments) + @('-i', $Path)
    & sqlcmd @arguments
    if ($LASTEXITCODE -ne 0) {
        Stop-WwiWithError "$relative failed against $Database (exit code $LASTEXITCODE)."
    }
}

function Invoke-SqlDirectory {
    param([Parameter(Mandatory)][string] $Database, [Parameter(Mandatory)][string] $Relative)

    $directory = Join-Path $repoRoot $Relative
    if (-not (Test-Path $directory)) {
        Write-WwiLog "$Relative is not present; skipping." 'WARN'
        return
    }
    Get-ChildItem -Path $directory -Filter '*.sql' -Recurse -File |
        Where-Object { $_.Name -ne '90_install_all_agent_jobs.sql' } |
        Sort-Object FullName |
        ForEach-Object { Invoke-SqlScript -Database $Database -Path $_.FullName }
}

$stageActions = [ordered]@{
    control = {
        Write-WwiLog '--- stage 1: control framework ---'
        Invoke-SqlDirectory -Database $env:SQLSERVER_STAGING_DB -Relative 'sqlserver/control'
        # Deployed into the warehouse as well: it keeps its own etl.* copy so a
        # staging outage cannot block a close reconciliation.
        Invoke-SqlDirectory -Database $env:SQLSERVER_DW_DB -Relative 'sqlserver/control'
    }
    oltp = {
        Write-WwiLog '--- stage 2: OLTP extensions ---'
        Invoke-SqlDirectory -Database $env:SQLSERVER_OLTP_DB -Relative 'sqlserver/oltp'
    }
    staging = {
        Write-WwiLog '--- stage 3: staging ---'
        Invoke-SqlDirectory -Database $env:SQLSERVER_STAGING_DB -Relative 'sqlserver/staging'
    }
    reference = {
        Write-WwiLog '--- stage 4: reference data ---'
        Invoke-SqlDirectory -Database $env:SQLSERVER_STAGING_DB -Relative 'sqlserver/reference'
    }
    warehouse = {
        Write-WwiLog '--- stage 5: warehouse ---'
        foreach ($relative in @('sqlserver/warehouse/dimensions', 'sqlserver/warehouse/facts',
                                'sqlserver/warehouse/aggregates', 'sqlserver/procedures/dimensions',
                                'sqlserver/procedures/facts', 'sqlserver/views')) {
            Invoke-SqlDirectory -Database $env:SQLSERVER_DW_DB -Relative $relative
        }
    }
    security = {
        Write-WwiLog '--- stage 6: security ---'
        # Each security script issues its own USE, so they are submitted against master.
        Invoke-SqlDirectory -Database 'master' -Relative 'sqlserver/security'
    }
    agent = {
        Write-WwiLog '--- stage 7: agent ---'
        Invoke-SqlDirectory -Database 'msdb' -Relative 'sqlserver/agent'
    }
}

$stageIndex = 0
foreach ($name in $stageActions.Keys) {
    $stageIndex++
    if ($Stage -and $Stage -ne $name) { continue }
    if ($stageIndex -lt $FromStage) { continue }
    & $stageActions[$name]
}

Write-WwiLog ("sql server stage complete{0}" -f $(if ($DryRun) { ' (dry run)' } else { '' }))
