<#
    Runs an orchestration root on the host that owns the catalog.

    catalog.create_execution and catalog.start_execution refuse a caller that
    authenticated with a SQL login - "The operation cannot be started by an
    account that uses SQL Server Authentication" - exactly as
    catalog.deploy_project and the environment procedures do. The estate
    reaches the instance over the public endpoint with SQL authentication and
    this host is not domain joined, so an orchestration driven from here cannot
    create an execution at all: it fails inside create_execution, before an
    execution id exists, with nothing in catalog.operation_messages to show for
    it.

    This script therefore puts the runner where a Windows principal exists. It
    ships the deployment scripts, the orchestration plan and the feed manifest
    to the SQL Server host over SSM Run Command and invokes
    Invoke-EstateOrchestration.ps1 there against localhost, where the catalog
    connection is NTLM and the control framework still uses the SQL login.

    Nothing secret crosses SSM. The control framework's SQL login is resolved on
    the host from AWS Secrets Manager with the instance role and assigned into
    that process's environment; the package credentials are not involved at all,
    since they live in the SSISDB environment bound to the connection managers.

    Usage:
      .\deployment\ssis\Invoke-EstateOrchestrationRemote.ps1 -InstanceId i-0123456789abcdef0 `
          -Root Master_File_Ingestion
      .\deployment\ssis\Invoke-EstateOrchestrationRemote.ps1 -InstanceId i-0... `
          -Root Master_File_Ingestion -DryRun

    -AdoptRunning is forwarded to the runner and is the documented recovery
    rerun: it adopts the batch this root already has open for the business date
    instead of refusing to start a second one.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $InstanceId,
    [Parameter(Mandatory)][string] $Root,
    [string] $Region = $env:AWS_DEFAULT_REGION,
    [string] $RemoteRoot = 'C:\WWI\orchestrate',
    [string] $Folder = 'WWI_DEV',
    [string] $SsisEnvironment = 'WWI_DEV',
    [ValidateSet('DEV', 'TEST', 'PROD')][string] $Environment = 'DEV',
    [int] $LoggingLevel = 1,
    [string] $BusinessDate,
    [int] $ExecutionTimeoutSeconds = 10800,
    [string] $SqlServerSecretId = 'legacy-demo/sqlserver/devin_migration',
    [switch] $AdoptRunning,
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'RemoteHost.ps1')
$script:WwiLogPrefix = 'wwi-orchestrate-remote'

if ([string]::IsNullOrWhiteSpace($Region)) { $Region = 'us-east-2' }
Assert-WwiTool -Name 'aws' -Hint 'install the AWS CLI; SSM Run Command is the only channel to the execution host'

$script:WwiRemoteInstanceId = $InstanceId
$script:WwiRemoteRegion     = $Region
$script:WwiRemoteDryRun     = [bool] $DryRun

$repoRoot = Get-WwiRepositoryRoot
$planPath = Join-Path (Join-Path $repoRoot 'ssis') 'orchestration-plan.json'
if (-not (Test-Path $planPath)) {
    Stop-WwiWithError "orchestration plan not found at $planPath; run ssis/00_orchestration/build_orchestration_packages.py."
}
$plan = Get-Content -Raw -Path $planPath | ConvertFrom-Json
if (-not ($plan.roots | Where-Object { $_.root -eq $Root })) {
    Stop-WwiWithError ("the plan holds no root named '$Root'. Known roots: " +
                       (($plan.roots | ForEach-Object { $_.root }) -join ', '))
}

# -- 1. ship the runner, the plan and the landing zone contract --------------

$payloadFiles = @()
foreach ($relative in @('deployment\lib', 'deployment\ssis')) {
    $payloadFiles += @(Get-ChildItem -Path (Join-Path $repoRoot $relative) -Filter '*.ps1' -File)
}
$payloadFiles += Get-Item -LiteralPath $planPath
$payloadFiles += Get-Item -LiteralPath (Join-Path $repoRoot 'deployment\preflight\feed-manifest.json')

foreach ($file in $payloadFiles) {
    $relative = $file.FullName.Substring($repoRoot.Length).TrimStart('\')
    Copy-WwiFileToHost -LocalPath $file.FullName -RemotePath (Join-Path $RemoteRoot $relative)
}

# -- 2. run the orchestration there ------------------------------------------

$run = @(
    "`$ErrorActionPreference = 'Stop'",
    "`$env:SSIS_SERVER = 'localhost'",
    "`$env:SSIS_FOLDER = '$Folder'",
    "`$env:SSIS_ENVIRONMENT = '$SsisEnvironment'",
    "`$env:WWI_ENVIRONMENT = '$Environment'"
)

# Non-credential configuration is forwarded from this host so the runner talks
# to the same databases the rest of the estate does.
foreach ($name in @('SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_OLTP_DB',
                    'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB')) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if (-not [string]::IsNullOrWhiteSpace($value)) {
        $run += "`$env:$name = '$($value.Replace("'", "''"))'"
    } else {
        Write-WwiLog "$name is not set here; the remote runner will report it missing" 'WARN'
    }
}

$run += @(
    # The landing root is the host's own machine variable: it is the file system
    # the packages enumerate, and the runner proves it exists before it opens a
    # batch. Stage-LandingZoneRemote.ps1 is what sets it.
    "`$env:WWI_LANDING_ROOT = [Environment]::GetEnvironmentVariable('WWI_LANDING_ROOT', 'Machine')",
    "`$sql = (aws secretsmanager get-secret-value --region $Region --secret-id '$SqlServerSecretId' --query SecretString --output text) | ConvertFrom-Json",
    "if (-not `$sql.password) { throw 'the SQL Server secret has no password member' }",
    "`$env:SQLSERVER_USER = `$sql.username",
    "`$env:SQLSERVER_PASSWORD = `$sql.password"
)

$invocation = "& '$RemoteRoot\deployment\ssis\Invoke-EstateOrchestration.ps1' -Root '$Root'" +
              " -LoggingLevel $LoggingLevel -LogPath '$RemoteRoot\logs\orchestration'"
if ($BusinessDate)  { $invocation += " -BusinessDate '$BusinessDate'" }
if ($AdoptRunning)  { $invocation += ' -AdoptRunning' }
if ($DryRun)        { $invocation += ' -DryRun' }
$run += $invocation

$output = Invoke-WwiRemotePowerShell -ExecutionTimeoutSeconds $ExecutionTimeoutSeconds `
    -Description "orchestrate $Root on $InstanceId" -Commands $run
foreach ($line in ($output -split "`r?`n" | Where-Object { $_ })) { Write-Host "    $line" }

Write-WwiLog "remote orchestration of $Root finished"
