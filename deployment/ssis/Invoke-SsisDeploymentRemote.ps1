<#
    Runs the SSISDB deployment on the host that owns the catalog.

    SSISDB refuses every catalog write from a SQL-authenticated login:
    catalog.create_folder, catalog.deploy_project and the environment procedures
    all answer "The operation cannot be started by an account that uses SQL
    Server Authentication". The estate reaches the instance over the public
    endpoint with SQL authentication and the deploy host is not domain joined,
    so there is no Windows principal to connect as from here.

    The deployment is therefore driven where a Windows principal exists: this
    script ships the built .ispac files and the deployment scripts to the SQL
    Server host over SSM Run Command and invokes Deploy-SsisCatalog.ps1 there
    with integrated authentication against localhost. It is still a client-side
    deployment in the sense that matters - the deploying process reads the
    .ispac and streams it into catalog.deploy_project - the client is simply the
    catalog's own host. The server never reads the artifact off its own disk,
    which is what the removed OPENROWSET path did.

    Nothing secret crosses SSM. Deployment needs no credential at all
    (integrated authentication, and an .ispac holds no password). The
    environment step does need the two account passwords, so it does not carry
    them either: the host reads them itself from AWS Secrets Manager with its
    instance role and hands them to Deploy-SsisEnvironment.ps1 through its own
    process environment, so no secret is ever in a command document, an SSM
    invocation record or this log.

    Usage:
      .\deployment\ssis\Invoke-SsisDeploymentRemote.ps1 -InstanceId i-0123456789abcdef0
      .\deployment\ssis\Invoke-SsisDeploymentRemote.ps1 -InstanceId i-0... -Step verify
      .\deployment\ssis\Invoke-SsisDeploymentRemote.ps1 -InstanceId i-0... -Step environment
      .\deployment\ssis\Invoke-SsisDeploymentRemote.ps1 -InstanceId i-0... -Step preflight

    The preflight step is the same read-only Preflight.ps1 the deploy host runs,
    executed where the catalog spawns ISServerExec, which is the only place its
    answer means anything.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $InstanceId,
    [ValidateSet('deploy', 'verify', 'environment', 'preflight')][string] $Step = 'deploy',
    [string] $Region = $env:AWS_DEFAULT_REGION,
    [string] $RemoteRoot = 'C:\WWI\deploy',
    [string] $Folder = 'WWI_DEV',
    [ValidateSet('DEV', 'TEST', 'PROD')][string] $Environment = 'DEV',
    [int] $ExpectedPackageCount = 0,
    # Secrets Manager ids holding the two accounts the catalog environment
    # binds. Each is a JSON document with username and password members.
    [string] $OracleSecretId = 'legacy-demo/oracle/admin',
    [string] $SqlServerSecretId = 'legacy-demo/sqlserver/devin_migration',
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-ssis-remote'

if ([string]::IsNullOrWhiteSpace($Region)) { $Region = 'us-east-2' }
Assert-WwiTool -Name 'aws' -Hint 'install the AWS CLI; SSM Run Command is the only channel to the execution host'

$repoRoot  = Get-WwiRepositoryRoot
$artifacts = Join-Path $repoRoot 'artifacts'

$projects = @(Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtproj' -Recurse -File |
    Where-Object { $_.FullName -notmatch '\\obj\\' } | Sort-Object BaseName | ForEach-Object { $_.BaseName })
if ($projects.Count -eq 0) { Stop-WwiWithError 'no .dtproj projects found under ssis/.' }

if ($ExpectedPackageCount -le 0) {
    $ExpectedPackageCount = @(Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtsx' -Recurse -File |
        Where-Object { $_.FullName -notmatch '\\obj\\' }).Count
}

# SSM caps a command document at 100 KB, so file content travels base64 in
# chunks that leave room for the surrounding PowerShell.
$chunkSize = 48000

function Invoke-RemotePowerShell {
    param(
        [Parameter(Mandatory)][string] $Description,
        [Parameter(Mandatory)][string[]] $Commands,
        [switch] $Quiet,
        [switch] $AllowFailure
    )

    if ($DryRun) {
        Write-WwiLog "WHATIF $Description"
        return ''
    }

    $payload = @{ commands = $Commands } | ConvertTo-Json -Depth 4 -Compress
    $temporary = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($temporary, $payload, (New-Object System.Text.UTF8Encoding($false)))
        $sent = aws ssm send-command --region $Region --instance-ids $InstanceId `
            --document-name 'AWS-RunPowerShellScript' --timeout-seconds 600 `
            --parameters ("file://{0}" -f $temporary) --output json | ConvertFrom-Json
        $commandId = $sent.Command.CommandId

        do {
            Start-Sleep -Seconds 3
            $result = aws ssm get-command-invocation --region $Region `
                --command-id $commandId --instance-id $InstanceId --output json | ConvertFrom-Json
        } while ($result.Status -in @('Pending', 'InProgress', 'Delayed'))

        if ($result.Status -ne 'Success' -and -not $AllowFailure) {
            Write-Host $result.StandardOutputContent
            Stop-WwiWithError ("{0} failed on {1}: {2} {3}" -f $Description, $InstanceId,
                $result.Status, $result.StandardErrorContent)
        }
        if (-not $Quiet) { Write-WwiLog "$Description ok" }
        return [string] ($result.StandardOutputContent + $result.StandardErrorContent)
    }
    finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Copy-FileToHost {
    <# Sends one file, then proves the bytes that arrived hash to the same value. #>
    param(
        [Parameter(Mandatory)][string] $LocalPath,
        [Parameter(Mandatory)][string] $RemotePath
    )

    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $LocalPath).Hash
    $probe = Invoke-RemotePowerShell -Quiet -AllowFailure -Description "check $RemotePath" -Commands @(
        "if (Test-Path -LiteralPath '$RemotePath') { (Get-FileHash -Algorithm SHA256 -LiteralPath '$RemotePath').Hash } else { 'absent' }"
    )
    if ($probe.Trim() -eq $hash) {
        Write-WwiLog "unchanged $([System.IO.Path]::GetFileName($LocalPath))"
        return
    }

    $encoded = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($LocalPath))
    $staging = "$RemotePath.b64"
    $first = $true
    for ($offset = 0; $offset -lt $encoded.Length; $offset += $chunkSize) {
        $chunk = $encoded.Substring($offset, [Math]::Min($chunkSize, $encoded.Length - $offset))
        $commands = @("`$ErrorActionPreference = 'Stop'",
                      "New-Item -ItemType Directory -Force -Path (Split-Path -Parent '$RemotePath') | Out-Null")
        if ($first) {
            $commands += "Set-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
            $first = $false
        } else {
            $commands += "Add-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
        }
        Invoke-RemotePowerShell -Quiet -Description "send part of $RemotePath" -Commands $commands | Out-Null
    }

    $arrived = Invoke-RemotePowerShell -Quiet -Description "materialise $RemotePath" -Commands @(
        "`$ErrorActionPreference = 'Stop'",
        "[System.IO.File]::WriteAllBytes('$RemotePath', [Convert]::FromBase64String((Get-Content -Raw -LiteralPath '$staging')))",
        "Remove-Item -LiteralPath '$staging' -Force",
        "(Get-FileHash -Algorithm SHA256 -LiteralPath '$RemotePath').Hash"
    )
    if (-not $DryRun -and $arrived.Trim() -ne $hash) {
        Stop-WwiWithError "$RemotePath did not arrive intact on $InstanceId."
    }
    Write-WwiLog "sent $([System.IO.Path]::GetFileName($LocalPath)) -> $RemotePath"
}

# -- 1. ship the deployment scripts and the artifacts ------------------------

$payloadFiles = @()
foreach ($relative in @('deployment\lib', 'deployment\ssis')) {
    $payloadFiles += @(Get-ChildItem -Path (Join-Path $repoRoot $relative) -Filter '*.ps1' -File)
}
if ($Step -eq 'deploy') {
    if (-not (Test-Path $artifacts)) {
        Stop-WwiWithError "$artifacts does not exist. Run deployment/ssis/Build-SsisProject.ps1 first."
    }
    $payloadFiles += @(Get-ChildItem -Path $artifacts -Filter '*.ispac' -File)
}
if ($Step -eq 'preflight') {
    $preflight = Join-Path $repoRoot 'deployment\preflight'
    $payloadFiles += @(Get-ChildItem -Path $preflight -File |
        Where-Object { $_.Extension -in @('.ps1', '.json') })
}
if ($Step -eq 'environment') {
    # Only the environment being deployed travels: the others are large and the
    # remote step would never read them.
    $environments = Join-Path $repoRoot 'deployment\ssis\environments'
    $prefix = "$($Environment.ToLower())_environment."
    $payloadFiles += @(Get-ChildItem -Path $environments -File |
        Where-Object { $_.Extension -in @('.sql', '.psd1') -and $_.Name.StartsWith($prefix) })
}

foreach ($file in $payloadFiles) {
    $relative = $file.FullName.Substring($repoRoot.Length).TrimStart('\')
    Copy-FileToHost -LocalPath $file.FullName -RemotePath (Join-Path $RemoteRoot $relative)
}

# -- 2. run the deployment there ---------------------------------------------

$projectList = ($projects | ForEach-Object { "'$_'" }) -join ','

$run = @(
    "`$ErrorActionPreference = 'Stop'",
    "`$env:SSIS_SERVER = 'localhost'",
    "`$env:SSIS_FOLDER = '$Folder'",
    "`$env:WWI_ENVIRONMENT = '$Environment'",
    "Remove-Item Env:\SQLSERVER_USER -ErrorAction SilentlyContinue"
)


if ($Step -eq 'environment') {
    # The passwords are resolved on the host, by the host's own identity. They
    # are assigned straight into the process environment: no echo, no file, and
    # nothing that the SSM invocation record keeps.
    # Non-credential configuration is forwarded from this host's environment,
    # so the catalog is bound to the estate's real hosts and accounts instead of
    # the placeholder defaults rendered into <env>_environment.vars.psd1.
    $forwarded = @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_PROVIDER',
                   'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_PROVIDER',
                   'SQLSERVER_TRUST_SERVER_CERTIFICATE', 'SQLSERVER_OLTP_DB', 'SQLSERVER_STAGING_DB',
                   'SQLSERVER_DW_DB', 'ETL_INBOUND_FILE_ROOT', 'ETL_ARCHIVE_FILE_ROOT',
                   'ETL_QUARANTINE_FILE_ROOT')
    foreach ($name in $forwarded) {
        $value = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $run += "`$env:$name = '$($value.Replace("'", "''"))'"
        } else {
            Write-WwiLog "$name is not set here; the rendered default will be bound instead" 'WARN'
        }
    }

    $run += @(
        "`$oracle = (aws secretsmanager get-secret-value --region $Region --secret-id '$OracleSecretId' --query SecretString --output text) | ConvertFrom-Json",
        "`$sql = (aws secretsmanager get-secret-value --region $Region --secret-id '$SqlServerSecretId' --query SecretString --output text) | ConvertFrom-Json",
        "if (-not `$oracle.password -or -not `$sql.password) { throw 'a credential secret has no password member' }",
        "`$env:ORACLE_USER = `$oracle.username",
        "`$env:ORACLE_PASSWORD = `$oracle.password",
        "`$env:SQLSERVER_USER = `$sql.username",
        "`$env:SQLSERVER_PASSWORD = `$sql.password",
        # -IntegratedSecurity: SQLSERVER_USER is the account the catalog binds,
        # not the login sqlcmd uses; the catalog only accepts a Windows login.
        "& '$RemoteRoot\deployment\ssis\Deploy-SsisEnvironment.ps1' -IntegratedSecurity"
    )
}
elseif ($Step -eq 'preflight') {
    # Same credential handling as the environment step: the host resolves the
    # two accounts itself, nothing crosses SSM.
    foreach ($name in @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE',
                        'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_OLTP_DB',
                        'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB')) {
        $value = [Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $run += "`$env:$name = '$($value.Replace("'", "''"))'"
        } else {
            Write-WwiLog "$name is not set here; the remote preflight will report it missing" 'WARN'
        }
    }
    $run += @(
        "`$oracle = (aws secretsmanager get-secret-value --region $Region --secret-id '$OracleSecretId' --query SecretString --output text) | ConvertFrom-Json",
        "`$sql = (aws secretsmanager get-secret-value --region $Region --secret-id '$SqlServerSecretId' --query SecretString --output text) | ConvertFrom-Json",
        "`$env:ORACLE_USER = `$oracle.username",
        "`$env:ORACLE_PASSWORD = `$oracle.password",
        "`$env:SQLSERVER_USER = `$sql.username",
        "`$env:SQLSERVER_PASSWORD = `$sql.password",
        "`$env:WWI_LANDING_ROOT = [Environment]::GetEnvironmentVariable('WWI_LANDING_ROOT', 'Machine')",
        "& '$RemoteRoot\deployment\preflight\Preflight.ps1' -Connectivity -ExecutionHost"
    )
}
else {
    $deployArguments = if ($Step -eq 'verify') { '-VerifyOnly' } else { '-UseCatalogProcedure' }
    $invocation = "& '$RemoteRoot\deployment\ssis\Deploy-SsisCatalog.ps1' $deployArguments" +
                  " -ExpectedProject @($projectList) -ExpectedPackageCount $ExpectedPackageCount"
    Write-Verbose "remote command: $invocation"
    $run += $invocation
}
$output = Invoke-RemotePowerShell -Description "$Step /SSISDB/$Folder on $InstanceId" -Commands $run
foreach ($line in ($output -split "`r?`n" | Where-Object { $_ })) { Write-Host "    $line" }

Write-WwiLog "remote $Step finished"
