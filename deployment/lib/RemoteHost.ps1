<#
    SSM Run Command as a file channel and a shell on the catalog host.

    The estate reaches its SQL Server over the public endpoint with SQL
    authentication, and the deploy host is not domain joined, so there is no
    Windows principal to connect as from here. Every catalog write - and
    catalog.create_execution is one - therefore has to be driven on the host
    itself, and SSM Run Command is the only channel to it.

    Callers set $script:WwiRemoteInstanceId, $script:WwiRemoteRegion and
    optionally $script:WwiRemoteDryRun before the first call.

    Nothing secret is ever put in a command document: a remote step that needs
    a credential resolves it on the host, with the host's own instance role.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# SSM caps a command document at 100 KB, so file content travels base64 in
# chunks that leave room for the surrounding PowerShell.
$script:WwiRemoteChunkSize = 48000

function Invoke-WwiRemotePowerShell {
    param(
        [Parameter(Mandatory)][string] $Description,
        [Parameter(Mandatory)][string[]] $Commands,
        [int] $ExecutionTimeoutSeconds = 3600,
        [switch] $Quiet,
        [switch] $AllowFailure
    )

    if ($script:WwiRemoteDryRun) {
        Write-WwiLog "WHATIF $Description"
        return ''
    }

    $instance = $script:WwiRemoteInstanceId
    $region   = $script:WwiRemoteRegion
    $payload  = @{
        commands         = $Commands
        executionTimeout = @([string] $ExecutionTimeoutSeconds)
    } | ConvertTo-Json -Depth 4 -Compress

    $temporary = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($temporary, $payload, (New-Object System.Text.UTF8Encoding($false)))
        $sent = aws ssm send-command --region $region --instance-ids $instance `
            --document-name 'AWS-RunPowerShellScript' --timeout-seconds 600 `
            --parameters ("file://{0}" -f $temporary) --output json | ConvertFrom-Json
        $commandId = $sent.Command.CommandId

        do {
            Start-Sleep -Seconds 5
            $result = aws ssm get-command-invocation --region $region `
                --command-id $commandId --instance-id $instance --output json | ConvertFrom-Json
        } while ($result.Status -in @('Pending', 'InProgress', 'Delayed'))

        if ($result.Status -ne 'Success' -and -not $AllowFailure) {
            Write-Host $result.StandardOutputContent
            Stop-WwiWithError ("{0} failed on {1}: {2} {3}" -f $Description, $instance,
                $result.Status, $result.StandardErrorContent)
        }
        if (-not $Quiet) { Write-WwiLog "$Description ok" }
        return [string] ($result.StandardOutputContent + $result.StandardErrorContent)
    }
    finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Copy-WwiFileToHost {
    <# Sends one file, then proves the bytes that arrived hash to the same value. #>
    param(
        [Parameter(Mandatory)][string] $LocalPath,
        [Parameter(Mandatory)][string] $RemotePath
    )

    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $LocalPath).Hash
    $probe = Invoke-WwiRemotePowerShell -Quiet -AllowFailure -Description "check $RemotePath" -Commands @(
        "if (Test-Path -LiteralPath '$RemotePath') { (Get-FileHash -Algorithm SHA256 -LiteralPath '$RemotePath').Hash } else { 'absent' }"
    )
    if ($probe.Trim() -eq $hash) {
        Write-WwiLog "unchanged $([System.IO.Path]::GetFileName($LocalPath))"
        return
    }

    $encoded = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($LocalPath))
    $staging = "$RemotePath.b64"
    $first = $true
    for ($offset = 0; $offset -lt $encoded.Length; $offset += $script:WwiRemoteChunkSize) {
        $chunk = $encoded.Substring($offset, [Math]::Min($script:WwiRemoteChunkSize, $encoded.Length - $offset))
        $commands = @("`$ErrorActionPreference = 'Stop'",
                      "New-Item -ItemType Directory -Force -Path (Split-Path -Parent '$RemotePath') | Out-Null")
        if ($first) {
            $commands += "Set-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
            $first = $false
        } else {
            $commands += "Add-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
        }
        Invoke-WwiRemotePowerShell -Quiet -Description "send part of $RemotePath" -Commands $commands | Out-Null
    }

    $arrived = Invoke-WwiRemotePowerShell -Quiet -Description "materialise $RemotePath" -Commands @(
        "`$ErrorActionPreference = 'Stop'",
        "[System.IO.File]::WriteAllBytes('$RemotePath', [Convert]::FromBase64String((Get-Content -Raw -LiteralPath '$staging')))",
        "Remove-Item -LiteralPath '$staging' -Force",
        "(Get-FileHash -Algorithm SHA256 -LiteralPath '$RemotePath').Hash"
    )
    if (-not $script:WwiRemoteDryRun -and $arrived.Trim() -ne $hash) {
        Stop-WwiWithError "$RemotePath did not arrive intact on $($script:WwiRemoteInstanceId)."
    }
    Write-WwiLog "sent $([System.IO.Path]::GetFileName($LocalPath)) -> $RemotePath"
}
