<#
    Place the file feeds on the host that actually executes the packages.

    The SSIS catalogue spawns ISServerExec on the SQL Server host, so a feed
    staged anywhere else is invisible to every ING_FILE_* package no matter how
    correct the paths are. This script drives the landing zone on that host over
    SSM Run Command: it creates the layout config/landing-zone.yaml defines,
    grants the SQL Server service identity the rights the packages need, copies
    each feed in deployment/preflight/feed-manifest.json, and then verifies what
    arrived by counting files and lines remotely.

    What it does not do: it does not generate feed content. The files come from
    -SourceRoot, which is the generated landing tree (loaders/landing staged
    output) or an existing landing root; generating them stays with the
    generators, so this only ever moves bytes that already passed generation.

    Usage:
      .\deployment\files\Stage-LandingZoneRemote.ps1 -InstanceId i-0123456789abcdef0 `
          -SourceRoot C:\WWI\DEV [-LandingRoot C:\WWI\DEV] [-Region us-east-2] [-DryRun]
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $InstanceId,
    [Parameter(Mandatory)][string] $SourceRoot,
    [string] $LandingRoot = 'C:\WWI\DEV',
    [string] $Region = $env:AWS_DEFAULT_REGION,
    [string] $ServiceAccount = 'NT SERVICE\MSSQLSERVER',
    [switch] $VerifyOnly,
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-landing'

if ([string]::IsNullOrWhiteSpace($Region)) { $Region = 'us-east-2' }
Assert-WwiTool -Name 'aws' -Hint 'install the AWS CLI; SSM Run Command is the only channel to the execution host'

$manifestPath = Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'preflight') 'feed-manifest.json'
if (-not (Test-Path $manifestPath)) {
    Stop-WwiWithError "feed manifest $manifestPath is missing; run tools/landing/render_feed_manifest.py"
}
$manifest = Get-Content -Raw $manifestPath | ConvertFrom-Json

# SSM commands are size limited, so file content travels as base64 in chunks
# that comfortably fit one command document.
$chunkSize = 6000

function Invoke-RemotePowerShell {
    param(
        [Parameter(Mandatory)][string] $Description,
        [Parameter(Mandatory)][string[]] $Commands,
        [switch] $Quiet
    )

    if ($DryRun) {
        Write-WwiLog "WHATIF $Description"
        return ''
    }

    $payload = @{ commands = $Commands } | ConvertTo-Json -Depth 4 -Compress
    $temporary = [System.IO.Path]::GetTempFileName()
    try {
        Set-Content -LiteralPath $temporary -Value $payload -Encoding utf8
        $sent = aws ssm send-command --region $Region --instance-ids $InstanceId `
            --document-name 'AWS-RunPowerShellScript' --parameters ("file://{0}" -f $temporary) `
            --output json | ConvertFrom-Json
        $commandId = $sent.Command.CommandId

        do {
            Start-Sleep -Seconds 2
            $result = aws ssm get-command-invocation --region $Region `
                --command-id $commandId --instance-id $InstanceId --output json | ConvertFrom-Json
        } while ($result.Status -in @('Pending', 'InProgress', 'Delayed'))

        if ($result.Status -ne 'Success') {
            Stop-WwiWithError ("{0} failed on {1}: {2} {3}" -f $Description, $InstanceId, $result.Status, $result.StandardErrorContent)
        }
        if (-not $Quiet) { Write-WwiLog "$Description ok" }
        return [string] $result.StandardOutputContent
    }
    finally {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
    }
}

function Get-LocalFeedFiles {
    param([Parameter(Mandatory)] $Feed)
    $directory = Join-Path $SourceRoot ($Feed.relative_path -replace '/', '\')
    if (-not (Test-Path $directory)) { return @() }
    return @(Get-ChildItem -Path $directory -Filter $Feed.file_spec -File | Sort-Object Name)
}

# -- 1. layout and rights ----------------------------------------------------

if (-not $VerifyOnly) {
    $directories = @()
    foreach ($top in $manifest.top_level_directories) { $directories += (Join-Path $LandingRoot $top) }
    foreach ($feed in $manifest.feeds) { $directories += (Join-Path $LandingRoot ($feed.relative_path -replace '/', '\')) }
    foreach ($working in $manifest.working_directories) {
        $directories += (Join-Path $LandingRoot ($working.relative_path -replace '/', '\'))
    }
    $directories += (Join-Path $LandingRoot 'work\Logs\Agent')
    $directories = $directories | Sort-Object -Unique

    $create = @("`$ErrorActionPreference = 'Stop'")
    foreach ($directory in $directories) {
        $create += "New-Item -ItemType Directory -Force -Path '$directory' | Out-Null"
    }
    # The catalogue runs the packages as the SQL Server service identity, so it
    # is that account - not the deploying login - that has to read the inbound
    # files and write archive, quarantine and work.
    $create += "icacls '$LandingRoot' /grant '$($ServiceAccount):(OI)(CI)M' /T /C | Out-Null"
    $create += "[Environment]::SetEnvironmentVariable('$($manifest.root_variable)', '$LandingRoot', 'Machine')"
    $create += "Write-Output 'layout ready'"
    Invoke-RemotePowerShell -Description "create the landing zone under $LandingRoot" -Commands $create | Out-Null
}

# -- 2. feed content ---------------------------------------------------------

if (-not $VerifyOnly) {
    foreach ($feed in $manifest.feeds) {
        $files = Get-LocalFeedFiles -Feed $feed
        if ($files.Count -eq 0) {
            Stop-WwiWithError ("feed {0} has no file matching {1} under {2}; generate the landing zone before staging it" -f
                $feed.feed, $feed.file_spec, $SourceRoot)
        }

        foreach ($file in $files) {
            $target = Join-Path (Join-Path $LandingRoot ($feed.relative_path -replace '/', '\')) $file.Name
            $encoded = [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($file.FullName))
            $chunks = [System.Collections.Generic.List[string]]::new()
            for ($offset = 0; $offset -lt $encoded.Length; $offset += $chunkSize) {
                $chunks.Add($encoded.Substring($offset, [Math]::Min($chunkSize, $encoded.Length - $offset)))
            }

            $staging = "$target.b64"
            $first = $true
            $index = 0
            foreach ($chunk in $chunks) {
                $index++
                $commands = @("`$ErrorActionPreference = 'Stop'")
                if ($first) {
                    $commands += "Set-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
                    $first = $false
                } else {
                    $commands += "Add-Content -LiteralPath '$staging' -Value '$chunk' -NoNewline -Encoding ascii"
                }
                Invoke-RemotePowerShell -Quiet -Description ("send {0} part {1}/{2}" -f $file.Name, $index, $chunks.Count) `
                    -Commands $commands | Out-Null
            }

            $finish = @(
                "`$ErrorActionPreference = 'Stop'",
                "[System.IO.File]::WriteAllBytes('$target', [Convert]::FromBase64String((Get-Content -Raw -LiteralPath '$staging')))",
                "Remove-Item -LiteralPath '$staging' -Force",
                "Write-Output ((Get-Item -LiteralPath '$target').Length)"
            )
            $size = (Invoke-RemotePowerShell -Quiet -Description "materialise $($file.Name)" -Commands $finish).Trim()
            if (-not $DryRun -and [int64] $size -ne $file.Length) {
                Stop-WwiWithError ("{0} arrived as {1} bytes but is {2} bytes locally" -f $file.Name, $size, $file.Length)
            }
            Write-WwiLog ("staged {0} -> {1} ({2} bytes)" -f $file.Name, $target, $file.Length)
        }
    }
}

# -- 3. verification ---------------------------------------------------------

$verify = @("`$ErrorActionPreference = 'Stop'")
foreach ($feed in $manifest.feeds) {
    $directory = Join-Path $LandingRoot ($feed.relative_path -replace '/', '\')
    $verify += @"
`$files = @(Get-ChildItem -Path '$directory' -Filter '$($feed.file_spec)' -File -ErrorAction SilentlyContinue)
`$rows = 0
foreach (`$f in `$files) { `$rows += @(Get-Content -LiteralPath `$f.FullName).Count }
Write-Output ('$($feed.feed)={0} files,{1} lines' -f `$files.Count, `$rows)
"@
}
$report = Invoke-RemotePowerShell -Description 'verify the staged feeds' -Commands $verify
if (-not $DryRun) {
    foreach ($line in ($report -split "`r?`n" | Where-Object { $_ })) { Write-WwiLog $line }
}

Write-WwiLog 'landing zone staging complete'
