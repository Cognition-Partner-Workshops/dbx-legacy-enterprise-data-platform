<#
    Shared helpers for the WWI estate deployment drivers (PowerShell variants).

    Dot-sourced by deploy-all.ps1 and the stage drivers. Mirrors
    deployment/lib/common.sh: environment checking, -WhatIf style dry running,
    logging and uniform failure.

    Nothing in deployment/ has been executed against a database server.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:WwiLogPrefix = 'wwi-deploy'

function Write-WwiLog {
    param([Parameter(Mandatory)][string] $Message, [string] $Level = 'INFO')
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    Write-Host "[$stamp] $script:WwiLogPrefix $Level $Message"
}

function Stop-WwiWithError {
    param([Parameter(Mandatory)][string] $Message)
    throw "[$script:WwiLogPrefix] $Message"
}

function Assert-WwiEnvironmentVariable {
    <# Fails once, listing every missing variable, instead of one at a time. #>
    param([Parameter(Mandatory)][string[]] $Name)

    $missing = @()
    foreach ($n in $Name) {
        $value = [Environment]::GetEnvironmentVariable($n)
        if ([string]::IsNullOrWhiteSpace($value)) { $missing += $n }
    }
    if ($missing.Count -gt 0) {
        Stop-WwiWithError ("required environment variables are not set: {0}. See config/.env.example." -f ($missing -join ', '))
    }
}

function Assert-WwiTool {
    param([Parameter(Mandatory)][string] $Name, [string] $Hint = '')
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Stop-WwiWithError "$Name is not on PATH. $Hint"
    }
}

function Get-WwiEnvironmentCode {
    $code = [Environment]::GetEnvironmentVariable('WWI_ENVIRONMENT')
    if ([string]::IsNullOrWhiteSpace($code)) { $code = 'DEV' }
    if ($code -notin @('DEV', 'TEST', 'PROD')) {
        Stop-WwiWithError "WWI_ENVIRONMENT must be DEV, TEST or PROD (got '$code')."
    }
    return $code
}

function Confirm-WwiProduction {
    if ((Get-WwiEnvironmentCode) -eq 'PROD' -and $env:WWI_CONFIRM_PROD -ne 'I-UNDERSTAND') {
        Stop-WwiWithError 'PROD deployments require WWI_CONFIRM_PROD=I-UNDERSTAND.'
    }
}

function Invoke-WwiCommand {
    <#
        Runs an external command, or prints it when -DryRun is supplied.
        Secrets are never passed as arguments; each driver hands them to the
        client through its own environment or stdin channel.
    #>
    param(
        [Parameter(Mandatory)][string]   $Description,
        [Parameter(Mandatory)][string]   $FilePath,
        [Parameter(Mandatory)][string[]] $ArgumentList,
        [switch] $DryRun
    )

    if ($DryRun) {
        Write-WwiLog "WHATIF $Description" 'INFO'
        Write-Host ("           {0} {1}" -f $FilePath, ($ArgumentList -join ' '))
        return 0
    }

    Write-WwiLog "RUN    $Description"
    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        Stop-WwiWithError "$Description failed with exit code $LASTEXITCODE."
    }
    return $LASTEXITCODE
}

function Get-WwiRepositoryRoot {
    return (Resolve-Path (Join-Path (Join-Path $PSScriptRoot '..') '..')).Path
}
