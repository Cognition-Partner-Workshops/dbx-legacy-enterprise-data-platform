<#
    WWI estate - single deployment entry point (Windows deploy host).

    Order (documented in deployment/README.md, and it matters):

        0. preflight   tooling, environment variables, repository layout
        1. oracle      WWIGERP schema objects, in dependency order
        2. sqlserver   control -> OLTP extensions -> staging -> warehouse ->
                       security -> agent
        3. ssis        build the .ispac, deploy it, create and bind the environment

    The Windows variant is the complete one: it is the only host that can build
    the SSIS project, so the ssis stage here includes Build-SsisProject.ps1,
    which the shell variant cannot run.

    Connection details come from the environment variables documented in
    config/.env.example. No script in deployment/ contains a credential and none
    of them has been executed against a server.

    Usage:
        .\deployment\deploy-all.ps1 -DryRun
        .\deployment\deploy-all.ps1 -Stage sqlserver
        $env:WWI_ENVIRONMENT='PROD'; $env:WWI_CONFIRM_PROD='I-UNDERSTAND'; .\deployment\deploy-all.ps1
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [ValidateSet('preflight', 'oracle', 'sqlserver', 'ssis')]
    [string] $Stage,
    [switch] $SkipPreflight,
    [switch] $ContinueOnError
)

. (Join-Path $PSScriptRoot 'lib' 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-all'

$environmentCode = Get-WwiEnvironmentCode
Confirm-WwiProduction

Write-WwiLog ("environment {0}{1}" -f $environmentCode, $(if ($DryRun) { ', dry run' } else { '' }))

$failed = [System.Collections.Generic.List[string]]::new()

function Invoke-Stage {
    param([string] $Name, [scriptblock] $Body)

    if ($Stage -and $Stage -ne $Name) { return }

    Write-WwiLog "======== stage: $Name ========"
    try {
        & $Body
        Write-WwiLog "stage $Name finished"
    }
    catch {
        $failed.Add($Name)
        if ($ContinueOnError) {
            Write-WwiLog "stage $Name failed: $($_.Exception.Message)" 'WARN'
        }
        else {
            throw
        }
    }
}

if (-not $SkipPreflight) {
    Invoke-Stage 'preflight' {
        & (Join-Path $PSScriptRoot 'preflight' 'Preflight.ps1') -Stage all
    }
}

Invoke-Stage 'oracle' {
    & (Join-Path $PSScriptRoot 'oracle' 'Deploy-Oracle.ps1') -DryRun:$DryRun
}

Invoke-Stage 'sqlserver' {
    & (Join-Path $PSScriptRoot 'sqlserver' 'Deploy-SqlServer.ps1') -DryRun:$DryRun
}

Invoke-Stage 'ssis' {
    & (Join-Path $PSScriptRoot 'ssis' 'Build-SsisProject.ps1')    -DryRun:$DryRun
    & (Join-Path $PSScriptRoot 'ssis' 'Deploy-SsisCatalog.ps1')   -DryRun:$DryRun
    & (Join-Path $PSScriptRoot 'ssis' 'Deploy-SsisEnvironment.ps1') -DryRun:$DryRun -Verify
}

if ($failed.Count -gt 0) {
    Stop-WwiWithError ("finished with failures in: {0}" -f ($failed -join ', '))
}

Write-WwiLog ("deploy-all finished{0}" -f $(if ($DryRun) { ' (dry run - nothing was submitted)' } else { '' }))
Write-WwiLog 'post-deployment steps are listed in deployment/README.md'
