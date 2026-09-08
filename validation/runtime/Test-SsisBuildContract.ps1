<#
.SYNOPSIS
    Build every SSIS project with the repository driver and check the archives.

.DESCRIPTION
    SSISBuild.exe reads its options as -argument:value; given the space
    separated form it exits non-zero without building anything, so the build
    driver is only known to work by running the installed tool. This test does
    that, into a scratch directory, and then compares the resulting .ispac
    archives with config/estate-catalog.yaml: every package must be present
    exactly once, in the project the catalog gives it.

    Nothing is deployed and no package is executed.

.EXAMPLE
    powershell -File validation/runtime/Test-SsisBuildContract.ps1
#>

[CmdletBinding()]
param(
    [string] $OutputPath,
    [switch] $KeepOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot 'deployment/lib/Common.ps1')
$script:WwiLogPrefix = 'wwi-build-contract'

if (-not $OutputPath) {
    $OutputPath = Join-Path ([System.IO.Path]::GetTempPath()) ("wwi-ispac-" + [guid]::NewGuid().ToString('n'))
}

try {
    & (Join-Path $repoRoot 'deployment/ssis/Build-SsisProject.ps1') -OutputPath $OutputPath
    if ($LASTEXITCODE -ne 0) {
        Write-WwiLog "FAIL  build driver exited $LASTEXITCODE"
        exit 1
    }

    & python (Join-Path $repoRoot 'validation/checks/check_ispac_contents.py') $OutputPath
    $checked = $LASTEXITCODE
    if ($checked -ne 0) {
        Write-WwiLog 'FAIL  built archives do not match the canonical package ownership'
        exit 1
    }
    Write-WwiLog 'PASS  every catalog package is built exactly once into its own project'
    exit 0
}
finally {
    if (-not $KeepOutput -and (Test-Path $OutputPath)) {
        Remove-Item -Recurse -Force $OutputPath
    }
}
