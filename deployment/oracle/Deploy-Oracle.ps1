<#
    Oracle stage driver for the WWIGERP schema (Windows deploy host).

    Runs everything under oracle/ in dependency order through SQL*Plus, or sqlcl
    when SQL*Plus is absent. Connection details come from ORACLE_HOST,
    ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER and ORACLE_PASSWORD; the password
    is piped to the client rather than passed as an argument so it never reaches
    the process table or the -DryRun output.

    oracle/ddl/03_create_schemas.sql creates each schema account with a
    &&WWI_<schema>_SECRET substitution variable. Those are DEFINEd from the
    matching environment variables (WWI_MDM_SECRET, WWI_PROC_SECRET,
    WWI_FIN_SECRET, WWI_REF_SECRET, WWI_AUDIT_SECRET, WWI_EXTRACT_SECRET) for
    the ddl stage only, and are UNDEFINEd again before the script exits.

    This script has not been executed against any Oracle instance.

    Usage: .\deployment\oracle\Deploy-Oracle.ps1 [-DryRun] [-Only ddl] [-FromStage 3]
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [string] $Only,
    [int]    $FromStage = 1
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-oracle'

Assert-WwiEnvironmentVariable @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_USER', 'ORACLE_PASSWORD')
Confirm-WwiProduction

$repoRoot   = Get-WwiRepositoryRoot
$oracleRoot = Join-Path $repoRoot 'oracle'
if (-not (Test-Path $oracleRoot)) { Stop-WwiWithError "oracle/ is not present in $repoRoot." }

$client = if (Get-Command sqlplus -ErrorAction SilentlyContinue) { 'sqlplus' }
          elseif (Get-Command sql -ErrorAction SilentlyContinue) { 'sql' }
          elseif ($DryRun) { 'sqlplus' }
          else { Stop-WwiWithError 'neither sqlplus nor sql (sqlcl) is on PATH.' }

$schemaSecretVariables = @('WWI_MDM_SECRET', 'WWI_PROC_SECRET', 'WWI_FIN_SECRET',
                           'WWI_REF_SECRET', 'WWI_AUDIT_SECRET', 'WWI_EXTRACT_SECRET')

# Same dependency order as the shell variant. Keep the two in step.
$stages = @('ddl', 'tables', 'views', 'functions', 'procedures', 'packages', 'reference', 'seed')

function Invoke-OracleScript {
    param([Parameter(Mandatory)][string] $Path)

    $relative = $Path.Substring($repoRoot.Length + 1)
    if ($DryRun) {
        Write-WwiLog "WHATIF $client @$relative"
        return
    }

    Write-WwiLog "RUN    $relative"
    # The DDL scripts rely on substitution; the data scripts turn DEFINE off
    # themselves around any literal ampersand.
    $defines = ($schemaSecretVariables |
        Where-Object { -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_)) } |
        ForEach-Object { "DEFINE $_ = `"$([Environment]::GetEnvironmentVariable($_))`"" }) -join "`n"
    $undefines = ($schemaSecretVariables | ForEach-Object { "UNDEFINE $_" }) -join "`n"
    $script = @"
WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER OSERROR EXIT 9
CONNECT $($env:ORACLE_USER)/$($env:ORACLE_PASSWORD)@$($env:ORACLE_HOST):$($env:ORACLE_PORT)/$($env:ORACLE_SERVICE)
SET DEFINE ON
$defines
SET ECHO OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS';
ALTER SESSION SET CURRENT_SCHEMA = $($env:ORACLE_USER);
@$Path
SHOW ERRORS
$undefines
EXIT SQL.SQLCODE
"@
    $script | & $client -S -L /nolog
    if ($LASTEXITCODE -ne 0) {
        Stop-WwiWithError "$relative failed with Oracle exit code $LASTEXITCODE."
    }
}

$stageIndex = 0
$fileCount  = 0

foreach ($stage in $stages) {
    $stageIndex++
    if ($Only -and $Only -ne $stage) { continue }
    if ($stageIndex -lt $FromStage) { continue }

    $stagePath = Join-Path $oracleRoot $stage
    if (-not (Test-Path $stagePath)) {
        Write-WwiLog "oracle/$stage is not present; skipping stage $stageIndex." 'WARN'
        continue
    }

    Write-WwiLog "--- stage ${stageIndex}: oracle/$stage ---"
    Get-ChildItem -Path $stagePath -Filter '*.sql' -Recurse -File |
        Sort-Object FullName |
        ForEach-Object { Invoke-OracleScript -Path $_.FullName; $fileCount++ }
}

if (-not $DryRun) {
    Write-WwiLog 'recompiling invalid objects'
    $recompile = @"
CONNECT $($env:ORACLE_USER)/$($env:ORACLE_PASSWORD)@$($env:ORACLE_HOST):$($env:ORACLE_PORT)/$($env:ORACLE_SERVICE)
SET SERVEROUTPUT ON
EXEC DBMS_UTILITY.COMPILE_SCHEMA(schema => USER, compile_all => FALSE);
SELECT object_type, object_name FROM user_objects WHERE status = 'INVALID' ORDER BY 1, 2;
EXIT SQL.SQLCODE
"@
    $recompile | & $client -S -L /nolog
} else {
    Write-WwiLog 'WHATIF recompile invalid objects'
}

Write-WwiLog ("oracle stage complete: {0} file(s) processed{1}" -f $fileCount, $(if ($DryRun) { ' (dry run)' } else { '' }))
