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

# An undefined substitution variable makes SQL*Plus read its value from the next
# line of standard input, so a missing secret silently creates the account with
# a fragment of the script as its password instead of failing. Require them all.
if (-not $DryRun -and (-not $Only -or $Only -eq 'ddl')) {
    Assert-WwiEnvironmentVariable $schemaSecretVariables
}

# Same dependency order as the shell variant. Keep the two in step.
# 05_grant_privileges.sql and 07_create_synonyms.sql name the objects in
# oracle/tables one by one, so they run as their own stage after those exist
# and before the views and packages that compile against them.
$lateDdl = @('05_grant_privileges.sql', '07_create_synonyms.sql')
# 08_grant_function_execute.sql grants EXECUTE on the scalar functions, so it
# runs between the functions and the views that call them.
$funcDdl = @('08_grant_function_execute.sql')
# 09_grant_package_execute.sql grants EXECUTE on the packages, so it runs
# between the package specifications and the bodies and procedures that call
# across schemas.
$pkgDdl = @('09_grant_package_execute.sql')
# ZZ_add_future_partitions.sql is the December runbook script, run by hand
# against a populated estate; it is not part of creating the objects.
$handRun = @('ZZ_add_future_partitions.sql')
# Views select through the scalar functions in oracle/functions, so functions
# compile first.
$stages  = [ordered]@{
    ddl        = @{ Directory = 'ddl'; Exclude = $lateDdl + $funcDdl + $pkgDdl }
    tables     = @{ Directory = 'tables'; Exclude = $handRun }
    grants     = @{ Directory = 'ddl'; Include = $lateDdl }
    functions  = @{ Directory = 'functions' }
    funcgrants = @{ Directory = 'ddl'; Include = $funcDdl }
    views      = @{ Directory = 'views' }
    pkgspecs   = @{ Directory = 'packages'; Pattern = '*.pks.sql' }
    pkggrants  = @{ Directory = 'ddl'; Include = $pkgDdl }
    pkgbodies  = @{ Directory = 'packages'; Pattern = '*.pkb.sql' }
    procedures = @{ Directory = 'procedures' }
    reference  = @{ Directory = 'reference' }
    seed       = @{ Directory = 'seed' }
}

function Get-DeployOrder {
    # Every generated artifact stamps "Deploy order : <n>" in its header. File
    # names sort alphabetically, which is not dependency order: WWI_FIN tables
    # carry foreign keys into WWI_MDM tables that sort after them.
    param([Parameter(Mandatory)][string] $Path)

    $header = (Get-Content -Path $Path -TotalCount 15) -join "`n"
    $match  = [regex]::Match($header, 'Deploy order\s*:\s*(\d+)')
    if ($match.Success) { return [int] $match.Groups[1].Value }
    return [int]::MaxValue
}

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
        ForEach-Object { "DEFINE $_ = `"$([Environment]::GetEnvironmentVariable($_))`"" }) -join "`n"
    $undefines = ($schemaSecretVariables | ForEach-Object { "UNDEFINE $_" }) -join "`n"
    $script = @"
WHENEVER SQLERROR EXIT SQL.SQLCODE
WHENEVER OSERROR EXIT 9
CONNECT $($env:ORACLE_USER)/$($env:ORACLE_PASSWORD)@$($env:ORACLE_HOST):$($env:ORACLE_PORT)/$($env:ORACLE_SERVICE)
SET DEFINE ON
SET VERIFY OFF
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

foreach ($stage in $stages.Keys) {
    $stageIndex++
    if ($Only -and $Only -ne $stage) { continue }
    if ($stageIndex -lt $FromStage) { continue }

    $spec      = $stages[$stage]
    $stagePath = Join-Path $oracleRoot $spec.Directory
    if (-not (Test-Path $stagePath)) {
        Write-WwiLog "oracle/$($spec.Directory) is not present; skipping stage $stageIndex." 'WARN'
        continue
    }

    $files = Get-ChildItem -Path $stagePath -Filter '*.sql' -Recurse -File |
        Sort-Object @{ Expression = { Get-DeployOrder -Path $_.FullName } }, FullName
    if ($spec.ContainsKey('Include')) { $files = $files | Where-Object { $spec.Include -contains $_.Name } }
    if ($spec.ContainsKey('Exclude')) { $files = $files | Where-Object { $spec.Exclude -notcontains $_.Name } }
    if ($spec.ContainsKey('Pattern')) { $files = $files | Where-Object { $_.Name -like $spec.Pattern } }

    Write-WwiLog "--- stage ${stageIndex}: $stage ---"
    $files | ForEach-Object { Invoke-OracleScript -Path $_.FullName; $fileCount++ }
}

if (-not $DryRun) {
    Write-WwiLog 'recompiling invalid objects'
    $recompile = @"
CONNECT $($env:ORACLE_USER)/$($env:ORACLE_PASSWORD)@$($env:ORACLE_HOST):$($env:ORACLE_PORT)/$($env:ORACLE_SERVICE)
SET SERVEROUTPUT ON
BEGIN
    FOR s IN (SELECT username FROM dba_users WHERE username LIKE 'WWI\_%' ESCAPE '\') LOOP
        DBMS_UTILITY.COMPILE_SCHEMA(schema => s.username, compile_all => FALSE);
    END LOOP;
END;
/
SELECT owner, object_type, object_name FROM dba_objects
 WHERE owner LIKE 'WWI\_%' ESCAPE '\' AND status = 'INVALID'
 ORDER BY 1, 2, 3;
EXIT SQL.SQLCODE
"@
    $recompile | & $client -S -L /nolog
} else {
    Write-WwiLog 'WHATIF recompile invalid objects'
}

Write-WwiLog ("oracle stage complete: {0} file(s) processed{1}" -f $fileCount, $(if ($DryRun) { ' (dry run)' } else { '' }))
