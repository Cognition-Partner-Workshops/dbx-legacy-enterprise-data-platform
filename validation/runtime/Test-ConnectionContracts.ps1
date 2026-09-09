<#
.SYNOPSIS
    Open the generated connection contracts against the installed providers.

.DESCRIPTION
    The string a generated connection manager hands the provider is its
    design-time literal with the ServerName / InitialCatalog / UserName
    property expressions folded back into it and the password the catalog
    applied to CM.<connection>.Password appended. This test composes exactly
    that (validation/checks/connection_expression.py), injects the project
    parameters and the two passwords from the environment exactly as
    deployment/ssis/Invoke-EstateOrchestration.ps1 and the catalog do, and
    opens the result.

    Two things it proves that no static check can:

      * OraOLEDB.Oracle.1 and MSOLEDBSQL19.1 authenticate with the password
        carried by CM.<connection>.Password;
      * OLE DB Driver 19 honours "Trust Server Certificate=True;" and not the
        unspaced SqlClient spelling, which it parses as an unknown keyword.

    Read-only: each connection runs a scalar probe and closes. Rendered
    connection strings are never written to the console or a log.

.EXAMPLE
    powershell -File validation/runtime/Test-ConnectionContracts.ps1
#>

[CmdletBinding()]
param(
    [string] $OracleConnectionManager = 'ssis/01_oracle_extract/WWI_Oracle_ERP.conmgr',
    [string] $SqlServerConnectionManager = 'ssis/04_staging/WWI_Source_DB.conmgr',
    [int]    $TimeoutSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot 'deployment/lib/Common.ps1')
$script:WwiLogPrefix = 'wwi-connection-contract'

$required = @('ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_USER', 'ORACLE_PASSWORD',
              'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
              'SQLSERVER_OLTP_DB')
$missing = $required | Where-Object { -not (Get-Item -Path "env:$_" -ErrorAction SilentlyContinue) }
if ($missing) {
    Write-WwiLog "SKIP no credentials in the environment: $($missing -join ', ')"
    exit 0
}

function Get-ConnectionString {
    <#
        The rendered string holds a password, so it is returned to the caller
        and never logged. --unmasked is what makes the renderer emit it.
    #>
    param(
        [Parameter(Mandatory)][string]   $ConnectionManager,
        [string]   $PasswordEnv = '',
        [string[]] $Set = @()
    )
    $arguments = @((Join-Path $repoRoot 'validation/checks/connection_expression.py'),
                   (Join-Path $repoRoot $ConnectionManager), '--unmasked')
    if ($PasswordEnv) { $arguments += @('--password-env', $PasswordEnv) }
    foreach ($assignment in $Set) { $arguments += @('--set', $assignment) }
    $rendered = & python @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "could not render $ConnectionManager"
    }
    return ($rendered | Select-Object -First 1)
}

function Test-OleDbConnection {
    param(
        [Parameter(Mandatory)][string] $ConnectionString,
        [Parameter(Mandatory)][string] $Probe
    )
    $connection = New-Object System.Data.OleDb.OleDbConnection $ConnectionString
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = $Probe
        $command.CommandTimeout = $TimeoutSeconds
        [void] $command.ExecuteScalar()
        return @{ Succeeded = $true; Error = '' }
    }
    catch {
        # The message can echo the connection string, so only the provider's
        # first line is kept and the password keyword is blanked out of it.
        $message = ($_.Exception.Message -split "`n")[0]
        $message = [regex]::Replace($message, '(?i)(password\s*=)[^;]*', '$1********')
        return @{ Succeeded = $false; Error = $message }
    }
    finally {
        $connection.Dispose()
    }
}

Add-Type -AssemblyName System.Data
$failures = @()

# 1. Oracle: CM.WWI_Oracle_ERP.Password must reach OraOLEDB.
$oracle = Test-OleDbConnection -Probe 'SELECT 1 FROM DUAL' -ConnectionString (
    Get-ConnectionString $OracleConnectionManager -PasswordEnv 'ORACLE_PASSWORD')
if ($oracle.Succeeded) {
    Write-WwiLog 'PASS  Oracle contract authenticates with the runtime OraclePassword'
} else {
    $failures += "Oracle contract did not authenticate: $($oracle.Error)"
    Write-WwiLog "FAIL  Oracle contract did not authenticate: $($oracle.Error)"
}

# 2. Wrong password must be refused - proves the password is what authenticated
#    above, rather than an ambient credential the provider fell back on.
$wrongOracle = Test-OleDbConnection -Probe 'SELECT 1 FROM DUAL' -ConnectionString (
    Get-ConnectionString $OracleConnectionManager -Set @('OraclePassword=not-the-password'))
if ($wrongOracle.Succeeded) {
    $failures += 'Oracle contract connected with a wrong password; the password is not being consumed'
    Write-WwiLog 'FAIL  Oracle contract connected with a wrong password'
} else {
    Write-WwiLog 'PASS  Oracle contract refuses a wrong runtime OraclePassword'
}

# 3. SQL Server: SQL authentication through $Project::SqlServerPassword with
#    the TLS keyword OLE DB Driver 19 actually parses.
$sql = Test-OleDbConnection -Probe 'SELECT 1' -ConnectionString (
    Get-ConnectionString $SqlServerConnectionManager -PasswordEnv 'SQLSERVER_PASSWORD')
if ($sql.Succeeded) {
    Write-WwiLog 'PASS  SQL Server contract authenticates with the runtime SqlServerPassword'
} else {
    $failures += "SQL Server contract did not authenticate: $($sql.Error)"
    Write-WwiLog "FAIL  SQL Server contract did not authenticate: $($sql.Error)"
}

$wrongSql = Test-OleDbConnection -Probe 'SELECT 1' -ConnectionString (
    Get-ConnectionString $SqlServerConnectionManager -Set @('SqlServerPassword=not-the-password'))
if ($wrongSql.Succeeded) {
    $failures += 'SQL Server contract connected with a wrong password; the password is not being consumed'
    Write-WwiLog 'FAIL  SQL Server contract connected with a wrong password'
} else {
    Write-WwiLog 'PASS  SQL Server contract refuses a wrong runtime SqlServerPassword'
}

# 4. The TLS keyword itself. The unspaced spelling is the SqlClient one; OLE DB
#    Driver 19 does not recognise it, so it cannot switch certificate
#    validation off. Only assert on it when the certificate is in fact
#    untrusted, otherwise there is nothing for the keyword to do.
$correct = Get-ConnectionString $SqlServerConnectionManager -PasswordEnv 'SQLSERVER_PASSWORD'
$unspaced = $correct.Replace('Trust Server Certificate=True;', 'TrustServerCertificate=True;')
$untrusted = Test-OleDbConnection -Probe 'SELECT 1' -ConnectionString (
    $correct.Replace('Trust Server Certificate=True;', ''))
if ($untrusted.Succeeded) {
    Write-WwiLog 'NOTE  the server certificate is trusted here; the TLS keyword is not exercised'
} else {
    $wrongKeyword = Test-OleDbConnection -Probe 'SELECT 1' -ConnectionString $unspaced
    if ($wrongKeyword.Succeeded) {
        $failures += 'MSOLEDBSQL19 accepted the unspaced TrustServerCertificate keyword'
        Write-WwiLog 'FAIL  MSOLEDBSQL19 accepted the unspaced TrustServerCertificate keyword'
    } else {
        Write-WwiLog 'PASS  MSOLEDBSQL19 needs the spaced Trust Server Certificate keyword'
    }
}

Write-WwiLog "$($failures.Count) failure(s)"
if ($failures) { exit 1 }
exit 0
