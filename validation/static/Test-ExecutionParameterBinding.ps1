<#
    Offline tests for the catalog execution parameter binding.

    The live failure they stand in for: the runner bound every package
    parameter as text, catalog.set_execution_parameter_value compared the
    sql_variant base type against the Int32 the package declares, refused it,
    and left the execution create_execution had already committed stranded in
    status 1 with nothing in catalog.operation_messages.

    Nothing here touches SSISDB: the declaration map is the shape
    Get-WwiPackageParameterDeclaration returns, so the conversion the runner
    performs before an execution exists is exercised on its own.

    Usage:
        pwsh -File validation/static/Test-ExecutionParameterBinding.ps1
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot 'deployment\lib\Common.ps1')
. (Join-Path $repoRoot 'deployment\lib\SsisCatalog.ps1')

function New-Declaration {
    param([Parameter(Mandatory)][string] $DataType, [switch] $Sensitive)
    return [pscustomobject] @{ DataType = $DataType; Sensitive = [bool] $Sensitive }
}

$declared = @{
    'BatchId'           = New-Declaration -DataType 'Int32'
    'ReloadFullHistory' = New-Declaration -DataType 'Boolean'
    'SourceSystemCode'  = New-Declaration -DataType 'String'
    'SqlServerPassword' = New-Declaration -DataType 'String' -Sensitive
    'Opaque'            = New-Declaration -DataType 'Object'
}

$failures = @()

function Test-Case {
    param([Parameter(Mandatory)][string] $Name, [Parameter(Mandatory)][scriptblock] $Body)
    try {
        & $Body
        Write-Host ("PASS  {0}" -f $Name)
    }
    catch {
        $script:failures += ("{0}: {1}" -f $Name, $_.Exception.Message)
        Write-Host ("FAIL  {0}: {1}" -f $Name, $_.Exception.Message)
    }
}

function Assert-Throws {
    param([Parameter(Mandatory)][scriptblock] $Body, [Parameter(Mandatory)][string] $Expected)
    try { & $Body }
    catch {
        if ($_.Exception.Message -notlike "*$Expected*") {
            throw "expected a message like '$Expected' but got '$($_.Exception.Message)'"
        }
        return
    }
    throw "expected a failure mentioning '$Expected', but the call succeeded"
}

Test-Case 'a string batch id is bound as Int32' {
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ BatchId = '3' })
    if ($bound['BatchId'].GetType() -ne [int32]) { throw "bound as $($bound['BatchId'].GetType().Name)" }
    if ($bound['BatchId'] -ne 3) { throw "bound as $($bound['BatchId'])" }
}

Test-Case 'an Int32 batch id stays Int32' {
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ BatchId = [int32] 7 })
    if ($bound['BatchId'].GetType() -ne [int32]) { throw "bound as $($bound['BatchId'].GetType().Name)" }
}

Test-Case 'an Int64 batch id narrows to the declared Int32' {
    # etl.usp_StartBatch returns BIGINT, so this is what the runner really holds.
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ BatchId = [int64] 3 })
    if ($bound['BatchId'].GetType() -ne [int32]) { throw "bound as $($bound['BatchId'].GetType().Name)" }
}

Test-Case 'a boolean parameter is bound as Boolean' {
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ ReloadFullHistory = 'True' })
    if ($bound['ReloadFullHistory'].GetType() -ne [bool]) {
        throw "bound as $($bound['ReloadFullHistory'].GetType().Name)"
    }
}

Test-Case 'a string parameter is bound as String' {
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ SourceSystemCode = 'PARTNER_FL' })
    if ($bound['SourceSystemCode'].GetType() -ne [string]) {
        throw "bound as $($bound['SourceSystemCode'].GetType().Name)"
    }
}

Test-Case 'a null is bound as DBNull' {
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
        -Parameters ([ordered] @{ SourceSystemCode = $null })
    if ($bound['SourceSystemCode'] -isnot [System.DBNull]) { throw 'null was not bound as DBNull' }
}

Test-Case 'a value that cannot convert fails before an execution exists' {
    Assert-Throws -Expected 'does not convert' -Body {
        ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
            -Parameters ([ordered] @{ BatchId = 'not-a-number' })
    }
}

Test-Case 'a parameter the package does not declare fails' {
    Assert-Throws -Expected 'does not declare a package parameter named' -Body {
        ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
            -Parameters ([ordered] @{ BusinessDate = '2026-09-08' })
    }
}

Test-Case 'a sensitive parameter is never set from the runner' {
    Assert-Throws -Expected 'sensitive parameter' -Body {
        ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
            -Parameters ([ordered] @{ SqlServerPassword = 'anything' })
    }
}

Test-Case 'a declared type the runner cannot bind fails loudly' {
    Assert-Throws -Expected 'no binding for' -Body {
        ConvertTo-WwiExecutionParameter -Declared $declared -Package 'ING_FILE_PartnerSales_NA' `
            -Parameters ([ordered] @{ Opaque = 1 })
    }
}

Write-Host ''
if ($failures.Count -gt 0) {
    Write-Host ("{0} test(s) failed" -f $failures.Count)
    exit 1
}
Write-Host 'all execution parameter binding tests passed'
