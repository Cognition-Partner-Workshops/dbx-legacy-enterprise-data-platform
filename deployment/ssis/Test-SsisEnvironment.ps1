<#
    Read-only proof that the catalog environment is really bound.

    A deployment that "succeeded" and an environment that resolves are different
    claims: a reference can exist with no variables behind it, and a sensitive
    parameter can hold a literal that looks identical from the outside. This
    reads catalog.environments, environment_variables, environment_references
    and object_parameters and fails on anything the estate cannot execute with.

    Reads work over SQL authentication, so unlike the deploy and environment
    steps this one runs from anywhere.

    Usage: .\deployment\ssis\Test-SsisEnvironment.ps1 [-Folder WWI_DEV] [-Environment DEV]
#>

[CmdletBinding()]
param(
    [string] $Folder = 'WWI_DEV',
    [string] $EnvironmentName = 'WWI_DEV',
    [int] $ExpectedProjectCount = 17
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'SsisCatalog.ps1')
$script:WwiLogPrefix = 'wwi-verify-ssis-env'

$failures = New-Object System.Collections.Generic.List[string]

$environment = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ f = $Folder; e = $EnvironmentName } -Query @'
SELECT COUNT(*) AS environment_count
FROM catalog.environments AS e
INNER JOIN catalog.folders AS f ON f.folder_id = e.folder_id
WHERE f.name = @f AND e.name = @e;
'@
if ([int] $environment[0].environment_count -ne 1) {
    $failures.Add("environment $Folder/$EnvironmentName does not exist exactly once")
}

$variables = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ f = $Folder; e = $EnvironmentName } -Query @'
SELECT v.name, v.type, v.sensitive
FROM catalog.environment_variables AS v
INNER JOIN catalog.environments AS e ON e.environment_id = v.environment_id
INNER JOIN catalog.folders AS f ON f.folder_id = e.folder_id
WHERE f.name = @f AND e.name = @e
ORDER BY v.name;
'@
Write-WwiLog ("{0}/{1}: {2} environment variable(s)" -f $Folder, $EnvironmentName, $variables.Count)

foreach ($required in @('OraclePassword', 'SqlServerPassword')) {
    $match = $variables | Where-Object { $_.name -eq $required }
    if (-not $match) { $failures.Add("environment variable $required is missing") }
    elseif ([int] $match.sensitive -ne 1) { $failures.Add("environment variable $required is not sensitive") }
}

$references = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ f = $Folder; e = $EnvironmentName } -Query @'
SELECT p.name AS project_name, COUNT(r.reference_id) AS reference_count
FROM catalog.projects AS p
INNER JOIN catalog.folders AS f ON f.folder_id = p.folder_id
LEFT JOIN catalog.environment_references AS r
       ON r.project_id = p.project_id AND r.environment_name = @e
WHERE f.name = @f
GROUP BY p.name
ORDER BY p.name;
'@
if ($references.Count -ne $ExpectedProjectCount) {
    $failures.Add("expected $ExpectedProjectCount project(s) in $Folder, found $($references.Count)")
}
foreach ($row in $references) {
    if ([int] $row.reference_count -ne 1) {
        $failures.Add("$($row.project_name) has $($row.reference_count) reference(s) to $EnvironmentName")
    }
}

$parameters = Invoke-WwiSqlQuery -Database 'SSISDB' -Parameters @{ f = $Folder } -Query @'
SELECT p.name AS project_name, op.parameter_name, op.value_type, op.sensitive,
       op.referenced_variable_name
FROM catalog.object_parameters AS op
INNER JOIN catalog.projects AS p ON p.project_id = op.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @f AND op.object_type = 20
ORDER BY p.name, op.parameter_name;
'@

$literalSensitive = @($parameters | Where-Object { [int] $_.sensitive -eq 1 -and $_.value_type -eq 'V' })
foreach ($row in $literalSensitive) {
    $failures.Add("$($row.project_name).$($row.parameter_name) holds a literal instead of a reference")
}

$passwordParameters = @($parameters | Where-Object { $_.parameter_name -like 'CM.*.Password' })
$unbound = @($passwordParameters | Where-Object { $_.value_type -ne 'R' })
foreach ($row in $unbound) {
    $failures.Add("$($row.project_name).$($row.parameter_name) is not bound by reference")
}
Write-WwiLog ("{0} connection manager password parameter(s), {1} bound by reference" -f
    $passwordParameters.Count, ($passwordParameters.Count - $unbound.Count))

$dangling = @($parameters | Where-Object {
    $_.value_type -eq 'R' -and $variables.name -notcontains $_.referenced_variable_name })
foreach ($row in $dangling) {
    $failures.Add("$($row.project_name).$($row.parameter_name) references missing variable $($row.referenced_variable_name)")
}
Write-WwiLog ("{0} project parameter(s), {1} bound by reference" -f $parameters.Count,
    @($parameters | Where-Object { $_.value_type -eq 'R' }).Count)

if ($failures.Count -gt 0) {
    foreach ($failure in $failures) { Write-WwiLog $failure 'ERROR' }
    Stop-WwiWithError "the $Folder/$EnvironmentName environment binding is not usable ($($failures.Count) problem(s))."
}

Write-WwiLog "environment $Folder/$EnvironmentName verified"
