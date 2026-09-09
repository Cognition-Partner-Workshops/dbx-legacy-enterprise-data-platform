<#
    Validates deployed packages against the live connections without running them.

    A package that builds, deploys and matches the catalog byte for byte can
    still be unable to start: the connection managers are only resolved, and the
    tasks and pipeline components only asked whether they can run, when the
    runtime validates the package. Six live runs of this estate have each burned
    an execution to surface one more defect that validation reports on its own,
    so validation is run here as its own gate.

    catalog.validate_package is that gate. It resolves the project parameters
    through the environment reference exactly as an execution would, opens the
    connections, and asks every task and component to validate - and it stops
    there: no data flow runs, nothing is read from a source and nothing is
    written to a target.

    A validation is judged on its messages, not on its status. Errors fail it.
    So do the metadata warnings that SSIS reports while still returning success -
    VS_NEEDSNEWMETADATA, external columns out of synchronization, flat-file
    column information, and parameter or result-set binding type mismatches -
    because each of them is a package that will fail, or silently drop columns,
    the first time it is executed against the live schema.

    Runs on the catalog host with integrated authentication: SSISDB refuses to
    start a validation for a SQL-authenticated login.

    Usage:
      .\Test-SsisPackageValidation.ps1 -Root Master_File_Ingestion
      .\Test-SsisPackageValidation.ps1 -Family ING_FILE_*,DQ_*,STG_*
      .\Test-SsisPackageValidation.ps1 -Package WWI_Ingest_Files/ING_FILE_CarrierScan
#>

[CmdletBinding()]
param(
    [string] $Server = $(if ($env:SSIS_SERVER) { $env:SSIS_SERVER } else { 'localhost' }),
    [string] $Folder = $(if ($env:SSIS_FOLDER) { $env:SSIS_FOLDER } else { 'WWI_DEV' }),
    [string] $Environment = $(if ($env:SSIS_ENVIRONMENT) { $env:SSIS_ENVIRONMENT } else { 'WWI_DEV' }),
    # Orchestration roots to expand into their child packages, from the plan.
    [string[]] $Root = @(),
    # Package name wildcards, matched against the packages the catalog holds.
    [string[]] $Family = @(),
    # Explicit <project>/<package> pairs.
    [string[]] $Package = @(),
    [string] $PlanPath,
    [string] $ResultPath,
    [int] $TimeoutSeconds = 900
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-ValidationLog {
    param([string] $Message, [string] $Level = 'INFO')
    Write-Host ("[{0}] wwi-package-validation {1} {2}" -f (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'), $Level, $Message)
}

# A login name is a credential in this estate: SQL Server names it in full in
# 'Login failed for user ...', and this log is shipped off the host.
function Hide-Principal {
    param([string] $Text)
    if (-not $Text) { return $Text }
    return [regex]::Replace($Text, "(?i)(login failed for user\s+')[^']*(')", '${1}<redacted>${2}')
}

# Warnings SSIS emits while still reporting the validation successful, each of
# which is a package that cannot run correctly against the live schema.
$script:MetadataWarningPatterns = @(
    'VS_NEEDSNEWMETADATA',
    'out of synchronization',
    'external metadata column',
    'external columns',
    'column information',
    'was not found in the datasource',
    'cannot convert between unicode and non-unicode',
    'the data type of the parameter',
    'the type of the value being assigned'
)

function Test-MetadataWarning {
    param([string] $Message)
    foreach ($pattern in $script:MetadataWarningPatterns) {
        if ($Message -and $Message.ToLowerInvariant().Contains($pattern.ToLowerInvariant())) { return $true }
    }
    return $false
}

$connectionString = "Server=$Server;Database=SSISDB;Integrated Security=SSPI;TrustServerCertificate=True;Connect Timeout=30"
$connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
$connection.Open()

function Invoke-Rows {
    param([string] $Sql, [hashtable] $Parameters = @{})
    $command = $connection.CreateCommand()
    $command.CommandTimeout = 300
    $command.CommandText = $Sql
    foreach ($name in $Parameters.Keys) {
        $parameter = $command.CreateParameter()
        $parameter.ParameterName = "@$name"
        $parameter.Value = $Parameters[$name]
        $command.Parameters.Add($parameter) | Out-Null
    }
    $rows = @()
    $reader = $command.ExecuteReader()
    try {
        while ($reader.Read()) {
            $row = @{}
            for ($i = 0; $i -lt $reader.FieldCount; $i++) {
                $row[$reader.GetName($i)] = if ($reader.IsDBNull($i)) { $null } else { $reader.GetValue($i) }
            }
            $rows += , $row
        }
    }
    finally { $reader.Close() }
    # Rows leave one at a time; every caller wraps the call in @(), because a
    # single row would otherwise arrive as a bare row rather than a list.
    return $rows
}

try {
    # -- what to validate ----------------------------------------------------

    $targets = [ordered] @{}

    foreach ($pair in $Package) {
        $parts = $pair -split '/'
        if ($parts.Count -ne 2) { throw "-Package expects <project>/<package>, got '$pair'" }
        $targets["$($parts[0])/$($parts[1])"] = @{ Project = $parts[0]; Package = $parts[1] }
    }

    if ($Root.Count -gt 0) {
        if (-not $PlanPath) { $PlanPath = Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) 'ssis\orchestration-plan.json' }
        if (-not (Test-Path -LiteralPath $PlanPath)) {
            throw "the orchestration plan $PlanPath is not here; -Root cannot be expanded without it"
        }
        $plan = Get-Content -Raw -LiteralPath $PlanPath | ConvertFrom-Json
        foreach ($name in $Root) {
            $node = @($plan.roots | Where-Object { $_.root -eq $name })
            if ($node.Count -ne 1) { throw "the plan has no single root named $name" }
            $targets["$($node[0].project)/$($node[0].root)"] = @{ Project = $node[0].project; Package = $node[0].root }
            foreach ($phase in $node[0].nodes) {
                if (-not ($phase.PSObject.Properties.Name -contains 'children')) { continue }
                foreach ($child in $phase.children) {
                    $targets["$($child.project)/$($child.package)"] = @{ Project = $child.project; Package = $child.package }
                }
            }
        }
    }

    if ($Family.Count -gt 0) {
        $all = @(Invoke-Rows -Sql @'
SELECT p.name AS project, k.name AS package
FROM catalog.packages AS k
INNER JOIN catalog.projects AS p ON p.project_id = k.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @folder;
'@ -Parameters @{ folder = $Folder })
        foreach ($row in $all) {
            $bare = [System.IO.Path]::GetFileNameWithoutExtension([string] $row.package)
            foreach ($pattern in $Family) {
                if ($bare -like $pattern) {
                    $targets["$($row.project)/$bare"] = @{ Project = [string] $row.project; Package = $bare }
                }
            }
        }
    }

    if ($targets.Count -eq 0) { throw 'nothing to validate: pass -Root, -Family or -Package.' }

    # -- the environment reference each project carries ----------------------

    $references = @{}
    $referenceRows = @(Invoke-Rows -Sql @'
SELECT p.name AS project, r.reference_id
FROM catalog.environment_references AS r
INNER JOIN catalog.projects AS p ON p.project_id = r.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @folder AND r.environment_name = @environment;
'@ -Parameters @{ folder = $Folder; environment = $Environment })
    foreach ($row in $referenceRows) {
        $references[[string] $row.project] = [long] $row.reference_id
    }

    # -- validate ------------------------------------------------------------

    $results = @()
    $failures = 0

    foreach ($key in $targets.Keys) {
        $target = $targets[$key]
        $project = $target.Project
        $package = "$($target.Package).dtsx"

        if (-not $references.ContainsKey($project)) {
            Write-ValidationLog "$key has no reference to environment $Environment; deploy the environment first" 'ERROR'
            $results += [pscustomobject] @{ Project = $project; Package = $target.Package; Status = 'no-reference'; Errors = @(); MetadataWarnings = @() }
            $failures++
            continue
        }

        $start = $connection.CreateCommand()
        $start.CommandTimeout = 300
        $start.CommandText = 'catalog.validate_package'
        $start.CommandType = [System.Data.CommandType]::StoredProcedure
        $start.Parameters.Add('@folder_name', [System.Data.SqlDbType]::NVarChar, 128).Value = [string] $Folder
        $start.Parameters.Add('@project_name', [System.Data.SqlDbType]::NVarChar, 128).Value = [string] $project
        $start.Parameters.Add('@package_name', [System.Data.SqlDbType]::NVarChar, 260).Value = [string] $package
        $start.Parameters.Add('@use32bitruntime', [System.Data.SqlDbType]::Bit).Value = 0
        $start.Parameters.Add('@environment_scope', [System.Data.SqlDbType]::Char, 1).Value = 'S'
        $start.Parameters.Add('@reference_id', [System.Data.SqlDbType]::BigInt).Value = $references[$project]
        $identifier = $start.Parameters.Add('@validation_id', [System.Data.SqlDbType]::BigInt)
        $identifier.Direction = [System.Data.ParameterDirection]::Output
        $start.ExecuteNonQuery() | Out-Null
        $validationId = [long] $identifier.Value

        # The catalog runs the validation out of process; 1 created, 2 running,
        # 5 pending and 8 stopping are all still in flight.
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
        do {
            Start-Sleep -Milliseconds 1500
            $state = @(Invoke-Rows -Sql 'SELECT status FROM catalog.operations WHERE operation_id = @id;' -Parameters @{ id = $validationId })
            $status = if ($state.Count -eq 1) { [int] $state[0].status } else { -1 }
        } while ($status -in @(1, 2, 5, 8) -and (Get-Date) -lt $deadline)

        if ($status -in @(1, 2, 5, 8)) {
            throw "validation $validationId of $key did not finish within $TimeoutSeconds seconds"
        }

        $messages = @(Invoke-Rows -Sql @'
SELECT message_type, message
FROM catalog.operation_messages
WHERE operation_id = @id
ORDER BY message_time, operation_message_id;
'@ -Parameters @{ id = $validationId })

        $errors = @()
        $metadata = @()
        foreach ($row in $messages) {
            $text = Hide-Principal ([string] $row.message)
            switch ([int] $row.message_type) {
                120 { $errors += $text }
                130 { $errors += $text }   # task failed
                110 { if (Test-MetadataWarning $text) { $metadata += $text } }
                default { }
            }
        }

        $verdict = if ($errors.Count -gt 0 -or $metadata.Count -gt 0) { 'FAIL' } elseif ($status -eq 7) { 'PASS' } else { 'FAIL' }
        if ($verdict -ne 'PASS') { $failures++ }

        $results += [pscustomobject] @{
            Project          = $project
            Package          = $target.Package
            Status           = $status
            Verdict          = $verdict
            Errors           = $errors
            MetadataWarnings = $metadata
        }

        Write-ValidationLog ("{0,-6} {1,-22} {2,-30} status {3}, {4} error(s), {5} metadata warning(s)" -f
            $verdict, $project, $target.Package, $status, $errors.Count, $metadata.Count) $(if ($verdict -eq 'PASS') { 'INFO' } else { 'ERROR' })
        foreach ($text in ($errors + $metadata | Select-Object -First 6)) {
            Write-ValidationLog ("    {0}" -f $text) 'ERROR'
        }
    }

    if ($ResultPath) {
        $results | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $ResultPath -Encoding UTF8
        Write-ValidationLog "wrote $ResultPath"
    }

    Write-ValidationLog ("{0} package(s) validated against the live connections, {1} failure(s)" -f $results.Count, $failures)
    if ($failures -gt 0) { exit 1 }
}
finally { $connection.Close() }
