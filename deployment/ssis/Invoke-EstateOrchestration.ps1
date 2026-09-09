<#
    Runs one master's orchestration from ssis/orchestration-plan.json.

    The estate is deployed as 17 project-deployment projects in one SSISDB
    folder. An Execute Package Task with UseProjectReference resolves its child
    inside the executing project, so a master in WWI_Orchestration cannot reach
    a child held in another project; those edges are generated into the plan
    instead of into the packages, and this driver is what executes them: one
    catalog execution per child, against the project that owns it, in the order
    the plan records.

    Execution is through the catalog, not through dtexec against an .ispac on
    disk. Each child is a catalog.create_execution against the deployed project
    with the project's environment reference attached, run SYNCHRONIZED so
    start_execution returns only when the package has finished, and judged on
    catalog.executions.status plus the estate's own row counts - never on a
    process exit code, which for a synchronous catalog execution says nothing
    about the package at all.

    That also removes the runner from the credential path entirely: the
    passwords live in the SSISDB environment as sensitive variables bound to the
    connection managers' CM.<connection>.Password parameters, so no secret is
    read, passed or logged here.

    The control framework the master owns (etl.usp_StartBatch, a batch step per
    phase, the reconciliation assert, etl.usp_EndBatch) is reproduced here so a
    child still runs inside the step its phase opened. A batch this runner opens
    is always closed: a fault anywhere in the walk logs to etl.ErrorLog and ends
    the batch Failed rather than leaving it Running for the next operator.

    The catalog half of the run authenticates as the Windows principal.
    catalog.create_execution refuses a SQL login outright, so the runner belongs
    on the catalog host - Invoke-EstateOrchestrationRemote.ps1 puts it there -
    while the control framework keeps using SQLSERVER_USER against the staging
    database. Both are checked before the first batch is opened.

    Usage:
      .\deployment\ssis\Invoke-EstateOrchestration.ps1 -Root Master_Daily_ETL `
          [-LogPath logs] [-AdoptRunning] [-DryRun]
      .\deployment\ssis\Invoke-EstateOrchestration.ps1 -ListRoots
#>

[CmdletBinding()]
param(
    [string]   $Root,
    [switch]   $ListRoots,
    [string]   $LogPath = 'logs/orchestration',
    [string]   $PlanPath,
    [int]      $LoggingLevel = 1,
    [string]   $BusinessDate = ((Get-Date).ToUniversalTime().AddDays(-1).ToString('yyyy-MM-dd')),
    [switch]   $AdoptRunning,
    [switch]   $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'SsisCatalog.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'PlanExpression.ps1')
$script:WwiLogPrefix = 'wwi-orchestrate'

$repoRoot = Get-WwiRepositoryRoot
if (-not $PlanPath) { $PlanPath = Join-Path (Join-Path $repoRoot 'ssis') 'orchestration-plan.json' }
if (-not (Test-Path $PlanPath)) {
    Stop-WwiWithError "orchestration plan not found at $PlanPath; run ssis/00_orchestration/build_orchestration_packages.py."
}
$plan = Get-Content -Raw -Path $PlanPath | ConvertFrom-Json

if ($ListRoots) {
    foreach ($rootPlan in $plan.roots) {
        Write-Host ("{0,-32} {1}" -f $rootPlan.root, $rootPlan.description)
    }
    return
}
if (-not $Root) { Stop-WwiWithError 'specify -Root <master package> or -ListRoots.' }

$rootPlan = $plan.roots | Where-Object { $_.root -eq $Root }
if (-not $rootPlan) { Stop-WwiWithError "the plan holds no root named '$Root'." }

# The runner needs the catalog and the control database. It deliberately does
# not require ORACLE_PASSWORD or SQLSERVER_PASSWORD as package inputs: those are
# sensitive SSISDB environment variables bound to the connection managers, and
# nothing here ever reads them.
Assert-WwiEnvironmentVariable @(
    'SSIS_SERVER', 'SSIS_FOLDER', 'SSIS_ENVIRONMENT',
    'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
    'SQLSERVER_STAGING_DB')

# Roots that execute a file ingestion package read their input from the catalog
# host's file system, so those runs additionally need the landing zone.
$fileChildren = @()
foreach ($node in $rootPlan.nodes) {
    if ($node.PSObject.Properties.Name -contains 'children') {
        $fileChildren += @($node.children | Where-Object { $_.project -eq 'WWI_Ingest_Files' })
    }
    if (($node.PSObject.Properties.Name -contains 'project') -and $node.project -eq 'WWI_Ingest_Files') {
        $fileChildren += $node
    }
}
$needsLandingZone = @($fileChildren).Count -gt 0
if ($needsLandingZone) { Assert-WwiEnvironmentVariable @('WWI_LANDING_ROOT') }
$environmentCode = Get-WwiEnvironmentCode
Confirm-WwiProduction

$folder      = $env:SSIS_FOLDER
$environment = $env:SSIS_ENVIRONMENT
$logs = if ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $repoRoot $LogPath }
if (-not $DryRun -and -not (Test-Path $logs)) { New-Item -ItemType Directory -Path $logs -Force | Out-Null }

# ---------------------------------------------------------------------------
# control framework
# ---------------------------------------------------------------------------

function Invoke-EtlScalar {
    <# Runs one statement against the staging database and returns its scalar. #>
    param([Parameter(Mandatory)][string] $Query, [hashtable] $Parameters = @{})

    if ($DryRun) {
        Write-WwiLog ("WHATIF control {0}" -f ($Query -replace '\s+', ' '))
        return 0
    }
    $rows = Invoke-WwiSqlQuery -Database $env:SQLSERVER_STAGING_DB -Query $Query -Parameters $Parameters
    if (@($rows).Count -eq 0) { return 0 }
    $first = $rows[0]
    $value = $first.PSObject.Properties | Select-Object -First 1 | ForEach-Object { $_.Value }
    return ($value -as [long])
}

function Start-EtlBatch {
    <#
        Opens the batch, or adopts the one this name and business date already
        has open when -AdoptRunning is given.

        Adoption is the recovery rerun etl.usp_StartBatch documents, and it is
        the operator's decision rather than the runner's: adopting silently
        would fold a second run into a batch whose first run may still be
        moving rows, so the default stays 0 and the switch has to be asked for.
    #>
    param([Parameter(Mandatory)][string] $BatchType)
    $adopt = if ($AdoptRunning) { 1 } else { 0 }
    $query = @"
SET NOCOUNT ON;
DECLARE @BatchId BIGINT;
EXEC etl.usp_StartBatch @BatchName = N'$Root', @BatchType = N'$BatchType',
     @BusinessDate = N'$BusinessDate', @EnvironmentCode = N'$environmentCode',
     @AllowAdoptRunning = $adopt, @Notes = N'external orchestration runner',
     @BatchId = @BatchId OUTPUT;
SELECT @BatchId;
"@
    return Invoke-EtlScalar -Query $query
}

function Start-EtlStep {
    param([Parameter(Mandatory)][long] $BatchId, [Parameter(Mandatory)] $Node)
    $query = @"
SET NOCOUNT ON;
DECLARE @BatchStepId BIGINT;
EXEC etl.usp_StartBatchStep @BatchId = $BatchId, @StepName = N'$($Node.name)',
     @StepSequence = $($Node.sequence), @StepGroup = N'$($Node.group)',
     @BatchStepId = @BatchStepId OUTPUT;
SELECT @BatchStepId;
"@
    return Invoke-EtlScalar -Query $query
}

function Stop-EtlStep {
    param([Parameter(Mandatory)][long] $BatchStepId, [Parameter(Mandatory)][string] $Status)
    Invoke-EtlScalar -Query "EXEC etl.usp_EndBatchStep @BatchStepId = $BatchStepId, @Status = N'$Status';" | Out-Null
}

function Stop-EtlBatch {
    param([Parameter(Mandatory)][long] $BatchId, [string] $ForceStatus)
    $force = if ($ForceStatus) { ", @ForceStatus = N'$ForceStatus'" } else { '' }
    Invoke-EtlScalar -Query "EXEC etl.usp_EndBatch @BatchId = $BatchId$force;" | Out-Null
}

function Assert-EtlReconciliation {
    param([Parameter(Mandatory)][long] $BatchId, [int] $RaiseOnFailure = 1)
    $query = @"
SET NOCOUNT ON;
DECLARE @FailedObjectCount INT;
EXEC etl.usp_AssertRowCountReconciliation @BatchId = $BatchId,
     @RaiseOnFailure = $RaiseOnFailure, @FailedObjectCount = @FailedObjectCount OUTPUT;
SELECT @FailedObjectCount;
"@
    return Invoke-EtlScalar -Query $query
}

function Test-EtlBatchRunning {
    param([Parameter(Mandatory)][long] $BatchId)
    if ($BatchId -le 0) { return $false }
    return (Invoke-EtlScalar -Query "SELECT COUNT(*) FROM etl.Batch WHERE BatchId = $BatchId AND Status = N'Running';") -gt 0
}

function Write-EtlError {
    <#
        Records a runner-side fault where the estate keeps every other failure.
        A crash between usp_StartBatch and usp_EndBatch used to leave the batch
        Running with nothing in etl.ErrorLog to say why, which is indistinguish-
        able from a run that is still going.

        Logging is best effort by design: if the control database is what broke,
        the original fault must still be the one the operator sees.
    #>
    param(
        [Parameter(Mandatory)][long] $BatchId,
        [Parameter(Mandatory)][string] $Message,
        [string] $Source = 'Invoke-EstateOrchestration.ps1'
    )

    if ($DryRun) { Write-WwiLog "WHATIF etl.usp_LogError for batch $BatchId"; return }
    if ($BatchId -le 0) { return }
    try {
        Invoke-WwiSqlQuery -Database $env:SQLSERVER_STAGING_DB -Parameters @{
            batch = $BatchId; source = $Source; description = $Message
        } -Query @'
EXEC etl.usp_LogError @BatchId = @batch, @ErrorSeverity = N'Error',
     @SourceName = @source, @ProcedureName = @source, @ErrorDescription = @description;
'@ | Out-Null
    }
    catch {
        Write-WwiLog "could not write etl.ErrorLog for batch ${BatchId}: $($_.Exception.Message)" 'WARN'
    }
}

# ---------------------------------------------------------------------------
# package execution
# ---------------------------------------------------------------------------

$script:Executed = @{}
$script:Results = @()
$script:References = @{}

# catalog.executions.status. 7 is the only one that means the package ran to
# completion successfully; 4 and 3 are the two that look like success in a log.
$script:ExecutionStatusName = @{
    1 = 'Created'; 2 = 'Running';  3 = 'Cancelled'; 4 = 'Failed'
    5 = 'Pending'; 6 = 'Ended unexpectedly'; 7 = 'Succeeded'
    8 = 'Stopping'; 9 = 'Completed'
}

function Get-EstateReference {
    <#
        The project's environment reference id. Every project carries exactly one
        local reference to the DEV environment; an execution created without it
        starts with unbound parameters and fails validation, so a missing
        reference is an error rather than something to run without.
    #>
    param([Parameter(Mandatory)][string] $Project)

    if ($script:References.ContainsKey($Project)) { return $script:References[$Project] }

    $rows = Invoke-WwiSqlQuery -Database 'SSISDB' -Integrated -Parameters @{
        folder = $folder; project = $Project; environment = $environment
    } -Query @'
SELECT r.reference_id, r.reference_type, r.environment_folder_name
FROM catalog.environment_references AS r
INNER JOIN catalog.projects AS p ON p.project_id = r.project_id
INNER JOIN catalog.folders  AS f ON f.folder_id  = p.folder_id
WHERE f.name = @folder AND p.name = @project AND r.environment_name = @environment;
'@

    if (@($rows).Count -eq 0) {
        Stop-WwiWithError ("project $Project has no reference to environment $environment in /SSISDB/$folder. " +
                           'Run deployment/ssis/Deploy-SsisEnvironment.ps1 first.')
    }
    if (@($rows).Count -gt 1) {
        Stop-WwiWithError "project $Project has more than one reference to environment $environment."
    }

    $script:References[$Project] = [long] $rows[0].reference_id
    return $script:References[$Project]
}

function Get-EstateRowCounts {
    <#
        What the package says it moved, from the estate's own control tables.
        Catalog status 7 only says the package did not error; these counts are
        the semantic half of the verdict.
    #>
    param([Parameter(Mandatory)][long] $BatchId, [Parameter(Mandatory)][string] $Package)

    $rows = Invoke-WwiSqlQuery -Database $env:SQLSERVER_STAGING_DB -Parameters @{
        batch = $BatchId; package = $Package
    } -Query @'
SELECT rows_read     = SUM(ISNULL(e.RowsRead, 0)),
       rows_inserted = SUM(ISNULL(e.RowsInserted, 0)),
       executions    = COUNT(*)
FROM etl.PackageExecution AS e
WHERE e.BatchId = @batch AND e.PackageName = @package;
'@
    if (@($rows).Count -eq 0) { return $null }
    return $rows[0]
}

function Invoke-EstatePackage {
    <#
        Runs one deployed package as a catalog execution and decides whether it
        succeeded.

        create_execution -> set_execution_parameter_value (object_type 50 for
        SYNCHRONIZED and LOGGING_LEVEL, 30 for the package's own parameters) ->
        start_execution. With SYNCHRONIZED = 1 start_execution returns when the
        package has finished, and the verdict is read back from
        catalog.executions.status and catalog.operation_messages.
    #>
    param(
        [Parameter(Mandatory)][string] $Package,
        [Parameter(Mandatory)][string] $Project,
        [Parameter(Mandatory)][long]   $BatchId,
        [hashtable] $PackageParameters
    )

    # One package runs once per orchestration; a plan that names the same child
    # under two phases (a retry path, for example) must not load it twice.
    if ($script:Executed.ContainsKey($Package)) {
        Write-WwiLog "SKIP   $Package (already executed in this run)"
        return $script:Executed[$Package]
    }

    $parameters = [ordered] @{ 'BatchId' = $BatchId }
    if ($PackageParameters) {
        foreach ($name in $PackageParameters.Keys) { $parameters[$name] = $PackageParameters[$name] }
    }

    $started = Get-Date
    if ($DryRun) {
        Write-WwiLog ("WHATIF catalog.create_execution /SSISDB/{0}/{1}/{2}.dtsx (reference to {3}, {4})" -f `
            $folder, $Project, $Package, $environment,
            (($parameters.Keys | ForEach-Object { "$_=$($parameters[$_])" }) -join ', '))
        $script:Executed[$Package] = $true
        return $true
    }

    $referenceId = Get-EstateReference -Project $Project

    # Every value is converted to the type the deployed package declares before
    # an execution exists. catalog.set_execution_parameter_value takes a
    # sql_variant and refuses a base type that does not match the declaration -
    # BatchId is Int32 in all 13 subtree packages, so a value sent as a string
    # failed with "The data type of the input value is not compatible with the
    # data type of the 'Int32'" after create_execution had already committed,
    # stranding the execution in status 1 with no operation message.
    $declared = Get-WwiPackageParameterDeclaration -Folder $folder -Project $Project `
                                                   -Package ("$Package.dtsx") -Integrated
    $bound = ConvertTo-WwiExecutionParameter -Declared $declared -Parameters $parameters -Package $Package

    # The package parameters are applied one call at a time, all inside the
    # single batch that also starts the execution, so a failure to set one never
    # leaves a half-configured execution running.
    $setParameters = ''
    $arguments = @{
        folder = $folder; project = $Project; package = ("$Package.dtsx")
        reference = $referenceId; logging = $LoggingLevel
    }
    $index = 0
    foreach ($name in $bound.Keys) {
        $index += 1
        $arguments["pname$index"] = $name
        $arguments["pvalue$index"] = $bound[$name]
        $setParameters += @"

EXEC catalog.set_execution_parameter_value @execution_id,
     @object_type = 30, @parameter_name = @pname$index, @parameter_value = @pvalue$index;
"@
    }

    $rows = Invoke-WwiSqlQuery -Database 'SSISDB' -Integrated -TimeoutSeconds 0 -Parameters $arguments -Query @"
SET NOCOUNT ON;
DECLARE @execution_id BIGINT;

EXEC catalog.create_execution
     @folder_name   = @folder,
     @project_name  = @project,
     @package_name  = @package,
     @reference_id  = @reference,
     @use32bitruntime = 0,
     @execution_id  = @execution_id OUTPUT;

EXEC catalog.set_execution_parameter_value @execution_id,
     @object_type = 50, @parameter_name = N'SYNCHRONIZED', @parameter_value = 1;
EXEC catalog.set_execution_parameter_value @execution_id,
     @object_type = 50, @parameter_name = N'LOGGING_LEVEL', @parameter_value = @logging;
$setParameters

EXEC catalog.start_execution @execution_id;

SELECT execution_id = @execution_id;
"@

    $executionId = [long] $rows[0].execution_id

    $execution = (Invoke-WwiSqlQuery -Database 'SSISDB' -Integrated -Parameters @{ execution = $executionId } -Query @'
SELECT e.status, e.start_time, e.end_time
FROM catalog.executions AS e
WHERE e.execution_id = @execution;
'@)[0]

    $status = [int] $execution.status
    $succeeded = ($status -eq 7)
    $statusName = if ($script:ExecutionStatusName.ContainsKey($status)) { $script:ExecutionStatusName[$status] } else { "status $status" }

    # message_type 120 is an error and 130 a warning; they are the catalog's own
    # account of what happened and are kept next to the run's other logs.
    $messages = Invoke-WwiSqlQuery -Database 'SSISDB' -Integrated -Parameters @{ execution = $executionId } -Query @'
SELECT m.message_time, m.message_type, m.message
FROM catalog.operation_messages AS m
WHERE m.operation_id = @execution AND m.message_type IN (120, 130)
ORDER BY m.message_time;
'@

    $log = Join-Path $logs ("{0}-{1}-{2}.log" -f $Package, $executionId, $started.ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
    $messages | ForEach-Object {
        "{0} {1} {2}" -f $_.message_time, $(if ($_.message_type -eq 120) { 'ERROR' } else { 'WARN ' }), $_.message
    } | Set-Content -Path $log -Encoding UTF8

    $counts = Get-EstateRowCounts -BatchId $BatchId -Package $Package
    if ($succeeded -and $counts -and $counts.executions -eq 0) {
        # The catalog is happy and the package logged nothing: it did not reach
        # the control framework, so this is not a semantic success.
        Write-WwiLog "$Package reported catalog success but wrote no etl.PackageExecution row" 'ERROR'
        $succeeded = $false
    }

    $script:Executed[$Package] = $succeeded
    $script:Results += [pscustomobject] @{
        Package      = $Package
        Project      = $Project
        ExecutionId  = $executionId
        Started      = $started.ToUniversalTime().ToString('s')
        Seconds      = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
        CatalogStatus = $statusName
        RowsRead     = $(if ($counts) { $counts.rows_read } else { $null })
        RowsInserted = $(if ($counts) { $counts.rows_inserted } else { $null })
        Errors       = @($messages | Where-Object { $_.message_type -eq 120 }).Count
        Status       = $(if ($succeeded) { 'Succeeded' } else { 'Failed' })
    }

    if ($succeeded) {
        Write-WwiLog ("OK     {0} (execution {1}, {2} rows inserted)" -f `
            $Package, $executionId, $(if ($counts) { $counts.rows_inserted } else { 'unknown' }))
    }
    else {
        Write-WwiLog "FAILED $Package (execution $executionId, $statusName) - messages in $log" 'ERROR'
        foreach ($message in @($messages | Where-Object { $_.message_type -eq 120 } | Select-Object -First 5)) {
            Write-WwiLog ("       {0}" -f $message.message) 'ERROR'
        }
    }
    return $succeeded
}

# ---------------------------------------------------------------------------
# plan walk
# ---------------------------------------------------------------------------

$nodesByName = @{}
foreach ($node in $rootPlan.nodes) { $nodesByName[$node.name] = $node }

$incoming = @{}
$outgoing = @{}
foreach ($node in $rootPlan.nodes) { $incoming[$node.name] = @(); $outgoing[$node.name] = @() }
foreach ($edge in $rootPlan.edges) {
    $incoming[$edge.to] += $edge
    $outgoing[$edge.from] += $edge
}

# Kahn order over the precedence edges: a node runs only once every node that
# can precede it has been decided, so a skip propagates before it is reached.
$order = @()
$remaining = [System.Collections.ArrayList] @($rootPlan.nodes.name)
$pending = @{}
foreach ($name in $remaining) { $pending[$name] = @($incoming[$name]).Count }
while ($remaining.Count -gt 0) {
    $ready = @($remaining | Where-Object { $pending[$_] -eq 0 })
    if ($ready.Count -eq 0) { Stop-WwiWithError "the plan for $Root is cyclic; cannot order $($remaining -join ', ')." }
    foreach ($name in $ready) {
        $order += $name
        $remaining.Remove($name)
        foreach ($edge in $outgoing[$name]) { $pending[$edge.to] -= 1 }
    }
}

$status = @{}   # node -> Succeeded / Failed / Skipped

# The master's parameters and variables at their declared defaults. A control
# node overwrites an entry as it is reached, and the conditional edges are
# evaluated against this table.
$variables = ConvertTo-WwiPlanVariableTable -RootPlan $rootPlan
$variables['$Package::BusinessDate']    = $BusinessDate
$variables['$Package::EnvironmentCode'] = $environmentCode

function Test-NodeShouldRun {
    <#
        A precedence constraint is its value AND its expression. Honouring the
        value alone takes both sides of a conditional fork - which is how
        ERR_Notify_Operations ran behind @[User::MissingFileCount] > 0 on a
        cycle where every feed had arrived.
    #>
    param([Parameter(Mandatory)][string] $Name)
    return (Test-WwiPlanNodeShouldRun -Name $Name -Edges @($incoming[$Name]) -Status $status `
                                      -Variables $variables -Log { param($m) Write-WwiLog $m })
}

function Invoke-ControlNode {
    <#
        Reproduces an in-package control task well enough for the conditional
        edges that read what it computes.

          expression  an Expression Task assignment, applied to the table
          query       an Execute SQL Task whose single-row result is bound to a
                      variable, re-run against the control database
          statement   an Execute SQL Task that binds nothing; it ordered the
                      master's graph and has no effect the runner reproduces
    #>
    param([Parameter(Mandatory)] $Node)

    $control = if ($Node.PSObject.Properties.Name -contains 'control') { $Node.control } else { 'statement' }
    switch ($control) {
        'expression' {
            $value = Set-WwiPlanAssignment -Assignment $Node.assignment -Variables $variables
            Write-WwiLog ("control {0}: {1} -> {2}" -f $Node.name, $Node.assignment, $value)
        }
        'query' {
            if ($Node.connection -ne 'WWI_Staging_DB') {
                Stop-WwiWithError ("control task $($Node.name) reads $($Node.connection); the runner only " +
                                   'reproduces control queries against the control database.')
            }
            # The task binds its ? placeholders positionally; the same values in
            # the same order become named parameters here.
            $arguments = @{}
            $query = $Node.sql
            $index = 0
            foreach ($name in @($Node.parameters)) {
                if (-not $variables.ContainsKey($name)) {
                    Stop-WwiWithError "control task $($Node.name) binds $name, which this root's plan does not declare."
                }
                $arguments["p$index"] = $variables[$name]
                $query = ([regex] '\?').Replace($query, "@p$index", 1)
                $index += 1
            }
            if ($query.Contains('?')) {
                Stop-WwiWithError "control task $($Node.name) has more ? placeholders than bound parameters."
            }
            if ($DryRun) {
                Write-WwiLog ("WHATIF control {0}; {1} keeps its default {2}" -f
                              $Node.name, $Node.result_variable, $variables[$Node.result_variable])
                return
            }
            $rows = Invoke-WwiSqlQuery -Database $env:SQLSERVER_STAGING_DB -Query "SET NOCOUNT ON;`n$query" -Parameters $arguments
            if (@($rows).Count -eq 0) {
                Stop-WwiWithError "control task $($Node.name) returned no row to bind to $($Node.result_variable)."
            }
            $value = $rows[0].PSObject.Properties | Select-Object -First 1 | ForEach-Object { $_.Value }
            $variables[$Node.result_variable] = ConvertTo-WwiPlanValue $value
            Write-WwiLog ("control {0}: {1} = {2}" -f $Node.name, $Node.result_variable, $variables[$Node.result_variable])
        }
        'statement' {
            Write-WwiLog ("control {0}: ordering only, no runner-side effect" -f $Node.name)
        }
        default {
            Stop-WwiWithError "control task $($Node.name) is of unknown kind '$control'."
        }
    }
}

Write-WwiLog ("orchestrating {0} ({1} node(s), {2} edge(s)) from /SSISDB/{3} with environment {4}" -f `
    $Root, $rootPlan.nodes.Count, $rootPlan.edges.Count, $folder, $environment)

# Everything that can be known before a batch exists is checked before one is
# opened. Both of these used to surface as a failed execution - or, worse, as a
# successful one over an empty directory - after the control framework had
# already recorded a run.
if (-not $DryRun) {
    Assert-WwiCatalogWindowsAuthentication -Database 'SSISDB' | Out-Null
    if ($needsLandingZone) {
        Assert-WwiExecutionHostLandingZone -LandingRoot $env:WWI_LANDING_ROOT -Integrated | Out-Null
    }
}

$batchId = 0
$forceStatus = $null
$fault = $null

try {
foreach ($name in $order) {
    $node = $nodesByName[$name]
    if (-not (Test-NodeShouldRun -Name $name)) {
        $status[$name] = 'Skipped'
        Write-WwiLog "SKIP   $name (precedence not satisfied)"
        continue
    }

    switch ($node.kind) {
        'batch_start' {
            $batchId = Start-EtlBatch -BatchType $node.batch_type
            Write-WwiLog "batch $batchId opened ($($node.batch_type))"
            $variables['User::BatchId'] = $batchId
            $status[$name] = 'Succeeded'
        }
        'batch_end' {
            $force = if ($node.PSObject.Properties.Name -contains 'force_status') { $node.force_status } else { $forceStatus }
            Stop-EtlBatch -BatchId $batchId -ForceStatus $force
            $status[$name] = 'Succeeded'
        }
        'reconcile' {
            try {
                $failed = Assert-EtlReconciliation -BatchId $batchId -RaiseOnFailure $node.raise_on_failure
                Write-WwiLog "reconciliation reported $failed failing object(s)"
                $status[$name] = 'Succeeded'
            }
            catch {
                Write-WwiLog "reconciliation failed: $($_.Exception.Message)" 'ERROR'
                $status[$name] = 'Failed'
                $forceStatus = 'Failed'
            }
        }
        'phase' {
            $stepId = Start-EtlStep -BatchId $batchId -Node $node
            $ok = $true
            foreach ($child in @($node.children)) {
                $parameters = @{}
                # BatchId is supplied by the runner; the remaining assignments
                # are the master's variables/parameters, which have no meaning
                # outside the package that declared them.
                $succeeded = Invoke-EstatePackage -Package $child.package -Project $child.project `
                                                  -BatchId $batchId -PackageParameters $parameters
                if (-not $succeeded) { $ok = $false; break }
            }
            Stop-EtlStep -BatchStepId $stepId -Status $(if ($ok) { 'Succeeded' } else { 'Failed' })
            $status[$name] = $(if ($ok) { 'Succeeded' } else { 'Failed' })
            if (-not $ok) { $forceStatus = 'Failed' }
        }
        'package' {
            $succeeded = Invoke-EstatePackage -Package $node.package -Project $node.project -BatchId $batchId
            $status[$name] = $(if ($succeeded) { 'Succeeded' } else { 'Failed' })
            if (-not $succeeded) { $forceStatus = 'Failed' }
        }
        'control' {
            Invoke-ControlNode -Node $node
            $status[$name] = 'Succeeded'
        }
        default {
            Stop-WwiWithError "the plan node '$name' is of kind '$($node.kind)', which the runner does not model."
        }
    }
}
}
catch {
    $fault = $_
    $forceStatus = 'Failed'
    Write-WwiLog "orchestration faulted: $($_.Exception.Message)" 'ERROR'
    Write-EtlError -BatchId $batchId -Message $_.Exception.Message
}
finally {
    # The batch the runner opened is the runner's to close, on every path out.
    if ($batchId -gt 0) {
        try {
            # The plan's own batch_end has usually closed it already; this is
            # for the paths that never reached that node.
            if (Test-EtlBatchRunning -BatchId $batchId) {
                Stop-EtlBatch -BatchId $batchId -ForceStatus $forceStatus
            }
        }
        catch {
            Write-WwiLog "could not close batch ${batchId}: $($_.Exception.Message)" 'ERROR'
        }
    }
}

if ($fault) {
    Write-WwiLog ("{0}: orchestration aborted; batch {1} closed Failed" -f $Root, $batchId) 'ERROR'
    throw $fault
}

$failed = @($script:Results | Where-Object { $_.Status -eq 'Failed' })
if ($script:Results.Count -gt 0) { $script:Results | Format-Table -AutoSize | Out-String | Write-Host }
Write-WwiLog ("{0}: {1} package(s) executed, {2} failed, {3} node(s) skipped" -f
              $Root, $script:Results.Count, $failed.Count,
              @($status.Values | Where-Object { $_ -eq 'Skipped' }).Count)
if ($failed.Count -gt 0) { exit 1 }
