<#
    Runs one master's orchestration from ssis/orchestration-plan.json.

    The estate is deployed as 17 project-deployment .ispac files. An Execute
    Package Task with UseProjectReference resolves its child inside the
    executing project, so a master in WWI_Orchestration cannot reach a child
    held in another .ispac; those edges are generated into the plan instead of
    into the packages, and this driver is what executes them: one dtexec per
    child, against the .ispac that owns it, in the order the plan records.

    The control framework the master owns (etl.usp_StartBatch, a batch step per
    phase, the reconciliation assert, etl.usp_EndBatch) is reproduced here
    through sqlcmd so a child still runs inside the step its phase opened.

    Credentials are read from the environment and passed as project parameters
    (/Par). They are never written to a log: every emitted command line is
    redacted through Format-RedactedCommand, and the sqlcmd calls take the
    password through the SQLCMDPASSWORD environment variable.

    Usage:
      .\deployment\ssis\Invoke-EstateOrchestration.ps1 -Root Master_Daily_ETL `
          [-IspacPath artifacts] [-LogPath logs] [-DryRun]
      .\deployment\ssis\Invoke-EstateOrchestration.ps1 -ListRoots
#>

[CmdletBinding()]
param(
    [string]   $Root,
    [switch]   $ListRoots,
    [string]   $IspacPath = 'artifacts',
    [string]   $LogPath = 'logs/orchestration',
    [string]   $PlanPath,
    [string]   $BusinessDate = ((Get-Date).ToUniversalTime().AddDays(-1).ToString('yyyy-MM-dd')),
    [switch]   $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
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

Assert-WwiEnvironmentVariable @(
    'ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_SERVICE', 'ORACLE_USER', 'ORACLE_PASSWORD',
    'SQLSERVER_HOST', 'SQLSERVER_PORT', 'SQLSERVER_USER', 'SQLSERVER_PASSWORD',
    'SQLSERVER_OLTP_DB', 'SQLSERVER_STAGING_DB', 'SQLSERVER_DW_DB',
    'ETL_INBOUND_FILE_ROOT', 'ETL_ARCHIVE_FILE_ROOT', 'ETL_REJECT_FILE_ROOT')
$environmentCode = Get-WwiEnvironmentCode
Confirm-WwiProduction

$artifacts = if ([System.IO.Path]::IsPathRooted($IspacPath)) { $IspacPath } else { Join-Path $repoRoot $IspacPath }
$logs = if ([System.IO.Path]::IsPathRooted($LogPath)) { $LogPath } else { Join-Path $repoRoot $LogPath }
if (-not $DryRun -and -not (Test-Path $logs)) { New-Item -ItemType Directory -Path $logs -Force | Out-Null }

# 32-bit DTExec cannot load the 64-bit Oracle and MSOLEDBSQL19 providers the
# estate binds to, so the 64-bit one is required rather than whatever is first
# on PATH.
$dtexec = Join-Path $env:ProgramFiles 'Microsoft SQL Server\160\DTS\Binn\DTExec.exe'
if (-not (Test-Path $dtexec)) {
    $candidate = Get-ChildItem -Path (Join-Path $env:ProgramFiles 'Microsoft SQL Server') -Filter 'DTExec.exe' -Recurse -File -ErrorAction SilentlyContinue |
                 Sort-Object FullName -Descending | Select-Object -First 1
    if (-not $candidate) { Stop-WwiWithError 'no 64-bit DTExec.exe found under Program Files\Microsoft SQL Server.' }
    $dtexec = $candidate.FullName
}
Assert-WwiTool -Name 'sqlcmd' -Hint 'install the SQL Server command line tools.'

# ---------------------------------------------------------------------------
# project parameters
# ---------------------------------------------------------------------------

# Name -> value. The two password parameters are the runtime binding surface
# for the connection managers: the generated ConnectionString expressions read
# them, so setting the connection string itself would be overwritten.
$projectParameters = [ordered] @{
    'OracleHost'                      = $env:ORACLE_HOST
    'OraclePort'                      = $env:ORACLE_PORT
    'OracleService'                   = $env:ORACLE_SERVICE
    'OracleUser'                      = $env:ORACLE_USER
    'OraclePassword'                  = $env:ORACLE_PASSWORD
    'SqlServerHost'                   = $env:SQLSERVER_HOST
    'SqlServerPort'                   = $env:SQLSERVER_PORT
    'SqlServerUser'                   = $env:SQLSERVER_USER
    'SqlServerPassword'               = $env:SQLSERVER_PASSWORD
    'SqlServerOltpDb'                 = $env:SQLSERVER_OLTP_DB
    'SqlServerStagingDb'              = $env:SQLSERVER_STAGING_DB
    'SqlServerDwDb'                   = $env:SQLSERVER_DW_DB
    'SqlServerTrustServerCertificate' = $(if ($env:SQLSERVER_TRUST_SERVER_CERTIFICATE) { $env:SQLSERVER_TRUST_SERVER_CERTIFICATE } else { 'False' })
    'InboundFileRoot'                 = $env:ETL_INBOUND_FILE_ROOT
    'ArchiveFileRoot'                 = $env:ETL_ARCHIVE_FILE_ROOT
    'RejectFileRoot'                  = $env:ETL_REJECT_FILE_ROOT
    'EnvironmentCode'                 = $environmentCode
}
$sensitiveParameters = @('OraclePassword', 'SqlServerPassword')

function Format-RedactedCommand {
    <# The command line as it would be logged, with every secret removed. #>
    param([Parameter(Mandatory)][string[]] $ArgumentList)
    $redacted = foreach ($argument in $ArgumentList) {
        $match = $sensitiveParameters | Where-Object { $argument -like ('*::{0};*' -f $_) }
        if ($match) { ($argument -replace ';.*$', ';********') } else { $argument }
    }
    return ($redacted -join ' ')
}

# ---------------------------------------------------------------------------
# control framework
# ---------------------------------------------------------------------------

function Invoke-EtlScalar {
    <# Runs one statement against the staging database and returns its scalar. #>
    param([Parameter(Mandatory)][string] $Query)

    if ($DryRun) {
        Write-WwiLog ("WHATIF sqlcmd {0}" -f ($Query -replace '\s+', ' '))
        return 0
    }
    $previous = $env:SQLCMDPASSWORD
    $env:SQLCMDPASSWORD = $env:SQLSERVER_PASSWORD
    try {
        $output = & sqlcmd -S ("{0},{1}" -f $env:SQLSERVER_HOST, $env:SQLSERVER_PORT) `
                           -U $env:SQLSERVER_USER -d $env:SQLSERVER_STAGING_DB `
                           -C -b -h -1 -W -Q $Query
        if ($LASTEXITCODE -ne 0) { Stop-WwiWithError "control statement failed with exit code $LASTEXITCODE." }
    }
    finally {
        $env:SQLCMDPASSWORD = $previous
    }
    return (($output | Where-Object { $_ -match '^\s*-?\d+\s*$' } | Select-Object -First 1) -as [long])
}

function Start-EtlBatch {
    param([Parameter(Mandatory)][string] $BatchType)
    $query = @"
SET NOCOUNT ON;
DECLARE @BatchId BIGINT;
EXEC etl.usp_StartBatch @BatchName = N'$Root', @BatchType = N'$BatchType',
     @BusinessDate = N'$BusinessDate', @EnvironmentCode = N'$environmentCode',
     @AllowAdoptRunning = 0, @Notes = N'external orchestration runner',
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

# ---------------------------------------------------------------------------
# package execution
# ---------------------------------------------------------------------------

$script:Executed = @{}
$script:Results = @()

function Invoke-EstatePackage {
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

    $ispac = Join-Path $artifacts "$Project.ispac"
    if (-not (Test-Path $ispac) -and -not $DryRun) {
        Stop-WwiWithError "$Project.ispac is not in $artifacts; run deployment/ssis/Build-SsisProject.ps1 first."
    }

    $arguments = @("/Project", $ispac, "/Package", "$Package.dtsx", "/Reporting", "EW")
    foreach ($name in $projectParameters.Keys) {
        $arguments += @("/Par", ('$Project::{0};{1}' -f $name, $projectParameters[$name]))
    }
    $arguments += @("/Par", ('$Package::BatchId;{0}' -f $BatchId))
    if ($PackageParameters) {
        foreach ($name in $PackageParameters.Keys) {
            $arguments += @("/Par", ('$Package::{0};{1}' -f $name, $PackageParameters[$name]))
        }
    }

    $started = Get-Date
    if ($DryRun) {
        Write-WwiLog ("WHATIF dtexec {0}" -f (Format-RedactedCommand -ArgumentList $arguments))
        $exit = 0
    }
    else {
        $log = Join-Path $logs ("{0}-{1}.log" -f $Package, $started.ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
        Write-WwiLog "RUN    $Package ($Project) -> $log"
        & $dtexec @arguments *>&1 | Tee-Object -FilePath $log | Out-Null
        $exit = $LASTEXITCODE
    }

    $succeeded = ($exit -eq 0)
    $script:Executed[$Package] = $succeeded
    $script:Results += [pscustomobject] @{
        Package  = $Package
        Project  = $Project
        Started  = $started.ToUniversalTime().ToString('s')
        Seconds  = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
        ExitCode = $exit
        Status   = $(if ($succeeded) { 'Succeeded' } else { 'Failed' })
    }
    if (-not $succeeded) { Write-WwiLog "FAILED $Package (dtexec exit $exit)" 'ERROR' }
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

function Test-NodeShouldRun {
    param([Parameter(Mandatory)][string] $Name)
    $edges = @($incoming[$Name])
    if ($edges.Count -eq 0) { return $true }
    foreach ($edge in $edges) {
        $upstream = $status[$edge.from]
        if (-not $upstream) { continue }
        switch ($edge.value) {
            'Success'    { if ($upstream -eq 'Succeeded')  { return $true } }
            'Failure'    { if ($upstream -eq 'Failed')     { return $true } }
            'Completion' { if ($upstream -ne 'Skipped')    { return $true } }
        }
    }
    return $false
}

Write-WwiLog "orchestrating $Root ($($rootPlan.nodes.Count) node(s), $($rootPlan.edges.Count) edge(s)) from $artifacts"
$batchId = 0
$forceStatus = $null

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
            $status[$name] = 'Succeeded'
        }
        'batch_end' {
            $force = if ($node.PSObject.Properties.Name -contains 'force_status') { $node.force_status } else { $null }
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
        default {
            # An in-package control task (expression, ad-hoc SQL) orders the
            # graph but has no effect the runner reproduces.
            $status[$name] = 'Succeeded'
        }
    }
}

if ($forceStatus -and $batchId -gt 0) { Stop-EtlBatch -BatchId $batchId -ForceStatus $forceStatus }

$failed = @($script:Results | Where-Object { $_.Status -eq 'Failed' })
if ($script:Results.Count -gt 0) { $script:Results | Format-Table -AutoSize | Out-String | Write-Host }
Write-WwiLog ("{0}: {1} package(s) executed, {2} failed, {3} node(s) skipped" -f
              $Root, $script:Results.Count, $failed.Count,
              @($status.Values | Where-Object { $_ -eq 'Skipped' }).Count)
if ($failed.Count -gt 0) { exit 1 }
