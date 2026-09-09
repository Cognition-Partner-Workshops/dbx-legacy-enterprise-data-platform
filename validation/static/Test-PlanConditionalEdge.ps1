<#
    Offline tests for the conditional precedence edges in
    ssis/orchestration-plan.json and the runner's evaluation of them.

    The live failure they stand in for: ERR_Notify_Operations (the master's
    Missing File Notice phase) executed on a cycle where all seven feeds had
    landed, although its only inbound edge carries
    @[User::MissingFileCount] > 0. The generated expression was right; the
    external runner honoured only the edge's precedence value and ignored its
    expression, so it took both sides of the fork.

    Nothing here touches SSISDB or the control database: the walk below drives
    the same Test-WwiPlanNodeShouldRun the runner uses, over the real plan, with
    the control task results supplied by the test.

    Usage:
        pwsh -File validation/static/Test-PlanConditionalEdge.ps1
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot 'deployment\lib\PlanExpression.ps1')

$plan = Get-Content -Raw -Path (Join-Path $repoRoot 'ssis\orchestration-plan.json') | ConvertFrom-Json

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

function Get-RootPlan {
    param([Parameter(Mandatory)][string] $Root)
    $rootPlan = $plan.roots | Where-Object { $_.root -eq $Root }
    if (-not $rootPlan) { throw "the plan holds no root named '$Root'" }
    return $rootPlan
}

function Invoke-PlanWalk {
    <#
        Walks a root the way Invoke-EstateOrchestration.ps1 walks it - Kahn
        order, Test-WwiPlanNodeShouldRun per node, expression tasks applied as
        they are reached - and returns node -> Succeeded / Skipped / Failed.

        -ControlResults supplies what a query control task would have read from
        the control database, and -Failures marks a node the run should treat as
        failed, so a branch can be driven without a database or an execution.
    #>
    param(
        [Parameter(Mandatory)] $RootPlan,
        [hashtable] $ControlResults = @{},
        [string[]] $Failures = @()
    )

    $incoming = @{}
    $outgoing = @{}
    foreach ($node in $RootPlan.nodes) { $incoming[$node.name] = @(); $outgoing[$node.name] = @() }
    foreach ($edge in $RootPlan.edges) {
        $incoming[$edge.to] += $edge
        $outgoing[$edge.from] += $edge
    }

    $remaining = [System.Collections.ArrayList] @($RootPlan.nodes.name)
    $pending = @{}
    foreach ($name in $remaining) { $pending[$name] = @($incoming[$name]).Count }
    $order = @()
    while ($remaining.Count -gt 0) {
        $ready = @($remaining | Where-Object { $pending[$_] -eq 0 })
        if ($ready.Count -eq 0) { throw "the plan for $($RootPlan.root) is cyclic" }
        foreach ($name in $ready) {
            $order += $name
            $remaining.Remove($name)
            foreach ($edge in $outgoing[$name]) { $pending[$edge.to] -= 1 }
        }
    }

    $variables = ConvertTo-WwiPlanVariableTable -RootPlan $RootPlan
    $nodesByName = @{}
    foreach ($node in $RootPlan.nodes) { $nodesByName[$node.name] = $node }

    $status = @{}
    foreach ($name in $order) {
        if (-not (Test-WwiPlanNodeShouldRun -Name $name -Edges @($incoming[$name]) -Status $status -Variables $variables)) {
            $status[$name] = 'Skipped'
            continue
        }
        $node = $nodesByName[$name]
        if ($node.kind -eq 'control') {
            switch ($node.control) {
                'expression' { Set-WwiPlanAssignment -Assignment $node.assignment -Variables $variables | Out-Null }
                'query'      {
                    if (-not $ControlResults.ContainsKey($name)) {
                        throw "the walk has no result for the control query '$name'"
                    }
                    $variables[$node.result_variable] = $ControlResults[$name]
                }
            }
        }
        $status[$name] = if ($Failures -contains $name) { 'Failed' } else { 'Succeeded' }
    }
    return $status
}

# ---------------------------------------------------------------------------
# the defect: the missing-file branch
# ---------------------------------------------------------------------------

$fileIngestion = Get-RootPlan -Root 'Master_File_Ingestion'

Test-Case 'no file missing: the notification branch is not taken' {
    $status = Invoke-PlanWalk -RootPlan $fileIngestion -ControlResults @{ 'Count Expected Files Missing' = 0 }
    if ($status['Missing File Notice'] -ne 'Skipped') {
        throw "Missing File Notice is $($status['Missing File Notice']) with MissingFileCount 0"
    }
    if ($status['End Batch With Warnings'] -ne 'Skipped') {
        throw "End Batch With Warnings is $($status['End Batch With Warnings']) with MissingFileCount 0"
    }
    if ($status['Partner Drops'] -ne 'Succeeded') {
        throw "Partner Drops is $($status['Partner Drops']) with MissingFileCount 0"
    }
    if ($status['End Batch'] -ne 'Succeeded') {
        throw "End Batch is $($status['End Batch']) with MissingFileCount 0"
    }
}

Test-Case 'a file missing: the notification branch is taken' {
    $status = Invoke-PlanWalk -RootPlan $fileIngestion -ControlResults @{ 'Count Expected Files Missing' = 2 }
    if ($status['Missing File Notice'] -ne 'Succeeded') {
        throw "Missing File Notice is $($status['Missing File Notice']) with MissingFileCount 2"
    }
    if ($status['End Batch With Warnings'] -ne 'Succeeded') {
        throw "End Batch With Warnings is $($status['End Batch With Warnings']) with MissingFileCount 2"
    }
}

Test-Case 'a file missing but partner files optional: ingestion still runs' {
    # RequirePartnerFiles defaults to False, so the ingestion branch stays open
    # alongside the notification - both edges are alternatives, not exclusives.
    $status = Invoke-PlanWalk -RootPlan $fileIngestion -ControlResults @{ 'Count Expected Files Missing' = 2 }
    if ($status['Partner Drops'] -ne 'Succeeded') {
        throw "Partner Drops is $($status['Partner Drops']) while partner files are optional"
    }
}

Test-Case 'a file missing and partner files required: ingestion is held back' {
    $required = $fileIngestion | ConvertTo-Json -Depth 12 | ConvertFrom-Json
    $required.parameters.RequirePartnerFiles = 'True'
    $status = Invoke-PlanWalk -RootPlan $required -ControlResults @{ 'Count Expected Files Missing' = 2 }
    if ($status['Partner Drops'] -ne 'Skipped') {
        throw "Partner Drops is $($status['Partner Drops']) while partner files are required and 2 are missing"
    }
    if ($status['Missing File Notice'] -ne 'Succeeded') {
        throw "Missing File Notice is $($status['Missing File Notice']) while 2 files are missing"
    }
}

# ---------------------------------------------------------------------------
# the general rule: value AND expression
# ---------------------------------------------------------------------------

Test-Case 'an expression edge is not taken when its upstream did not succeed' {
    $edges = @([pscustomobject] @{ from = 'A'; to = 'B'; value = 'Success'; expression = '@[User::MissingFileCount] > 0' })
    $variables = @{ 'User::MissingFileCount' = 5 }
    if (Test-WwiPlanNodeShouldRun -Name 'B' -Edges $edges -Status @{ A = 'Failed' } -Variables $variables) {
        throw 'a true expression took an edge whose upstream failed'
    }
}

Test-Case 'a node with no inbound edge always runs' {
    if (-not (Test-WwiPlanNodeShouldRun -Name 'A' -Edges @() -Status @{} -Variables @{})) {
        throw 'a start node was skipped'
    }
}

Test-Case 'alternative inbound edges are ORed' {
    $edges = @(
        [pscustomobject] @{ from = 'A'; to = 'C'; value = 'Success'; expression = '@[User::MissingFileCount] > 0' },
        [pscustomobject] @{ from = 'B'; to = 'C'; value = 'Completion' }
    )
    $variables = @{ 'User::MissingFileCount' = 0 }
    if (-not (Test-WwiPlanNodeShouldRun -Name 'C' -Edges $edges -Status @{ A = 'Succeeded'; B = 'Succeeded' } -Variables $variables)) {
        throw 'an unconditional alternative edge did not run the node'
    }
}

Test-Case 'an expression the runner cannot evaluate is an error, not an edge' {
    $edges = @([pscustomobject] @{ from = 'A'; to = 'B'; value = 'Success'; expression = 'FINDSTRING(@[User::Feed],"x",1) > 0' })
    Assert-Throws -Expected 'cannot evaluate the SSIS expression' -Body {
        Test-WwiPlanNodeShouldRun -Name 'B' -Edges $edges -Status @{ A = 'Succeeded' } -Variables @{ 'User::Feed' = 'x' }
    }
}

Test-Case 'an expression over a variable the plan does not declare is an error' {
    $edges = @([pscustomobject] @{ from = 'A'; to = 'B'; value = 'Success'; expression = '@[User::Nope] > 0' })
    Assert-Throws -Expected 'does not declare' -Body {
        Test-WwiPlanNodeShouldRun -Name 'B' -Edges $edges -Status @{ A = 'Succeeded' } -Variables @{}
    }
}

# ---------------------------------------------------------------------------
# every conditional edge in the estate
# ---------------------------------------------------------------------------

Test-Case 'every conditional edge in the plan evaluates to a boolean at its declared defaults' {
    $checked = 0
    foreach ($rootPlan in $plan.roots) {
        $variables = ConvertTo-WwiPlanVariableTable -RootPlan $rootPlan
        foreach ($edge in $rootPlan.edges) {
            if ($edge.PSObject.Properties.Name -notcontains 'expression') { continue }
            $value = Test-WwiPlanExpression -Expression $edge.expression -Variables $variables
            if ($value -isnot [bool]) {
                throw "$($rootPlan.root): $($edge.expression) is not boolean"
            }
            $checked += 1
        }
    }
    if ($checked -eq 0) { throw 'the plan holds no conditional edge, so this proves nothing' }
    Write-Host ("      {0} conditional edge(s) evaluated" -f $checked)
}

Test-Case 'every control task the plan declares is one the runner can reproduce' {
    foreach ($rootPlan in $plan.roots) {
        $variables = ConvertTo-WwiPlanVariableTable -RootPlan $rootPlan
        foreach ($node in $rootPlan.nodes) {
            if ($node.kind -ne 'control') { continue }
            switch ($node.control) {
                'expression' { Set-WwiPlanAssignment -Assignment $node.assignment -Variables $variables | Out-Null }
                'query'      {
                    if ($node.connection -ne 'WWI_Staging_DB') {
                        throw "$($rootPlan.root)/$($node.name) reads $($node.connection)"
                    }
                    if (-not $variables.ContainsKey($node.result_variable)) {
                        throw "$($rootPlan.root)/$($node.name) binds an undeclared $($node.result_variable)"
                    }
                    $placeholders = ([regex] '\?').Matches($node.sql).Count
                    if ($placeholders -ne @($node.parameters).Count) {
                        throw ("{0}/{1} has {2} placeholder(s) and {3} bound parameter(s)" -f
                               $rootPlan.root, $node.name, $placeholders, @($node.parameters).Count)
                    }
                }
                'statement'  { }
                default      { throw "$($rootPlan.root)/$($node.name) is of unknown control kind '$($node.control)'" }
            }
        }
    }
}

# ---------------------------------------------------------------------------
# the retry and stand-down branches, which are conditional the same way
# ---------------------------------------------------------------------------

Test-Case 'the hourly root stands down while a nightly batch is running' {
    $hourly = Get-RootPlan -Root 'Master_Hourly_Incremental'
    $running = Invoke-PlanWalk -RootPlan $hourly -ControlResults @{ 'Check Nightly Batch' = 1 }
    $idle    = Invoke-PlanWalk -RootPlan $hourly -ControlResults @{ 'Check Nightly Batch' = 0 }
    if ($running['Log Stand Down'] -ne 'Succeeded') {
        throw "Log Stand Down is $($running['Log Stand Down']) while a nightly batch is running"
    }
    if ($idle['Log Stand Down'] -ne 'Skipped') {
        throw "Log Stand Down is $($idle['Log Stand Down']) while no nightly batch is running"
    }
}

Write-Host ''
if ($failures.Count -gt 0) {
    Write-Host ("{0} test(s) failed" -f $failures.Count)
    exit 1
}
Write-Host 'all plan conditional edge tests passed'
