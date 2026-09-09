<#
    The SSIS expression subset the orchestration plan's precedence edges use.

    A conditional precedence constraint in a master package is a value (Success
    / Failure / Completion) *and* an expression, joined by a logical AND: the
    path is taken only when the upstream status matches and the expression is
    true. The external runner walks the same graph from
    ssis/orchestration-plan.json, so it has to evaluate the expression too - a
    runner that honours only the value takes both sides of a fork, which is how
    ERR_Notify_Operations ran on a cycle where no file was missing.

    The evaluator models the operators the generated expressions actually use -
    || && ! == != < <= > >= + - * / % and parentheses over variable references,
    numeric literals, string literals and the boolean keywords - and refuses
    anything else rather than guessing. An expression that cannot be evaluated
    is an error the operator sees, never a silently taken edge.

    Variables are addressed the way SSIS addresses them: @[User::Name] for a
    package variable and @[$Package::Name] for a package parameter.
#>

$script:WwiPlanTokenPattern = @'
(?<space>\s+)
|(?<variable>@\[[$]?[A-Za-z0-9_]+::[A-Za-z0-9_]+\])
|(?<number>\d+(\.\d+)?)
|(?<string>"([^"\\]|\\.)*")
|(?<operator><=|>=|==|!=|&&|\|\||[!<>+\-*/%()])
|(?<word>[A-Za-z_][A-Za-z0-9_]*)
'@

function ConvertTo-WwiPlanValue {
    <#
        The typed value of a plan literal. The plan is JSON, so a boolean
        package parameter arrives as the string SSIS writes it ("True"), and a
        comparison against it has to be a boolean comparison.
    #>
    param([Parameter()] $Value)

    if ($null -eq $Value) { return $null }
    if ($Value -is [bool]) { return $Value }
    if ($Value -is [string]) {
        if ($Value -ceq 'True')  { return $true }
        if ($Value -ceq 'False') { return $false }
        return $Value
    }
    if ($Value -is [int] -or $Value -is [long] -or $Value -is [double] -or $Value -is [decimal]) {
        return $Value
    }
    return [string] $Value
}

function ConvertTo-WwiPlanVariableTable {
    <#
        The variables an expression on this root's edges can reference: the
        master's package parameters and its variables, at their declared
        defaults. The runner overwrites an entry as the control task that owns
        it is reached.
    #>
    param([Parameter(Mandatory)] $RootPlan)

    $table = @{}
    foreach ($property in $RootPlan.parameters.PSObject.Properties) {
        $table['$Package::' + $property.Name] = ConvertTo-WwiPlanValue $property.Value
    }
    foreach ($property in $RootPlan.variables.PSObject.Properties) {
        $table['User::' + $property.Name] = ConvertTo-WwiPlanValue $property.Value
    }
    return $table
}

function Get-WwiPlanToken {
    param([Parameter(Mandatory)][AllowEmptyString()][string] $Expression)

    $tokens = @()
    $position = 0
    $regex = [regex]::new($script:WwiPlanTokenPattern,
                          [System.Text.RegularExpressions.RegexOptions]::IgnorePatternWhitespace)
    while ($position -lt $Expression.Length) {
        $match = $regex.Match($Expression, $position)
        if (-not $match.Success -or $match.Index -ne $position) {
            throw ("cannot evaluate the SSIS expression '{0}': unsupported syntax at offset {1}" -f
                   $Expression, $position)
        }
        $position += $match.Length
        if ($match.Groups['space'].Success) { continue }
        foreach ($name in 'variable', 'number', 'string', 'operator', 'word') {
            if ($match.Groups[$name].Success) {
                $tokens += [pscustomobject] @{ Kind = $name; Text = $match.Groups[$name].Value }
                break
            }
        }
    }
    return $tokens
}

function Invoke-WwiPlanExpression {
    <#
        Evaluates one SSIS expression against a variable table and returns its
        value. Unknown variables and unsupported syntax throw.
    #>
    param(
        [Parameter(Mandatory)][string] $Expression,
        [Parameter(Mandatory)][hashtable] $Variables
    )

    $tokens = @(Get-WwiPlanToken -Expression $Expression)
    # The cursor lives in a hashtable so the recursive-descent readers below,
    # which see the enclosing scope read-only, can advance it.
    $state = @{ Cursor = 0 }

    function Peek { if ($state.Cursor -lt $tokens.Count) { return $tokens[$state.Cursor] } return $null }
    function Next { $token = Peek; $state.Cursor += 1; return $token }
    function Read-Operator {
        param([string[]] $Text)
        $token = Peek
        if ($token -and $token.Kind -eq 'operator' -and $Text -contains $token.Text) { return (Next).Text }
        return $null
    }
    function Fail { param([string] $Message) throw ("cannot evaluate the SSIS expression '{0}': {1}" -f $Expression, $Message) }

    function Read-Primary {
        $token = Next
        if (-not $token) { Fail 'the expression ends where a value was expected' }
        switch ($token.Kind) {
            'variable' {
                $name = $token.Text.Substring(2, $token.Text.Length - 3)
                if (-not $Variables.ContainsKey($name)) {
                    Fail ("it reads {0}, which this root's plan does not declare" -f $token.Text)
                }
                return $Variables[$name]
            }
            'number' {
                if ($token.Text.Contains('.')) { return [double] $token.Text }
                return [long] $token.Text
            }
            'string' {
                $inner = $token.Text.Substring(1, $token.Text.Length - 2)
                return $inner -replace '\\(.)', '$1'
            }
            'word' {
                if ($token.Text -ceq 'True')  { return $true }
                if ($token.Text -ceq 'False') { return $false }
                Fail ("it uses the identifier '{0}', which this evaluator does not model" -f $token.Text)
            }
            'operator' {
                if ($token.Text -eq '(') {
                    $value = Read-Or
                    if (-not (Read-Operator -Text @(')'))) { Fail 'an opening parenthesis is never closed' }
                    return $value
                }
                Fail ("it starts a value with '{0}'" -f $token.Text)
            }
        }
    }

    function Read-Unary {
        if (Read-Operator -Text @('!')) {
            $value = Read-Unary
            if ($value -isnot [bool]) { Fail '! was applied to something that is not a boolean' }
            return (-not $value)
        }
        if (Read-Operator -Text @('-')) { return (0 - (Read-Unary)) }
        return Read-Primary
    }

    function Read-Multiplicative {
        $left = Read-Unary
        while ($true) {
            $operator = Read-Operator -Text @('*', '/', '%')
            if (-not $operator) { return $left }
            $right = Read-Unary
            switch ($operator) {
                '*' { $left = $left * $right }
                '/' { $left = $left / $right }
                '%' { $left = $left % $right }
            }
        }
    }

    function Read-Additive {
        $left = Read-Multiplicative
        while ($true) {
            $operator = Read-Operator -Text @('+', '-')
            if (-not $operator) { return $left }
            $right = Read-Multiplicative
            if ($operator -eq '+') { $left = $left + $right } else { $left = $left - $right }
        }
    }

    function Read-Comparison {
        $left = Read-Additive
        $operator = Read-Operator -Text @('==', '!=', '<=', '>=', '<', '>')
        if (-not $operator) { return $left }
        $right = Read-Additive
        switch ($operator) {
            '==' { return ($left -eq $right) }
            '!=' { return ($left -ne $right) }
            '<'  { return ($left -lt $right) }
            '<=' { return ($left -le $right) }
            '>'  { return ($left -gt $right) }
            '>=' { return ($left -ge $right) }
        }
    }

    function Read-And {
        $left = Read-Comparison
        while (Read-Operator -Text @('&&')) {
            $right = Read-Comparison
            if ($left -isnot [bool] -or $right -isnot [bool]) { Fail '&& was applied to something that is not a boolean' }
            $left = ($left -and $right)
        }
        return $left
    }

    function Read-Or {
        $left = Read-And
        while (Read-Operator -Text @('||')) {
            $right = Read-And
            if ($left -isnot [bool] -or $right -isnot [bool]) { Fail '|| was applied to something that is not a boolean' }
            $left = ($left -or $right)
        }
        return $left
    }

    $value = Read-Or
    if ($state.Cursor -ne $tokens.Count) {
        Fail ("it has trailing input from '{0}'" -f $tokens[$state.Cursor].Text)
    }
    return $value
}

function Test-WwiPlanExpression {
    <#
        The boolean verdict of a precedence expression. An expression that does
        not evaluate to a boolean is a defect in the plan, not a false edge.
    #>
    param(
        [Parameter(Mandatory)][string] $Expression,
        [Parameter(Mandatory)][hashtable] $Variables
    )

    $value = Invoke-WwiPlanExpression -Expression $Expression -Variables $Variables
    if ($value -isnot [bool]) {
        throw ("the precedence expression '{0}' evaluated to '{1}', which is not a boolean" -f $Expression, $value)
    }
    return $value
}

function Set-WwiPlanAssignment {
    <#
        Applies an Expression Task assignment - @[User::Name] = <expression> -
        to the variable table, which is how the retry counters the conditional
        edges compare against get their value.
    #>
    param(
        [Parameter(Mandatory)][string] $Assignment,
        [Parameter(Mandatory)][hashtable] $Variables
    )

    $match = [regex]::Match($Assignment, '^\s*@\[(?<name>[$]?[A-Za-z0-9_]+::[A-Za-z0-9_]+)\]\s*=\s*(?<value>.+?)\s*$')
    if (-not $match.Success) {
        throw ("cannot apply the expression task assignment '{0}': it is not <variable> = <expression>" -f $Assignment)
    }
    $name = $match.Groups['name'].Value
    if (-not $Variables.ContainsKey($name)) {
        throw ("the expression task assigns @[{0}], which this root's plan does not declare" -f $name)
    }
    $Variables[$name] = Invoke-WwiPlanExpression -Expression $match.Groups['value'].Value -Variables $Variables
    return $Variables[$name]
}

function Test-WwiPlanNodeShouldRun {
    <#
        Whether a node's inbound precedence constraints let it run.

        A constraint is its value AND its expression: an expression-bearing edge
        is taken only when the upstream status matches *and* the expression is
        true. Several inbound edges remain alternatives - SSIS' default logical
        OR - so one satisfied constraint runs the node.

        A node with no inbound edge is a start node and always runs. An
        expression that cannot be evaluated throws rather than resolving to an
        edge taken or dropped.
    #>
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][AllowEmptyCollection()][array] $Edges,
        [Parameter(Mandatory)][hashtable] $Status,
        [Parameter(Mandatory)][hashtable] $Variables,
        [scriptblock] $Log
    )

    if ($Edges.Count -eq 0) { return $true }
    foreach ($edge in $Edges) {
        $upstream = $Status[$edge.from]
        if (-not $upstream) { continue }
        $satisfied = switch ($edge.value) {
            'Success'    { $upstream -eq 'Succeeded' }
            'Failure'    { $upstream -eq 'Failed' }
            'Completion' { $upstream -ne 'Skipped' }
            default      {
                throw ("the edge {0} -> {1} carries precedence value '{2}', which the runner does not model." -f
                       $edge.from, $edge.to, $edge.value)
            }
        }
        if (-not $satisfied) { continue }
        if ($edge.PSObject.Properties.Name -contains 'expression') {
            if (-not (Test-WwiPlanExpression -Expression $edge.expression -Variables $Variables)) {
                if ($Log) { & $Log ("edge {0} -> {1} not taken: {2} is false" -f $edge.from, $edge.to, $edge.expression) }
                continue
            }
        }
        return $true
    }
    return $false
}


