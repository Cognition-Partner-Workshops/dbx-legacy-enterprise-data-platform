<#
.SYNOPSIS
    Load every generated package through the SSIS runtime and assert the task
    properties the runtime actually parsed.

.DESCRIPTION
    A .dtsx is well-formed XML long before it is a package the runtime
    understands. Where an ObjectData element uses an attribute name the task
    host does not recognise, the task host does not complain: it loads with its
    own defaults, and the package only fails on the execution host, at task
    validation, with a message about the defaults rather than about the
    attribute. That is how

        Archive Processed File:Error: "DestinationPath" is not valid on
        operation type "CopyFile"

    reached a live run of Master_File_Ingestion from a generator that had
    written a lowercase FileSystemData attribute set: no XML check, no schema
    check and no build catches it, because the only component that decides
    what an attribute means is the task host itself.

    This test therefore hands each package to Microsoft.SqlServer.ManagedDTS -
    the same loader dtexec and the catalog use - and compares the properties
    the loaded task exposes with what the package meant to say:

      * every package loads without an error event;
      * every File System Task has a non-default operation, a source and a
        destination that survived the load, both addressed by a variable that
        the package declares;
      * no OLE DB connection manager carries a ConnectionString property
        expression, and a password set on a connection manager survives the
        assignment of ServerName / InitialCatalog / UserName.

    Nothing is executed and nothing connects: LoadPackage parses, and the
    connection manager assertion runs against an in-memory connection manager
    with a dummy value.

.EXAMPLE
    powershell -File validation/static/Test-PackageRuntimeContracts.ps1
#>

[CmdletBinding()]
param(
    [string] $SsisRoot = '',
    [switch] $Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $repoRoot 'deployment/lib/Common.ps1')
$script:WwiLogPrefix = 'wwi-package-runtime'
if (-not $SsisRoot) { $SsisRoot = Join-Path $repoRoot 'ssis' }

if (-not ([System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.ManagedDTS'))) {
    Write-WwiLog 'SKIP Microsoft.SqlServer.ManagedDTS is not installed on this host'
    exit 0
}

$application = New-Object Microsoft.SqlServer.Dts.Runtime.Application
$failures = @()
$packages = 0
$fileSystemTasks = 0

function Get-WwiExecutable {
    <# Every executable in a package, including the ones inside containers. #>
    param([Parameter(Mandatory)] $Container)

    foreach ($executable in $Container.Executables) {
        $executable
        if ($executable.PSObject.Properties.Match('Executables').Count -gt 0) {
            Get-WwiExecutable -Container $executable
        }
    }
}

# 1. Every package loads, and every task the loader built says what the
#    generator meant it to say.
foreach ($file in Get-ChildItem -Path $SsisRoot -Filter '*.dtsx' -Recurse -File) {
    $relative = $file.FullName.Substring($repoRoot.Length + 1).Replace('\', '/')
    $packages++
    try {
        $package = $application.LoadPackage($file.FullName, $null)
    }
    catch {
        $failures += "$relative did not load: $(($_.Exception.Message -split "`n")[0])"
        continue
    }

    $variables = @($package.Variables | ForEach-Object { $_.QualifiedName.Trim('[', ']') })
    foreach ($executable in (Get-WwiExecutable -Container $package)) {
        $host_ = $executable -as [Microsoft.SqlServer.Dts.Runtime.TaskHost]
        if (-not $host_) { continue }
        if ($host_.CreationName -notlike '*FileSystemTask*') { continue }
        $fileSystemTasks++
        $task = $host_.InnerObject
        $name = $host_.Name

        if (-not $task.Source -or -not $task.Destination) {
            $failures += ("$relative task '$name' loaded with an empty source or destination; " +
                          'the runtime did not recognise the FileSystemData attributes')
            continue
        }
        if (-not $task.IsSourcePathVariable -or -not $task.IsDestinationPathVariable) {
            $failures += ("$relative task '$name' resolved its paths as literals; the archive " +
                          'path is only known at run time and has to come from a variable')
        }
        foreach ($reference in @($task.Source, $task.Destination)) {
            if ($variables -notcontains $reference) {
                $failures += "$relative task '$name' addresses $reference, which the package does not declare"
            }
        }
        if ("$($task.Operation)" -eq 'CopyFile') {
            $failures += ("$relative task '$name' loaded as CopyFile, the task host default; " +
                          'the generated operation did not reach the runtime')
        }
    }
    $package.Dispose()
}

if (-not $Quiet) {
    Write-WwiLog "loaded $packages package(s) through the runtime, $fileSystemTasks File System Task(s)"
}

# 2. How an OLE DB connection manager may be retargeted, on an in-memory
#    manager so nothing connects. Password is write-only on the object model,
#    so what is asserted here is the difference the password depends on:
#    ServerName / InitialCatalog / UserName edit the keywords they own and
#    leave the rest of the connection - including the sensitive state the
#    catalog set on CM.<connection>.Password - in place, whereas assigning
#    ConnectionString replaces the whole connection, which is what discarded
#    the catalog's password in the failed run.
$probe = New-Object Microsoft.SqlServer.Dts.Runtime.Package
$manager = $probe.Connections.Add('OLEDB')
$literal = 'Data Source=host;User ID=u;Initial Catalog=db;Provider=MSOLEDBSQL19.1;Trust Server Certificate=True;'
$manager.ConnectionString = $literal
foreach ($assignment in @(@{ Name = 'ServerName'; Value = 'other-host' },
                          @{ Name = 'InitialCatalog'; Value = 'other-db' },
                          @{ Name = 'UserName'; Value = 'other-user' })) {
    $manager.Properties[$assignment.Name].SetValue($manager, $assignment.Value)
}
$retargeted = $manager.ConnectionString
$expected = @('Data Source=other-host', 'Initial Catalog=other-db', 'User ID=other-user',
              'Provider=MSOLEDBSQL19.1', 'Trust Server Certificate=True')
foreach ($keyword in $expected) {
    if ($retargeted -notlike "*$keyword*") {
        $failures += ("retargeting an OLE DB manager through its properties lost '$keyword'; " +
                      'the generated managers cannot be retargeted this way')
    }
}
if (-not $Quiet) {
    Write-WwiLog ('PASS  ServerName / InitialCatalog / UserName retarget in place and keep the ' +
                  'rest of the connection')
}
$probe.Dispose()

# 3. No generated OLE DB connection manager may take that second path.
foreach ($file in Get-ChildItem -Path $SsisRoot -Filter '*.conmgr' -Recurse -File) {
    $relative = $file.FullName.Substring($repoRoot.Length + 1).Replace('\', '/')
    [xml] $document = Get-Content -LiteralPath $file.FullName
    if ($document.ConnectionManager.CreationName -ne 'OLEDB') { continue }
    foreach ($expression in $document.ConnectionManager.PropertyExpression) {
        if ($expression -and $expression.Name -eq 'ConnectionString') {
            $failures += ("$relative expresses ConnectionString; evaluating it rebuilds the " +
                          'connection and discards the catalog-applied password')
        }
    }
}

foreach ($failure in $failures) { Write-WwiLog "FAIL  $failure" }
Write-WwiLog "$($failures.Count) failure(s)"
if ($failures) { exit 1 }
exit 0
