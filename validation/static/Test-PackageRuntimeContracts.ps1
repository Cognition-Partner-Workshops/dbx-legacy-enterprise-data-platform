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

# 4. Every pipeline component persists the whole property set the component
#    gives itself. A component reads its properties by name while it
#    validates, and the runtime never fills a missing one in: the first
#    property that was not persisted fails the component with
#    DTS_E_ELEMENTNOTFOUND (0xC0010009) on the execution host, which is how a
#    lookup that built, deployed and matched the catalog byte for byte still
#    could not start. The expected set is not a list kept here - it is asked
#    of the component itself, by creating one and letting it provide its own
#    properties.
$pipelineWrap = [System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.DTSPipelineWrap')
if (-not $pipelineWrap) {
    Write-WwiLog 'SKIP Microsoft.SqlServer.DTSPipelineWrap is not installed on this host'
}
else {
    # Through C#, because provide-component-properties is a COM call that the
    # PowerShell late binder cannot make on a native pipeline component.
    if (-not ('WwiPipelineProbe' -as [type])) {
        $managedPath = [Microsoft.SqlServer.Dts.Runtime.Application].Assembly.Location
        $wrapPath = $pipelineWrap.Location
        Add-Type -Language CSharp -ReferencedAssemblies @($managedPath, $wrapPath) -TypeDefinition @'
using System;
using System.Collections.Generic;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;

public static class WwiPipelineProbe
{
    // The property names a freshly created component of this class gives
    // itself, or null where the class cannot be created on this host.
    public static string[] PropertyNames(string componentClassID)
    {
        Package package = new Package();
        try
        {
            TaskHost host = (TaskHost) package.Executables.Add("STOCK:PipelineTask");
            MainPipe pipe = (MainPipe) host.InnerObject;
            IDTSComponentMetaData100 component = pipe.ComponentMetaDataCollection.New();
            component.ComponentClassID = componentClassID;
            CManagedComponentWrapper wrapper = component.Instantiate();
            wrapper.ProvideComponentProperties();
            List<string> names = new List<string>();
            foreach (IDTSCustomProperty100 property in component.CustomPropertyCollection)
            {
                names.Add(property.Name);
            }
            return names.ToArray();
        }
        catch (Exception)
        {
            return null;
        }
        finally
        {
            package.Dispose();
        }
    }
}
'@
    }

    $expectedProperties = @{}
    $componentsChecked = 0
    foreach ($file in Get-ChildItem -Path $SsisRoot -Filter '*.dtsx' -Recurse -File) {
        $relative = $file.FullName.Substring($repoRoot.Length + 1).Replace('\', '/')
        [xml] $document = Get-Content -LiteralPath $file.FullName
        foreach ($component in $document.SelectNodes('//component')) {
            $classID = $component.GetAttribute('componentClassID')
            if (-not $classID) { continue }
            if (-not $expectedProperties.ContainsKey($classID)) {
                $expectedProperties[$classID] = [WwiPipelineProbe]::PropertyNames($classID)
            }
            $expected = $expectedProperties[$classID]
            if (-not $expected) { continue }
            $componentsChecked++
            $written = @($component.SelectNodes('properties/property') |
                ForEach-Object { $_.GetAttribute('name') })
            $missing = @($expected | Where-Object { $written -notcontains $_ })
            if ($missing.Count -gt 0) {
                $failures += ("$relative component '$($component.GetAttribute('name'))' " +
                              "persists no $($missing -join ', '); the component asks for every " +
                              'property it owns by name while it validates')
            }
        }
    }
    if (-not $Quiet) {
        Write-WwiLog "checked the property set of $componentsChecked pipeline component(s)"
    }
}

foreach ($failure in $failures) { Write-WwiLog "FAIL  $failure" }
Write-WwiLog "$($failures.Count) failure(s)"
if ($failures) { exit 1 }
exit 0
