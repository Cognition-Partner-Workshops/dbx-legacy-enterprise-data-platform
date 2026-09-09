<#
    Proves the catalog holds the packages that were built, not merely as many.

    catalog.projects and catalog.packages answer "how many", which a stale
    deployment satisfies just as well as a current one: a project deployed
    before a generator fix still reports its 7 packages. SSISDB keeps the
    deployed project stream - the .ispac as it arrived - and hands it back
    through catalog.get_project, so the honest comparison is the SHA256 of every
    .dtsx inside the deployed stream against the SHA256 of the same package in
    the built .ispac. The stream must come from the procedure and not from
    internal.object_versions: the stored bytes are encrypted with the catalog's
    key and are not a readable archive.

    Runs on the catalog host with integrated authentication, because that is the
    only principal SSISDB accepts for catalog access here.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $ManifestPath,
    [string] $Server = $(if ($env:SSIS_SERVER) { $env:SSIS_SERVER } else { 'localhost' }),
    [string] $Folder = $(if ($env:SSIS_FOLDER) { $env:SSIS_FOLDER } else { 'WWI_DEV' })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Add-Type -AssemblyName 'System.IO.Compression'
Add-Type -AssemblyName 'System.IO.Compression.FileSystem'

function Write-ParityLog {
    param([string] $Message, [string] $Level = 'INFO')
    Write-Host ("[{0}] wwi-catalog-parity {1} {2}" -f (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'), $Level, $Message)
}

if (-not (Test-Path -LiteralPath $ManifestPath)) {
    throw "manifest $ManifestPath does not exist; build it with validation/checks/build_package_manifest.py"
}

$manifest = Get-Content -Raw -LiteralPath $ManifestPath | ConvertFrom-Json
$built = @{}
foreach ($project in $manifest.PSObject.Properties) {
    $packages = @{}
    foreach ($package in $project.Value.PSObject.Properties) { $packages[$package.Name] = [string] $package.Value }
    $built[$project.Name] = $packages
}

$connectionString = "Server=$Server;Database=SSISDB;Integrated Security=SSPI;TrustServerCertificate=True;Connect Timeout=30"
$connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
$connection.Open()
try {
    $list = $connection.CreateCommand()
    $list.CommandTimeout = 300
    $list.CommandText = @'
SELECT p.name
FROM catalog.projects p
JOIN catalog.folders f ON f.folder_id = p.folder_id
WHERE f.name = @folder
ORDER BY p.name;
'@
    $list.Parameters.Add('@folder', [System.Data.SqlDbType]::NVarChar, 128).Value = $Folder

    $names = @()
    $reader = $list.ExecuteReader()
    try { while ($reader.Read()) { $names += [string] $reader[0] } }
    finally { $reader.Close() }

    $deployed = @{}
    foreach ($name in $names) {
        $get = $connection.CreateCommand()
        $get.CommandTimeout = 300
        $get.CommandText = 'catalog.get_project'
        $get.CommandType = [System.Data.CommandType]::StoredProcedure
        $get.Parameters.Add('@folder_name', [System.Data.SqlDbType]::NVarChar, 128).Value = $Folder
        $get.Parameters.Add('@project_name', [System.Data.SqlDbType]::NVarChar, 128).Value = $name
        $stream = $get.ExecuteScalar()
        if ($null -eq $stream -or $stream -is [System.DBNull]) {
            throw "catalog.get_project returned no stream for $name; the catalog cannot be compared to the build"
        }
        $deployed[$name] = [byte[]] $stream
    }
}
finally { $connection.Close() }

$failures = @()
$packagesCompared = 0

foreach ($project in ($built.Keys | Sort-Object)) {
    if (-not $deployed.ContainsKey($project)) {
        $failures += "project $project is built but not deployed to /SSISDB/$Folder"
        continue
    }

    $stream = New-Object System.IO.MemoryStream (, $deployed[$project])
    $archive = New-Object System.IO.Compression.ZipArchive $stream, ([System.IO.Compression.ZipArchiveMode]::Read)
    try {
        $deployedHashes = @{}
        foreach ($entry in $archive.Entries) {
            if (-not $entry.FullName.ToLowerInvariant().EndsWith('.dtsx')) { continue }
            $entryStream = $entry.Open()
            try {
                $buffer = New-Object System.IO.MemoryStream
                $entryStream.CopyTo($buffer)
                $sha = [System.Security.Cryptography.SHA256]::Create()
                try {
                    $hash = ($sha.ComputeHash($buffer.ToArray()) | ForEach-Object { $_.ToString('x2') }) -join ''
                }
                finally { $sha.Dispose() }
                $deployedHashes[[System.IO.Path]::GetFileName($entry.FullName)] = $hash.ToUpperInvariant()
            }
            finally { $entryStream.Dispose() }
        }
    }
    finally {
        $archive.Dispose()
        $stream.Dispose()
    }

    $expected = $built[$project]
    $drift = 0
    foreach ($package in ($expected.Keys | Sort-Object)) {
        $packagesCompared++
        if (-not $deployedHashes.ContainsKey($package)) {
            $failures += "$project/$package is in the build but not in the deployed stream"
            $drift++
            continue
        }
        if ($deployedHashes[$package] -ne $expected[$package]) {
            $failures += "$project/$package deployed content differs from the built package"
            $drift++
        }
    }
    foreach ($package in ($deployedHashes.Keys | Sort-Object)) {
        if (-not $expected.ContainsKey($package)) {
            $failures += "$project/$package is deployed but is not in the build"
            $drift++
        }
    }

    if ($drift -eq 0) {
        Write-ParityLog ("{0,-24} {1,3} package(s) identical to the build" -f $project, $expected.Count)
    } else {
        Write-ParityLog ("{0,-24} {1} package(s) differ from the build" -f $project, $drift) 'ERROR'
    }
}

foreach ($project in ($deployed.Keys | Sort-Object)) {
    if (-not $built.ContainsKey($project)) {
        $failures += "project $project is deployed to /SSISDB/$Folder but was not built"
    }
}

Write-ParityLog ("projects built {0}, deployed {1}, packages compared {2}" -f $built.Count, $deployed.Count, $packagesCompared)

if ($failures.Count -gt 0) {
    foreach ($failure in $failures | Select-Object -First 50) { Write-ParityLog $failure 'ERROR' }
    Write-ParityLog ("{0} parity failure(s)" -f $failures.Count) 'ERROR'
    exit 1
}

Write-ParityLog 'catalog content matches the built packages.'
