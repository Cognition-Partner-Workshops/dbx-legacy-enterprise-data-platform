<#
    Deploys the estate's .ispac projects into the SSISDB catalog folder.

    Deployment is always client-side. The .ispac files are built on the deploy
    host and streamed to the catalog over the connection, either through the
    Integration Services managed API (Folder.DeployProject, the SSMS code path)
    or through ISDeploymentWizard.exe /Silent. There is deliberately no
    server-side path: the earlier OPENROWSET(BULK ...) fallback read the .ispac
    from the SQL Server's own filesystem, where the artifacts do not exist and
    where no share is mounted, so it could only ever fail - or worse, deploy a
    stale copy left behind by someone else.

    The catalog must already exist; run deployment/ssis/New-SsisCatalog.ps1
    first. Deploy-SsisEnvironment.ps1 runs afterwards to create the environment,
    its references and the parameter bindings.

    Verification is done against catalog.projects and catalog.packages, not
    against an exit code: the run fails unless the expected projects are present
    and the package total matches, with no package name deployed twice.

    Environment: SSIS_SERVER, SSIS_FOLDER, and SQLSERVER_USER /
    SQLSERVER_PASSWORD when the instance uses SQL authentication.

    Usage:
        .\deployment\ssis\Deploy-SsisCatalog.ps1 -DryRun
        .\deployment\ssis\Deploy-SsisCatalog.ps1
        .\deployment\ssis\Deploy-SsisCatalog.ps1 -ExpectedPackageCount 205
#>

[CmdletBinding()]
param(
    [string[]] $IspacPath,
    [string[]] $ExpectedProject,
    [switch] $UseWizard,
    [switch] $UseCatalogProcedure,
    [int] $ExpectedPackageCount = 0,
    [switch] $VerifyOnly,
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'SsisCatalog.ps1')
$script:WwiLogPrefix = 'wwi-deploy-ssis'

Assert-WwiEnvironmentVariable @('SSIS_SERVER', 'SSIS_FOLDER')
Confirm-WwiProduction

$repoRoot        = Get-WwiRepositoryRoot
$environmentCode = Get-WwiEnvironmentCode
$folder          = $env:SSIS_FOLDER
$artifacts       = Join-Path $repoRoot 'artifacts'

function Get-WwiExpectedProjects {
    <#
        One project per .dtproj in the estate; the source tree is the contract.

        -ExpectedProject supplies that same list explicitly, for the deploy host
        that only receives the built artifacts and the deployment scripts - the
        SQL Server host, where the catalog refuses SQL-authenticated callers and
        the deployment therefore has to run locally.
    #>
    if ($ExpectedProject) { return ($ExpectedProject | Sort-Object) }

    $sourceTree = Join-Path $repoRoot 'ssis'
    if (-not (Test-Path $sourceTree)) {
        Stop-WwiWithError ("$sourceTree does not exist, so the expected project list cannot be read from the " +
                           'estate. Pass -ExpectedProject when deploying from outside a checkout.')
    }
    Get-ChildItem -Path $sourceTree -Filter '*.dtproj' -Recurse -File |
        Where-Object { $_.FullName -notmatch '\\obj\\' } |
        Sort-Object BaseName |
        ForEach-Object { $_.BaseName }
}

function Test-WwiCatalogContents {
    <#
        The deployment's own acceptance test. Reports what the catalog holds and
        fails when it does not match the estate: a missing project, a project
        with no packages, a package name deployed more than once, or a total
        that differs from the expectation.
    #>
    param(
        [Parameter(Mandatory)][string[]] $Expected,
        [int] $ExpectedPackages = 0
    )

    $inventory = Get-WwiCatalogInventory -Folder $folder
    $deployed  = @($inventory.Projects | ForEach-Object { $_.project_name })
    $problems  = @()

    foreach ($name in $Expected) {
        if ($deployed -notcontains $name) { $problems += "project $name is not in /SSISDB/$folder" }
    }
    foreach ($project in $inventory.Projects) {
        if ($project.package_count -eq 0) {
            $problems += "project $($project.project_name) deployed with zero packages"
        }
    }

    $duplicates = $inventory.Packages | Group-Object package_name | Where-Object { $_.Count -gt 1 }
    foreach ($duplicate in $duplicates) {
        $owners = ($duplicate.Group | ForEach-Object { $_.project_name }) -join ', '
        $problems += "package $($duplicate.Name) is deployed $($duplicate.Count) times ($owners)"
    }

    $total = @($inventory.Packages).Count
    if ($ExpectedPackages -gt 0 -and $total -ne $ExpectedPackages) {
        $problems += "the catalog holds $total packages; $ExpectedPackages were expected"
    }

    Write-WwiLog ("catalog /SSISDB/{0}: {1} project(s), {2} package(s)" -f $folder, @($deployed).Count, $total)
    foreach ($project in $inventory.Projects) {
        Write-WwiLog ("    {0,-24} {1,4} package(s)  deployed {2}" -f `
            $project.project_name, $project.package_count, $project.last_deployed_time)
    }

    if ($problems.Count -gt 0) {
        foreach ($problem in $problems) { Write-WwiLog $problem 'ERROR' }
        Stop-WwiWithError ("the catalog does not match the estate: {0} problem(s)." -f $problems.Count)
    }
    return $inventory
}

$expectedProjects = @(Get-WwiExpectedProjects)

if ($VerifyOnly) {
    Test-WwiCatalogContents -Expected $expectedProjects -ExpectedPackages $ExpectedPackageCount | Out-Null
    Write-WwiLog 'catalog contents verified.'
    return
}

if (-not $IspacPath) {
    if (Test-Path $artifacts) {
        $IspacPath = @(Get-ChildItem -Path $artifacts -Filter '*.ispac' -File | Sort-Object Name | ForEach-Object { $_.FullName })
    } elseif ($DryRun) {
        # Nothing is built yet in a rehearsal; report what the build stage would
        # hand over instead of failing.
        $IspacPath = @($expectedProjects | ForEach-Object { Join-Path $artifacts ($_ + '.ispac') })
    } else {
        Stop-WwiWithError "$artifacts does not exist. Run deployment/ssis/Build-SsisProject.ps1 first."
    }
}

if ($IspacPath.Count -eq 0) {
    Stop-WwiWithError "no .ispac files in $artifacts. Run deployment/ssis/Build-SsisProject.ps1 first."
}

foreach ($ispac in $IspacPath) {
    if (-not $DryRun -and -not (Test-Path $ispac)) {
        Stop-WwiWithError "$ispac does not exist. Run deployment/ssis/Build-SsisProject.ps1 first."
    }
}

$missing = @($expectedProjects | Where-Object {
    $name = $_
    -not ($IspacPath | Where-Object { [System.IO.Path]::GetFileNameWithoutExtension($_) -eq $name })
})
if ($missing.Count -gt 0) {
    Stop-WwiWithError ("the build did not produce an .ispac for: {0}." -f ($missing -join ', '))
}

if ($DryRun) {
    Write-WwiLog "WHATIF create catalogue folder [$folder] on $($env:SSIS_SERVER)"
    foreach ($ispac in $IspacPath) {
        $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)
        Write-WwiLog "WHATIF deploy $ispac as [$folder]\[$projectName] ($environmentCode)"
    }
    Write-WwiLog "WHATIF verify catalog.projects and catalog.packages afterwards"
    return
}

$catalogState = Test-WwiSsisCatalog
if (-not $catalogState.CatalogUsable) {
    Stop-WwiWithError 'there is no usable SSISDB catalog on this instance. Run deployment/ssis/New-SsisCatalog.ps1 first.'
}

Write-WwiLog "ensuring catalogue folder [$folder] exists"
Invoke-WwiSqlNonQuery -Database 'SSISDB' -Integrated -Parameters @{ folder = $folder } -Query @'
IF NOT EXISTS (SELECT 1 FROM catalog.folders WHERE name = @folder)
    EXEC catalog.create_folder @folder_name = @folder;
'@ | Out-Null

$useManagedApi = (-not $UseWizard) -and (-not $UseCatalogProcedure) -and (Import-WwiIntegrationServicesApi)
$wizard = if ($UseCatalogProcedure) { $null } else { Get-Command ISDeploymentWizard.exe -ErrorAction SilentlyContinue }

if (-not $useManagedApi -and -not $UseCatalogProcedure -and -not $wizard) {
    Stop-WwiWithError ('none of the Integration Services managed API, catalog.deploy_project and ' +
                       'ISDeploymentWizard.exe is available on this deploy host.')
}

if ($UseCatalogProcedure) {
    # catalog.deploy_project with the .ispac passed as a varbinary parameter.
    # The bytes are still read by this process and streamed over the connection
    # - this is not the old OPENROWSET path, which made the server read a file
    # from its own disk - and unlike ISDeploymentWizard it reports the catalog's
    # own error text instead of trying to raise a dialog box.
    Write-WwiLog 'deploying through catalog.deploy_project'
    foreach ($ispac in $IspacPath) {
        $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)
        $bytes = [System.IO.File]::ReadAllBytes($ispac)
        Write-WwiLog ("deploying {0} ({1:N0} bytes) -> /SSISDB/{2}" -f $projectName, $bytes.Length, $folder)
        Invoke-WwiSqlNonQuery -Database 'SSISDB' -Integrated -Parameters @{
            folder_name  = $folder
            project_name = $projectName
            stream       = $bytes
        } -Query @'
DECLARE @operation_id bigint;
EXEC catalog.deploy_project @folder_name = @folder_name, @project_name = @project_name,
                            @project_stream = @stream, @operation_id = @operation_id OUTPUT;
'@ | Out-Null
    }
}
elseif ($useManagedApi) {
    Write-WwiLog 'deploying through the Integration Services managed API'
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection (Get-WwiSsisConnectionString -Database 'master')
    try {
        $serverConnection = New-Object Microsoft.SqlServer.Management.Common.ServerConnection $sqlConnection
        $integrationServices = New-Object Microsoft.SqlServer.Management.IntegrationServices.IntegrationServices $serverConnection
        $catalog = $integrationServices.Catalogs['SSISDB']
        if (-not $catalog) { Stop-WwiWithError 'the instance has no SSISDB catalog object.' }

        $catalogFolder = $catalog.Folders[$folder]
        if (-not $catalogFolder) { Stop-WwiWithError "catalog folder [$folder] was not found after creating it." }

        foreach ($ispac in $IspacPath) {
            $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)
            $bytes = [System.IO.File]::ReadAllBytes($ispac)
            Write-WwiLog ("deploying {0} ({1:N0} bytes) -> /SSISDB/{2}" -f $projectName, $bytes.Length, $folder)
            $catalogFolder.DeployProject($projectName, $bytes)
        }
    }
    finally {
        $sqlConnection.Dispose()
    }
}
else {
    Write-WwiLog 'deploying through ISDeploymentWizard.exe /Silent'
    foreach ($ispac in $IspacPath) {
        $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)
        $arguments = @(
            '/Silent',
            ('/SourcePath:{0}' -f $ispac),
            ('/DestinationServer:{0}' -f $env:SSIS_SERVER),
            ('/DestinationPath:/SSISDB/{0}/{1}' -f $folder, $projectName)
        )
        # ISDeploymentWizard is a GUI-subsystem binary: the call operator returns
        # before it finishes and never sets $LASTEXITCODE, so it is started and
        # waited for explicitly. The exit code is not the verdict either way -
        # the catalog check below is.
        Write-WwiLog "RUN    ISDeploymentWizard $projectName -> /SSISDB/$folder"
        $process = Start-Process -FilePath $wizard.Source -ArgumentList $arguments -Wait -PassThru -NoNewWindow
        if ($process.ExitCode -ne 0) {
            Write-WwiLog "ISDeploymentWizard reported exit code $($process.ExitCode) for $projectName" 'WARN'
        }
    }
}

if ($ExpectedPackageCount -le 0 -and (Test-Path (Join-Path $repoRoot 'ssis'))) {
    $ExpectedPackageCount = @(Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtsx' -Recurse -File |
        Where-Object { $_.FullName -notmatch '\\obj\\' }).Count
    Write-WwiLog "expecting $ExpectedPackageCount package(s), counted from the estate source tree"
}

Test-WwiCatalogContents -Expected $expectedProjects -ExpectedPackages $ExpectedPackageCount | Out-Null

Write-WwiLog 'Run deployment/ssis/Deploy-SsisEnvironment.ps1 next to create and bind the environment.'
