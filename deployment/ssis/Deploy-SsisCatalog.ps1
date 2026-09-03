<#
    Deploys the WWI .ispac into the SSIS catalogue and creates the folder.

    Uses ISDeploymentWizard.exe in silent mode when it is available, and falls
    back to catalog.deploy_project through sqlcmd when it is not - the estate
    has both kinds of deploy host, and the wizard is the older of the two paths.

    The SSISDB catalogue itself is assumed to exist: creating it requires a
    catalogue master key decision that belongs to the DBA team, not to this
    repository. Deploy-SsisEnvironment.ps1 must run afterwards to create the
    environment and bind parameters.

    Connection details come from SSIS_SERVER, SSIS_FOLDER and, for the sqlcmd
    fallback, SQLSERVER_USER / SQLSERVER_PASSWORD.

    This script has not been executed against any SSIS catalogue.

    The estate builds one .ispac per area project, so with no -IspacPath every
    .ispac in artifacts/ is deployed under SSIS_FOLDER, each keeping its own
    project name.

    Usage: .\deployment\ssis\Deploy-SsisCatalog.ps1 [-IspacPath artifacts\WWI_Facts.ispac] [-DryRun]
#>

[CmdletBinding()]
param(
    [string[]] $IspacPath,
    [switch] $UseSqlcmdFallback,
    [switch] $DryRun
)

. (Join-Path (Join-Path (Join-Path $PSScriptRoot '..') 'lib') 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-ssis'

Assert-WwiEnvironmentVariable @('SSIS_SERVER', 'SSIS_FOLDER')
Confirm-WwiProduction

$repoRoot        = Get-WwiRepositoryRoot
$environmentCode = Get-WwiEnvironmentCode
$folder          = $env:SSIS_FOLDER
$artifacts       = Join-Path $repoRoot 'artifacts'

if (-not $IspacPath) {
    if (Test-Path $artifacts) {
        $IspacPath = @(Get-ChildItem -Path $artifacts -Filter '*.ispac' -File | Sort-Object Name | ForEach-Object { $_.FullName })
    } elseif ($DryRun) {
        # Nothing is built yet in a rehearsal; report what the build stage would
        # hand over instead of failing.
        $IspacPath = @(Get-ChildItem -Path (Join-Path $repoRoot 'ssis') -Filter '*.dtproj' -Recurse -File |
            Where-Object { $_.FullName -notmatch '\\obj\\' } |
            Sort-Object BaseName |
            ForEach-Object { Join-Path $artifacts ($_.BaseName + '.ispac') })
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

# The folder is created first; the wizard will not create one for you.
$createFolder = @"
IF NOT EXISTS (SELECT 1 FROM SSISDB.catalog.folders WHERE name = N'$folder')
    EXEC SSISDB.catalog.create_folder @folder_name = N'$folder';
"@

$sqlcmdBase = @('-S', $env:SSIS_SERVER, '-b', '-I', '-X1')
if (-not [string]::IsNullOrWhiteSpace($env:SQLSERVER_USER)) {
    Assert-WwiEnvironmentVariable @('SQLSERVER_PASSWORD')
    $env:SQLCMDPASSWORD = $env:SQLSERVER_PASSWORD
    $sqlcmdBase += @('-U', $env:SQLSERVER_USER)
} else {
    Write-WwiLog 'SQLSERVER_USER is not set; using Windows authentication for catalogue calls.'
}

if ($DryRun) {
    Write-WwiLog "WHATIF create catalogue folder [$folder] on $($env:SSIS_SERVER)"
    foreach ($ispac in $IspacPath) {
        $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)
        Write-WwiLog "WHATIF deploy $ispac as [$folder]\[$projectName] ($environmentCode)"
    }
    return
}

Write-WwiLog "ensuring catalogue folder [$folder] exists"
& sqlcmd @sqlcmdBase '-Q' $createFolder
if ($LASTEXITCODE -ne 0) { Stop-WwiWithError "could not create or verify catalogue folder [$folder]." }

$wizard = Get-Command ISDeploymentWizard.exe -ErrorAction SilentlyContinue

foreach ($ispac in $IspacPath) {
    $projectName = [System.IO.Path]::GetFileNameWithoutExtension($ispac)

    if ($wizard -and -not $UseSqlcmdFallback) {
        # /Silent is positional in the wizard's odd command line; the order below is
        # the one that works with the 2016-era wizard the estate still deploys with.
        $arguments = @(
            '/Silent',
            ('/SourcePath:{0}' -f $ispac),
            ('/DestinationServer:{0}' -f $env:SSIS_SERVER),
            ('/DestinationPath:/SSISDB/{0}/{1}' -f $folder, $projectName)
        )
        Invoke-WwiCommand -Description "ISDeploymentWizard $projectName -> /SSISDB/$folder" `
                          -FilePath $wizard.Source -ArgumentList $arguments | Out-Null
    }
    else {
        Write-WwiLog 'ISDeploymentWizard.exe unavailable or bypassed; deploying through catalog.deploy_project.' 'WARN'

        $deploy = @"
    DECLARE @ProjectBinary VARBINARY(MAX);
    DECLARE @OperationId   BIGINT;

    SELECT @ProjectBinary = CAST(BulkColumn AS VARBINARY(MAX))
    FROM OPENROWSET(BULK N'$ispac', SINGLE_BLOB) AS ProjectFile;

    EXEC SSISDB.catalog.deploy_project
         @folder_name  = N'$folder',
         @project_name = N'$projectName',
         @project_stream = @ProjectBinary,
         @operation_id = @OperationId OUTPUT;

    SELECT operation_id = @OperationId;

    SELECT m.message_time, m.message
    FROM SSISDB.catalog.operation_messages AS m
    WHERE m.operation_id = @OperationId
      AND m.message_type IN (120, 130)   /* error, warning */
    ORDER BY m.message_time;
"@

        # OPENROWSET runs on the SQL Server host, so the .ispac must be readable by
        # the service account. This is why the fallback path is documented as
        # "deploy from the server itself" in the deployment README.
        & sqlcmd @sqlcmdBase '-Q' $deploy
        if ($LASTEXITCODE -ne 0) { Stop-WwiWithError "catalog.deploy_project reported a failure for $projectName." }
    }

    Write-WwiLog "deployed /SSISDB/$folder/$projectName"
}

Write-WwiLog ("catalogue deployment step finished: {0} project(s) under /SSISDB/{1}" -f $IspacPath.Count, $folder)
Write-WwiLog 'Run deployment/ssis/Deploy-SsisEnvironment.ps1 next to create and bind the environment.'
