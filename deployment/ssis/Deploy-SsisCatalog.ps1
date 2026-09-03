<#
    Deploys the WWI .ispac into the SSIS catalogue and creates the folder.

    Uses ISDeploymentWizard.exe in silent mode when it is available, and falls
    back to catalog.deploy_project through sqlcmd when it is not - the estate
    has both kinds of deploy host, and the wizard is the older of the two paths.

    The SSISDB catalogue itself is assumed to exist: creating it requires a
    catalogue master key decision that belongs to the DBA team, not to this
    repository. Deploy-SsisEnvironment.ps1 must run afterwards to create the
    environment and bind parameters.

    Connection details come from SSIS_SERVER, SSIS_FOLDER, SSIS_PROJECT and, for
    the sqlcmd fallback, SQLSERVER_USER / SQLSERVER_PASSWORD.

    This script has not been executed against any SSIS catalogue.

    Usage: .\deployment\ssis\Deploy-SsisCatalog.ps1 [-IspacPath artifacts\WWI_Estate.ispac] [-DryRun]
#>

[CmdletBinding()]
param(
    [string] $IspacPath,
    [switch] $UseSqlcmdFallback,
    [switch] $DryRun
)

. (Join-Path $PSScriptRoot '..' 'lib' 'Common.ps1')
$script:WwiLogPrefix = 'wwi-deploy-ssis'

Assert-WwiEnvironmentVariable @('SSIS_SERVER', 'SSIS_FOLDER', 'SSIS_PROJECT')
Confirm-WwiProduction

$repoRoot        = Get-WwiRepositoryRoot
$environmentCode = Get-WwiEnvironmentCode
$folder          = $env:SSIS_FOLDER
$project         = $env:SSIS_PROJECT

if ([string]::IsNullOrWhiteSpace($IspacPath)) {
    $IspacPath = Join-Path $repoRoot (Join-Path 'artifacts' "$project.ispac")
}

if (-not $DryRun -and -not (Test-Path $IspacPath)) {
    Stop-WwiWithError "$IspacPath does not exist. Run deployment/ssis/Build-SsisProject.ps1 first."
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
    Write-WwiLog "WHATIF deploy $IspacPath as [$folder]\[$project] ($environmentCode)"
    return
}

Write-WwiLog "ensuring catalogue folder [$folder] exists"
& sqlcmd @sqlcmdBase '-Q' $createFolder
if ($LASTEXITCODE -ne 0) { Stop-WwiWithError "could not create or verify catalogue folder [$folder]." }

$wizard = Get-Command ISDeploymentWizard.exe -ErrorAction SilentlyContinue

if ($wizard -and -not $UseSqlcmdFallback) {
    # /Silent is positional in the wizard's odd command line; the order below is
    # the one that works with the 2016-era wizard the estate still deploys with.
    $arguments = @(
        '/Silent',
        '/SourcePath:{0}' -f $IspacPath,
        '/DestinationServer:{0}' -f $env:SSIS_SERVER,
        '/DestinationPath:/SSISDB/{0}/{1}' -f $folder, $project
    )
    Invoke-WwiCommand -Description "ISDeploymentWizard $project -> /SSISDB/$folder" `
                      -FilePath $wizard.Source -ArgumentList $arguments | Out-Null
}
else {
    Write-WwiLog 'ISDeploymentWizard.exe unavailable or bypassed; deploying through catalog.deploy_project.' 'WARN'

    $deploy = @"
DECLARE @ProjectBinary VARBINARY(MAX);
DECLARE @OperationId   BIGINT;

SELECT @ProjectBinary = CAST(BulkColumn AS VARBINARY(MAX))
FROM OPENROWSET(BULK N'$IspacPath', SINGLE_BLOB) AS ProjectFile;

EXEC SSISDB.catalog.deploy_project
     @folder_name  = N'$folder',
     @project_name = N'$project',
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
    if ($LASTEXITCODE -ne 0) { Stop-WwiWithError 'catalog.deploy_project reported a failure.' }
}

Write-WwiLog "catalogue deployment step finished for /SSISDB/$folder/$project"
Write-WwiLog 'Run deployment/ssis/Deploy-SsisEnvironment.ps1 next to create and bind the environment.'
