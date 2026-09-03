/*
    Object          : SQL Agent job "WWI - Daily ETL"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 10 - after categories, operators and proxies
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : SSIS package Master_Daily_ETL (folder 00_orchestration,
                      project WWI_Orchestration) plus the pre-flight and
                      reconciliation gates around it.
    Notes           : Idempotent - the job is dropped and rebuilt so that step
                      ordering and failure branching stay exactly as declared.
                      The script does not require the SQL Agent service to be
                      running; it only writes msdb catalogue rows. It has not
                      been executed against any server.

    sqlcmd variables:
        $(SsisFolder)      - SSIS catalogue folder, e.g. WWI_DEV
        $(SsisProject)     - SSIS catalogue project, e.g. WWI_Orchestration
        $(StagingDatabase) - staging database name
        $(AgentLogRoot)    - directory for job step output files
        $(EnvironmentCode) - DEV / TEST / PROD
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Daily ETL';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Nightly',
    @description            = N'Nightly full warehouse load: reference -> dimensions -> facts -> aggregates -> publish. Runs Master_Daily_ETL under the etl batch framework.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/*
    Step 1 - pre-flight. Fails the job before anything expensive starts if disk,
    configuration or the previous batch state is not clean. On failure it jumps
    to the notification step rather than the generic failure path so the
    operator gets the pre-flight reason rather than a package error.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Pre-flight checks',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @OpenBatches INT;
SELECT @OpenBatches = COUNT(*)
FROM etl.Batch
WHERE BatchStatus = N''Running''
  AND BatchType = N''DAILY'';
IF @OpenBatches > 0
    RAISERROR (N''A DAILY batch is still marked Running. Close or fail it before restarting.'', 16, 1);
IF etl.ufn_GetConfigurationValue(N''$(EnvironmentCode)'', N''Daily.EnableLoad'') <> N''1''
    RAISERROR (N''Daily.EnableLoad is switched off for this environment.'', 16, 1);
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 6,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_preflight.log',
    @flags              = 2;

/*
    Step 2 - the orchestration package itself. Two retries with a fifteen minute
    gap absorbs the ERP's nightly listener bounce, which historically drops the
    first Oracle extract of the run.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Master_Daily_ETL',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\$(SsisProject)\Master_Daily_ETL.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /Par "\"$ServerOption::SYNCHRONIZED(Boolean)\"";True /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 2,
    @retry_interval     = 15,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_master.log',
    @flags              = 2;

/*
    Step 3 - row-count balance gate. A tolerance breach is a warning in DEV and
    an error everywhere else; the procedure decides based on etl.Configuration.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Row count reconciliation',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @BatchId BIGINT;
SELECT TOP (1) @BatchId = BatchId
FROM etl.Batch
WHERE BatchType = N''DAILY''
ORDER BY BatchId DESC;
EXEC etl.usp_AssertRowCountTolerance @BatchId = @BatchId, @Scope = N''ALL'';
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_reconcile.log',
    @flags              = 2;

/* Step 4 - housekeeping that must run whether or not step 3 warned. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Post-load statistics refresh',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Update_Statistics.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 6,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_stats.log',
    @flags              = 2;

/* Step 5 - failure branch: close the batch, then fall through to notification. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Mark batch failed',
    @step_id            = 5,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @BatchId BIGINT;
SELECT TOP (1) @BatchId = BatchId
FROM etl.Batch
WHERE BatchType = N''DAILY''
  AND BatchStatus = N''Running''
ORDER BY BatchId DESC;
IF @BatchId IS NOT NULL
    EXEC etl.usp_EndBatch @BatchId = @BatchId, @BatchStatus = N''Failed'';
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 1,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_failbranch.log',
    @flags              = 2;

/* Step 6 - notification branch. Always the last step so both paths end here. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'06 - Raise operator notification',
    @step_id            = 6,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_ErrorHandling\ERR_Notify_Operations.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Daily_ETL_notify.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/*
    Schedule: 01:30 local instance time, every day. The NA warehouse close is at
    23:00 NA-Eastern and the EU ledger cut is 22:00 UTC, so 01:30 is the first
    slot after both.
*/
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Nightly 0130')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name      = N'WWI - Nightly 0130',
        @enabled            = 1,
        @freq_type          = 4,
        @freq_interval      = 1,
        @freq_subday_type   = 1,
        @active_start_time  = 013000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Nightly 0130';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
