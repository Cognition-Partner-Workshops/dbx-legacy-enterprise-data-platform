/*
    Object          : SQL Agent job "WWI - Control History Purge"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 20
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : MNT_Purge_ControlHistory and etl.usp_PurgeControlHistory
    Notes           : Retention is not uniform. Batch and package execution
                      history is kept for thirteen months so year-on-year run
                      comparisons survive a close cycle; error and reject detail
                      is kept for the regionally-mandated period, which is
                      shortest in the EU. Idempotent; not executed anywhere.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(DwDatabase) $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Control History Purge';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI Platform - Maintenance',
    @description            = N'Purges etl control history in the staging and warehouse databases according to per-table retention, and trims SQL Agent and SSIS catalogue history.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI Platform DBA',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Purge staging control history',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_PurgeControlHistory
    @ExecutionHistoryMonths = 13,
    @ErrorHistoryMonths     = 24,
    @RejectHistoryMonths    = 12;
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 10,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Control_Purge_staging.log',
    @flags              = 2;

/*
    The warehouse copy of the control schema keeps error history longer because
    the finance audit trail is read from the warehouse, not from staging.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Purge warehouse control history',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(DwDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_PurgeControlHistory
    @ExecutionHistoryMonths = 13,
    @ErrorHistoryMonths     = 84,
    @RejectHistoryMonths    = 24;
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 10,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Control_Purge_dw.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - MNT_Purge_ControlHistory',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Purge_ControlHistory.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Control_Purge_package.log',
    @flags              = 2;

/* Step 4 - msdb housekeeping. Job history is the first thing to fill msdb. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Trim SQL Agent job history',
    @step_id            = 4,
    @subsystem          = N'TSQL',
    @database_name      = N'msdb',
    @command            = N'
SET NOCOUNT ON;
DECLARE @OldestDate DATETIME = DATEADD(DAY, -120, GETDATE());
EXEC msdb.dbo.sp_purge_jobhistory @oldest_date = @OldestDate;
',
    @on_success_action  = 1,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Control_Purge_msdb.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/* Sundays at 05:00, after the weekly reference refresh has finished. */
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Weekly Sunday 0500')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Weekly Sunday 0500',
        @enabled                = 1,
        @freq_type              = 8,
        @freq_interval          = 1,
        @freq_recurrence_factor = 1,
        @freq_subday_type       = 1,
        @active_start_time      = 050000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Weekly Sunday 0500';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
