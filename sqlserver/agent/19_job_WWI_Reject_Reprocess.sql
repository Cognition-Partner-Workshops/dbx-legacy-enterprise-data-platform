/*
    Object          : SQL Agent job "WWI - Reject Reprocessing"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 19
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : DQ_Reject_Reprocess, ERR_Route_RejectedRows,
                      ERR_Retry_FailedSteps
    Notes           : Rejected rows are never dropped. Most rejects are late
                      dimension arrivals that resolve themselves once the master
                      catches up, so this job re-drives them four times a day
                      and only escalates rows that have failed five attempts.
                      Idempotent; not executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Reject Reprocessing';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Recovery',
    @description            = N'Re-drives rejected rows after late dimension arrival, retries transient extract failures and escalates rows past their attempt limit.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/* Step 1 - anything to reprocess? Exit early rather than starting SSIS. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Reject queue check',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Pending INT;
SELECT @Pending = COUNT(*)
FROM etl.RejectedRecord
WHERE ReprocessStatus IN (N''Pending'', N''Retry'')
  AND ISNULL(AttemptCount, 0) < 5;
IF @Pending = 0
BEGIN
    RAISERROR (N''No rejects pending reprocessing.'', 0, 1) WITH NOWAIT;
END
',
    @on_success_action  = 3,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Reject_Reprocess_queue.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - DQ_Reject_Reprocess',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_DataQuality\DQ_Reject_Reprocess.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Reject_Reprocess_dq.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Retry transient extract failures',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_ErrorHandling\ERR_Retry_FailedSteps.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 2,
    @retry_interval     = 3,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Reject_Reprocess_retry.log',
    @flags              = 2;

/*
    Step 4 - escalation. Rows that have burned five attempts are routed to the
    error file share for the data steward to work by hand; region matters here
    because EU rejects containing personal data go to a restricted share.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Route exhausted rejects',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_ErrorHandling\ERR_Route_RejectedRows.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Reject_Reprocess_route.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Failure branch',
    @step_id            = 5,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_LogError
    @SourceName   = N''WWI - Reject Reprocessing'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Reject reprocessing failed; the reject queue keeps growing until this is cleared.'';
RAISERROR (N''Reject reprocessing failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Reject_Reprocess_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/* Every four hours, offset by ten minutes so it never collides with the hourly load. */
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Every 4 Hours 0010')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Every 4 Hours 0010',
        @enabled                = 1,
        @freq_type              = 4,
        @freq_interval          = 1,
        @freq_subday_type       = 8,
        @freq_subday_interval   = 4,
        @active_start_time      = 001000,
        @active_end_time        = 235959;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Every 4 Hours 0010';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
