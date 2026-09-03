/*
    Object          : SQL Agent job "WWI - Hourly Incremental"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 11
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Hourly_Incremental (00_orchestration)
    Notes           : Idempotent, does not require a running SQL Agent service,
                      and has not been executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Hourly Incremental';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Intraday',
    @description            = N'Hourly incremental order, invoice and shipment load. Skips itself while the nightly DAILY batch holds the warehouse.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/*
    Step 1 - concurrency gate. The hourly run must never overlap the nightly
    load; historically it did and produced duplicate invoice-line keys. Exit
    quietly (success) rather than failing, so the operator is not paged for a
    normal skip.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Nightly batch overlap gate',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
IF EXISTS (SELECT 1 FROM etl.Batch WHERE BatchType = N''DAILY'' AND BatchStatus = N''Running'')
BEGIN
    EXEC etl.usp_LogError
        @SourceName    = N''WWI - Hourly Incremental'',
        @Severity      = N''Warning'',
        @ErrorMessage  = N''Hourly incremental skipped: nightly DAILY batch is running.'';
    RAISERROR (N''SKIP'', 0, 1) WITH NOWAIT;
END
',
    @on_success_action  = 3,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Hourly_Incremental_gate.log',
    @flags              = 2;

/* Step 2 - narrow the incremental window before the packages read it. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Advance intraday watermark floor',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @LookbackMinutes INT =
    TRY_CONVERT(INT, etl.ufn_GetConfigurationValue(N''$(EnvironmentCode)'', N''Hourly.LookbackMinutes''));
IF @LookbackMinutes IS NULL SET @LookbackMinutes = 90;
EXEC etl.usp_SetWatermark
    @WatermarkName  = N''Hourly.SafeFloor'',
    @WatermarkType  = N''Timestamp'',
    @TimestampValue = DATEADD(MINUTE, -@LookbackMinutes, SYSUTCDATETIME());
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 1,
    @retry_interval     = 2,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Hourly_Incremental_watermark.log',
    @flags              = 2;

/*
    Step 3 - the incremental orchestration. Three quick retries: the OLTP
    replica is failed over during the business day and reconnects inside a
    couple of minutes.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Master_Hourly_Incremental',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Hourly_Incremental.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 3,
    @retry_interval     = 2,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Hourly_Incremental_master.log',
    @flags              = 2;

/*
    Step 4 - failure branch. A single hourly failure is tolerated; three in a
    row disables the schedule so the estate is not hammered all afternoon.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Failure escalation',
    @step_id            = 4,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @RecentFailures INT;
SELECT @RecentFailures = COUNT(*)
FROM etl.PackageExecution
WHERE PackageName = N''Master_Hourly_Incremental''
  AND ExecutionStatus = N''Failed''
  AND StartTimeUtc >= DATEADD(HOUR, -4, SYSUTCDATETIME());
IF @RecentFailures >= 3
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - Hourly Incremental'',
        @Severity     = N''Error'',
        @ErrorMessage = N''Three consecutive hourly failures; schedule disabled pending investigation.'';
    EXEC msdb.dbo.sp_update_job @job_name = N''WWI - Hourly Incremental'', @enabled = 0;
END
RAISERROR (N''Hourly incremental failed; see etl.ErrorLog.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Hourly_Incremental_escalate.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/* Hourly between 05:00 and 23:00; the 00:00-04:00 window belongs to the nightly load. */
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Hourly 0500-2300')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Hourly 0500-2300',
        @enabled                = 1,
        @freq_type              = 4,
        @freq_interval          = 1,
        @freq_subday_type       = 8,
        @freq_subday_interval   = 1,
        @active_start_time      = 050000,
        @active_end_time        = 230000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Hourly 0500-2300';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
