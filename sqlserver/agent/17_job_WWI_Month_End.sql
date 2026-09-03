/*
    Object          : SQL Agent job "WWI - Month End"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 17
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Month_End (00_orchestration)
    Notes           : Month-end snapshot, correction re-processing and aggregate
                      rebuild. The job is disabled by default after deployment
                      because the close calendar is entered by hand each year;
                      the operations team enables it once the calendar rows are
                      loaded. Idempotent; not executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Month End';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 0,
    @category_name          = N'WWI ETL - Period Close',
    @description            = N'Month-end snapshot, correction reprocessing and aggregate rebuild. Enabled by operations once the close calendar for the year is loaded.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI Finance Systems',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/*
    Step 1 - calendar gate. The fiscal calendars differ: NA closes on the last
    calendar day, EU on the last banking day, APAC on the first business day of
    the following month. The calendar table decides whether tonight is a close
    night for any region at all.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Fiscal calendar gate',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Today DATE = CAST(SYSDATETIME() AS DATE);
DECLARE @Regions NVARCHAR(100) = N'''';
SELECT @Regions = @Regions + RegionCode + N'',''
FROM ref.FiscalCalendar
WHERE PeriodCloseDate = @Today;
IF LEN(@Regions) = 0
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - Month End'',
        @Severity     = N''Information'',
        @ErrorMessage = N''Not a close date for any region; month end skipped.'';
    RAISERROR (N''SKIP'', 0, 1) WITH NOWAIT;
END
ELSE
BEGIN
    EXEC etl.usp_SetWatermark
        @WatermarkName = N''MonthEnd.RegionsInScope'',
        @WatermarkType = N''DateWindow'',
        @StringValue   = @Regions;
END
',
    @on_success_action  = 3,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Month_End_calendar.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Master_Month_End',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Month_End.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";2 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 1,
    @retry_interval     = 30,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Month_End_master.log',
    @flags              = 2;

/* Step 3 - snapshot balance gate; a close that does not balance is not a close. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Close balance assertion',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @BatchId BIGINT;
SELECT TOP (1) @BatchId = BatchId
FROM etl.Batch
WHERE BatchType = N''MONTH_END''
ORDER BY BatchId DESC;
EXEC etl.usp_AssertRowCountTolerance @BatchId = @BatchId, @Scope = N''FINANCE'';
',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Month_End_balance.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Close failure branch',
    @step_id            = 4,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_LogError
    @SourceName   = N''WWI - Month End'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Month-end close did not complete. The period stays open; finance must be told before the ledger cut-off.'';
RAISERROR (N''Month-end close failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Month_End_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/*
    Nightly at 04:00 with the calendar gate deciding whether to proceed. A
    monthly Agent schedule cannot express three different regional close rules,
    which is why the gate lives in the first step.
*/
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Nightly 0400 Calendar Gated')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name      = N'WWI - Nightly 0400 Calendar Gated',
        @enabled            = 1,
        @freq_type          = 4,
        @freq_interval      = 1,
        @freq_subday_type   = 1,
        @active_start_time  = 040000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Nightly 0400 Calendar Gated';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
