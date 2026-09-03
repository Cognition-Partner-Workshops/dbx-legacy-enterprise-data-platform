/*
    Object          : SQL Agent job "WWI - Weekly Maintenance"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 16
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Weekly_Maintenance (00_orchestration),
                      MNT_Rebuild_Indexes, MNT_Purge_StagingHistory
    Notes           : Runs inside the Saturday maintenance window declared in
                      infrastructure/monitoring-alerting.yaml. Every step is
                      restartable; the job deliberately continues past a failed
                      index rebuild so the purge still frees space. Idempotent;
                      not executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(DwDatabase) $(AgentLogRoot)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Weekly Maintenance';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI Platform - Maintenance',
    @description            = N'Weekend maintenance window: index and columnstore maintenance, statistics, staging purge, integrity checks.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI Platform DBA',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Master_Weekly_Maintenance',
    @step_id            = 1,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Weekly_Maintenance.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Maintenance_master.log',
    @flags              = 2;

/* Step 2 - integrity check on the warehouse; the DBA list owns any failure. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - DBCC CHECKDB (warehouse)',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(DwDatabase)',
    @command            = N'
SET NOCOUNT ON;
DBCC CHECKDB WITH PHYSICAL_ONLY, NO_INFOMSGS;
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Maintenance_checkdb.log',
    @flags              = 2;

/* Step 3 - staging purge honours per-schema retention from etl.Configuration. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Purge staging history',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Purge_StagingHistory.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 10,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Maintenance_purge.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Rebuild indexes and columnstore',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Rebuild_Indexes.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 15,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Maintenance_indexes.log',
    @flags              = 2;

/* Step 5 - final summary; fails the job if any earlier step logged an error. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Maintenance window summary',
    @step_id            = 5,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Errors INT;
SELECT @Errors = COUNT(*)
FROM etl.ErrorLog
WHERE Severity = N''Error''
  AND SourceName LIKE N''MNT_%''
  AND LoggedAtUtc >= DATEADD(HOUR, -12, SYSUTCDATETIME());
IF @Errors > 0
    RAISERROR (N''Maintenance window completed with errors; see etl.vw_ErrorsLast7Days.'', 16, 1);
',
    @on_success_action  = 1,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Maintenance_summary.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Weekly Saturday 2200')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Weekly Saturday 2200',
        @enabled                = 1,
        @freq_type              = 8,
        @freq_interval          = 64,
        @freq_recurrence_factor = 1,
        @freq_subday_type       = 1,
        @active_start_time      = 220000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Weekly Saturday 2200';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
