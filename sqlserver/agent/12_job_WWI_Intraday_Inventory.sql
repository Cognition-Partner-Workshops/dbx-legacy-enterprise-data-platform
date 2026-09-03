/*
    Object          : SQL Agent job "WWI - Intraday Inventory Refresh"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 12
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Intraday_Inventory (00_orchestration)
    Notes           : Runs every twenty minutes during warehouse operating
                      hours, which differ by region: the NA and EU distribution
                      centres run 05:00-22:00 instance time, APAC picks up the
                      same schedule one calendar day behind because the Sydney
                      instance is +10/+11. Idempotent; not executed anywhere.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Intraday Inventory Refresh';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Intraday',
    @description            = N'Twenty-minute inventory movement refresh feeding the stock-holding and backorder screens.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 3,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Master_Intraday_Inventory',
    @step_id            = 1,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Intraday_Inventory.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";0 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 3,
    @retry_attempts     = 2,
    @retry_interval     = 1,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Intraday_Inventory.log',
    @flags              = 4;

/*
    Step 2 - stale-feed detector. The refresh can "succeed" with nothing to do
    when the warehouse handheld gateway stops posting; treat more than two hours
    of silence during operating hours as an error.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Stale feed detection',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @LastRows BIGINT;
SELECT @LastRows = SUM(TargetRowCount)
FROM etl.RowCountAudit
WHERE ObjectName LIKE N''%StockItemTransaction%''
  AND AuditTimeUtc >= DATEADD(HOUR, -2, SYSUTCDATETIME());
IF ISNULL(@LastRows, 0) = 0
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - Intraday Inventory Refresh'',
        @Severity     = N''Error'',
        @ErrorMessage = N''No inventory movement rows landed in the last two hours; check the handheld gateway feed.'';
    RAISERROR (N''Stale inventory feed.'', 16, 1);
END
',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Intraday_Inventory_stale.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Record failure',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_LogError
    @SourceName   = N''WWI - Intraday Inventory Refresh'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Intraday inventory refresh failed. Next slot will re-attempt from the last watermark.'';
RAISERROR (N''Intraday inventory refresh failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Intraday_Inventory_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Every 20 Minutes 0500-2200')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Every 20 Minutes 0500-2200',
        @enabled                = 1,
        @freq_type              = 4,
        @freq_interval          = 1,
        @freq_subday_type       = 4,
        @freq_subday_interval   = 20,
        @active_start_time      = 050000,
        @active_end_time        = 220000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Every 20 Minutes 0500-2200';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
