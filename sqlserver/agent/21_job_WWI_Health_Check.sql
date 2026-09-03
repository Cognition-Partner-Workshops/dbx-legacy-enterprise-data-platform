/*
    Object          : SQL Agent job "WWI - ETL Health Check"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 21
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : MNT_Check_DiskSpace, MNT_Validate_Configuration and the
                      etl operational views listed in
                      infrastructure/monitoring-alerting.yaml.
    Notes           : This is the estate's only monitoring surface. There is no
                      external monitoring agent on the ETL instance, so the
                      checks are expressed as SQL over the control tables and
                      raise through the operator. Idempotent; not executed
                      against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - ETL Health Check';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI Platform - Monitoring',
    @description            = N'Half-hourly health check over the etl control views: stuck batches, long-running packages, error rate, reject rate, watermark drift, disk headroom.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/* Step 1 - stuck batch and long-running package detection. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Stuck batch and duration outliers',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Problems NVARCHAR(MAX) = N'''';

SELECT @Problems = @Problems + N''Batch '' + CONVERT(NVARCHAR(20), BatchId)
                 + N'' ('' + BatchType + N'') running for ''
                 + CONVERT(NVARCHAR(10), DATEDIFF(MINUTE, StartTimeUtc, SYSUTCDATETIME())) + N'' minutes. ''
FROM etl.vw_BatchStatus
WHERE BatchStatus = N''Running''
  AND DATEDIFF(MINUTE, StartTimeUtc, SYSUTCDATETIME()) > 240;

SELECT @Problems = @Problems + N''Package '' + PackageName
                 + N'' ran '' + CONVERT(NVARCHAR(10), DurationSeconds)
                 + N''s against a median of '' + CONVERT(NVARCHAR(10), MedianDurationSeconds) + N''s. ''
FROM etl.vw_PackageDurations
WHERE DurationSeconds > MedianDurationSeconds * 3
  AND MedianDurationSeconds > 60;

IF LEN(@Problems) > 0
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - ETL Health Check'',
        @Severity     = N''Warning'',
        @ErrorMessage = @Problems;
END
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Health_Check_batches.log',
    @flags              = 2;

/* Step 2 - error, reject and variance rates against configured thresholds. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Error and reject rate thresholds',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @MaxRejectPercent INT =
    TRY_CONVERT(INT, etl.ufn_GetConfigurationValue(N''$(EnvironmentCode)'', N''Reject.MaxPercent''));
IF @MaxRejectPercent IS NULL SET @MaxRejectPercent = 5;

DECLARE @ErrorCount INT;
SELECT @ErrorCount = COUNT(*) FROM etl.vw_ErrorsLast7Days WHERE Severity = N''Error'' AND LoggedAtUtc >= DATEADD(HOUR, -1, SYSUTCDATETIME());

DECLARE @Variance INT;
SELECT @Variance = COUNT(*) FROM etl.vw_RowCountVariance WHERE ABS(VariancePercent) > @MaxRejectPercent;

IF @ErrorCount > 0 OR @Variance > 0
    RAISERROR (N''Health check: errors or row-count variance outside tolerance in the last hour.'', 16, 1);
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Health_Check_rates.log',
    @flags              = 2;

/* Step 3 - watermark drift: a watermark that has not moved is a silent outage. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Watermark drift',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
IF EXISTS (SELECT 1
           FROM etl.vw_WatermarkCurrent
           WHERE WatermarkType = N''Timestamp''
             AND LastUpdatedUtc < DATEADD(HOUR, -6, SYSUTCDATETIME()))
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - ETL Health Check'',
        @Severity     = N''Warning'',
        @ErrorMessage = N''One or more timestamp watermarks have not advanced in six hours.'';
END
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Health_Check_watermarks.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Disk headroom and configuration',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Check_DiskSpace.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Health_Check_disk.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Configuration key assertion',
    @step_id            = 5,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Validate_Configuration.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Health_Check_config.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Every 30 Minutes')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Every 30 Minutes',
        @enabled                = 1,
        @freq_type              = 4,
        @freq_interval          = 1,
        @freq_subday_type       = 4,
        @freq_subday_interval   = 30,
        @active_start_time      = 000000,
        @active_end_time        = 235959;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Every 30 Minutes';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
