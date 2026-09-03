/*
    Object          : SQL Agent job "WWI - Weekly Reference Refresh"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 15
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Weekly_Reference_Load (00_orchestration)
    Notes           : Reference data is region-divergent: NA carries state sales
                      tax jurisdictions, EU carries VAT rates and reverse-charge
                      indicators, APAC carries GST registration classes. Each
                      set has its own upstream publication day, so the job
                      refreshes them in three separate steps with different
                      tolerance for staleness. Idempotent; not executed.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Weekly Reference Refresh';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Reference',
    @description            = N'Weekly full refresh of reference data: tax jurisdictions, VAT rates, GST classes, currency and country code sets.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Master_Weekly_Reference_Load',
    @step_id            = 1,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Weekly_Reference_Load.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 2,
    @retry_interval     = 20,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Reference.log',
    @flags              = 2;

/*
    Step 2 - staleness gate per region. The EU VAT feed publishes on Thursdays,
    the NA jurisdiction file monthly, and the APAC GST class list quarterly, so
    each has a different acceptable age.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Regional reference staleness gate',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Stale TABLE (RegionCode NCHAR(4), ObjectName SYSNAME, MaxAgeDays INT);
INSERT INTO @Stale (RegionCode, ObjectName, MaxAgeDays)
VALUES (N''NA'',   N''ref.TaxJurisdictionNA'',  45),
       (N''EU'',   N''ref.VatRateEU'',          10),
       (N''APAC'', N''ref.GstClassAPAC'',      120);

DECLARE @Offenders NVARCHAR(MAX) = N'''';
SELECT @Offenders = @Offenders + s.RegionCode + N'':'' + s.ObjectName + N'' ''
FROM @Stale AS s
LEFT JOIN etl.RowCountAudit AS a
       ON a.ObjectName = s.ObjectName
      AND a.AuditTimeUtc >= DATEADD(DAY, -s.MaxAgeDays, SYSUTCDATETIME())
WHERE a.ObjectName IS NULL;

IF LEN(@Offenders) > 0
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - Weekly Reference Refresh'',
        @Severity     = N''Warning'',
        @ErrorMessage = @Offenders;
END
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Reference_staleness.log',
    @flags              = 2;

/* Step 3 - referential screen: reference changes orphan dimension members. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Referential integrity screen',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_DataQuality\DQ_Referential_Screen.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Reference_referential.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Failure branch',
    @step_id            = 4,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_LogError
    @SourceName   = N''WWI - Weekly Reference Refresh'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Weekly reference refresh failed; dimension loads will translate against last week''''s code sets.'';
RAISERROR (N''Weekly reference refresh failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Weekly_Reference_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/* Sundays at 03:00, between the Saturday maintenance window and Monday trading. */
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Weekly Sunday 0300')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Weekly Sunday 0300',
        @enabled                = 1,
        @freq_type              = 8,
        @freq_interval          = 1,
        @freq_recurrence_factor = 1,
        @freq_subday_type       = 1,
        @active_start_time      = 030000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Weekly Sunday 0300';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
