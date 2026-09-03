/*
    Object          : SQL Agent job "WWI - File Ingestion"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 14
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_File_Ingestion (00_orchestration),
                      ERR_Quarantine_BadFiles and MNT_Archive_ProcessedFiles.
    Notes           : File steps run under WWI_FileOps_Proxy because the Agent
                      service account has no rights on the partner landing zone
                      (see infrastructure/service-accounts.yaml). Idempotent;
                      not executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(InboundFileRoot) $(QuarantineFileRoot)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - File Ingestion';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Intraday',
    @description            = N'Polls the partner/carrier/bank landing zone, ingests what parses, quarantines what does not, archives what succeeded.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

/* Step 1 - is there anything to do? An empty landing zone ends the run cleanly. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Poll landing zone',
    @step_id            = 1,
    @subsystem          = N'CmdExec',
    @command            = N'cmd.exe /c "dir /b "$(InboundFileRoot)\*.*" 1>nul 2>nul"',
    @proxy_name         = N'WWI_FileOps_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 1,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_File_Ingestion_poll.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Master_File_Ingestion',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_File_Ingestion.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 4,
    @retry_attempts     = 1,
    @retry_interval     = 3,
    @output_file_name   = N'$(AgentLogRoot)\WWI_File_Ingestion_master.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Archive processed files',
    @step_id            = 3,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Maintenance\MNT_Archive_ProcessedFiles.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 3,
    @retry_attempts     = 2,
    @retry_interval     = 2,
    @output_file_name   = N'$(AgentLogRoot)\WWI_File_Ingestion_archive.log',
    @flags              = 2;

/*
    Step 4 - quarantine branch. A malformed partner file is routine, not an
    incident: move it aside, log the reject and let the next poll continue.
*/
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Quarantine unparsable files',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_ErrorHandling\ERR_Quarantine_BadFiles.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 2,
    @retry_attempts     = 1,
    @retry_interval     = 1,
    @output_file_name   = N'$(AgentLogRoot)\WWI_File_Ingestion_quarantine.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Quarantine threshold check',
    @step_id            = 5,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @Quarantined INT;
SELECT @Quarantined = COUNT(*)
FROM etl.RejectedRecord
WHERE RejectReason LIKE N''%unparsable file%''
  AND RejectedAtUtc >= DATEADD(HOUR, -24, SYSUTCDATETIME());
IF @Quarantined > 25
BEGIN
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - File Ingestion'',
        @Severity     = N''Error'',
        @ErrorMessage = N''More than 25 files quarantined in 24 hours; a partner has probably changed their layout.'';
    RAISERROR (N''Quarantine threshold exceeded.'', 16, 1);
END
',
    @on_success_action  = 1,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_File_Ingestion_threshold.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Every 15 Minutes')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Every 15 Minutes',
        @enabled                = 1,
        @freq_type              = 4,
        @freq_interval          = 1,
        @freq_subday_type       = 4,
        @freq_subday_interval   = 15,
        @active_start_time      = 000000,
        @active_end_time        = 235959;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Every 15 Minutes';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
