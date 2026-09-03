/*
    Object          : SQL Agent job "WWI - Customer Master Sync"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 13
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Customer_Sync (00_orchestration) and the regional
                      consent/retention enforcement that must follow it.
    Notes           : The three regions are not symmetrical. EU rows are subject
                      to consent withdrawal and a shorter retention clock, APAC
                      carries a separate cross-border transfer flag, and NA has
                      neither. The job therefore runs one shared load followed
                      by region-specific post-steps. Idempotent; not executed.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Customer Master Sync';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 1,
    @category_name          = N'WWI ETL - Nightly',
    @description            = N'Nightly customer master synchronisation from the ERP, followed by regional consent and retention enforcement.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI ETL On-Call',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - Master_Customer_Sync',
    @step_id            = 1,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Customer_Sync.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";1 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 2,
    @retry_interval     = 10,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Customer_Sync_master.log',
    @flags              = 2;

/* Step 2 - EU: consent withdrawal wins over the ERP feed, always. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - EU consent and retention enforcement',
    @step_id            = 2,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @RetentionMonths INT =
    TRY_CONVERT(INT, etl.ufn_GetConfigurationValue(N''$(EnvironmentCode)'', N''EU.CustomerRetentionMonths''));
IF @RetentionMonths IS NULL SET @RetentionMonths = 24;
EXEC etl.usp_LogRowCount
    @ObjectName     = N''stg.Customer_EU_ConsentSweep'',
    @SourceRowCount = 0,
    @TargetRowCount = 0,
    @Notes          = N''EU consent sweep started'';
EXEC stg.usp_ApplyCustomerConsentEU @RetentionMonths = @RetentionMonths;
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Customer_Sync_eu_consent.log',
    @flags              = 2;

/* Step 3 - APAC: cross-border transfer flag, no consent withdrawal concept. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - APAC cross-border transfer flagging',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC stg.usp_FlagCustomerCrossBorderAPAC
    @RestrictedCountryList = N''AU,NZ,SG,JP,KR'';
',
    @on_success_action  = 3,
    @on_fail_action     = 3,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Customer_Sync_apac_xborder.log',
    @flags              = 2;

/* Step 4 - NA: postal standardisation refresh only. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - NA address standardisation',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Staging\STG_Load_CustomerAddress.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 1,
    @retry_interval     = 5,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Customer_Sync_na_address.log',
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
    @SourceName   = N''WWI - Customer Master Sync'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Customer master sync failed. Downstream DIM_*_Load_Customer packages will run against yesterday''''s master.'';
RAISERROR (N''Customer master sync failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Customer_Sync_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Nightly 0015')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name      = N'WWI - Nightly 0015',
        @enabled            = 1,
        @freq_type          = 4,
        @freq_interval      = 1,
        @freq_subday_type   = 1,
        @active_start_time  = 001500;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Nightly 0015';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
