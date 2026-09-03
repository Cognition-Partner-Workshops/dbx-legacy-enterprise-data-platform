/*
    Object          : SQL Agent job "WWI - Finance Close"
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 18
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Drives          : Master_Finance_Close (00_orchestration)
    Notes           : The finance close is the only job that carries a
                      hand-maintained FX rate dependency: EU consolidates at the
                      ECB closing rate, APAC at the RBA/MAS rate published the
                      following morning, NA at the internal corporate rate. The
                      first step refuses to run until all three are present for
                      the period. Idempotent; not executed against any server.

    sqlcmd variables: $(SsisFolder) $(SsisServer) $(StagingDatabase)
                      $(AgentLogRoot) $(EnvironmentCode)
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

USE msdb;
GO

DECLARE @JobName SYSNAME = N'WWI - Finance Close';
DECLARE @JobId UNIQUEIDENTIFIER;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = @JobName)
    EXEC msdb.dbo.sp_delete_job @job_name = @JobName, @delete_unused_schedule = 0;

EXEC msdb.dbo.sp_add_job
    @job_name               = @JobName,
    @enabled                = 0,
    @category_name          = N'WWI ETL - Period Close',
    @description            = N'Month-end finance close sequence: FX gate, ledger extract, AP/AR aging, consolidation and finance aggregate rebuild.',
    @notify_level_eventlog  = 2,
    @notify_level_email     = 2,
    @notify_email_operator_name = N'WWI Finance Systems',
    @owner_login_name       = N'sa',
    @job_id                 = @JobId OUTPUT;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'01 - FX rate availability gate',
    @step_id            = 1,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
DECLARE @PeriodEnd DATE = EOMONTH(DATEADD(MONTH, -1, SYSDATETIME()));
DECLARE @Missing NVARCHAR(200) = N'''';

IF NOT EXISTS (SELECT 1 FROM ref.FxRate WHERE RateSetCode = N''ECB_CLOSING''      AND RateDate = @PeriodEnd)
    SET @Missing = @Missing + N''ECB_CLOSING '';
IF NOT EXISTS (SELECT 1 FROM ref.FxRate WHERE RateSetCode = N''APAC_MORNING''     AND RateDate = @PeriodEnd)
    SET @Missing = @Missing + N''APAC_MORNING '';
IF NOT EXISTS (SELECT 1 FROM ref.FxRate WHERE RateSetCode = N''CORP_INTERNAL_NA'' AND RateDate = @PeriodEnd)
    SET @Missing = @Missing + N''CORP_INTERNAL_NA '';

IF LEN(@Missing) > 0
BEGIN
    DECLARE @Message NVARCHAR(400) = N''Finance close blocked; missing FX rate sets: '' + @Missing;
    EXEC etl.usp_LogError
        @SourceName   = N''WWI - Finance Close'',
        @Severity     = N''Error'',
        @ErrorMessage = @Message;
    RAISERROR (@Message, 16, 1);
END
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 4,
    @retry_interval     = 60,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Finance_Close_fxgate.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'02 - Master_Finance_Close',
    @step_id            = 2,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Orchestration\Master_Finance_Close.dtsx\"" /SERVER "$(SsisServer)" /Par "\"$ServerOption::LOGGING_LEVEL(Int16)\"";2 /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 1,
    @retry_interval     = 20,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Finance_Close_master.log',
    @flags              = 2;

/* Step 3 - tax treatment reconciliation: sales tax, VAT and GST are separate. */
EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'03 - Regional tax reconciliation',
    @step_id            = 3,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_EvaluateDataQualityRules
    @RuleGroup = N''FINANCE_TAX_CLOSE'';

IF EXISTS (SELECT 1 FROM etl.vw_DataQualityFailures WHERE RuleGroup = N''FINANCE_TAX_CLOSE'')
    RAISERROR (N''Regional tax reconciliation failed; see etl.vw_DataQualityFailures.'', 16, 1);
',
    @on_success_action  = 3,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Finance_Close_tax.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'04 - Rebuild finance aggregates',
    @step_id            = 4,
    @subsystem          = N'SSIS',
    @command            = N'/ISSERVER "\"\SSISDB\$(SsisFolder)\WWI_Aggregates\AGG_Refresh_FinanceCloseSummary.dtsx\"" /SERVER "$(SsisServer)" /CALLERINFO SQLAGENT /REPORTING E',
    @proxy_name         = N'WWI_SSIS_Proxy',
    @on_success_action  = 1,
    @on_fail_action     = 4,
    @on_fail_step_id    = 5,
    @retry_attempts     = 1,
    @retry_interval     = 10,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Finance_Close_aggregates.log',
    @flags              = 2;

EXEC msdb.dbo.sp_add_jobstep
    @job_id             = @JobId,
    @step_name          = N'05 - Close failure branch',
    @step_id            = 5,
    @subsystem          = N'TSQL',
    @database_name      = N'$(StagingDatabase)',
    @command            = N'
SET NOCOUNT ON;
EXEC etl.usp_LogError
    @SourceName   = N''WWI - Finance Close'',
    @Severity     = N''Error'',
    @ErrorMessage = N''Finance close sequence failed. The ledger period remains open and the consolidation pack cannot be issued.'';
RAISERROR (N''Finance close failed.'', 16, 1);
',
    @on_success_action  = 2,
    @on_fail_action     = 2,
    @retry_attempts     = 0,
    @output_file_name   = N'$(AgentLogRoot)\WWI_Finance_Close_fail.log',
    @flags              = 2;

EXEC msdb.dbo.sp_update_job @job_id = @JobId, @start_step_id = 1;

/* First calendar day of every month at 05:00, after the month-end snapshot. */
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'WWI - Monthly Day1 0500')
BEGIN
    EXEC msdb.dbo.sp_add_schedule
        @schedule_name          = N'WWI - Monthly Day1 0500',
        @enabled                = 1,
        @freq_type              = 16,
        @freq_interval          = 1,
        @freq_recurrence_factor = 1,
        @freq_subday_type       = 1,
        @active_start_time      = 050000;
END

EXEC msdb.dbo.sp_attach_schedule @job_id = @JobId, @schedule_name = N'WWI - Monthly Day1 0500';
EXEC msdb.dbo.sp_add_jobserver @job_id = @JobId, @server_name = N'(LOCAL)';
GO
