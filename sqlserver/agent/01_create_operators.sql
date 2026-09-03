/*
    Object          : SQL Agent operators and notification targets
    Deploy target   : msdb
    Deploy order    : sqlserver/agent step 2 - after 00_create_job_categories.sql
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1 (agent stage)
    Notes           : Operator e-mail addresses are distribution lists, not
                      individuals, and are overridden per environment by the
                      sqlcmd variables below. No mail profile is created here;
                      Database Mail configuration is an infrastructure task
                      described in infrastructure/monitoring-alerting.yaml.
                      Idempotent. Not executed against any server.

    sqlcmd variables (supplied by the deployment driver):
        $(EtlOperatorEmail)      - nightly batch owner distribution list
        $(DbaOperatorEmail)      - platform / DBA on-call distribution list
        $(FinanceOperatorEmail)  - finance close business owner list
*/

SET NOCOUNT ON;
GO

USE msdb;
GO

:setvar EtlOperatorEmail "wwi-etl-oncall@example.internal"
:setvar DbaOperatorEmail "wwi-dba-oncall@example.internal"
:setvar FinanceOperatorEmail "wwi-finance-systems@example.internal"
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'WWI ETL On-Call')
BEGIN
    EXEC msdb.dbo.sp_add_operator
        @name                = N'WWI ETL On-Call',
        @enabled             = 1,
        @email_address       = N'$(EtlOperatorEmail)',
        @pager_days          = 127,
        @weekday_pager_start_time = 000000,
        @weekday_pager_end_time   = 235959;
END
ELSE
BEGIN
    EXEC msdb.dbo.sp_update_operator
        @name          = N'WWI ETL On-Call',
        @enabled       = 1,
        @email_address = N'$(EtlOperatorEmail)';
END
GO

IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'WWI Platform DBA')
BEGIN
    EXEC msdb.dbo.sp_add_operator
        @name          = N'WWI Platform DBA',
        @enabled       = 1,
        @email_address = N'$(DbaOperatorEmail)',
        @pager_days    = 127;
END
ELSE
BEGIN
    EXEC msdb.dbo.sp_update_operator
        @name          = N'WWI Platform DBA',
        @enabled       = 1,
        @email_address = N'$(DbaOperatorEmail)';
END
GO

/*
    The finance operator is only notified for period-close jobs; it is
    deliberately weekday-only because the close calendar never runs at weekends
    in the NA and EU ledgers (APAC closes one business day later - see
    infrastructure/monitoring-alerting.yaml).
*/
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysoperators WHERE name = N'WWI Finance Systems')
BEGIN
    EXEC msdb.dbo.sp_add_operator
        @name          = N'WWI Finance Systems',
        @enabled       = 1,
        @email_address = N'$(FinanceOperatorEmail)',
        @pager_days    = 62;
END
ELSE
BEGIN
    EXEC msdb.dbo.sp_update_operator
        @name          = N'WWI Finance Systems',
        @enabled       = 1,
        @email_address = N'$(FinanceOperatorEmail)';
END
GO

/* Fail-safe operator selection is an instance-level setting; the DBA list owns it. */
EXEC msdb.dbo.sp_set_sqlagent_properties
    @email_save_in_sent_folder = 1;
GO
