/*
    Object          : Agent installation driver
    Deploy target   : msdb (run with sqlcmd, from the repository root)
    Deploy order    : sqlserver/agent - the single entry point for the agent stage
    Called by       : deployment/sqlserver/deploy-sqlserver.ps1,
                      deployment/sqlserver/deploy-sqlserver.sh
    Notes           : Runs the agent scripts in dependency order. Requires
                      sqlcmd mode (-i with sqlcmd, or "SQLCMD Mode" in SSMS)
                      because it uses :r and :setvar. Every script it calls is
                      idempotent, so re-running the driver is safe. It writes
                      msdb catalogue rows only and does not require the SQL
                      Agent service to be running. Nothing here has been
                      executed against any server.

    Required sqlcmd variables (the deployment driver supplies all of them):
        SsisServer, SsisFolder,
        StagingDatabase, DwDatabase, OltpDatabase,
        AgentLogRoot, InboundFileRoot, QuarantineFileRoot,
        EnvironmentCode, SsisProxyAccount, FileProxyAccount,
        SsisProxySecret, FileProxySecret
*/

SET NOCOUNT ON;
GO

IF N'$(EnvironmentCode)' NOT IN (N'DEV', N'TEST', N'PROD')
BEGIN
    RAISERROR (N'EnvironmentCode must be DEV, TEST or PROD.', 20, 1) WITH LOG;
END
GO

PRINT N'--- WWI agent stage: categories, operators, proxies ---';
GO
:r sqlserver/agent/00_create_job_categories.sql
:r sqlserver/agent/01_create_operators.sql
:r sqlserver/agent/02_create_proxies_and_credentials.sql
GO

PRINT N'--- WWI agent stage: load jobs ---';
GO
:r sqlserver/agent/10_job_WWI_Daily_ETL.sql
:r sqlserver/agent/11_job_WWI_Hourly_Incremental.sql
:r sqlserver/agent/12_job_WWI_Intraday_Inventory.sql
:r sqlserver/agent/13_job_WWI_Customer_Sync.sql
:r sqlserver/agent/14_job_WWI_File_Ingestion.sql
GO

PRINT N'--- WWI agent stage: periodic and close jobs ---';
GO
:r sqlserver/agent/15_job_WWI_Weekly_Reference_Load.sql
:r sqlserver/agent/16_job_WWI_Weekly_Maintenance.sql
:r sqlserver/agent/17_job_WWI_Month_End.sql
:r sqlserver/agent/18_job_WWI_Finance_Close.sql
GO

PRINT N'--- WWI agent stage: recovery, maintenance and monitoring jobs ---';
GO
:r sqlserver/agent/19_job_WWI_Reject_Reprocess.sql
:r sqlserver/agent/20_job_WWI_Control_History_Purge.sql
:r sqlserver/agent/21_job_WWI_Health_Check.sql
GO

/*
    Post-install inventory. Prints what the driver created so the operator can
    compare it with sqlserver/agent/README.md.
*/
SELECT j.name         AS JobName,
       c.name         AS CategoryName,
       j.enabled      AS IsEnabled,
       COUNT(s.step_id) AS StepCount
FROM msdb.dbo.sysjobs AS j
INNER JOIN msdb.dbo.syscategories AS c ON c.category_id = j.category_id
LEFT JOIN msdb.dbo.sysjobsteps AS s ON s.job_id = j.job_id
WHERE j.name LIKE N'WWI - %'
GROUP BY j.name, c.name, j.enabled
ORDER BY c.name, j.name;
GO
