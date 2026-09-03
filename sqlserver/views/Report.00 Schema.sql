/*
    Report schema

    Object        : Report
    Deploy target : WideWorldImportersDW
    Deploy order  : before every Report.vw_* view.

    The semantic layer the BI tools connect to. Nothing outside this schema is
    granted to the reporting logins - which is the only reason the aggregate
    tables are still allowed to change shape without a change request.
*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'Report')
    EXECUTE (N'CREATE SCHEMA Report AUTHORIZATION dbo;');
GO
