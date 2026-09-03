/*
    Integration.DeletedRowLog

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2300 - after 2240
    Depends on    : none
    Called by     : deletion triggers in 07_triggers, incremental extract packages

    Generic deletion log. Incremental extracts chase LastEditedWhen, which
    cannot see a deleted row, so every table that is extracted incrementally
    carries a delete trigger that writes here. The key is rendered as text
    because the tables involved have integer, bigint and composite keys.

    Rows are never removed by the application; the retention sweep is manual
    and has not been run since the log was created.
*/
CREATE TABLE [Integration].[DeletedRowLog] (
    [DeletedRowLogID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [SourceSchemaName]      NVARCHAR (30)   NOT NULL,
    [SourceTableName]       NVARCHAR (60)   NOT NULL,
    [SourceKeyValue]        NVARCHAR (120)  NOT NULL,
    [SecondaryKeyValue]     NVARCHAR (120)  NULL,
    [DeletedWhen]           DATETIME2 (7)   CONSTRAINT [DF_Integration_DeletedRowLog_DeletedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [DeletedByLogin]        NVARCHAR (128)  CONSTRAINT [DF_Integration_DeletedRowLog_DeletedByLogin] DEFAULT (SUSER_SNAME()) NOT NULL,
    [DeletedByApplication]  NVARCHAR (128)  CONSTRAINT [DF_Integration_DeletedRowLog_DeletedByApplication] DEFAULT (APP_NAME()) NOT NULL,
    [DeleteReasonCode]      NVARCHAR (10)   NULL,
    [RowSnapshotText]       NVARCHAR (MAX)  NULL,
    [IsPurgeNotDelete]      BIT             CONSTRAINT [DF_Integration_DeletedRowLog_IsPurgeNotDelete] DEFAULT (0) NOT NULL,
    CONSTRAINT [PK_Integration_DeletedRowLog] PRIMARY KEY CLUSTERED ([DeletedRowLogID] ASC)
);
GO

CREATE NONCLUSTERED INDEX [IX_Integration_DeletedRowLog_Table_When]
    ON [Integration].[DeletedRowLog] ([SourceSchemaName] ASC, [SourceTableName] ASC, [DeletedWhen] ASC)
    INCLUDE ([SourceKeyValue]);
GO
