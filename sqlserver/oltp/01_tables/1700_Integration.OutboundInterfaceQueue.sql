/*
    Integration.OutboundInterfaceQueue

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1700 - after 00_schemas
    Depends on    : Application.People
    Called by     : Integration.usp_EnqueueOutboundChanges, downstream interface readers

    The outbound message queue every consumer of this OLTP reads: the finance
    ERP, the third-party WMS, the tax engine and the data warehouse extracts.
    Messages are claimed by a reader, retried on failure with an increasing
    NextAttemptWhen, and moved to POISON after MaxAttempts so that a bad
    message never blocks the queue - it just sits there until somebody looks.

    PayloadText is the message body as XML or JSON depending on the interface,
    which is recorded in PayloadFormat. Nothing validates it.
*/
CREATE TABLE [Integration].[OutboundInterfaceQueue] (
    [OutboundMessageID]     BIGINT          CONSTRAINT [DF_Integration_OutboundInterfaceQueue_OutboundMessageID] DEFAULT (NEXT VALUE FOR [Sequences].[OutboundMessageID]) NOT NULL,
    [InterfaceCode]         NVARCHAR (20)   NOT NULL,
    [MessageType]           NVARCHAR (30)   NOT NULL,
    [SourceSchemaName]      NVARCHAR (30)   NOT NULL,
    [SourceTableName]       NVARCHAR (60)   NOT NULL,
    [SourceKeyValue]        NVARCHAR (60)   NOT NULL,
    [OperationCode]         NCHAR (1)       NOT NULL,
    [EnqueuedWhen]          DATETIME2 (7)   CONSTRAINT [DF_Integration_OutboundInterfaceQueue_EnqueuedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [EnqueuedByProcess]     NVARCHAR (60)   NOT NULL,
    [PriorityLevel]         TINYINT         CONSTRAINT [DF_Integration_OutboundInterfaceQueue_PriorityLevel] DEFAULT (5) NOT NULL,
    [PayloadFormat]         NVARCHAR (10)   CONSTRAINT [DF_Integration_OutboundInterfaceQueue_PayloadFormat] DEFAULT (N'XML') NOT NULL,
    [PayloadText]           NVARCHAR (MAX)  NULL,
    [CorrelationReference]  NVARCHAR (60)   NULL,
    [MessageStatus]         NVARCHAR (12)   CONSTRAINT [DF_Integration_OutboundInterfaceQueue_MessageStatus] DEFAULT (N'READY') NOT NULL,
    [AttemptCount]          SMALLINT        CONSTRAINT [DF_Integration_OutboundInterfaceQueue_AttemptCount] DEFAULT (0) NOT NULL,
    [MaxAttempts]           SMALLINT        CONSTRAINT [DF_Integration_OutboundInterfaceQueue_MaxAttempts] DEFAULT (5) NOT NULL,
    [NextAttemptWhen]       DATETIME2 (7)   NULL,
    [ClaimedByWorker]       NVARCHAR (60)   NULL,
    [ClaimedWhen]           DATETIME2 (7)   NULL,
    [LastAttemptWhen]       DATETIME2 (7)   NULL,
    [LastErrorText]         NVARCHAR (1000) NULL,
    [DeliveredWhen]         DATETIME2 (7)   NULL,
    [PoisonedWhen]          DATETIME2 (7)   NULL,
    [BatchID]               BIGINT          NULL,
    CONSTRAINT [PK_Integration_OutboundInterfaceQueue] PRIMARY KEY CLUSTERED ([OutboundMessageID] ASC),
    CONSTRAINT [CK_Integration_OutboundInterfaceQueue_Operation] CHECK ([OperationCode] IN (N'I', N'U', N'D')),
    CONSTRAINT [CK_Integration_OutboundInterfaceQueue_Format] CHECK ([PayloadFormat] IN (N'XML', N'JSON', N'FLAT', N'EDI')),
    CONSTRAINT [CK_Integration_OutboundInterfaceQueue_Status] CHECK ([MessageStatus] IN (N'READY', N'CLAIMED', N'RETRY', N'DELIVERED', N'POISON', N'CANCELLED')),
    CONSTRAINT [CK_Integration_OutboundInterfaceQueue_Attempts] CHECK ([AttemptCount] >= 0 AND [AttemptCount] <= 999),
    CONSTRAINT [CK_Integration_OutboundInterfaceQueue_Poison] CHECK ([MessageStatus] <> N'POISON' OR [PoisonedWhen] IS NOT NULL)
);
GO

CREATE NONCLUSTERED INDEX [IX_Integration_OutboundInterfaceQueue_Ready]
    ON [Integration].[OutboundInterfaceQueue] ([InterfaceCode] ASC, [PriorityLevel] ASC, [EnqueuedWhen] ASC)
    INCLUDE ([MessageType], [SourceKeyValue], [AttemptCount])
    WHERE [MessageStatus] IN (N'READY', N'RETRY');
GO

CREATE NONCLUSTERED INDEX [IX_Integration_OutboundInterfaceQueue_Poison]
    ON [Integration].[OutboundInterfaceQueue] ([PoisonedWhen] ASC)
    INCLUDE ([InterfaceCode], [MessageType], [LastErrorText])
    WHERE [MessageStatus] = N'POISON';
GO

CREATE NONCLUSTERED INDEX [IX_Integration_OutboundInterfaceQueue_Source]
    ON [Integration].[OutboundInterfaceQueue] ([SourceSchemaName] ASC, [SourceTableName] ASC, [SourceKeyValue] ASC);
GO
