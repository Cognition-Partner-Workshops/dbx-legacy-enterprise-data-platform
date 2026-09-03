/*
    Integration.InboundFileRegister

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1710 - after 00_schemas
    Depends on    : Application.People
    Called by     : Integration.usp_RegisterInboundFile, inbound file loaders

    Register of every file the OLTP has been handed: carrier tracking files,
    supplier ASNs, bank statements, the tax engine's rate drops. Duplicate
    detection is by file hash where the sender provides one and by name plus
    size where they do not, which is why both are stored and neither is
    unique on its own.

    Rejected rows go to etl.RejectedRecord in the control framework; only the
    file-level outcome is held here.
*/
CREATE TABLE [Integration].[InboundFileRegister] (
    [InboundFileID]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [InterfaceCode]         NVARCHAR (20)   NOT NULL,
    [FileName]              NVARCHAR (260)  NOT NULL,
    [FileDirectory]         NVARCHAR (400)  NULL,
    [FileSizeBytes]         BIGINT          NULL,
    [FileHashText]          NVARCHAR (64)   NULL,
    [SenderCode]            NVARCHAR (20)   NULL,
    [ReceivedWhen]          DATETIME2 (7)   CONSTRAINT [DF_Integration_InboundFileRegister_ReceivedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [FileBusinessDate]      DATE            NULL,
    [SequenceNumberInDay]   SMALLINT        NULL,
    [ProcessingStatus]      NVARCHAR (12)   CONSTRAINT [DF_Integration_InboundFileRegister_ProcessingStatus] DEFAULT (N'REGISTERED') NOT NULL,
    [ProcessingStartedWhen] DATETIME2 (7)   NULL,
    [ProcessingEndedWhen]   DATETIME2 (7)   NULL,
    [RowsReadCount]         BIGINT          NULL,
    [RowsAcceptedCount]     BIGINT          NULL,
    [RowsRejectedCount]     BIGINT          NULL,
    [IsDuplicateOfFileID]   BIGINT          NULL,
    [ArchivePath]           NVARCHAR (400)  NULL,
    [BatchID]               BIGINT          NULL,
    [LastErrorText]         NVARCHAR (1000) NULL,
    [RetryCount]            SMALLINT        CONSTRAINT [DF_Integration_InboundFileRegister_RetryCount] DEFAULT (0) NOT NULL,
    CONSTRAINT [PK_Integration_InboundFileRegister] PRIMARY KEY CLUSTERED ([InboundFileID] ASC),
    CONSTRAINT [CK_Integration_InboundFileRegister_Status] CHECK ([ProcessingStatus] IN (N'REGISTERED', N'LOADING', N'LOADED', N'PARTIAL', N'FAILED', N'DUPLICATE', N'QUARANTINED')),
    CONSTRAINT [CK_Integration_InboundFileRegister_Duplicate] CHECK ([ProcessingStatus] <> N'DUPLICATE' OR [IsDuplicateOfFileID] IS NOT NULL),
    CONSTRAINT [FK_Integration_InboundFileRegister_Duplicate] FOREIGN KEY ([IsDuplicateOfFileID]) REFERENCES [Integration].[InboundFileRegister] ([InboundFileID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Integration_InboundFileRegister_Interface_Received]
    ON [Integration].[InboundFileRegister] ([InterfaceCode] ASC, [ReceivedWhen] DESC)
    INCLUDE ([ProcessingStatus], [FileName], [RowsAcceptedCount]);
GO

CREATE NONCLUSTERED INDEX [IX_Integration_InboundFileRegister_Hash]
    ON [Integration].[InboundFileRegister] ([FileHashText] ASC)
    WHERE [FileHashText] IS NOT NULL;
GO
