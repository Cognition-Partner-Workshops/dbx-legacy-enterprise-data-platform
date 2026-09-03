/*
    Integration.usp_EnqueueOutboundChanges

    Catalog entry : sqlserver_oltp.procedures - Integration.EnqueueOutboundChanges
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6190 - after 6180
    Depends on    : Integration.OutboundInterfaceQueue, Integration.ChangeTrackingWatermark,
                    Integration.DeletedRowLog
    Called by     : interface dispatcher job

    Enqueues one interface's worth of changes. The payload is built with
    dynamic SQL from the watermark row's schema and table names, using
    FOR XML PATH or FOR JSON depending on the interface's declared payload
    format, because the 2009 interfaces speak XML and everything after 2016
    speaks JSON and neither side will move.

    Deletes are enqueued from the deletion log in the same pass so ordering
    between an update and a later delete of the same key is preserved only by
    the enqueue timestamp, which is a known source of out-of-order delivery.
*/
CREATE PROCEDURE [Integration].[usp_EnqueueOutboundChanges]
    @InterfaceCode      NVARCHAR (20),
    @SourceSchemaName   NVARCHAR (30),
    @SourceTableName    NVARCHAR (60),
    @KeyColumnName      NVARCHAR (60),
    @PayloadFormat      NVARCHAR (8) = N'JSON',
    @MaxRows            INT = 5000,
    @BatchID            BIGINT = NULL,
    @MessagesEnqueued   INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @LastExtracted  DATETIME2 (7);
    DECLARE @Overlap        SMALLINT;
    DECLARE @Sql            NVARCHAR (MAX);
    DECLARE @Since          DATETIME2 (7);

    SELECT
        @LastExtracted = w.[LastExtractedWhen],
        @Overlap = ISNULL(w.[OverlapMinutes], 15)
    FROM [Integration].[ChangeTrackingWatermark] AS w
    WHERE w.[ConsumerCode] = @InterfaceCode
        AND w.[SourceSchemaName] = @SourceSchemaName
        AND w.[SourceTableName] = @SourceTableName;

    SET @Since = ISNULL(DATEADD(MINUTE, -@Overlap, @LastExtracted), CONVERT(DATETIME2 (7), N'1900-01-01'));
    SET @MessagesEnqueued = 0;

    IF OBJECT_ID(QUOTENAME(@SourceSchemaName) + N'.' + QUOTENAME(@SourceTableName), N'U') IS NULL
    BEGIN
        RAISERROR (N'Source table %s.%s does not exist.', 16, 1, @SourceSchemaName, @SourceTableName);
        RETURN;
    END

    SET @Sql = N'INSERT INTO [Integration].[OutboundInterfaceQueue]
                 (
                     [InterfaceCode], [MessageType], [SourceSchemaName], [SourceTableName],
                     [SourceKeyValue], [OperationCode], [EnqueuedByProcess], [PriorityLevel],
                     [PayloadFormat], [PayloadText], [CorrelationReference], [MessageStatus],
                     [MaxAttempts], [BatchID]
                 )
                 SELECT TOP (@Rows)
                     @Interface,
                     @Table + N''.UPSERT'',
                     @Schema,
                     @Table,
                     CONVERT(NVARCHAR (120), src.' + QUOTENAME(@KeyColumnName) + N'),
                     N''U'',
                     N''usp_EnqueueOutboundChanges'',
                     5,
                     @Format,
                     CASE WHEN @Format = N''XML''
                          THEN (SELECT src.* FOR XML PATH(''Row''), TYPE).value(N''.'', N''NVARCHAR(MAX)'')
                          ELSE (SELECT src.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
                     END,
                     CONVERT(NVARCHAR (60), NEWID()),
                     N''PENDING'',
                     5,
                     @Batch
                 FROM ' + QUOTENAME(@SourceSchemaName) + N'.' + QUOTENAME(@SourceTableName) + N' AS src
                 WHERE src.[LastEditedWhen] > @Since
                 ORDER BY src.[LastEditedWhen] ASC;';

    EXEC sp_executesql @Sql,
         N'@Rows INT, @Interface NVARCHAR (20), @Schema NVARCHAR (30), @Table NVARCHAR (60), @Format NVARCHAR (8), @Since DATETIME2 (7), @Batch BIGINT',
         @Rows = @MaxRows,
         @Interface = @InterfaceCode,
         @Schema = @SourceSchemaName,
         @Table = @SourceTableName,
         @Format = @PayloadFormat,
         @Since = @Since,
         @Batch = @BatchID;

    SET @MessagesEnqueued = @@ROWCOUNT;

    INSERT INTO [Integration].[OutboundInterfaceQueue]
    (
        [InterfaceCode], [MessageType], [SourceSchemaName], [SourceTableName],
        [SourceKeyValue], [OperationCode], [EnqueuedByProcess], [PriorityLevel],
        [PayloadFormat], [PayloadText], [MessageStatus], [MaxAttempts], [BatchID]
    )
    SELECT
        @InterfaceCode,
        @SourceTableName + N'.DELETE',
        d.[SourceSchemaName],
        d.[SourceTableName],
        d.[SourceKeyValue],
        N'D',
        N'usp_EnqueueOutboundChanges',
        3,
        @PayloadFormat,
        NULL,
        N'READY',
        5,
        @BatchID
    FROM [Integration].[DeletedRowLog] AS d
    WHERE d.[SourceSchemaName] = @SourceSchemaName
        AND d.[SourceTableName] = @SourceTableName
        AND d.[DeletedWhen] > @Since;

    SET @MessagesEnqueued = @MessagesEnqueued + @@ROWCOUNT;
END
GO
