/*
    Integration.usp_SetChangeWatermark

    Catalog entry : sqlserver_oltp.procedures - Integration.SetChangeWatermark
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6220 - after 6210
    Depends on    : Integration.ChangeTrackingWatermark
    Called by     : every incremental SSIS extract package, on success only

    Advances the watermark after a successful extract and clears any full
    reload request. The row count is stored for the reconciliation report.

    The watermark is only ever moved forward: a package that is re-run with an
    older window will not rewind it, which protects the estate from a stale
    re-run and also makes a deliberate backfill impossible without an update
    by hand.
*/
CREATE PROCEDURE [Integration].[usp_SetChangeWatermark]
    @ConsumerCode       NVARCHAR (20),
    @SourceSchemaName   NVARCHAR (30),
    @SourceTableName    NVARCHAR (60),
    @ExtractedTo        DATETIME2 (7),
    @RowCount           BIGINT = NULL,
    @BatchID            BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [Integration].[ChangeTrackingWatermark]
    SET [LastExtractedWhen] = @ExtractedTo,
        [LastRowCount] = @RowCount,
        [LastRunBatchID] = @BatchID,
        [FullReloadRequested] = 0,
        [LastUpdatedWhen] = SYSDATETIME(),
        [UpdatedByProcess] = N'usp_SetChangeWatermark'
    WHERE [ConsumerCode] = @ConsumerCode
        AND [SourceSchemaName] = @SourceSchemaName
        AND [SourceTableName] = @SourceTableName
        AND ([LastExtractedWhen] IS NULL OR [LastExtractedWhen] < @ExtractedTo);

    IF @@ROWCOUNT = 0
        AND NOT EXISTS (SELECT 1 FROM [Integration].[ChangeTrackingWatermark]
                        WHERE [ConsumerCode] = @ConsumerCode
                            AND [SourceSchemaName] = @SourceSchemaName
                            AND [SourceTableName] = @SourceTableName)
        INSERT INTO [Integration].[ChangeTrackingWatermark]
        (
            [ConsumerCode], [SourceSchemaName], [SourceTableName], [WatermarkColumnName],
            [LastExtractedWhen], [OverlapMinutes], [FullReloadRequested],
            [LastRunBatchID], [LastRowCount], [UpdatedByProcess]
        )
        VALUES
        (
            @ConsumerCode, @SourceSchemaName, @SourceTableName, N'LastEditedWhen',
            @ExtractedTo, 15, 0,
            @BatchID, @RowCount, N'usp_SetChangeWatermark'
        );
END
GO
