/*
    Integration.usp_GetChangeWatermark

    Catalog entry : sqlserver_oltp.procedures - Integration.GetChangeWatermark
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6210 - after 6200
    Depends on    : Integration.ChangeTrackingWatermark
    Called by     : every incremental SSIS extract package

    Returns the extract window for one consumer and table, creating the
    watermark row on first use so a new extract does not have to be seeded by
    hand. A full reload request is honoured by returning the epoch window and
    is cleared by Integration.usp_SetChangeWatermark, not here - a package
    that dies mid-run therefore reloads again next time, which is the intended
    behaviour and surprises people annually.
*/
CREATE PROCEDURE [Integration].[usp_GetChangeWatermark]
    @ConsumerCode       NVARCHAR (20),
    @SourceSchemaName   NVARCHAR (30),
    @SourceTableName    NVARCHAR (60),
    @WatermarkColumnName NVARCHAR (60) = N'LastEditedWhen',
    @WindowFrom         DATETIME2 (7) = NULL OUTPUT,
    @WindowTo           DATETIME2 (7) = NULL OUTPUT,
    @IsFullReload       BIT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @LastExtracted  DATETIME2 (7);
    DECLARE @Overlap        SMALLINT;
    DECLARE @FullReload     BIT;

    IF NOT EXISTS (SELECT 1 FROM [Integration].[ChangeTrackingWatermark]
                   WHERE [ConsumerCode] = @ConsumerCode
                       AND [SourceSchemaName] = @SourceSchemaName
                       AND [SourceTableName] = @SourceTableName)
        INSERT INTO [Integration].[ChangeTrackingWatermark]
        (
            [ConsumerCode], [SourceSchemaName], [SourceTableName], [WatermarkColumnName],
            [OverlapMinutes], [FullReloadRequested], [UpdatedByProcess]
        )
        VALUES
        (
            @ConsumerCode, @SourceSchemaName, @SourceTableName, @WatermarkColumnName,
            15, 1, N'usp_GetChangeWatermark'
        );

    SELECT
        @LastExtracted = w.[LastExtractedWhen],
        @Overlap = ISNULL(w.[OverlapMinutes], 15),
        @FullReload = w.[FullReloadRequested]
    FROM [Integration].[ChangeTrackingWatermark] AS w
    WHERE w.[ConsumerCode] = @ConsumerCode
        AND w.[SourceSchemaName] = @SourceSchemaName
        AND w.[SourceTableName] = @SourceTableName;

    SET @IsFullReload = ISNULL(@FullReload, 0);
    SET @WindowTo = SYSDATETIME();
    SET @WindowFrom = CASE WHEN ISNULL(@FullReload, 0) = 1 OR @LastExtracted IS NULL
                           THEN CONVERT(DATETIME2 (7), N'1900-01-01')
                           ELSE DATEADD(MINUTE, -@Overlap, @LastExtracted) END;

    SELECT
        @WindowFrom     AS [WindowFrom],
        @WindowTo       AS [WindowTo],
        @IsFullReload   AS [IsFullReload];
END
GO
