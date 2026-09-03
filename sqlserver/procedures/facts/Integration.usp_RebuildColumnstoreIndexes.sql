/*
    Integration.usp_RebuildColumnstoreIndexes

    Object        : Integration.usp_RebuildColumnstoreIndexes
    Deploy target : WideWorldImportersDW
    Deploy order  : last.
    Called by     : MAINT_Rebuild_Columnstore (Sunday 02:00, and after any
                    full reload of a large fact).
    Reads         : sys.indexes, sys.dm_db_column_store_row_group_physical_stats.
    Depends on    : the etl control procedures.

    Reorganise or rebuild the clustered columnstore indexes on the fact tables
    depending on how much deleted-row rubbish they are carrying. The thresholds
    are the ones from the 2016 tuning exercise: reorganise above 10% deleted
    rows, rebuild above 30%, leave alone below.

    The statement is built as a string and executed one index at a time so a
    single failing rebuild does not abandon the rest, which is what happened
    every quarter-end when Fact.Sale was still being loaded at 02:00.
*/
IF OBJECT_ID(N'Integration.usp_RebuildColumnstoreIndexes', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RebuildColumnstoreIndexes;
GO

CREATE PROCEDURE Integration.usp_RebuildColumnstoreIndexes
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SchemaFilter       NVARCHAR(50) = N'Fact',
    @ReorganiseThreshold DECIMAL(5, 2) = 10.00,
    @RebuildThreshold    DECIMAL(5, 2) = 30.00,
    @MaxObjects         INT = 40
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @ObjectCount    INT = 0;
    DECLARE @ActionCount    INT = 0;
    DECLARE @FailureCount   INT = 0;
    DECLARE @SchemaName     NVARCHAR(128);
    DECLARE @TableName      NVARCHAR(128);
    DECLARE @IndexName      NVARCHAR(128);
    DECLARE @DeletedPercent DECIMAL(9, 2);
    DECLARE @Sql            NVARCHAR(MAX);
    DECLARE @StepError      NVARCHAR(MAX);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'MAINT_Rebuild_Columnstore',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'RebuildColumnstoreIndexes',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DECLARE index_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT TOP (@MaxObjects)
                   s.name, t.name, i.name,
                   CASE WHEN SUM(rg.total_rows) = 0 THEN 0
                        ELSE ROUND(100.0 * SUM(rg.deleted_rows) / SUM(rg.total_rows), 2) END
            FROM sys.indexes AS i
            INNER JOIN sys.tables AS t ON t.object_id = i.object_id
            INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            LEFT JOIN sys.dm_db_column_store_row_group_physical_stats AS rg
                ON rg.object_id = i.object_id AND rg.index_id = i.index_id
            WHERE i.type IN (5, 6)
              AND (@SchemaFilter IS NULL OR s.name = @SchemaFilter)
            GROUP BY s.name, t.name, i.name
            ORDER BY 4 DESC;

        OPEN index_cursor;
        FETCH NEXT FROM index_cursor
            INTO @SchemaName, @TableName, @IndexName, @DeletedPercent;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @ObjectCount = @ObjectCount + 1;
            SET @Sql = NULL;

            IF @DeletedPercent >= @RebuildThreshold
                SET @Sql = N'ALTER INDEX ' + QUOTENAME(@IndexName) + N' ON '
                         + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName)
                         + N' REBUILD WITH (MAXDOP = 4);';
            ELSE IF @DeletedPercent >= @ReorganiseThreshold
                SET @Sql = N'ALTER INDEX ' + QUOTENAME(@IndexName) + N' ON '
                         + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName)
                         + N' REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);';

            IF @Sql IS NOT NULL
            BEGIN
                BEGIN TRY
                    EXECUTE sp_executesql @Sql;
                    SET @ActionCount = @ActionCount + 1;
                END TRY
                BEGIN CATCH
                    SET @StepError = ERROR_MESSAGE();
                    SET @FailureCount = @FailureCount + 1;

                    EXECUTE etl.usp_LogError
                        @PackageExecutionId = @PackageExecutionId,
                        @BatchId            = @BatchId,
                        @ErrorSeverity      = N'Warning',
                        @ErrorCode          = 0,
                        @SourceName         = @TableName,
                        @SourceComponent    = N'Columnstore maintenance',
                        @ProcedureName      = N'Integration.usp_RebuildColumnstoreIndexes',
                        @ErrorDescription   = @StepError;
                END CATCH;
            END;

            FETCH NEXT FROM index_cursor
                INTO @SchemaName, @TableName, @IndexName, @DeletedPercent;
        END;

        CLOSE index_cursor;
        DEALLOCATE index_cursor;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Columnstore maintenance',
            @SourceRowCount     = @ObjectCount,
            @UpdateRowCount     = @ActionCount,
            @RejectRowCount     = @FailureCount;

        IF @OwnsExecution = 1
            DECLARE @StatusValue NVARCHAR(200) = CASE WHEN @FailureCount = 0 THEN N'Succeeded' ELSE N'Warning' END;
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = @StatusValue,
                @RowsRead           = @ObjectCount,
                @RowsUpdated        = @ActionCount,
                @RowsRejected       = @FailureCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'index_cursor') >= 0
        BEGIN
            CLOSE index_cursor;
            DEALLOCATE index_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Columnstore maintenance',
            @SourceComponent    = N'Index maintenance',
            @ProcedureName      = N'Integration.usp_RebuildColumnstoreIndexes',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
