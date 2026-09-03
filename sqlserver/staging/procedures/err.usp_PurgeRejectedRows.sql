/*
    err.usp_PurgeRejectedRows

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : MAINT_PURGE_REJECTS (SSIS), Sunday 03:00 local
    Reads         : sys.tables, etl.Configuration
    Writes        : every err.* table (dynamic)
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Weekly housekeeping over the reject tables. It walks every table in the err
    schema and deletes rows that are older than the retention window and are in a
    terminal reprocessing state.

    Retention:
      * the default window comes from etl.Configuration key
        'STAGING.REJECT_RETENTION_DAYS', falling back to @DefaultRetentionDays;
      * personal-data reject tables (customer and address rows) use the shorter
        @PersonalDataRetentionDays window because the EU retention policy applies
        to reject copies of personal data as much as to the staged rows;
      * rows still in NEW or PENDING are never purged, however old they are - the
        stewards' backlog is not the purge job's business.

    Deletes are batched in @DeleteBatchSize chunks so the weekly run does not
    hold a table lock long enough to collide with the Sunday reload.
*/

IF OBJECT_ID(N'err.usp_PurgeRejectedRows', N'P') IS NOT NULL
    DROP PROCEDURE err.usp_PurgeRejectedRows;
GO

CREATE PROCEDURE err.usp_PurgeRejectedRows
(
    @BatchId                   BIGINT = NULL,
    @PackageExecutionId        BIGINT = NULL,
    @DefaultRetentionDays      INT = 400,
    @PersonalDataRetentionDays INT = 180,
    @DeleteBatchSize           INT = 5000,
    @WhatIfOnly                BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ConfiguredDays  INT;
    DECLARE @TableName       NVARCHAR(128);
    DECLARE @RetentionDays   INT;
    DECLARE @CutoffUtc       DATETIME2(3);
    DECLARE @Sql             NVARCHAR(MAX);
    DECLARE @DeletedThisTable BIGINT;
    DECLARE @DeletedTotal    BIGINT = 0;
    DECLARE @Chunk           BIGINT;

    BEGIN TRY
        SELECT TOP (1) @ConfiguredDays = TRY_CONVERT(INT, c.ConfigurationValue)
        FROM etl.Configuration AS c
        WHERE c.ConfigurationKey = N'STAGING.REJECT_RETENTION_DAYS';

        SET @DefaultRetentionDays = ISNULL(@ConfiguredDays, @DefaultRetentionDays);

        DECLARE PurgeCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT t.name
            FROM sys.tables AS t
            WHERE SCHEMA_NAME(t.schema_id) = N'err'
              AND EXISTS (SELECT 1 FROM sys.columns AS c
                          WHERE c.object_id = t.object_id AND c.name = N'RejectedAtUtc')
              AND EXISTS (SELECT 1 FROM sys.columns AS c
                          WHERE c.object_id = t.object_id AND c.name = N'ReprocessStatusCode')
            ORDER BY t.name;

        OPEN PurgeCursor;
        FETCH NEXT FROM PurgeCursor INTO @TableName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @RetentionDays =
                CASE
                    WHEN @TableName IN (N'RejectedCustomer', N'RejectedFileRow')
                        THEN @PersonalDataRetentionDays
                    ELSE @DefaultRetentionDays
                END;

            SET @CutoffUtc         = DATEADD(DAY, -@RetentionDays, SYSUTCDATETIME());
            SET @DeletedThisTable  = 0;
            SET @Chunk             = 1;

            IF @WhatIfOnly = 1
            BEGIN
                SET @Sql = N'
                    SELECT @Chunk = COUNT_BIG(*)
                    FROM err.' + QUOTENAME(@TableName) + N'
                    WHERE RejectedAtUtc < @CutoffUtc
                      AND ReprocessStatusCode NOT IN (N''NEW'', N''PENDING'');';

                EXEC sys.sp_executesql
                    @Sql,
                    N'@CutoffUtc DATETIME2(3), @Chunk BIGINT OUTPUT',
                    @CutoffUtc = @CutoffUtc,
                    @Chunk     = @DeletedThisTable OUTPUT;
            END
            ELSE
            BEGIN
                SET @Sql = N'
                    DELETE TOP (@DeleteBatchSize)
                    FROM err.' + QUOTENAME(@TableName) + N'
                    WHERE RejectedAtUtc < @CutoffUtc
                      AND ReprocessStatusCode NOT IN (N''NEW'', N''PENDING'');
                    SET @Chunk = @@ROWCOUNT;';

                WHILE @Chunk > 0
                BEGIN
                    EXEC sys.sp_executesql
                        @Sql,
                        N'@CutoffUtc DATETIME2(3), @DeleteBatchSize INT, @Chunk BIGINT OUTPUT',
                        @CutoffUtc       = @CutoffUtc,
                        @DeleteBatchSize = @DeleteBatchSize,
                        @Chunk           = @Chunk OUTPUT;

                    SET @DeletedThisTable = @DeletedThisTable + @Chunk;
                END;
            END;

            IF @DeletedThisTable > 0
                EXEC etl.usp_LogRowCount
                    @PackageExecutionId = @PackageExecutionId,
                    @ObjectName         = @TableName,
                    @DeleteRowCount     = @DeletedThisTable;

            SET @DeletedTotal = @DeletedTotal + @DeletedThisTable;

            FETCH NEXT FROM PurgeCursor INTO @TableName;
        END;

        CLOSE PurgeCursor;
        DEALLOCATE PurgeCursor;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'err.*',
            @DeleteRowCount     = @DeletedTotal;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'PurgeCursor') >= 0
        BEGIN
            CLOSE PurgeCursor;
            DEALLOCATE PurgeCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'MAINT_PURGE_REJECTS',
            @SourceComponent    = N'err.*',
            @ProcedureName      = N'err.usp_PurgeRejectedRows';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
