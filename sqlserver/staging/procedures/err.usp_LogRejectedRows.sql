/*
    err.usp_LogRejectedRows

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : every stg.* load procedure after it has filled its err.* table
    Reads         : any err.* table (dynamic)
    Control       : etl.usp_LogRejectedRecord, etl.usp_LogError

    The generic reject-routing helper.

    Each load procedure writes its own typed reject table because the payload
    columns differ; this procedure is what then pushes those rows into the shared
    etl.RejectedRecord audit through etl.usp_LogRejectedRecord, so that the
    control database has one reject feed regardless of which of the twenty-odd
    err.* tables the row landed in.

    It is dynamic SQL over a cursor. Both are deliberate: the column list is not
    known until run time, and etl.usp_LogRejectedRecord takes one row at a time.
    On a bad file night this loops tens of thousands of times and takes twenty
    minutes; the nightly schedule has that window allowed for.

    @RejectTableName must be an err.* table in this database. The name is
    resolved through OBJECT_ID and re-emitted through QUOTENAME, and every value
    is passed as a parameter, so the caller cannot inject through it.
*/

IF OBJECT_ID(N'err.usp_LogRejectedRows', N'P') IS NOT NULL
    DROP PROCEDURE err.usp_LogRejectedRows;
GO

CREATE PROCEDURE err.usp_LogRejectedRows
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @RejectTableName    NVARCHAR(256),
    @ObjectName         NVARCHAR(200),
    @BusinessKeyColumn  NVARCHAR(128) = NULL,
    @RejectStage        NVARCHAR(50) = N'Stage',
    @SourceSystemCode   NVARCHAR(20) = NULL,
    @MaxRowsToForward   INT = 100000
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @RejectObjectId  INT = OBJECT_ID(@RejectTableName, N'U');
    DECLARE @SchemaName      NVARCHAR(128);
    DECLARE @TableName       NVARCHAR(128);
    DECLARE @Sql             NVARCHAR(MAX);
    DECLARE @ParamList       NVARCHAR(500);
    DECLARE @HasSourceSystem BIT = 0;
    DECLARE @HasPayload      BIT = 0;
    DECLARE @KeyExpression   NVARCHAR(300) = N'NULL';
    DECLARE @ForwardedRows   BIGINT = 0;

    DECLARE @RowBusinessKey  NVARCHAR(200);
    DECLARE @RowReasonCode   NVARCHAR(50);
    DECLARE @RowReasonText   NVARCHAR(500);
    DECLARE @RowSourceSystem NVARCHAR(20);
    DECLARE @RowPayload      NVARCHAR(MAX);

    BEGIN TRY
        IF @RejectObjectId IS NULL
            THROW 55021, N'err.usp_LogRejectedRows: reject table does not exist', 1;

        SELECT
            @SchemaName = SCHEMA_NAME(t.schema_id),
            @TableName  = t.name
        FROM sys.tables AS t
        WHERE t.object_id = @RejectObjectId;

        IF @SchemaName <> N'err'
            THROW 55022, N'err.usp_LogRejectedRows: only err.* tables may be forwarded', 1;

        SELECT @HasSourceSystem = MAX(CASE WHEN c.name = N'SourceSystemCode' THEN 1 ELSE 0 END),
               @HasPayload      = MAX(CASE WHEN c.name = N'RecordPayload'    THEN 1 ELSE 0 END)
        FROM sys.columns AS c
        WHERE c.object_id = @RejectObjectId;

        IF @BusinessKeyColumn IS NOT NULL
           AND EXISTS (SELECT 1 FROM sys.columns AS c
                       WHERE c.object_id = @RejectObjectId AND c.name = @BusinessKeyColumn)
            SET @KeyExpression = N'CONVERT(NVARCHAR(200), r.' + QUOTENAME(@BusinessKeyColumn) + N')';

        SET @Sql = N'
            SELECT TOP (@MaxRowsToForward)
                BusinessKey  = ' + @KeyExpression + N',
                ReasonCode   = r.RejectReasonCode,
                ReasonText   = r.RejectReason,
                SourceSystem = '
                    + CASE WHEN @HasSourceSystem = 1
                           THEN N'CONVERT(NVARCHAR(20), r.SourceSystemCode)'
                           ELSE N'@SourceSystemCode' END + N',
                Payload      = '
                    + CASE WHEN @HasPayload = 1 THEN N'r.RecordPayload' ELSE N'CONVERT(NVARCHAR(MAX), NULL)' END + N'
            FROM ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) + N' AS r
            WHERE r.BatchId = @BatchId
              AND r.ReprocessStatusCode = N''NEW''
            ORDER BY r.RejectId;';

        SET @ParamList = N'@BatchId BIGINT, @MaxRowsToForward INT, @SourceSystemCode NVARCHAR(20)';

        CREATE TABLE #ForwardQueue
        (
            QueueId      INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            BusinessKey  NVARCHAR(200) NULL,
            ReasonCode   NVARCHAR(50)  NULL,
            ReasonText   NVARCHAR(500) NULL,
            SourceSystem NVARCHAR(20)  NULL,
            Payload      NVARCHAR(MAX) NULL
        );

        INSERT INTO #ForwardQueue (BusinessKey, ReasonCode, ReasonText, SourceSystem, Payload)
        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId          = @BatchId,
            @MaxRowsToForward = @MaxRowsToForward,
            @SourceSystemCode = @SourceSystemCode;

        DECLARE ForwardCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT q.BusinessKey, q.ReasonCode, q.ReasonText, q.SourceSystem, q.Payload
            FROM #ForwardQueue AS q
            ORDER BY q.QueueId;

        OPEN ForwardCursor;
        FETCH NEXT FROM ForwardCursor
            INTO @RowBusinessKey, @RowReasonCode, @RowReasonText, @RowSourceSystem, @RowPayload;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = @RowSourceSystem,
                @ObjectName         = @ObjectName,
                @BusinessKey        = @RowBusinessKey,
                @RejectReasonCode   = @RowReasonCode,
                @RejectReason       = @RowReasonText,
                @RejectStage        = @RejectStage,
                @RecordPayload      = @RowPayload;

            SET @ForwardedRows = @ForwardedRows + 1;

            FETCH NEXT FROM ForwardCursor
                INTO @RowBusinessKey, @RowReasonCode, @RowReasonText, @RowSourceSystem, @RowPayload;
        END;

        CLOSE ForwardCursor;
        DEALLOCATE ForwardCursor;

        --  Mark the forwarded rows so a rerun of the helper does not double-log.
        SET @Sql = N'
            UPDATE r
            SET r.ReprocessStatusCode = N''LOGGED''
            FROM ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) + N' AS r
            WHERE r.BatchId = @BatchId
              AND r.ReprocessStatusCode = N''NEW'';';

        EXEC sys.sp_executesql
            @Sql,
            N'@BatchId BIGINT',
            @BatchId = @BatchId;

        DROP TABLE #ForwardQueue;
    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'ForwardCursor') >= 0
        BEGIN
            CLOSE ForwardCursor;
            DEALLOCATE ForwardCursor;
        END;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'ERR_FORWARD_REJECTS',
            @SourceComponent    = @RejectTableName,
            @ProcedureName      = N'err.usp_LogRejectedRows';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
