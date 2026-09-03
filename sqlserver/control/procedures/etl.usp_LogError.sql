/*
    etl.usp_LogError

    Deploy target : WideWorldImportersStaging
    Called by     : every package OnError event handler, and every TRY/CATCH block
                    in the staging, dimension and fact procedures
    Depends on    : etl.ErrorLog

    Safe to call from inside a CATCH block with an open, doomed transaction: the
    insert runs in its own scope and never re-raises, so error capture cannot
    itself become the failure the operator sees.
*/
IF OBJECT_ID(N'etl.usp_LogError', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogError;
GO

CREATE PROCEDURE etl.usp_LogError
(
    @PackageExecutionId BIGINT = NULL,
    @BatchId            BIGINT = NULL,
    @ErrorSeverity      NVARCHAR(20) = N'Error',
    @ErrorCode          INT = NULL,
    @SourceName         NVARCHAR(200) = NULL,
    @SourceComponent    NVARCHAR(200) = NULL,
    @ProcedureName      NVARCHAR(200) = NULL,
    @ErrorDescription   NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @BatchId = 0 SET @BatchId = NULL;
    IF @PackageExecutionId = 0 SET @PackageExecutionId = NULL;

    IF @ErrorDescription IS NULL AND ERROR_NUMBER() IS NOT NULL
        SET @ErrorDescription = ERROR_MESSAGE();

    IF @BatchId IS NULL AND @PackageExecutionId IS NOT NULL
        SELECT @BatchId = pe.BatchId
        FROM etl.PackageExecution AS pe
        WHERE pe.PackageExecutionId = @PackageExecutionId;

    BEGIN TRY
        INSERT INTO etl.ErrorLog
            (PackageExecutionId, BatchId, ErrorSeverity, ErrorCode, ErrorNumber, ErrorState,
             ErrorLine, SourceName, SourceComponent, ProcedureName, ErrorDescription)
        VALUES
            (@PackageExecutionId, @BatchId, @ErrorSeverity, @ErrorCode, ERROR_NUMBER(), ERROR_STATE(),
             ERROR_LINE(), @SourceName, @SourceComponent,
             COALESCE(@ProcedureName, ERROR_PROCEDURE()), @ErrorDescription);
    END TRY
    BEGIN CATCH
        -- Logging must never mask the original failure.
        RETURN 0;
    END CATCH;

    RETURN 0;
END;
GO
