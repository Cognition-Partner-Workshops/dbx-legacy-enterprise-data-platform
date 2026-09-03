/*
    etl.usp_LogRejectedRecord

    Deploy target : WideWorldImportersStaging
    Called by     : data-quality screen packages (DQ_*), staging procedures, and
                    the err.* reject destinations in the extract data flows
    Depends on    : etl.RejectedRecord

    Set-based overload: pass a delimited payload for a single row, or call
    etl.usp_LogRejectedRecordSet from a procedure that already has the rejects in
    a table. Rejects are data, not failures - they are counted, routed and
    reported, and only breach the batch when the tolerance in
    etl.usp_LogPackageEnd is exceeded.
*/
IF OBJECT_ID(N'etl.usp_LogRejectedRecord', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogRejectedRecord;
GO

CREATE PROCEDURE etl.usp_LogRejectedRecord
(
    @PackageExecutionId BIGINT = NULL,
    @BatchId            BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = NULL,
    @ObjectName         NVARCHAR(200),
    @BusinessKey        NVARCHAR(200) = NULL,
    @RejectReasonCode   NVARCHAR(50),
    @RejectReason       NVARCHAR(500) = NULL,
    @RejectStage        NVARCHAR(50) = N'Stage',
    @SsisErrorCode      INT = NULL,
    @SsisErrorColumn    INT = NULL,
    @RecordPayload      NVARCHAR(MAX) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @BatchId = 0 SET @BatchId = NULL;
    IF @PackageExecutionId = 0 SET @PackageExecutionId = NULL;

    INSERT INTO etl.RejectedRecord
        (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
         RejectReasonCode, RejectReason, RejectStage, SsisErrorCode, SsisErrorColumn, RecordPayload)
    VALUES
        (@PackageExecutionId, @BatchId, @SourceSystemCode, @ObjectName, @BusinessKey,
         @RejectReasonCode, @RejectReason, @RejectStage, @SsisErrorCode, @SsisErrorColumn, @RecordPayload);

    UPDATE etl.PackageExecution
    SET RowsRejected = ISNULL(RowsRejected, 0) + 1
    WHERE PackageExecutionId = @PackageExecutionId;

    RETURN 0;
END;
GO
