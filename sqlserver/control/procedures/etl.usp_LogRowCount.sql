/*
    etl.usp_LogRowCount

    Deploy target : WideWorldImportersStaging
    Called by     : every package after its data flow, and by staging/dimension/fact
                    procedures that move rows without a data flow
    Depends on    : etl.RowCountAudit, etl.PackageExecution

    One row per (execution, object). The computed VarianceRowCount column on
    etl.RowCountAudit is what the reconciliation views and the
    etl.usp_AssertRowCountReconciliation gate read.
*/
IF OBJECT_ID(N'etl.usp_LogRowCount', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogRowCount;
GO

CREATE PROCEDURE etl.usp_LogRowCount
(
    @PackageExecutionId BIGINT,
    @ObjectName         NVARCHAR(200),
    @SourceRowCount     BIGINT = NULL,
    @TargetRowCount     BIGINT = NULL,
    @InsertRowCount     BIGINT = NULL,
    @UpdateRowCount     BIGINT = NULL,
    @DeleteRowCount     BIGINT = NULL,
    @RejectRowCount     BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    INSERT INTO etl.RowCountAudit
        (PackageExecutionId, ObjectName, SourceRowCount, TargetRowCount,
         InsertRowCount, UpdateRowCount, DeleteRowCount, RejectRowCount)
    VALUES
        (@PackageExecutionId, @ObjectName, @SourceRowCount, @TargetRowCount,
         @InsertRowCount, @UpdateRowCount, @DeleteRowCount, @RejectRowCount);

    UPDATE pe
    SET RowsRead     = ISNULL(pe.RowsRead, 0)     + ISNULL(@SourceRowCount, 0),
        RowsInserted = ISNULL(pe.RowsInserted, 0) + ISNULL(@InsertRowCount, 0),
        RowsUpdated  = ISNULL(pe.RowsUpdated, 0)  + ISNULL(@UpdateRowCount, 0),
        RowsDeleted  = ISNULL(pe.RowsDeleted, 0)  + ISNULL(@DeleteRowCount, 0),
        RowsRejected = ISNULL(pe.RowsRejected, 0) + ISNULL(@RejectRowCount, 0)
    FROM etl.PackageExecution AS pe
    WHERE pe.PackageExecutionId = @PackageExecutionId;

    RETURN 0;
END;
GO
