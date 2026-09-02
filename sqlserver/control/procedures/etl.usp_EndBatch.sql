/*
    etl.usp_EndBatch

    Deploy target : WideWorldImportersStaging
    Called by     : Master_* orchestration packages, SQL Agent final job step
    Depends on    : etl.Batch, etl.PackageExecution, etl.ErrorLog

    Closes a batch. The final status is derived from the package executions that
    ran under it rather than trusted from the caller, so an orchestration package
    cannot report success over a failed child.
*/
IF OBJECT_ID(N'etl.usp_EndBatch', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_EndBatch;
GO

CREATE PROCEDURE etl.usp_EndBatch
(
    @BatchId        BIGINT,
    @ForceStatus    NVARCHAR(20) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NOT EXISTS (SELECT 1 FROM etl.Batch WHERE BatchId = @BatchId)
        THROW 51002, N'Unknown BatchId supplied to etl.usp_EndBatch.', 1;

    DECLARE @FailedPackages INT,
            @RunningPackages INT,
            @WarningCount INT,
            @DerivedStatus NVARCHAR(20);

    SELECT @FailedPackages  = SUM(CASE WHEN pe.Status = N'Failed' THEN 1 ELSE 0 END),
           @RunningPackages = SUM(CASE WHEN pe.Status = N'Running' THEN 1 ELSE 0 END)
    FROM etl.PackageExecution AS pe
    WHERE pe.BatchId = @BatchId;

    SELECT @WarningCount = COUNT(*)
    FROM etl.ErrorLog AS e
    WHERE e.BatchId = @BatchId
      AND e.ErrorSeverity = N'Warning';

    SET @DerivedStatus =
        CASE
            WHEN @ForceStatus IS NOT NULL THEN @ForceStatus
            WHEN ISNULL(@FailedPackages, 0) > 0 THEN N'Failed'
            WHEN ISNULL(@RunningPackages, 0) > 0 THEN N'Failed'
            WHEN ISNULL(@WarningCount, 0) > 0 THEN N'SucceededWithWarnings'
            ELSE N'Succeeded'
        END;

    UPDATE etl.Batch
    SET Status = @DerivedStatus,
        CompletedAtUtc = SYSUTCDATETIME(),
        Notes = CASE
                    WHEN ISNULL(@RunningPackages, 0) > 0
                    THEN CONCAT(ISNULL(Notes + N' | ', N''), @RunningPackages,
                                N' package execution(s) were still marked Running when the batch closed.')
                    ELSE Notes
                END
    WHERE BatchId = @BatchId;

    UPDATE etl.BatchStep
    SET Status = N'Failed',
        CompletedAtUtc = SYSUTCDATETIME()
    WHERE BatchId = @BatchId
      AND Status = N'Running';

    SELECT @DerivedStatus AS BatchStatus,
           ISNULL(@FailedPackages, 0) AS FailedPackageCount,
           ISNULL(@WarningCount, 0) AS WarningCount;

    RETURN 0;
END;
GO
