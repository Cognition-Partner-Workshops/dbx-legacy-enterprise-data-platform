/*
    etl.usp_LogPackageStart

    Deploy target : WideWorldImportersStaging
    Called by     : every .dtsx package, first Execute SQL Task
    Depends on    : etl.PackageExecution, etl.BatchStep

    Registers a package execution and hands the caller back the identity that all
    subsequent logging for that run is keyed on. The attempt number increments
    when the same package runs more than once inside a batch (retry or rerun).
*/
IF OBJECT_ID(N'etl.usp_LogPackageStart', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogPackageStart;
GO

CREATE PROCEDURE etl.usp_LogPackageStart
(
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200),
    @ProjectName        NVARCHAR(100) = NULL,
    @StepName           NVARCHAR(100) = NULL,
    @PackageExecutionId BIGINT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @BatchId = 0 SET @BatchId = NULL;

    DECLARE @BatchStepId BIGINT;

    IF @StepName IS NOT NULL AND @BatchId IS NOT NULL
        SELECT TOP (1) @BatchStepId = bs.BatchStepId
        FROM etl.BatchStep AS bs
        WHERE bs.BatchId = @BatchId
          AND bs.StepName = @StepName
        ORDER BY bs.BatchStepId DESC;

    DECLARE @AttemptNumber INT;

    SELECT @AttemptNumber = ISNULL(MAX(pe.AttemptNumber), 0) + 1
    FROM etl.PackageExecution AS pe
    WHERE pe.PackageName = @PackageName
      AND (@BatchId IS NULL OR pe.BatchId = @BatchId);

    INSERT INTO etl.PackageExecution
        (BatchId, BatchStepId, PackageName, ProjectName, Status, AttemptNumber,
         RowsRead, RowsInserted, RowsUpdated, RowsRejected)
    VALUES
        (@BatchId, @BatchStepId, @PackageName, @ProjectName, N'Running', @AttemptNumber,
         0, 0, 0, 0);

    SET @PackageExecutionId = SCOPE_IDENTITY();

    SELECT @PackageExecutionId AS PackageExecutionId;

    RETURN 0;
END;
GO
