/*
    etl.usp_StartBatchStep / etl.usp_EndBatchStep

    Deploy target : WideWorldImportersStaging
    Called by     : master orchestration packages at the head and tail of each
                    sequence container
    Depends on    : etl.BatchStep

    A batch step groups the packages that a master container runs, so a restart
    can resume at "Load Dimensions" rather than replaying the whole night.
*/
IF OBJECT_ID(N'etl.usp_StartBatchStep', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_StartBatchStep;
GO

CREATE PROCEDURE etl.usp_StartBatchStep
(
    @BatchId        BIGINT,
    @StepName       NVARCHAR(100),
    @StepSequence   INT,
    @StepGroup      NVARCHAR(50) = NULL,
    @BatchStepId    BIGINT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AttemptNumber INT;

    SELECT @AttemptNumber = ISNULL(MAX(bs.AttemptNumber), 0) + 1
    FROM etl.BatchStep AS bs
    WHERE bs.BatchId = @BatchId
      AND bs.StepName = @StepName;

    INSERT INTO etl.BatchStep (BatchId, StepName, StepSequence, StepGroup, Status, AttemptNumber)
    VALUES (@BatchId, @StepName, @StepSequence, @StepGroup, N'Running', @AttemptNumber);

    SET @BatchStepId = SCOPE_IDENTITY();

    SELECT @BatchStepId AS BatchStepId;

    RETURN 0;
END;
GO

IF OBJECT_ID(N'etl.usp_EndBatchStep', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_EndBatchStep;
GO

CREATE PROCEDURE etl.usp_EndBatchStep
(
    @BatchStepId    BIGINT,
    @Status         NVARCHAR(20) = N'Succeeded'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @Status = N'Succeeded'
       AND EXISTS (SELECT 1
                   FROM etl.PackageExecution AS pe
                   WHERE pe.BatchStepId = @BatchStepId
                     AND pe.Status IN (N'Failed', N'Running'))
        SET @Status = N'Failed';

    UPDATE etl.BatchStep
    SET Status = @Status,
        CompletedAtUtc = SYSUTCDATETIME()
    WHERE BatchStepId = @BatchStepId;

    RETURN 0;
END;
GO
