/*
    etl.usp_StartBatch

    Deploy target : WideWorldImportersStaging
    Called by     : Master_Daily_ETL, Master_Intraday_Sales, Master_Month_End_Close,
                    Master_File_Ingestion, SQL Agent job step 1
    Depends on    : etl.Batch, etl.Configuration

    Opens a batch for a business date. A batch is the unit of restartability:
    if a batch for the same name/date is still Running the procedure either
    adopts it (recovery rerun) or raises, depending on @AllowAdoptRunning.
*/
IF OBJECT_ID(N'etl.usp_StartBatch', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_StartBatch;
GO

CREATE PROCEDURE etl.usp_StartBatch
(
    @BatchName          NVARCHAR(100),
    @BatchType          NVARCHAR(30)    = N'Daily',
    @BusinessDate       DATE            = NULL,
    @EnvironmentCode    NVARCHAR(10)    = NULL,
    @AllowAdoptRunning  BIT             = 0,
    @Notes              NVARCHAR(1000)  = NULL,
    @BatchId            BIGINT          OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @BusinessDate = ISNULL(@BusinessDate, CAST(SYSDATETIME() AS DATE));

    IF @EnvironmentCode IS NULL
        SELECT @EnvironmentCode = c.ConfigurationValue
        FROM etl.Configuration AS c
        WHERE c.ConfigurationKey = N'EnvironmentCode'
          AND c.EnvironmentCode = N'ALL';

    SET @EnvironmentCode = ISNULL(@EnvironmentCode, N'DEV');

    DECLARE @ExistingBatchId BIGINT;

    SELECT TOP (1) @ExistingBatchId = b.BatchId
    FROM etl.Batch AS b
    WHERE b.BatchName = @BatchName
      AND b.BusinessDate = @BusinessDate
      AND b.Status = N'Running'
    ORDER BY b.BatchId DESC;

    IF @ExistingBatchId IS NOT NULL
    BEGIN
        IF @AllowAdoptRunning = 1
        BEGIN
            SET @BatchId = @ExistingBatchId;

            UPDATE etl.Batch
            SET Notes = CONCAT(ISNULL(Notes + N' | ', N''), N'Adopted by recovery rerun at ',
                               CONVERT(NVARCHAR(30), SYSUTCDATETIME(), 126))
            WHERE BatchId = @BatchId;

            RETURN 0;
        END;

        DECLARE @msg NVARCHAR(400) =
            CONCAT(N'Batch ', @BatchName, N' for business date ',
                   CONVERT(NVARCHAR(10), @BusinessDate, 23),
                   N' is already running (BatchId ', @ExistingBatchId,
                   N'). Complete or cancel it, or rerun with @AllowAdoptRunning = 1.');
        THROW 51001, @msg, 1;
    END;

    INSERT INTO etl.Batch (BatchName, BatchType, BusinessDate, EnvironmentCode, Status, Notes)
    VALUES (@BatchName, @BatchType, @BusinessDate, @EnvironmentCode, N'Running', @Notes);

    SET @BatchId = SCOPE_IDENTITY();

    RETURN 0;
END;
GO
