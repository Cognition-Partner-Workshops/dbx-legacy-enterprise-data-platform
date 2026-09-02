/*
    etl.usp_LogPackageEnd

    Deploy target : WideWorldImportersStaging
    Called by     : every .dtsx package, final Execute SQL Task and OnError handler
    Depends on    : etl.PackageExecution, etl.Configuration, etl.ErrorLog

    Closes a package execution and applies the reject-tolerance rule: if the
    rejected share of rows read exceeds MaxRejectPercent the execution is marked
    Failed even when the package itself completed, which is what makes the
    downstream reconciliation gate meaningful.
*/
IF OBJECT_ID(N'etl.usp_LogPackageEnd', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogPackageEnd;
GO

CREATE PROCEDURE etl.usp_LogPackageEnd
(
    @PackageExecutionId BIGINT,
    @Status             NVARCHAR(20) = N'Succeeded',
    @RowsRead           BIGINT = NULL,
    @RowsInserted       BIGINT = NULL,
    @RowsUpdated        BIGINT = NULL,
    @RowsDeleted        BIGINT = NULL,
    @RowsRejected       BIGINT = NULL,
    @WatermarkFrom      NVARCHAR(50) = NULL,
    @WatermarkTo        NVARCHAR(50) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @MaxRejectPercent DECIMAL(9,4) = 5.0;

    SELECT @MaxRejectPercent = TRY_CONVERT(DECIMAL(9,4), c.ConfigurationValue)
    FROM etl.Configuration AS c
    WHERE c.ConfigurationKey = N'MaxRejectPercent'
      AND c.EnvironmentCode IN (N'ALL');

    DECLARE @RejectPercent DECIMAL(9,4) =
        CASE WHEN ISNULL(@RowsRead, 0) = 0 THEN 0
             ELSE (CAST(ISNULL(@RowsRejected, 0) AS DECIMAL(19,4)) * 100.0) / @RowsRead
        END;

    IF @Status = N'Succeeded' AND @RejectPercent > ISNULL(@MaxRejectPercent, 5.0)
    BEGIN
        SET @Status = N'Failed';

        INSERT INTO etl.ErrorLog
            (PackageExecutionId, BatchId, ErrorSeverity, SourceName, ProcedureName, ErrorDescription)
        SELECT pe.PackageExecutionId,
               pe.BatchId,
               N'Critical',
               pe.PackageName,
               N'etl.usp_LogPackageEnd',
               CONCAT(N'Reject tolerance breached: ', CONVERT(NVARCHAR(20), @RejectPercent),
                      N'% of ', CONVERT(NVARCHAR(20), @RowsRead),
                      N' rows rejected, tolerance is ', CONVERT(NVARCHAR(20), @MaxRejectPercent), N'%.')
        FROM etl.PackageExecution AS pe
        WHERE pe.PackageExecutionId = @PackageExecutionId;
    END;

    UPDATE etl.PackageExecution
    SET Status = @Status,
        CompletedAtUtc = SYSUTCDATETIME(),
        RowsRead = COALESCE(@RowsRead, RowsRead),
        RowsInserted = COALESCE(@RowsInserted, RowsInserted),
        RowsUpdated = COALESCE(@RowsUpdated, RowsUpdated),
        RowsDeleted = COALESCE(@RowsDeleted, RowsDeleted),
        RowsRejected = COALESCE(@RowsRejected, RowsRejected),
        WatermarkFrom = COALESCE(@WatermarkFrom, WatermarkFrom),
        WatermarkTo = COALESCE(@WatermarkTo, WatermarkTo)
    WHERE PackageExecutionId = @PackageExecutionId;

    RETURN 0;
END;
GO
