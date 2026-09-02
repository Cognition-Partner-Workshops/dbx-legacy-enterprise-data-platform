/*
    etl.usp_SetWatermark

    Deploy target : WideWorldImportersStaging
    Called by     : every incremental extract package, after the data flow succeeds
    Depends on    : etl.Watermark

    Advances a watermark, keeping the previous value so an operator can roll a
    load back by one cycle. The watermark never moves backwards unless
    @AllowRewind is set, which protects against a partial extract that reported a
    lower high-water value than the last successful run.
*/
IF OBJECT_ID(N'etl.usp_SetWatermark', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_SetWatermark;
GO

CREATE PROCEDURE etl.usp_SetWatermark
(
    @SourceSystemCode   NVARCHAR(20),
    @ObjectName         NVARCHAR(200),
    @WatermarkTo        NVARCHAR(50),
    @PackageExecutionId BIGINT = NULL,
    @AllowRewind        BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @WatermarkTo IS NULL OR LEN(@WatermarkTo) = 0
    BEGIN
        -- Nothing extracted; leave the watermark where it was so the next run retries the window.
        RETURN 0;
    END;

    DECLARE @WatermarkType NVARCHAR(20), @LastValue NVARCHAR(50);

    SELECT @WatermarkType = w.WatermarkType, @LastValue = w.LastValue
    FROM etl.Watermark AS w
    WHERE w.SourceSystemCode = @SourceSystemCode
      AND w.ObjectName = @ObjectName;

    IF @WatermarkType IS NULL
    BEGIN
        INSERT INTO etl.Watermark
            (SourceSystemCode, ObjectName, WatermarkType, LastValue, LastLoadedAtUtc, LastPackageExecutionId)
        VALUES
            (@SourceSystemCode, @ObjectName, N'Timestamp', @WatermarkTo, SYSUTCDATETIME(), @PackageExecutionId);

        RETURN 0;
    END;

    DECLARE @MovesForward BIT =
        CASE
            WHEN @LastValue IS NULL THEN 1
            WHEN @WatermarkType = N'NumericKey'
                 AND TRY_CONVERT(BIGINT, @WatermarkTo) >= TRY_CONVERT(BIGINT, @LastValue) THEN 1
            WHEN @WatermarkType <> N'NumericKey'
                 AND TRY_CONVERT(DATETIME2(3), @WatermarkTo) >= TRY_CONVERT(DATETIME2(3), @LastValue) THEN 1
            ELSE 0
        END;

    IF @MovesForward = 0 AND @AllowRewind = 0
    BEGIN
        INSERT INTO etl.ErrorLog (PackageExecutionId, ErrorSeverity, ProcedureName, ErrorDescription)
        VALUES (@PackageExecutionId, N'Warning', N'etl.usp_SetWatermark',
                CONCAT(N'Refused to rewind watermark for ', @SourceSystemCode, N'.', @ObjectName,
                       N' from ', @LastValue, N' to ', @WatermarkTo, N'.'));
        RETURN 0;
    END;

    UPDATE etl.Watermark
    SET PreviousValue = LastValue,
        LastValue = @WatermarkTo,
        LastLoadedAtUtc = SYSUTCDATETIME(),
        LastPackageExecutionId = COALESCE(@PackageExecutionId, LastPackageExecutionId)
    WHERE SourceSystemCode = @SourceSystemCode
      AND ObjectName = @ObjectName;

    RETURN 0;
END;
GO
