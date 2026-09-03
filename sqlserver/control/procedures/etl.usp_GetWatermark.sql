/*
    etl.usp_GetWatermark

    Deploy target : WideWorldImportersStaging
    Called by     : every incremental extract package, before the data flow
    Depends on    : etl.Watermark, etl.SourceSystem, etl.Configuration

    Returns the [from, to) window an extract should pull. Three watermark styles
    are supported because the legacy estate uses all three:

      Timestamp  - LAST_UPDATE_DT style columns, with a lookback to absorb the
                   clock skew and open-transaction lag between Oracle and SQL Server.
      NumericKey - monotonically increasing identity/sequence keys.
      DateWindow - fixed business-date windows for journal and ledger extracts.

    @ReloadFullHistory = 1 collapses the window to the configured epoch so a
    package can be rerun as a full reload without editing the watermark row.
*/
IF OBJECT_ID(N'etl.usp_GetWatermark', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_GetWatermark;
GO

CREATE PROCEDURE etl.usp_GetWatermark
(
    @SourceSystemCode   NVARCHAR(20),
    @ObjectName         NVARCHAR(200),
    @ReloadFullHistory  BIT = 0,
    @WatermarkFrom      NVARCHAR(50) OUTPUT,
    @WatermarkTo        NVARCHAR(50) OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Epoch NVARCHAR(50) = N'1900-01-01T00:00:00';

    SELECT @Epoch = c.ConfigurationValue
    FROM etl.Configuration AS c
    WHERE c.ConfigurationKey = N'WatermarkEpoch'
      AND c.EnvironmentCode = N'ALL';

    DECLARE @WatermarkType  NVARCHAR(20),
            @LastValue      NVARCHAR(50),
            @LookbackMinutes INT,
            @IsLocked       BIT;

    SELECT @WatermarkType   = w.WatermarkType,
           @LastValue       = w.LastValue,
           @LookbackMinutes = w.LookbackMinutes,
           @IsLocked        = w.IsLocked
    FROM etl.Watermark AS w
    WHERE w.SourceSystemCode = @SourceSystemCode
      AND w.ObjectName = @ObjectName;

    IF @WatermarkType IS NULL
    BEGIN
        -- First run for this object: register it as a timestamp watermark at the epoch.
        INSERT INTO etl.Watermark (SourceSystemCode, ObjectName, WatermarkType, LastValue, LookbackMinutes)
        VALUES (@SourceSystemCode, @ObjectName, N'Timestamp', @Epoch, 60);

        SELECT @WatermarkType = N'Timestamp',
               @LastValue = @Epoch,
               @LookbackMinutes = 60,
               @IsLocked = 0;
    END;

    IF @IsLocked = 1
    BEGIN
        DECLARE @lockMsg NVARCHAR(400) =
            CONCAT(N'Watermark for ', @SourceSystemCode, N'.', @ObjectName,
                   N' is locked. A previous load left it in an indeterminate state and it must be ',
                   N'reviewed before the extract can run again.');
        THROW 51010, @lockMsg, 1;
    END;

    IF @ReloadFullHistory = 1
        SET @LastValue = CASE WHEN @WatermarkType = N'NumericKey' THEN N'0' ELSE @Epoch END;

    IF @WatermarkType = N'NumericKey'
    BEGIN
        SET @WatermarkFrom = ISNULL(@LastValue, N'0');
        -- The extract itself resolves the upper bound from MAX(key); NULL means "open ended".
        SET @WatermarkTo = NULL;
    END
    ELSE IF @WatermarkType = N'DateWindow'
    BEGIN
        SET @WatermarkFrom = CONVERT(NVARCHAR(50), CAST(ISNULL(TRY_CONVERT(DATETIME2(3), @LastValue),
                                                               CAST(@Epoch AS DATETIME2(3))) AS DATE), 126);
        SET @WatermarkTo = CONVERT(NVARCHAR(50), CAST(SYSDATETIME() AS DATE), 126);
    END
    ELSE
    BEGIN
        SET @WatermarkFrom = CONVERT(NVARCHAR(50),
            DATEADD(MINUTE, -ISNULL(@LookbackMinutes, 0),
                    ISNULL(TRY_CONVERT(DATETIME2(3), @LastValue), CAST(@Epoch AS DATETIME2(3)))), 126);
        SET @WatermarkTo = CONVERT(NVARCHAR(50), SYSUTCDATETIME(), 126);
    END;

    SELECT @WatermarkFrom AS WatermarkFrom, @WatermarkTo AS WatermarkTo;

    RETURN 0;
END;
GO
