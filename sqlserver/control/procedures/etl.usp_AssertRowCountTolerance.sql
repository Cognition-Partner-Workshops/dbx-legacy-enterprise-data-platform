/*
    Object        : [etl].[usp_AssertRowCountTolerance]
    Deploy target : WWI_Staging and WideWorldImportersDW
    Deploy order  : after etl.usp_AssertRowCountReconciliation.sql and
                    etl.usp_LogError.sql
    Depends on    : etl.RowCountAudit, etl.PackageExecution, etl.Configuration,
                    etl.ReconciliationExemption, etl.usp_AssertRowCountReconciliation,
                    etl.usp_LogError
    Called by     : the SQL Agent reconciliation steps of WWI - Daily ETL and
                    WWI - Month End, and the DQ_Batch_Reconciliation package

    The scoped front door to reconciliation. etl.usp_AssertRowCountReconciliation
    balances every object in a batch; this narrows that to one scope - a domain,
    a load phase, or a single object - so the nightly job can gate the sales
    stream without waiting for finance, and so the finance close can re-assert
    only its own objects against a tighter tolerance.

    Scope resolution, in order:
        @ObjectName supplied  -> that object only
        @Scope = 'ALL'        -> every object in the batch
        otherwise             -> objects whose load phase or domain matches the
                                 scope code recorded on their package execution

    Tolerances come from etl.Configuration (ReconAbsoluteTolerance /
    ReconPercentTolerance) unless passed explicitly, so a scope can be held to a
    stricter number than the estate default without a code change.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.usp_AssertRowCountTolerance', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_AssertRowCountTolerance;
GO

CREATE PROCEDURE etl.usp_AssertRowCountTolerance
(
    @BatchId            BIGINT,
    @Scope              NVARCHAR(50)    = N'ALL',
    @ObjectName         NVARCHAR(200)   = NULL,
    @AbsoluteTolerance  BIGINT          = NULL,
    @PercentTolerance   DECIMAL(9, 4)   = NULL,
    @RaiseOnFailure     BIT             = 1,
    @FailedObjectCount  INT             = NULL OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @FailedObjectCount = 0;
    SET @Scope = NULLIF(LTRIM(RTRIM(ISNULL(@Scope, N'ALL'))), N'');

    IF @BatchId IS NULL
    BEGIN
        RAISERROR (N'etl.usp_AssertRowCountTolerance requires a batch id.', 16, 1);
        RETURN 1;
    END

    /*
        The unscoped case is exactly the full reconciliation, so hand it over
        rather than keeping two copies of the balance arithmetic.
    */
    IF @ObjectName IS NULL AND (@Scope IS NULL OR @Scope = N'ALL')
    BEGIN
        EXEC etl.usp_AssertRowCountReconciliation
             @BatchId           = @BatchId,
             @RaiseOnFailure    = @RaiseOnFailure,
             @FailedObjectCount = @FailedObjectCount OUTPUT;
        RETURN 0;
    END

    IF @AbsoluteTolerance IS NULL
        SET @AbsoluteTolerance = TRY_CONVERT(BIGINT, etl.ufn_GetConfigurationValue(N'ReconAbsoluteTolerance', N'ALL'));
    IF @PercentTolerance IS NULL
        SET @PercentTolerance = TRY_CONVERT(DECIMAL(9, 4), etl.ufn_GetConfigurationValue(N'ReconPercentTolerance', N'ALL'));

    SET @AbsoluteTolerance = ISNULL(@AbsoluteTolerance, 0);
    SET @PercentTolerance  = ISNULL(@PercentTolerance, 0);

    DECLARE @Balance TABLE
    (
        ObjectName      NVARCHAR(200)   NOT NULL,
        SourceRows      BIGINT          NULL,
        TargetRows      BIGINT          NULL,
        RejectedRows    BIGINT          NULL,
        Variance        BIGINT          NULL,
        VariancePercent DECIMAL(18, 6)  NULL,
        IsExempt        BIT             NOT NULL
    );

    INSERT INTO @Balance (ObjectName, SourceRows, TargetRows, RejectedRows, Variance, VariancePercent, IsExempt)
    SELECT  a.ObjectName,
            SUM(ISNULL(a.SourceRowCount, 0))    AS SourceRows,
            SUM(ISNULL(a.TargetRowCount, 0))    AS TargetRows,
            SUM(ISNULL(a.RejectRowCount, 0))    AS RejectedRows,
            SUM(ISNULL(a.SourceRowCount, 0)) - SUM(ISNULL(a.TargetRowCount, 0))
                - SUM(ISNULL(a.RejectRowCount, 0)) AS Variance,
            CASE WHEN SUM(ISNULL(a.SourceRowCount, 0)) = 0 THEN 0
                 ELSE ABS(CAST(SUM(ISNULL(a.SourceRowCount, 0)) - SUM(ISNULL(a.TargetRowCount, 0))
                              - SUM(ISNULL(a.RejectRowCount, 0)) AS DECIMAL(18, 6)))
                      * 100.0 / SUM(ISNULL(a.SourceRowCount, 0))
            END AS VariancePercent,
            CASE WHEN EXISTS (SELECT 1
                              FROM   etl.ReconciliationExemption AS e
                              WHERE  e.ObjectName = a.ObjectName)
                 THEN 1 ELSE 0
            END AS IsExempt
    FROM    etl.RowCountAudit AS a
            INNER JOIN etl.PackageExecution AS pe
                ON pe.PackageExecutionId = a.PackageExecutionId
    WHERE   pe.BatchId = @BatchId
            AND (@ObjectName IS NULL OR a.ObjectName = @ObjectName)
            AND (@ObjectName IS NOT NULL
                 OR @Scope IS NULL
                 OR @Scope = N'ALL'
                 OR UPPER(ISNULL(pe.PackageName, N'')) LIKE N'%' + UPPER(@Scope) + N'%'
                 OR UPPER(ISNULL(pe.ProjectName, N'')) LIKE N'%' + UPPER(@Scope) + N'%'
                 OR UPPER(a.ObjectName) LIKE N'%' + UPPER(@Scope) + N'%')
    GROUP BY a.ObjectName;

    IF NOT EXISTS (SELECT 1 FROM @Balance)
    BEGIN
        /*
            An empty scope is not a pass. A phase that produced no audit rows at
            all usually means its packages never ran, which is exactly what the
            gate exists to notice.
        */
        DECLARE @EmptyMessage NVARCHAR(400) =
            N'No row count audit rows for batch ' + CAST(@BatchId AS NVARCHAR(20))
            + N' in scope ' + ISNULL(@ObjectName, ISNULL(@Scope, N'ALL')) + N'.';

        EXEC etl.usp_LogError
             @BatchId          = @BatchId,
             @SourceName       = N'etl.usp_AssertRowCountTolerance',
             @ErrorSeverity    = N'Warning',
             @ErrorDescription = @EmptyMessage;

        IF @RaiseOnFailure = 1
        BEGIN
            RAISERROR (@EmptyMessage, 16, 1);
            RETURN 1;
        END

        RETURN 0;
    END

    SELECT  @FailedObjectCount = COUNT(*)
    FROM    @Balance
    WHERE   IsExempt = 0
            AND ABS(ISNULL(Variance, 0)) > @AbsoluteTolerance
            AND ISNULL(VariancePercent, 0) > @PercentTolerance;

    IF @FailedObjectCount > 0
    BEGIN
        DECLARE @Detail NVARCHAR(MAX) = N'';

        SELECT  @Detail = @Detail + ObjectName
                          + N' src=' + CAST(ISNULL(SourceRows, 0) AS NVARCHAR(20))
                          + N' tgt=' + CAST(ISNULL(TargetRows, 0) AS NVARCHAR(20))
                          + N' rej=' + CAST(ISNULL(RejectedRows, 0) AS NVARCHAR(20))
                          + N' var=' + CAST(ISNULL(Variance, 0) AS NVARCHAR(20)) + NCHAR(10)
        FROM    @Balance
        WHERE   IsExempt = 0
                AND ABS(ISNULL(Variance, 0)) > @AbsoluteTolerance
                AND ISNULL(VariancePercent, 0) > @PercentTolerance
        ORDER BY ABS(ISNULL(Variance, 0)) DESC;

        DECLARE @Message NVARCHAR(MAX) =
            N'Row count tolerance breached for scope ' + ISNULL(@ObjectName, ISNULL(@Scope, N'ALL'))
            + N' in batch ' + CAST(@BatchId AS NVARCHAR(20)) + N' ('
            + CAST(@FailedObjectCount AS NVARCHAR(10)) + N' object(s)):' + NCHAR(10) + @Detail;

        EXEC etl.usp_LogError
             @BatchId          = @BatchId,
             @SourceName       = N'etl.usp_AssertRowCountTolerance',
             @ErrorSeverity    = N'Critical',
             @ErrorDescription = @Message;

        IF @RaiseOnFailure = 1
        BEGIN
            DECLARE @RaiseText NVARCHAR(2044) = LEFT(@Message, 2044);
            RAISERROR (@RaiseText, 16, 1);
            RETURN 1;
        END
    END

    SELECT  ObjectName,
            SourceRows,
            TargetRows,
            RejectedRows,
            Variance,
            VariancePercent,
            IsExempt,
            CASE WHEN IsExempt = 1 THEN N'Exempt'
                 WHEN ABS(ISNULL(Variance, 0)) > @AbsoluteTolerance
                      AND ISNULL(VariancePercent, 0) > @PercentTolerance THEN N'Breached'
                 ELSE N'WithinTolerance'
            END AS ToleranceStatus
    FROM    @Balance
    ORDER BY ABS(ISNULL(Variance, 0)) DESC, ObjectName;

    RETURN 0;
END
GO
