/*
    etl.usp_AssertRowCountReconciliation

    Deploy target : WideWorldImportersStaging
    Called by     : Master_Daily_ETL final control container, AUD_* audit packages
    Depends on    : etl.RowCountAudit, etl.PackageExecution, etl.ErrorLog,
                    etl.Configuration, etl.ReconciliationExemption

    Static balance check across a batch: for every audited object, rows read must
    equal rows landed plus rows rejected, within the configured absolute and
    percentage tolerances. Objects whose load pattern legitimately changes the
    row count (aggregates, deduplication, SCD2 expansion) are exempted through
    etl.ReconciliationExemption.

    This is an arithmetic consistency check over recorded counts. It does not,
    and cannot, prove that the values loaded are correct.
*/
IF OBJECT_ID(N'etl.usp_AssertRowCountReconciliation', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_AssertRowCountReconciliation;
GO

CREATE PROCEDURE etl.usp_AssertRowCountReconciliation
(
    @BatchId            BIGINT,
    @RaiseOnFailure     BIT = 1,
    @FailedObjectCount  INT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AbsoluteTolerance BIGINT =
        TRY_CONVERT(BIGINT, etl.ufn_GetConfigurationValue(N'ReconAbsoluteTolerance', N'ALL'));
    DECLARE @PercentTolerance DECIMAL(9,4) =
        TRY_CONVERT(DECIMAL(9,4), etl.ufn_GetConfigurationValue(N'ReconPercentTolerance', N'ALL'));

    SET @AbsoluteTolerance = ISNULL(@AbsoluteTolerance, 0);
    SET @PercentTolerance = ISNULL(@PercentTolerance, 0.0);

    IF OBJECT_ID(N'tempdb..#Recon') IS NOT NULL DROP TABLE #Recon;

    CREATE TABLE #Recon
    (
        ObjectName      NVARCHAR(200) NOT NULL,
        SourceRowCount  BIGINT NULL,
        TargetRowCount  BIGINT NULL,
        RejectRowCount  BIGINT NULL,
        Variance        BIGINT NULL,
        VariancePercent DECIMAL(9,4) NULL
    );

    WITH BatchAudit AS
    (
        SELECT rca.ObjectName,
               SUM(ISNULL(rca.SourceRowCount, 0)) AS SourceRowCount,
               SUM(ISNULL(rca.TargetRowCount, 0)) AS TargetRowCount,
               SUM(ISNULL(rca.RejectRowCount, 0)) AS RejectRowCount
        FROM etl.RowCountAudit AS rca
        INNER JOIN etl.PackageExecution AS pe
                ON pe.PackageExecutionId = rca.PackageExecutionId
        WHERE pe.BatchId = @BatchId
        GROUP BY rca.ObjectName
    )
    INSERT INTO #Recon (ObjectName, SourceRowCount, TargetRowCount, RejectRowCount, Variance, VariancePercent)
    SELECT ba.ObjectName,
           ba.SourceRowCount,
           ba.TargetRowCount,
           ba.RejectRowCount,
           ba.SourceRowCount - ba.TargetRowCount - ba.RejectRowCount,
           CASE WHEN ba.SourceRowCount = 0 THEN 0
                ELSE (CAST(ABS(ba.SourceRowCount - ba.TargetRowCount - ba.RejectRowCount) AS DECIMAL(19,4)) * 100.0)
                     / ba.SourceRowCount
           END
    FROM BatchAudit AS ba
    WHERE NOT EXISTS (SELECT 1
                      FROM etl.ReconciliationExemption AS rx
                      WHERE rx.ObjectName = ba.ObjectName);

    SELECT @FailedObjectCount = COUNT(*)
    FROM #Recon
    WHERE ABS(Variance) > @AbsoluteTolerance
      AND VariancePercent > @PercentTolerance;

    INSERT INTO etl.ErrorLog (BatchId, ErrorSeverity, ProcedureName, SourceName, ErrorDescription)
    SELECT @BatchId,
           N'Error',
           N'etl.usp_AssertRowCountReconciliation',
           r.ObjectName,
           CONCAT(N'Row count reconciliation variance of ', r.Variance, N' row(s) (',
                  CONVERT(NVARCHAR(20), r.VariancePercent), N'%): source ', r.SourceRowCount,
                  N', target ', r.TargetRowCount, N', rejected ', r.RejectRowCount, N'.')
    FROM #Recon AS r
    WHERE ABS(r.Variance) > @AbsoluteTolerance
      AND r.VariancePercent > @PercentTolerance;

    SELECT * FROM #Recon ORDER BY ABS(Variance) DESC, ObjectName;

    IF @FailedObjectCount > 0 AND @RaiseOnFailure = 1
    BEGIN
        DECLARE @msg NVARCHAR(400) =
            CONCAT(N'Row count reconciliation failed for ', @FailedObjectCount,
                   N' object(s) in batch ', @BatchId, N'. See etl.ErrorLog for detail.');
        THROW 51030, @msg, 1;
    END;

    RETURN 0;
END;
GO
