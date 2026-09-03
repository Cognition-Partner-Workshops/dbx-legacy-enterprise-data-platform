/*
    Runtime validation 02 - row count reconciliation.

    Run against  : WideWorldImporters_Staging, then WideWorldImportersDW
    Reads        : etl.RowCountAudit, etl.PackageExecution, raw.*, stg.*
    Writes       : nothing

    Never run. The queries reconstruct the source -> raw -> stg -> warehouse
    hop counts from what the loads logged; whether the loads log them
    faithfully is exactly the thing that has not been established.

    etl.RowCountAudit.VarianceRowCount is a computed column:
        SourceRowCount - TargetRowCount - RejectRowCount
    so a healthy hop is variance zero. A non-zero variance is not automatically
    a defect: several loads deduplicate on purpose and book the difference
    nowhere.
*/
SET NOCOUNT ON;
GO

/* 1. Every hop logged for the current business date, worst variance first. */
SELECT
    pe.PackageName,
    rca.ObjectName,
    rca.SourceRowCount,
    rca.TargetRowCount,
    rca.InsertRowCount,
    rca.UpdateRowCount,
    rca.DeleteRowCount,
    rca.RejectRowCount,
    rca.VarianceRowCount,
    rca.RecordedAtUtc
FROM etl.RowCountAudit AS rca
INNER JOIN etl.PackageExecution AS pe
    ON pe.PackageExecutionId = rca.PackageExecutionId
WHERE rca.RecordedAtUtc >= DATEADD(DAY, -1, SYSUTCDATETIME())
ORDER BY ABS(rca.VarianceRowCount) DESC;
GO

/* 2. Hops whose variance is outside the tolerance held in etl.Configuration.
      The tolerance is a percentage stored as a string; the cast is deliberate
      and will fail loudly if someone has written a non-numeric value. */
DECLARE @TolerancePercent DECIMAL(9, 4) =
(
    SELECT TRY_CAST(c.ConfigurationValue AS DECIMAL(9, 4))
    FROM etl.Configuration AS c
    WHERE c.ConfigurationKey = N'RowCountVarianceTolerancePercent'
      AND c.EnvironmentCode IN (N'ALL', N'DEV', N'TEST', N'PROD')
);

SELECT
    pe.PackageName,
    rca.ObjectName,
    rca.SourceRowCount,
    rca.TargetRowCount,
    rca.RejectRowCount,
    rca.VarianceRowCount,
    CASE
        WHEN ISNULL(rca.SourceRowCount, 0) = 0 THEN NULL
        ELSE CAST(100.0 * ABS(rca.VarianceRowCount) / rca.SourceRowCount AS DECIMAL(9, 4))
    END AS VariancePercent,
    @TolerancePercent AS TolerancePercent
FROM etl.RowCountAudit AS rca
INNER JOIN etl.PackageExecution AS pe
    ON pe.PackageExecutionId = rca.PackageExecutionId
WHERE rca.RecordedAtUtc >= DATEADD(DAY, -1, SYSUTCDATETIME())
  AND ISNULL(rca.SourceRowCount, 0) > 0
  AND 100.0 * ABS(rca.VarianceRowCount) / rca.SourceRowCount > ISNULL(@TolerancePercent, 0.5)
ORDER BY VariancePercent DESC;
GO

/* 3. Loads that ran but logged no row count at all. The control-framework
      static check reports which packages contain the etl.usp_LogRowCount call;
      this reports which ones actually wrote a row. */
SELECT
    pe.PackageName,
    pe.BatchId,
    pe.StartedAtUtc,
    pe.Status,
    pe.RowsRead,
    pe.RowsInserted
FROM etl.PackageExecution AS pe
WHERE pe.StartedAtUtc >= DATEADD(DAY, -1, SYSUTCDATETIME())
  AND pe.Status = N'Succeeded'
  AND NOT EXISTS
      (
          SELECT 1
          FROM etl.RowCountAudit AS rca
          WHERE rca.PackageExecutionId = pe.PackageExecutionId
      )
ORDER BY pe.PackageName;
GO

/* 4. Landed-versus-staged comparison for the raw layer. Run in the staging
      database only. Extend the list as new raw tables are added; there is no
      dynamic version on purpose, because a dynamic one has previously counted
      tables that no longer existed. */
SELECT N'raw.OracleCustomerMaster' AS ObjectName, COUNT_BIG(*) AS RowCount_ FROM raw.OracleCustomerMaster
UNION ALL SELECT N'stg.Customer',                 COUNT_BIG(*) FROM stg.Customer
UNION ALL SELECT N'raw.OracleSupplierMaster',     COUNT_BIG(*) FROM raw.OracleSupplierMaster
UNION ALL SELECT N'stg.Supplier',                 COUNT_BIG(*) FROM stg.Supplier
UNION ALL SELECT N'raw.OracleProductMaster',      COUNT_BIG(*) FROM raw.OracleProductMaster
UNION ALL SELECT N'stg.Product',                  COUNT_BIG(*) FROM stg.Product
ORDER BY ObjectName;
GO

/* 5. Rejected rows that were never reprocessed and are older than the SLA in
      the runbook. These are rows that silently left the pipeline. */
SELECT
    r.ObjectName,
    r.RejectStage,
    r.RejectReasonCode,
    COUNT(*)            AS OutstandingRejects,
    MIN(r.LoggedAtUtc)  AS OldestRejectAtUtc
FROM etl.RejectedRecord AS r
WHERE r.IsReprocessed = 0
  AND r.LoggedAtUtc < DATEADD(DAY, -3, SYSUTCDATETIME())
GROUP BY r.ObjectName, r.RejectStage, r.RejectReasonCode
ORDER BY OutstandingRejects DESC;
GO
