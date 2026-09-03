/*
    Runtime validation 01 - control framework health.

    Run against  : WideWorldImporters_Staging, then WideWorldImportersDW
    Reads        : etl.Batch, etl.BatchStep, etl.PackageExecution, etl.ErrorLog,
                   etl.RejectedRecord, etl.Watermark, etl.Configuration
    Writes       : nothing

    This script has never been run. It is written from the table definitions in
    sqlserver/control/02_tables_control_framework.sql and describes what an
    operator should look at after a batch, not what was observed.

    The control framework exists twice - once in staging, once in the warehouse -
    so every query here has to be run in both databases and the two results
    compared by hand.
*/
SET NOCOUNT ON;
GO

/* 1. The last fourteen days of batches, and how they ended. */
SELECT
    b.BusinessDate,
    b.BatchName,
    b.BatchType,
    b.Status,
    b.StartedAtUtc,
    b.CompletedAtUtc,
    DATEDIFF(MINUTE, b.StartedAtUtc, ISNULL(b.CompletedAtUtc, SYSUTCDATETIME())) AS ElapsedMinutes,
    b.RestartFromStep
FROM etl.Batch AS b
WHERE b.BusinessDate >= DATEADD(DAY, -14, CAST(SYSUTCDATETIME() AS DATE))
ORDER BY b.BusinessDate DESC, b.StartedAtUtc DESC;
GO

/* 2. Batches that never closed. A row here is either a run still in flight or
      a master that died without reaching etl.usp_EndBatch. */
SELECT
    b.BatchId,
    b.BatchName,
    b.BusinessDate,
    b.StartedAtUtc,
    DATEDIFF(HOUR, b.StartedAtUtc, SYSUTCDATETIME()) AS HoursOpen
FROM etl.Batch AS b
WHERE b.Status = N'Running'
  AND b.StartedAtUtc < DATEADD(HOUR, -12, SYSUTCDATETIME())
ORDER BY b.StartedAtUtc;
GO

/* 3. Package executions with no end. The same failure one level down: the
      package logged a start through etl.usp_LogPackageStart and never logged
      an end. */
SELECT
    pe.PackageName,
    pe.BatchId,
    pe.StartedAtUtc,
    pe.MachineName,
    pe.AttemptNumber
FROM etl.PackageExecution AS pe
WHERE pe.Status = N'Running'
  AND pe.StartedAtUtc < DATEADD(HOUR, -6, SYSUTCDATETIME())
ORDER BY pe.StartedAtUtc;
GO

/* 4. Packages that have never appeared in the control tables at all. Feed the
      expected list from config/estate-catalog.yaml; the 204 declared packages
      should all show up here within one full weekly cycle. */
SELECT
    pe.PackageName,
    COUNT(*)                        AS Executions,
    MAX(pe.StartedAtUtc)            AS LastStartedAtUtc,
    SUM(CASE WHEN pe.Status = N'Failed' THEN 1 ELSE 0 END) AS FailedExecutions
FROM etl.PackageExecution AS pe
WHERE pe.StartedAtUtc >= DATEADD(DAY, -8, SYSUTCDATETIME())
GROUP BY pe.PackageName
ORDER BY FailedExecutions DESC, LastStartedAtUtc;
GO

/* 5. Watermarks that have stopped moving, or that a failed run left locked.
      A locked watermark silently stops an incremental load from advancing. */
SELECT
    w.SourceSystemCode,
    w.ObjectName,
    w.WatermarkType,
    w.LastValue,
    w.PreviousValue,
    w.LastLoadedAtUtc,
    w.IsLocked,
    w.LookbackMinutes
FROM etl.Watermark AS w
WHERE w.IsLocked = 1
   OR w.LastLoadedAtUtc IS NULL
   OR w.LastLoadedAtUtc < DATEADD(DAY, -2, SYSUTCDATETIME())
ORDER BY w.IsLocked DESC, w.LastLoadedAtUtc;
GO

/* 6. Error and reject volume by object over the last week. */
SELECT
    r.ObjectName,
    r.RejectStage,
    r.RejectReasonCode,
    COUNT(*)                                                AS RejectedRows,
    SUM(CASE WHEN r.IsReprocessed = 1 THEN 1 ELSE 0 END)    AS Reprocessed,
    MAX(r.LoggedAtUtc)                                      AS LastRejectAtUtc
FROM etl.RejectedRecord AS r
WHERE r.LoggedAtUtc >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY r.ObjectName, r.RejectStage, r.RejectReasonCode
ORDER BY RejectedRows DESC;
GO

/* 7. Configuration drift between the two control copies. Run in both databases
      and diff the two result sets; the keys should agree apart from the
      database-specific ones. */
SELECT
    c.ConfigurationKey,
    c.EnvironmentCode,
    CASE WHEN c.IsSensitive = 1 THEN N'(sensitive - not shown)' ELSE c.ConfigurationValue END AS ConfigurationValue,
    c.ValueDataType,
    c.ModifiedAtUtc
FROM etl.Configuration AS c
ORDER BY c.ConfigurationKey, c.EnvironmentCode;
GO
