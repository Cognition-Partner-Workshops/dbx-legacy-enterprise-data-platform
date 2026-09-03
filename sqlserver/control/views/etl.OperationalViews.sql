/*
    Operational views over the ETL control framework.

    Deploy target : WideWorldImportersStaging
    Depends on    : 02_tables_control_framework.sql

    These are what the operators actually open at 06:00 when the nightly run is
    late: batch status, the slowest packages, the objects that did not balance,
    and the reject profile by reason code.
*/

IF OBJECT_ID(N'etl.vw_BatchStatus', N'V') IS NOT NULL DROP VIEW etl.vw_BatchStatus;
GO
CREATE VIEW etl.vw_BatchStatus
AS
SELECT b.BatchId,
       b.BatchName,
       b.BatchType,
       b.BusinessDate,
       b.EnvironmentCode,
       b.Status,
       b.StartedAtUtc,
       b.CompletedAtUtc,
       DATEDIFF(MINUTE, b.StartedAtUtc, ISNULL(b.CompletedAtUtc, SYSUTCDATETIME())) AS ElapsedMinutes,
       COUNT(pe.PackageExecutionId)                                                  AS PackageCount,
       SUM(CASE WHEN pe.Status = N'Failed' THEN 1 ELSE 0 END)                        AS FailedPackageCount,
       SUM(CASE WHEN pe.Status = N'Running' THEN 1 ELSE 0 END)                       AS RunningPackageCount,
       SUM(ISNULL(pe.RowsInserted, 0))                                               AS RowsInserted,
       SUM(ISNULL(pe.RowsRejected, 0))                                               AS RowsRejected
FROM etl.Batch AS b
LEFT JOIN etl.PackageExecution AS pe
       ON pe.BatchId = b.BatchId
GROUP BY b.BatchId, b.BatchName, b.BatchType, b.BusinessDate, b.EnvironmentCode,
         b.Status, b.StartedAtUtc, b.CompletedAtUtc;
GO

IF OBJECT_ID(N'etl.vw_PackageExecutionHistory', N'V') IS NOT NULL DROP VIEW etl.vw_PackageExecutionHistory;
GO
CREATE VIEW etl.vw_PackageExecutionHistory
AS
SELECT pe.PackageExecutionId,
       pe.BatchId,
       b.BusinessDate,
       pe.PackageName,
       pe.ProjectName,
       pe.Status,
       pe.AttemptNumber,
       pe.StartedAtUtc,
       pe.CompletedAtUtc,
       pe.DurationSeconds,
       pe.RowsRead,
       pe.RowsInserted,
       pe.RowsUpdated,
       pe.RowsRejected,
       AVG(CAST(pe.DurationSeconds AS FLOAT))
           OVER (PARTITION BY pe.PackageName ORDER BY pe.StartedAtUtc
                 ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)                       AS TrailingAvgDurationSeconds,
       ROW_NUMBER() OVER (PARTITION BY pe.PackageName ORDER BY pe.StartedAtUtc DESC) AS RecencyRank
FROM etl.PackageExecution AS pe
LEFT JOIN etl.Batch AS b
       ON b.BatchId = pe.BatchId;
GO

IF OBJECT_ID(N'etl.vw_SlowPackages', N'V') IS NOT NULL DROP VIEW etl.vw_SlowPackages;
GO
CREATE VIEW etl.vw_SlowPackages
AS
SELECT h.PackageName,
       h.BatchId,
       h.BusinessDate,
       h.DurationSeconds,
       h.TrailingAvgDurationSeconds,
       CASE WHEN ISNULL(h.TrailingAvgDurationSeconds, 0) = 0 THEN NULL
            ELSE CAST(h.DurationSeconds AS FLOAT) / h.TrailingAvgDurationSeconds
       END AS DurationRatio
FROM etl.vw_PackageExecutionHistory AS h
WHERE h.DurationSeconds IS NOT NULL
  AND h.TrailingAvgDurationSeconds IS NOT NULL
  AND h.DurationSeconds > h.TrailingAvgDurationSeconds * 1.5;
GO

IF OBJECT_ID(N'etl.vw_RowCountReconciliation', N'V') IS NOT NULL DROP VIEW etl.vw_RowCountReconciliation;
GO
CREATE VIEW etl.vw_RowCountReconciliation
AS
SELECT pe.BatchId,
       b.BusinessDate,
       rca.ObjectName,
       SUM(ISNULL(rca.SourceRowCount, 0)) AS SourceRowCount,
       SUM(ISNULL(rca.TargetRowCount, 0)) AS TargetRowCount,
       SUM(ISNULL(rca.InsertRowCount, 0)) AS InsertRowCount,
       SUM(ISNULL(rca.UpdateRowCount, 0)) AS UpdateRowCount,
       SUM(ISNULL(rca.RejectRowCount, 0)) AS RejectRowCount,
       SUM(ISNULL(rca.SourceRowCount, 0))
         - SUM(ISNULL(rca.TargetRowCount, 0))
         - SUM(ISNULL(rca.RejectRowCount, 0)) AS VarianceRowCount,
       CASE WHEN EXISTS (SELECT 1 FROM etl.ReconciliationExemption AS rx WHERE rx.ObjectName = rca.ObjectName)
            THEN 1 ELSE 0 END AS IsExempt
FROM etl.RowCountAudit AS rca
INNER JOIN etl.PackageExecution AS pe
        ON pe.PackageExecutionId = rca.PackageExecutionId
LEFT JOIN etl.Batch AS b
        ON b.BatchId = pe.BatchId
GROUP BY pe.BatchId, b.BusinessDate, rca.ObjectName;
GO

IF OBJECT_ID(N'etl.vw_RejectSummary', N'V') IS NOT NULL DROP VIEW etl.vw_RejectSummary;
GO
CREATE VIEW etl.vw_RejectSummary
AS
SELECT rr.BatchId,
       rr.ObjectName,
       rr.RejectStage,
       rr.RejectReasonCode,
       COUNT(*)                                        AS RejectCount,
       SUM(CASE WHEN rr.IsReprocessed = 1 THEN 1 ELSE 0 END) AS ReprocessedCount,
       MIN(rr.LoggedAtUtc)                             AS FirstSeenUtc,
       MAX(rr.LoggedAtUtc)                             AS LastSeenUtc
FROM etl.RejectedRecord AS rr
GROUP BY rr.BatchId, rr.ObjectName, rr.RejectStage, rr.RejectReasonCode;
GO

IF OBJECT_ID(N'etl.vw_WatermarkStatus', N'V') IS NOT NULL DROP VIEW etl.vw_WatermarkStatus;
GO
CREATE VIEW etl.vw_WatermarkStatus
AS
SELECT w.SourceSystemCode,
       ss.SourceSystemName,
       ss.Platform,
       w.ObjectName,
       w.WatermarkType,
       w.LastValue,
       w.PreviousValue,
       w.LastLoadedAtUtc,
       w.IsLocked,
       DATEDIFF(HOUR, w.LastLoadedAtUtc, SYSUTCDATETIME()) AS HoursSinceLastLoad
FROM etl.Watermark AS w
LEFT JOIN etl.SourceSystem AS ss
       ON ss.SourceSystemCode = w.SourceSystemCode;
GO

IF OBJECT_ID(N'etl.vw_RecentErrors', N'V') IS NOT NULL DROP VIEW etl.vw_RecentErrors;
GO
CREATE VIEW etl.vw_RecentErrors
AS
SELECT TOP (1000)
       e.ErrorLogId,
       e.BatchId,
       e.PackageExecutionId,
       pe.PackageName,
       e.ErrorSeverity,
       e.SourceName,
       e.ProcedureName,
       e.ErrorDescription,
       e.LoggedAtUtc
FROM etl.ErrorLog AS e
LEFT JOIN etl.PackageExecution AS pe
       ON pe.PackageExecutionId = e.PackageExecutionId
ORDER BY e.LoggedAtUtc DESC;
GO
