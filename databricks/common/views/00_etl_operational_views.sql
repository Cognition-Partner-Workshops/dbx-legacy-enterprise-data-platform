-- Operational views for the WWI ETL control framework (etl.v_*).
-- GENERATED from dbx_etl_common.views by databricks/common/tools/render_sql.py - do not edit by hand.
-- Legacy source: sqlserver/control/views/etl.OperationalViews.sql.

CREATE OR REPLACE VIEW ${catalog}.etl.row_count_log AS
SELECT * FROM ${catalog}.etl.row_count_audit;

CREATE OR REPLACE VIEW ${catalog}.etl.v_batch_status AS
SELECT b.BatchId,
       b.BatchName,
       b.BatchType,
       b.BusinessDate,
       b.EnvironmentCode,
       b.Status,
       b.StartedAtUtc,
       b.CompletedAtUtc,
       timestampdiff(MINUTE, b.StartedAtUtc, coalesce(b.CompletedAtUtc, current_timestamp())) AS ElapsedMinutes,
       count(pe.PackageExecutionId) AS PackageCount,
       sum(CASE WHEN pe.Status = 'Failed' THEN 1 ELSE 0 END) AS FailedPackageCount,
       sum(CASE WHEN pe.Status = 'Running' THEN 1 ELSE 0 END) AS RunningPackageCount,
       sum(coalesce(pe.RowsInserted, 0)) AS RowsInserted,
       sum(coalesce(pe.RowsRejected, 0)) AS RowsRejected
FROM ${catalog}.etl.batch AS b
LEFT JOIN ${catalog}.etl.package_execution AS pe
       ON pe.BatchId = b.BatchId
GROUP BY b.BatchId, b.BatchName, b.BatchType, b.BusinessDate,
         b.EnvironmentCode, b.Status, b.StartedAtUtc, b.CompletedAtUtc;

CREATE OR REPLACE VIEW ${catalog}.etl.v_package_execution_history AS
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
       avg(CAST(pe.DurationSeconds AS DOUBLE))
           OVER (PARTITION BY pe.PackageName ORDER BY pe.StartedAtUtc
                 ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)                          AS TrailingAvgDurationSeconds,
       row_number() OVER (PARTITION BY pe.PackageName ORDER BY pe.StartedAtUtc DESC) AS RecencyRank
FROM ${catalog}.etl.package_execution AS pe
LEFT JOIN ${catalog}.etl.batch AS b
       ON b.BatchId = pe.BatchId;

CREATE OR REPLACE VIEW ${catalog}.etl.v_slow_packages AS
SELECT h.PackageName,
       h.BatchId,
       h.BusinessDate,
       h.DurationSeconds,
       h.TrailingAvgDurationSeconds,
       CASE WHEN coalesce(h.TrailingAvgDurationSeconds, 0) = 0 THEN NULL
            ELSE CAST(h.DurationSeconds AS DOUBLE) / h.TrailingAvgDurationSeconds
       END AS DurationRatio
FROM ${catalog}.etl.v_package_execution_history AS h
WHERE h.DurationSeconds IS NOT NULL
  AND h.TrailingAvgDurationSeconds IS NOT NULL
  AND h.DurationSeconds > h.TrailingAvgDurationSeconds * 1.5;

CREATE OR REPLACE VIEW ${catalog}.etl.v_row_count_reconciliation AS
SELECT pe.BatchId,
       b.BusinessDate,
       rca.ObjectName,
       sum(coalesce(rca.SourceRowCount, 0)) AS SourceRowCount,
       sum(coalesce(rca.TargetRowCount, 0)) AS TargetRowCount,
       sum(coalesce(rca.InsertRowCount, 0)) AS InsertRowCount,
       sum(coalesce(rca.UpdateRowCount, 0)) AS UpdateRowCount,
       sum(coalesce(rca.RejectRowCount, 0)) AS RejectRowCount,
       sum(coalesce(rca.SourceRowCount, 0))
         - sum(coalesce(rca.TargetRowCount, 0))
         - sum(coalesce(rca.RejectRowCount, 0)) AS VarianceRowCount,
       CASE WHEN max(rx.ObjectName) IS NOT NULL THEN 1 ELSE 0 END AS IsExempt
FROM ${catalog}.etl.row_count_audit AS rca
INNER JOIN ${catalog}.etl.package_execution AS pe
        ON pe.PackageExecutionId = rca.PackageExecutionId
LEFT JOIN ${catalog}.etl.batch AS b
        ON b.BatchId = pe.BatchId
LEFT JOIN ${catalog}.etl.reconciliation_exemption AS rx
        ON rx.ObjectName = rca.ObjectName
GROUP BY pe.BatchId, b.BusinessDate, rca.ObjectName;

CREATE OR REPLACE VIEW ${catalog}.etl.v_reject_summary AS
SELECT rr.BatchId,
       rr.ObjectName,
       rr.RejectStage,
       rr.RejectReasonCode,
       count(*)                                              AS RejectCount,
       sum(CASE WHEN rr.IsReprocessed THEN 1 ELSE 0 END)     AS ReprocessedCount,
       min(rr.LoggedAtUtc)                                   AS FirstSeenUtc,
       max(rr.LoggedAtUtc)                                   AS LastSeenUtc
FROM ${catalog}.etl.rejected_record AS rr
GROUP BY rr.BatchId, rr.ObjectName, rr.RejectStage, rr.RejectReasonCode;

CREATE OR REPLACE VIEW ${catalog}.etl.v_watermark_status AS
SELECT w.SourceSystemCode,
       ss.SourceSystemName,
       ss.Platform,
       w.ObjectName,
       w.WatermarkType,
       w.LastValue,
       w.PreviousValue,
       w.LastLoadedAtUtc,
       w.IsLocked,
       timestampdiff(HOUR, w.LastLoadedAtUtc, current_timestamp()) AS HoursSinceLastLoad
FROM ${catalog}.etl.watermark AS w
LEFT JOIN ${catalog}.etl.source_system AS ss
       ON ss.SourceSystemCode = w.SourceSystemCode;

CREATE OR REPLACE VIEW ${catalog}.etl.v_recent_errors AS
SELECT e.ErrorLogId,
       e.BatchId,
       e.PackageExecutionId,
       pe.PackageName,
       e.ErrorSeverity,
       e.SourceName,
       e.ProcedureName,
       e.ErrorDescription,
       e.LoggedAtUtc
FROM ${catalog}.etl.error_log AS e
LEFT JOIN ${catalog}.etl.package_execution AS pe
       ON pe.PackageExecutionId = e.PackageExecutionId
ORDER BY e.LoggedAtUtc DESC
LIMIT 1000;
