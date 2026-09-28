"""Operational views (``sqlserver/control/views/etl.OperationalViews.sql``) as ``etl.v_*``.

``{etl}`` is replaced by ``<catalog>.etl`` at creation time.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .naming import ETL, table

VIEWS: Tuple[Tuple[str, str], ...] = (
    ("row_count_log",
     "SELECT * FROM {etl}.row_count_audit"),
    ("v_batch_status", """
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
FROM {etl}.batch AS b
LEFT JOIN {etl}.package_execution AS pe
       ON pe.BatchId = b.BatchId
GROUP BY b.BatchId, b.BatchName, b.BatchType, b.BusinessDate,
         b.EnvironmentCode, b.Status, b.StartedAtUtc, b.CompletedAtUtc"""),
    ("v_package_execution_history", """
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
FROM {etl}.package_execution AS pe
LEFT JOIN {etl}.batch AS b
       ON b.BatchId = pe.BatchId"""),
    ("v_slow_packages", """
SELECT h.PackageName,
       h.BatchId,
       h.BusinessDate,
       h.DurationSeconds,
       h.TrailingAvgDurationSeconds,
       CASE WHEN coalesce(h.TrailingAvgDurationSeconds, 0) = 0 THEN NULL
            ELSE CAST(h.DurationSeconds AS DOUBLE) / h.TrailingAvgDurationSeconds
       END AS DurationRatio
FROM {etl}.v_package_execution_history AS h
WHERE h.DurationSeconds IS NOT NULL
  AND h.TrailingAvgDurationSeconds IS NOT NULL
  AND h.DurationSeconds > h.TrailingAvgDurationSeconds * 1.5"""),
    ("v_row_count_reconciliation", """
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
FROM {etl}.row_count_audit AS rca
INNER JOIN {etl}.package_execution AS pe
        ON pe.PackageExecutionId = rca.PackageExecutionId
LEFT JOIN {etl}.batch AS b
        ON b.BatchId = pe.BatchId
LEFT JOIN {etl}.reconciliation_exemption AS rx
        ON rx.ObjectName = rca.ObjectName
GROUP BY pe.BatchId, b.BusinessDate, rca.ObjectName"""),
    ("v_reject_summary", """
SELECT rr.BatchId,
       rr.ObjectName,
       rr.RejectStage,
       rr.RejectReasonCode,
       count(*)                                              AS RejectCount,
       sum(CASE WHEN rr.IsReprocessed THEN 1 ELSE 0 END)     AS ReprocessedCount,
       min(rr.LoggedAtUtc)                                   AS FirstSeenUtc,
       max(rr.LoggedAtUtc)                                   AS LastSeenUtc
FROM {etl}.rejected_record AS rr
GROUP BY rr.BatchId, rr.ObjectName, rr.RejectStage, rr.RejectReasonCode"""),
    ("v_watermark_status", """
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
FROM {etl}.watermark AS w
LEFT JOIN {etl}.source_system AS ss
       ON ss.SourceSystemCode = w.SourceSystemCode"""),
    ("v_recent_errors", """
SELECT e.ErrorLogId,
       e.BatchId,
       e.PackageExecutionId,
       pe.PackageName,
       e.ErrorSeverity,
       e.SourceName,
       e.ProcedureName,
       e.ErrorDescription,
       e.LoggedAtUtc
FROM {etl}.error_log AS e
LEFT JOIN {etl}.package_execution AS pe
       ON pe.PackageExecutionId = e.PackageExecutionId
ORDER BY e.LoggedAtUtc DESC
LIMIT 1000"""),
)

VIEW_NAMES: List[str] = [name for name, _ in VIEWS]

LEGACY_VIEW_MAP: Dict[str, str] = {
    "etl.vw_BatchStatus": "v_batch_status",
    "etl.vw_PackageExecutionHistory": "v_package_execution_history",
    "etl.vw_SlowPackages": "v_slow_packages",
    "etl.vw_RowCountReconciliation": "v_row_count_reconciliation",
    "etl.vw_RejectSummary": "v_reject_summary",
    "etl.vw_WatermarkStatus": "v_watermark_status",
    "etl.vw_RecentErrors": "v_recent_errors",
}


def renderView(name: str, body: str, catalog: str = "${catalog}") -> str:
    etl = f"{catalog}.{ETL}"
    return f"CREATE OR REPLACE VIEW {table(catalog, ETL, name)} AS\n{body.format(etl=etl).strip()};"


def renderAll(catalog: str = "${catalog}") -> str:
    return "\n\n".join(renderView(n, b, catalog) for n, b in VIEWS)


def createViews(spark: Any, catalog: str) -> None:
    for name, body in VIEWS:
        spark.sql(renderView(name, body, catalog).rstrip(";"))
