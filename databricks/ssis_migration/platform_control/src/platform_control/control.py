"""Control framework: the Delta equivalent of etl.usp_StartBatch / usp_EndBatch / usp_StartBatchStep /
usp_LogPackageStart / usp_LogPackageEnd / usp_LogError / usp_LogRowCount / usp_LogRejectedRecord /
usp_GetWatermark / usp_SetWatermark / usp_GetConfiguration.

One ``ControlFramework`` instance wraps a SparkSession and a ``PlatformConfig``; every task in the
bundle goes through it so the control rows are written consistently.
"""

from __future__ import annotations

import getpass
import json
import platform
import random
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from platform_control.config import PlatformConfig
from platform_control.rules import deriveBatchStatus
from platform_control.tables import CONTROL_TABLES, ID_COLUMNS


def utcNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def sqlLiteral(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


class ControlFramework:
    def __init__(self, spark: SparkSession, cfg: PlatformConfig):
        self.spark = spark
        self.cfg = cfg

    # ------------------------------------------------------------------ primitives
    def t(self, name: str) -> str:
        return self.cfg.table(name)

    def schemaDdl(self, name: str) -> str:
        return ", ".join(f"{c} {ty}" for c, ty in CONTROL_TABLES[name][0])

    def nextId(self, name: str) -> int:
        """Identity substitute: time-ordered BIGINT (unix micros << 10 | random) so concurrent job
        tasks never collide the way MAX(id)+1 did; ORDER BY id still means chronological."""
        ID_COLUMNS[name]
        return (int(time.time() * 1_000_000) << 10) | random.getrandbits(10)

    def insertRows(self, name: str, rows: list[dict]) -> list[dict]:
        """Insert dict rows into a control table, allocating identity values when absent."""
        if not rows:
            return []
        columns = [c for c, _t in CONTROL_TABLES[name][0]]
        idColumn = ID_COLUMNS.get(name)
        nextId = self.nextId(name) if idColumn else None
        prepared = []
        for row in rows:
            record = {c: row.get(c) for c in columns}
            if idColumn and record.get(idColumn) is None:
                record[idColumn] = nextId
                nextId += 1
            prepared.append(record)
        df = self.spark.createDataFrame([tuple(r[c] for c in columns) for r in prepared], schema=self.schemaDdl(name))
        df.write.format("delta").mode("append").saveAsTable(self.t(name))
        return prepared

    def update(self, name: str, assignments: dict, where: str) -> None:
        setSql = ", ".join(f"{k} = {sqlLiteral(v) if not (isinstance(v, str) and v.startswith('SQL:')) else v[4:]}" for k, v in assignments.items())
        self.spark.sql(f"UPDATE {self.t(name)} SET {setSql} WHERE {where}")

    def scalar(self, sql: str, default=None):
        row = self.spark.sql(sql).first()
        if row is None:
            return default
        value = row[0]
        return default if value is None else value

    def table(self, name: str) -> DataFrame:
        return self.spark.table(self.t(name))

    def count(self, name: str, where: str = "1=1") -> int:
        return int(self.scalar(f"SELECT COUNT(*) FROM {self.t(name)} WHERE {where}", 0))

    # ------------------------------------------------------------------ batches
    def startBatch(
        self,
        batchName: str,
        batchType: str,
        businessDate: date | None = None,
        restartFromStep: str | None = None,
        notes: str | None = None,
        jobRunId: str | None = None,
        allowAdoptRunning: bool = False,
        initiatedBy: str | None = None,
    ) -> int:
        """etl.usp_StartBatch: one Running batch per name/date; adopt (recovery) or raise."""
        businessDate = businessDate or utcNow().date()
        running = self.spark.sql(
            f"SELECT batch_id FROM {self.t('etl_batch')} WHERE batch_name = {sqlLiteral(batchName)} "
            f"AND business_date = {sqlLiteral(businessDate)} AND status = 'Running' ORDER BY batch_id DESC LIMIT 1"
        ).first()
        if running is not None:
            if allowAdoptRunning:
                self.update(
                    "etl_batch",
                    {"restart_from_step": restartFromStep, "notes": notes, "job_run_id": jobRunId},
                    f"batch_id = {running['batch_id']}",
                )
                return int(running["batch_id"])
            raise RuntimeError(
                f"Batch {batchName} for {businessDate} is already Running (BatchId {running['batch_id']}). "
                "Complete or cancel it, or rerun with allowAdoptRunning=True."
            )
        rows = self.insertRows(
            "etl_batch",
            [
                {
                    "batch_name": batchName,
                    "batch_type": batchType,
                    "business_date": businessDate,
                    "environment_code": self.cfg.environmentCode,
                    "started_at_utc": utcNow(),
                    "status": "Running",
                    "restart_from_step": restartFromStep or None,
                    "initiated_by": initiatedBy or getpass.getuser(),
                    "notes": notes,
                    "job_run_id": jobRunId,
                }
            ],
        )
        return int(rows[0]["batch_id"])

    def endBatch(self, batchId: int, forceStatus: str | None = None) -> dict:
        """etl.usp_EndBatch: derive Failed / SucceededWithWarnings / Succeeded and close running executions."""
        counts = self.spark.sql(
            f"SELECT SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed, "
            f"SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) AS running "
            f"FROM {self.t('etl_package_execution')} WHERE batch_id = {batchId}"
        ).first()
        warningCount = self.count("etl_error_log", f"batch_id = {batchId} AND error_severity = 'Warning'")
        failedPackages = int(counts["failed"] or 0)
        runningPackages = int(counts["running"] or 0)
        status = deriveBatchStatus(failedPackages, runningPackages, warningCount, forceStatus)
        now = utcNow()
        self.update("etl_batch", {"status": status, "completed_at_utc": now}, f"batch_id = {batchId}")
        self.update(
            "etl_package_execution",
            {"status": "Failed", "completed_at_utc": now, "status_detail": "Closed by EndBatch while still Running"},
            f"batch_id = {batchId} AND status = 'Running'",
        )
        self.update(
            "etl_batch_step",
            {"status": "Failed", "completed_at_utc": now},
            f"batch_id = {batchId} AND status = 'Running'",
        )
        return {"batchStatus": status, "failedPackageCount": failedPackages, "warningCount": warningCount}

    def runningBatchCount(self, batchType: str) -> int:
        return self.count("etl_batch", f"batch_type = {sqlLiteral(batchType)} AND status = 'Running'")

    # ------------------------------------------------------------------ steps
    def startBatchStep(
        self,
        batchId: int,
        stepName: str,
        stepSequence: int,
        stepGroup: str,
        packageName: str | None = None,
        criticality: str | None = None,
    ) -> int:
        """etl.usp_StartBatchStep: a re-run of the same step name bumps AttemptNumber."""
        previous = self.scalar(
            f"SELECT MAX(attempt_number) FROM {self.t('etl_batch_step')} "
            f"WHERE batch_id = {batchId} AND step_name = {sqlLiteral(stepName)}",
            0,
        )
        rows = self.insertRows(
            "etl_batch_step",
            [
                {
                    "batch_id": batchId,
                    "step_name": stepName,
                    "step_sequence": stepSequence,
                    "step_group": stepGroup,
                    "started_at_utc": utcNow(),
                    "status": "Running",
                    "attempt_number": int(previous or 0) + 1,
                    "package_name": packageName,
                    "is_retryable": None,
                    "criticality": criticality,
                }
            ],
        )
        return int(rows[0]["batch_step_id"])

    def endBatchStep(self, batchStepId: int, status: str, errorMessage: str | None = None, isRetryable: bool | None = None) -> None:
        assignments = {"status": status, "completed_at_utc": utcNow()}
        if errorMessage is not None:
            assignments["error_message"] = errorMessage[:4000]
        if isRetryable is not None:
            assignments["is_retryable"] = isRetryable
        self.update("etl_batch_step", assignments, f"batch_step_id = {batchStepId}")

    # ------------------------------------------------------------------ package executions
    def startPackageExecution(
        self,
        batchId: int | None,
        batchStepId: int | None,
        packageName: str,
        projectName: str | None = None,
        jobName: str | None = None,
        jobRunId: str | None = None,
        attemptNumber: int = 1,
        watermarkFrom: str | None = None,
        watermarkTo: str | None = None,
    ) -> int:
        rows = self.insertRows(
            "etl_package_execution",
            [
                {
                    "batch_id": batchId,
                    "batch_step_id": batchStepId,
                    "package_name": packageName,
                    "project_name": projectName,
                    "machine_name": platform.node(),
                    "executed_by": getpass.getuser(),
                    "started_at_utc": utcNow(),
                    "status": "Running",
                    "attempt_number": attemptNumber,
                    "job_name": jobName,
                    "job_run_id": jobRunId,
                    "watermark_from": watermarkFrom,
                    "watermark_to": watermarkTo,
                }
            ],
        )
        return int(rows[0]["package_execution_id"])

    def endPackageExecution(
        self,
        packageExecutionId: int,
        status: str,
        rowsRead: int | None = None,
        rowsInserted: int | None = None,
        rowsUpdated: int | None = None,
        rowsDeleted: int | None = None,
        rowsRejected: int | None = None,
        statusDetail: str | None = None,
    ) -> None:
        now = utcNow()
        self.update(
            "etl_package_execution",
            {
                "status": status,
                "completed_at_utc": now,
                "duration_seconds": "SQL:CAST(unix_timestamp(" + sqlLiteral(now) + ") - unix_timestamp(started_at_utc) AS BIGINT)",
                "rows_read": rowsRead,
                "rows_inserted": rowsInserted,
                "rows_updated": rowsUpdated,
                "rows_deleted": rowsDeleted,
                "rows_rejected": rowsRejected,
                "status_detail": statusDetail[:2000] if statusDetail else None,
            },
            f"package_execution_id = {packageExecutionId}",
        )

    # ------------------------------------------------------------------ watermarks
    def getWatermark(self, sourceSystemCode: str, objectName: str) -> dict | None:
        row = self.spark.sql(
            f"SELECT * FROM {self.t('etl_watermark')} WHERE source_system_code = {sqlLiteral(sourceSystemCode)} "
            f"AND object_name = {sqlLiteral(objectName)}"
        ).first()
        return None if row is None else row.asDict()

    def setWatermark(
        self,
        sourceSystemCode: str,
        objectName: str,
        newValue: str,
        watermarkType: str = "Timestamp",
        packageExecutionId: int | None = None,
        lookbackMinutes: int = 0,
    ) -> None:
        """etl.usp_SetWatermark: PreviousValue <- LastValue, LastValue <- new; locked marks are left alone."""
        existing = self.getWatermark(sourceSystemCode, objectName)
        if existing is None:
            self.insertRows(
                "etl_watermark",
                [
                    {
                        "source_system_code": sourceSystemCode,
                        "object_name": objectName,
                        "watermark_type": watermarkType,
                        "last_value": str(newValue),
                        "previous_value": None,
                        "last_loaded_at_utc": utcNow(),
                        "last_package_execution_id": packageExecutionId,
                        "lookback_minutes": lookbackMinutes,
                        "is_locked": False,
                    }
                ],
            )
            return
        if existing.get("is_locked"):
            return
        self.update(
            "etl_watermark",
            {
                "previous_value": existing.get("last_value"),
                "last_value": str(newValue),
                "last_loaded_at_utc": utcNow(),
                "last_package_execution_id": packageExecutionId,
            },
            f"watermark_id = {existing['watermark_id']}",
        )

    # ------------------------------------------------------------------ logging
    def logError(
        self,
        batchId: int | None,
        description: str,
        severity: str = "Error",
        errorCode: int | None = None,
        packageExecutionId: int | None = None,
        sourceName: str | None = None,
        sourceComponent: str | None = None,
        procedureName: str | None = None,
    ) -> int:
        rows = self.insertRows(
            "etl_error_log",
            [
                {
                    "package_execution_id": packageExecutionId,
                    "batch_id": batchId,
                    "error_severity": severity,
                    "error_code": errorCode,
                    "source_name": sourceName,
                    "source_component": sourceComponent,
                    "procedure_name": procedureName,
                    "error_description": (description or "")[:8000],
                    "logged_at_utc": utcNow(),
                }
            ],
        )
        return int(rows[0]["error_log_id"])

    def logRowCount(
        self,
        packageExecutionId: int | None,
        batchId: int | None,
        objectName: str,
        countStage: str = "Target",
        sourceRowCount: int | None = None,
        targetRowCount: int | None = None,
        insertRowCount: int | None = None,
        updateRowCount: int | None = None,
        deleteRowCount: int | None = None,
        rejectRowCount: int | None = None,
    ) -> None:
        variance = (sourceRowCount or 0) - (targetRowCount or 0) - (rejectRowCount or 0)
        self.insertRows(
            "etl_row_count_audit",
            [
                {
                    "package_execution_id": packageExecutionId,
                    "batch_id": batchId,
                    "object_name": objectName,
                    "count_stage": countStage,
                    "source_row_count": sourceRowCount,
                    "target_row_count": targetRowCount,
                    "insert_row_count": insertRowCount,
                    "update_row_count": updateRowCount,
                    "delete_row_count": deleteRowCount,
                    "reject_row_count": rejectRowCount,
                    "variance_row_count": variance,
                    "recorded_at_utc": utcNow(),
                }
            ],
        )

    def logRejectedRecords(self, records: list[dict]) -> int:
        now = utcNow()
        rows = []
        for r in records:
            rows.append(
                {
                    "package_execution_id": r.get("packageExecutionId"),
                    "batch_id": r.get("batchId"),
                    "source_system_code": r.get("sourceSystemCode"),
                    "object_name": r["objectName"],
                    "business_key": r.get("businessKey"),
                    "reject_reason_code": r.get("rejectReasonCode"),
                    "reject_reason": r.get("rejectReason"),
                    "reject_stage": r.get("rejectStage"),
                    "ssis_error_code": r.get("ssisErrorCode"),
                    "ssis_error_column": r.get("ssisErrorColumn"),
                    "record_payload": r.get("recordPayload") if isinstance(r.get("recordPayload"), str) else json.dumps(r.get("recordPayload"), default=str),
                    "is_reprocessed": False,
                    "logged_at_utc": r.get("loggedAtUtc", now),
                }
            )
        self.insertRows("etl_rejected_record", rows)
        return len(rows)

    def raiseNotification(
        self,
        batchId: int | None,
        notificationTypeCode: str,
        severity: str,
        subject: str,
        body: str,
        objectName: str | None = None,
        oncePerBatchAndType: bool = False,
    ) -> bool:
        if oncePerBatchAndType and batchId is not None:
            existing = self.count(
                "etl_operator_notification",
                f"batch_id = {batchId} AND notification_type_code = {sqlLiteral(notificationTypeCode)}",
            )
            if existing > 0:
                return False
        self.insertRows(
            "etl_operator_notification",
            [
                {
                    "batch_id": batchId,
                    "notification_type_code": notificationTypeCode,
                    "severity": severity,
                    "subject": subject[:400],
                    "body": body,
                    "object_name": objectName,
                    "raised_at_utc": utcNow(),
                    "is_acknowledged": False,
                }
            ],
        )
        return True

    # ------------------------------------------------------------------ configuration
    def getConfiguration(self, key: str, default: str | None = None, environmentCode: str | None = None) -> str | None:
        env = environmentCode or self.cfg.environmentCode
        row = self.spark.sql(
            f"SELECT configuration_value FROM {self.t('etl_configuration')} "
            f"WHERE configuration_key = {sqlLiteral(key)} AND environment_code IN ({sqlLiteral(env)}, 'ALL') "
            "ORDER BY CASE WHEN environment_code = 'ALL' THEN 1 ELSE 0 END LIMIT 1"
        ).first()
        return default if row is None else row["configuration_value"]

    # ------------------------------------------------------------------ orchestration variables
    def setVariable(self, jobRunId: str, name: str, value) -> None:
        self.spark.sql(
            f"MERGE INTO {self.t('etl_orchestration_variable')} AS target "
            f"USING (SELECT {sqlLiteral(jobRunId)} AS job_run_id, {sqlLiteral(name)} AS variable_name, "
            f"{sqlLiteral(json.dumps(value, default=str))} AS variable_value, {sqlLiteral(utcNow())} AS updated_at_utc) AS source "
            "ON target.job_run_id = source.job_run_id AND target.variable_name = source.variable_name "
            "WHEN MATCHED THEN UPDATE SET variable_value = source.variable_value, updated_at_utc = source.updated_at_utc "
            "WHEN NOT MATCHED THEN INSERT *"
        )

    def getVariables(self, jobRunId: str) -> dict:
        rows = self.spark.sql(
            f"SELECT variable_name, variable_value FROM {self.t('etl_orchestration_variable')} WHERE job_run_id = {sqlLiteral(jobRunId)}"
        ).collect()
        return {r["variable_name"]: json.loads(r["variable_value"]) for r in rows}
