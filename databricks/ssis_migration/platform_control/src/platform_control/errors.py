"""ERR_* packages re-implemented on top of the Delta control framework.

ERR_Handle_PackageFailure, ERR_Retry_FailedSteps, ERR_Reconcile_RowCounts, ERR_Route_RejectedRows,
ERR_Quarantine_BadFiles and ERR_Notify_Operations. Each function mirrors the control flow of the
generated .dtsx (see ssis/15_error_handling/build_error_handling_packages.py).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from decimal import Decimal

from platform_control.control import ControlFramework, sqlLiteral, utcNow
from platform_control.files import FileOps
from platform_control.rules import (
    classifyBadFile,
    classifyFailure,
    computeBackoffSeconds,
    isEscalated,
    notificationSeverity,
    quarantineFolder,
    rejectFileName,
    retryDecision,
)
from platform_control.rules import (
    reconcileRowCounts as reconcileRowCountsRule,
)

# ------------------------------------------------------------------------------ ERR_Handle_PackageFailure


def handlePackageFailure(
    cf: ControlFramework,
    batchId: int,
    failedPackage: str,
    failureErrorCode: int | None,
    failureMessage: str,
    failBatchOnPermanent: bool = True,
) -> dict:
    cf.logError(
        batchId,
        failureMessage,
        severity="Error",
        errorCode=failureErrorCode,
        sourceName=failedPackage,
        sourceComponent="ERR_Handle_PackageFailure",
    )
    failureClass, retryable = classifyFailure(failureErrorCode, failureMessage)
    cf.update(
        "etl_batch_step",
        {"status": "Failed", "completed_at_utc": utcNow(), "is_retryable": retryable, "error_message": failureMessage[:4000]},
        f"batch_id = {batchId} AND package_name = {sqlLiteral(failedPackage)} "
        "AND (status = 'Running' OR (status = 'Failed' AND is_retryable IS NULL))",
    )
    cf.update(
        "etl_package_execution",
        {"status": "Failed", "completed_at_utc": utcNow(), "status_detail": failureMessage[:2000]},
        f"batch_id = {batchId} AND package_name = {sqlLiteral(failedPackage)} AND status = 'Running'",
    )
    batchOutcome = None
    if failureClass == "PERMANENT" and failBatchOnPermanent:
        batchOutcome = cf.endBatch(batchId, forceStatus="Failed")["batchStatus"]
    elif failureClass == "TRANSIENT":
        cf.update("etl_batch", {"status": "RetryPending"}, f"batch_id = {batchId} AND status = 'Running'")
        batchOutcome = "RetryPending"
    return {
        "failedPackage": failedPackage,
        "failureClass": failureClass,
        "retryableFlag": retryable,
        "batchOutcome": batchOutcome,
        "errorLogRows": cf.count("etl_error_log", f"batch_id = {batchId}"),
    }


# ------------------------------------------------------------------------------ ERR_Retry_FailedSteps


def retryFailedSteps(
    cf: ControlFramework,
    batchId: int,
    maxRetryAttempts: int = 3,
    backoffBaseSeconds: int = 30,
    rerunStep: Callable[[dict], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    maxBackoffSeconds: int = 600,
) -> dict:
    """Bounded retry driver. ``rerunStep(step) -> bool`` executes one rerun request when the caller
    (the orchestrator) knows how to dispatch the package; otherwise requests stay ``Requested``
    for the control framework, exactly like the legacy package."""
    steps = cf.t("etl_batch_step")
    retryableWhere = (
        f"batch_id = {batchId} AND status = 'Failed' AND COALESCE(is_retryable, false) "
        f"AND COALESCE(attempt_number, 1) < {maxRetryAttempts}"
    )
    attemptNumber = 1
    retryableStepCount = cf.count("etl_batch_step", retryableWhere)
    sweeps = 0
    while retryDecision(attemptNumber, maxRetryAttempts, retryableStepCount) == "sweep":
        sweeps += 1
        backoff = min(computeBackoffSeconds(backoffBaseSeconds, attemptNumber), maxBackoffSeconds)
        sleep(backoff)
        cf.update(
            "etl_batch_step",
            {"status": "Pending", "attempt_number": "SQL:COALESCE(attempt_number, 1) + 1", "completed_at_utc": None},
            retryableWhere,
        )
        pending = cf.spark.sql(
            f"SELECT batch_step_id, package_name, attempt_number, step_name FROM {steps} "
            f"WHERE batch_id = {batchId} AND status = 'Pending' AND COALESCE(is_retryable, false)"
        ).collect()
        requests = cf.insertRows(
            "etl_batch_step_rerun_request",
            [
                {
                    "batch_id": batchId,
                    "batch_step_id": p["batch_step_id"],
                    "package_name": p["package_name"],
                    "attempt_number": p["attempt_number"],
                    "requested_at_utc": utcNow(),
                    "request_status": "Requested",
                }
                for p in pending
            ],
        )
        if rerunStep is not None:
            for request in requests:
                cf.update("etl_batch_step_rerun_request", {"request_status": "Running"}, f"rerun_request_id = {request['rerun_request_id']}")
                cf.update("etl_batch_step", {"status": "Running", "started_at_utc": utcNow()}, f"batch_step_id = {request['batch_step_id']}")
                succeeded = False
                try:
                    succeeded = bool(rerunStep(request))
                except Exception as exc:  # noqa: BLE001 - the driver must keep sweeping
                    cf.logError(batchId, str(exc), errorCode=None, sourceName=request["package_name"], sourceComponent="ERR_Retry_FailedSteps")
                cf.update(
                    "etl_batch_step",
                    {"status": "Succeeded" if succeeded else "Failed", "completed_at_utc": utcNow()},
                    f"batch_step_id = {request['batch_step_id']}",
                )
                cf.update(
                    "etl_batch_step_rerun_request",
                    {"request_status": "Completed", "completed_at_utc": utcNow(), "result_status": "Succeeded" if succeeded else "Failed"},
                    f"rerun_request_id = {request['rerun_request_id']}",
                )
        retryableStepCount = cf.count(
            "etl_batch_step",
            f"batch_id = {batchId} AND status IN ('Failed', 'Pending') AND COALESCE(is_retryable, false) "
            f"AND COALESCE(attempt_number, 1) < {maxRetryAttempts}",
        )
        attemptNumber += 1
        cf.update(
            "etl_batch_step",
            {"attempt_number": attemptNumber},
            f"batch_id = {batchId} AND status IN ('Failed', 'Pending') AND COALESCE(is_retryable, false)",
        )
    cf.update(
        "etl_batch_step",
        {
            "status": "Failed",
            "is_retryable": False,
            "error_message": "SQL:CONCAT(COALESCE(error_message, ''), ' | retry limit reached')",
        },
        f"batch_id = {batchId} AND COALESCE(attempt_number, 1) >= {maxRetryAttempts} AND status IN ('Failed', 'Pending')",
    )
    cf.update(
        "etl_batch_step_rerun_request",
        {"request_status": "Abandoned"},
        f"batch_id = {batchId} AND request_status = 'Requested'" if rerunStep is not None else "1 = 0",
    )
    stillFailing = cf.count("etl_batch_step", f"batch_id = {batchId} AND status = 'Failed'")
    return {"sweeps": sweeps, "attemptNumber": attemptNumber, "stillFailingCount": stillFailing}


# ------------------------------------------------------------------------------ ERR_Reconcile_RowCounts


def reconcileRowCounts(cf: ControlFramework, batchId: int, defaultTolerancePercent: int = 0, raiseOnFailure: bool = True) -> dict:
    spark = cf.spark
    spark.sql(f"DELETE FROM {cf.t('work_row_count_reconciliation')} WHERE batch_id = {batchId}")
    reconciliationSet = spark.sql(
        f"""
        SELECT a.batch_id, a.object_name,
               MAX(CASE WHEN a.count_stage = 'Source'  THEN COALESCE(a.source_row_count, a.target_row_count) ELSE 0 END) AS source_row_count,
               MAX(CASE WHEN a.count_stage = 'Staging' THEN COALESCE(a.target_row_count, 0) ELSE 0 END) AS staging_row_count,
               MAX(CASE WHEN a.count_stage = 'Target'  THEN COALESCE(a.target_row_count, 0) ELSE 0 END) AS target_row_count,
               COALESCE(r.rejected_row_count, 0) AS rejected_row_count,
               COALESCE(t.tolerance_percent, {defaultTolerancePercent}) AS tolerance_percent,
               t.explanation_code
        FROM {cf.t('etl_row_count_audit')} AS a
        LEFT JOIN {cf.t('etl_row_count_tolerance')} AS t ON t.object_name = a.object_name
        LEFT JOIN (SELECT batch_id, object_name, COUNT(*) AS rejected_row_count FROM {cf.t('etl_rejected_record')} GROUP BY batch_id, object_name) AS r
               ON r.batch_id = a.batch_id AND r.object_name = a.object_name
        WHERE a.batch_id = {batchId}
        GROUP BY a.batch_id, a.object_name, r.rejected_row_count, t.tolerance_percent, t.explanation_code
        """
    ).collect()
    now = utcNow()
    workRows, resultRows, failed, tolerated = [], [], 0, 0
    for row in reconciliationSet:
        outcome = reconcileRowCountsRule(
            row["staging_row_count"], row["target_row_count"], row["rejected_row_count"], row["tolerance_percent"], row["explanation_code"]
        )
        workRows.append(
            {
                "batch_id": batchId,
                "object_name": row["object_name"],
                "source_row_count": row["source_row_count"],
                "staging_row_count": row["staging_row_count"],
                "target_row_count": row["target_row_count"],
                "rejected_row_count": row["rejected_row_count"],
                "tolerance_percent": Decimal(str(row["tolerance_percent"])),
                "explanation_code": row["explanation_code"],
                "expected_target_row_count": outcome.expectedTargetRowCount,
                "difference_row_count": outcome.differenceRowCount,
                "difference_percent": outcome.differencePercent,
                "reconciliation_status": outcome.reconciliationStatus,
                "evaluated_at_utc": now,
            }
        )
        resultRows.append(
            {
                "batch_id": batchId,
                "reconciliation_name": "ROW_COUNT",
                "object_name": row["object_name"],
                "source_amount": Decimal(outcome.expectedTargetRowCount),
                "target_amount": Decimal(row["target_row_count"]),
                "variance_amount": Decimal(outcome.differenceRowCount),
                "variance_status": outcome.reconciliationStatus,
                "explanation_code": row["explanation_code"],
                "evaluated_at_utc": now,
            }
        )
        failed += outcome.reconciliationStatus == "FAILED"
        tolerated += outcome.reconciliationStatus == "TOLERATED"
    if workRows:
        cf.insertRows("work_row_count_reconciliation", workRows)
        cf.insertRows("etl_reconciliation_result", resultRows)
    if failed > 0 and raiseOnFailure:
        failedObjects = [w["object_name"] for w in workRows if w["reconciliation_status"] == "FAILED"]
        message = f"Row count reconciliation failed for {failed} object(s): {', '.join(failedObjects)}"
        cf.logError(batchId, message, sourceName="ERR_Reconcile_RowCounts", procedureName="etl.usp_AssertRowCountReconciliation")
        raise RuntimeError(message)
    return {"objectsReconciled": len(workRows), "failedObjectCount": failed, "toleratedCount": tolerated}


# ------------------------------------------------------------------------------ ERR_Route_RejectedRows


def routeRejectedRows(
    cf: ControlFramework,
    batchId: int,
    fileOps: FileOps,
    rejectEscalationDays: int = 5,
    objectScope: str = "ALL",
) -> dict:
    scopeFilter = "" if objectScope == "ALL" else f" AND r.object_name = {sqlLiteral(objectScope)}"
    unrouted = cf.spark.sql(
        f"""
        SELECT r.rejected_record_id, r.batch_id, r.object_name, r.reject_stage, r.reject_reason_code,
               r.reject_reason, r.business_key, r.logged_at_utc,
               datediff(current_timestamp(), r.logged_at_utc) AS age_days
        FROM {cf.t('etl_rejected_record')} AS r
        WHERE NOT r.is_reprocessed {scopeFilter}
          AND NOT EXISTS (SELECT 1 FROM {cf.t('work_reject_routing_history')} AS h WHERE h.rejected_record_id = r.rejected_record_id)
        ORDER BY r.object_name, r.logged_at_utc
        """
    ).collect()
    now = utcNow()
    fileName = rejectFileName(batchId, now.date())
    historyRows, escalationRows, fileLines = [], [], []
    for r in unrouted:
        escalated = isEscalated(int(r["age_days"] or 0), rejectEscalationDays)
        line = f"{r['object_name']}|{r['reject_reason_code']}|{r['business_key']}"
        fileLines.append(line)
        historyRows.append(
            {
                "rejected_record_id": r["rejected_record_id"],
                "batch_id": r["batch_id"],
                "object_name": r["object_name"],
                "reject_stage": r["reject_stage"],
                "reject_reason_code": r["reject_reason_code"],
                "reject_reason_description": r["reject_reason"],
                "source_key": r["business_key"],
                "rejected_at_utc": r["logged_at_utc"],
                "age_days": int(r["age_days"] or 0),
                "is_escalated": escalated,
                "reject_file_line": line,
                "reject_file_name": fileName,
                "routed_at_utc": now,
            }
        )
        if escalated:
            escalationRows.append(
                {
                    "rejected_record_id": r["rejected_record_id"],
                    "batch_id": r["batch_id"],
                    "object_name": r["object_name"],
                    "reject_reason_code": r["reject_reason_code"],
                    "source_key": r["business_key"],
                    "age_days": int(r["age_days"] or 0),
                    "escalated_at_utc": now,
                }
            )
    rejectFilePath = None
    if historyRows:
        cf.insertRows("work_reject_routing_history", historyRows)
        rejectFilePath = fileOps.writeText(fileOps.join("errors", fileName), "\n".join(fileLines) + "\n")
    if escalationRows:
        cf.insertRows("work_reject_escalation", escalationRows)
        byObject: dict[str, int] = {}
        for e in escalationRows:
            byObject[e["object_name"]] = byObject.get(e["object_name"], 0) + 1
        for objectName, n in byObject.items():
            cf.raiseNotification(
                batchId,
                "REJECT_ESCALATION",
                "WARNING",
                f"Aged rejects for {objectName}",
                f"Rejects unresolved beyond the escalation window: {n}",
                objectName=objectName,
            )
    return {"routedRowCount": len(historyRows), "escalatedCount": len(escalationRows), "rejectFile": rejectFilePath}


# ------------------------------------------------------------------------------ ERR_Notify_Operations


def notifyOperations(cf: ControlFramework, batchId: int, notifyOnWarnings: bool = False, environmentCode: str | None = None) -> dict:
    env = environmentCode or cf.cfg.environmentCode
    assessment = cf.spark.sql(
        f"SELECT COUNT(*) AS failed_steps, SUM(CASE WHEN criticality = 'high' THEN 1 ELSE 0 END) AS high_failed, "
        f"concat_ws(', ', collect_list(step_name)) AS step_names FROM {cf.t('etl_batch_step')} "
        f"WHERE batch_id = {batchId} AND status = 'Failed'"
    ).first()
    failedSteps = int(assessment["failed_steps"] or 0)
    severity = notificationSeverity(failedSteps, int(assessment["high_failed"] or 0))
    raised = 0
    if failedSteps > 0:
        batch = cf.spark.sql(f"SELECT batch_type FROM {cf.t('etl_batch')} WHERE batch_id = {batchId}").first()
        lastError = cf.scalar(
            f"SELECT error_description FROM {cf.t('etl_error_log')} WHERE batch_id = {batchId} ORDER BY logged_at_utc DESC LIMIT 1",
            "none recorded",
        )
        raised += cf.raiseNotification(
            batchId,
            "BATCH_FAILURE",
            severity,
            f"[{env}] Batch {batchId} ({batch['batch_type'] if batch else 'unknown'}) failed",
            f"Failed steps: {assessment['step_names']}. Last error: {lastError}",
            oncePerBatchAndType=True,
        )
    elif notifyOnWarnings:
        rejected = cf.count("etl_rejected_record", f"batch_id = {batchId} AND NOT is_reprocessed")
        if rejected > 0:
            raised += cf.raiseNotification(
                batchId, "BATCH_WARNING", "WARNING", "Batch completed with rejected rows", f"Rejected rows in this batch: {rejected}"
            )
    notificationCount = cf.count("etl_operator_notification", f"batch_id = {batchId} AND NOT is_acknowledged")
    return {"failedStepCount": failedSteps, "severity": severity, "raised": raised, "notificationCount": notificationCount}


# ------------------------------------------------------------------------------ ERR_Quarantine_BadFiles


def quarantineBadFiles(
    cf: ControlFramework,
    batchId: int,
    fileOps: FileOps,
    quarantineFolderName: str = "quarantine",
    deleteZeroLengthFiles: bool = False,
) -> dict:
    failedFiles = cf.spark.sql(
        f"SELECT inbound_file_id, file_name, file_path, file_size_bytes, structural_check_status, feed_code "
        f"FROM {cf.t('etl_inbound_file_register')} WHERE processing_status = 'Failed' AND NOT COALESCE(is_quarantined, false)"
    ).collect()
    now = utcNow()
    queued = cf.insertRows(
        "work_bad_file_queue",
        [
            {
                "batch_id": batchId,
                "file_name": f["file_name"],
                "file_path": f["file_path"],
                "quarantine_reason_code": classifyBadFile(f["file_size_bytes"], f["structural_check_status"], f["feed_code"]),
                "detected_at_utc": now,
                "is_moved": False,
            }
            for f in failedFiles
        ],
    )
    badFileCount = cf.count("work_bad_file_queue", "NOT is_moved")
    moved = 0
    if badFileCount > 0:
        targetFolder = quarantineFolder(fileOps.join(quarantineFolderName), now)
        pending = cf.spark.sql(f"SELECT bad_file_queue_id, file_path, quarantine_reason_code FROM {cf.t('work_bad_file_queue')} WHERE NOT is_moved").collect()
        for q in pending:
            destination = None
            if fileOps.exists(q["file_path"]):
                if q["quarantine_reason_code"] == "ZERO_LENGTH" and deleteZeroLengthFiles:
                    fileOps.delete(q["file_path"])
                    destination = "(deleted: zero length)"
                else:
                    destination = fileOps.move(q["file_path"], targetFolder)
            cf.update(
                "work_bad_file_queue",
                {"is_moved": True, "moved_at_utc": utcNow(), "quarantine_path": destination or "(file no longer present)"},
                f"bad_file_queue_id = {q['bad_file_queue_id']}",
            )
            moved += 1
        cf.spark.sql(
            f"""
            MERGE INTO {cf.t('etl_inbound_file_register')} AS r
            USING (SELECT DISTINCT file_path, quarantine_reason_code, quarantine_path FROM {cf.t('work_bad_file_queue')} WHERE is_moved) AS q
            ON q.file_path = r.file_path
            WHEN MATCHED AND NOT COALESCE(r.is_quarantined, false) THEN UPDATE SET
                r.is_quarantined = true, r.quarantined_at_utc = current_timestamp(),
                r.processing_status = 'Quarantined', r.quarantine_reason = q.quarantine_reason_code
            """
        )
        if moved > 0:
            cf.raiseNotification(
                batchId, "FILE_QUARANTINE", "WARNING", "Inbound files quarantined", f"Files moved to quarantine: {moved}"
            )
    return {"badFileCount": badFileCount, "movedCount": moved, "queuedNow": len(queued)}
