"""Registry: owned (non-master) package name -> callable executing its Databricks-native equivalent.

Used by the standalone ``ssis_platform_control_<package>`` jobs and by the master orchestrations when a
phase child is one of our own packages. ``context`` carries ``batchId``, ``batchStepId``,
``packageExecutionId``, merged ``parameters`` (job + Execute Package Task bindings) and ``variables``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Callable

from platform_control import errors, maintenance, quality
from platform_control.control import ControlFramework
from platform_control.files import FileOps

Runner = Callable[[ControlFramework, dict], dict]


def _int(context: dict, name: str, default: int) -> int:
    value = context.get("parameters", {}).get(name)
    return default if value in (None, "") else int(value)


def _bool(context: dict, name: str, default: bool) -> bool:
    value = context.get("parameters", {}).get(name)
    if value in (None, ""):
        return default
    return str(value).lower() in ("1", "true", "yes")


def _fileOps(cf: ControlFramework, context: dict) -> FileOps:
    return FileOps(context.get("volumeRoot") or cf.cfg.volumeRoot)


def _lastFailedExecution(cf: ControlFramework, batchId: int) -> tuple[str, int | None, str]:
    row = cf.spark.sql(
        f"SELECT pe.package_name, el.error_code, COALESCE(el.error_description, pe.status_detail) AS message "
        f"FROM {cf.t('etl_package_execution')} pe LEFT JOIN {cf.t('etl_error_log')} el "
        "ON el.package_execution_id = pe.package_execution_id "
        f"WHERE pe.batch_id = {batchId} AND pe.status = 'Failed' ORDER BY pe.completed_at_utc DESC, el.error_log_id DESC LIMIT 1"
    ).first()
    if row is None:
        return "(none)", None, "No failed package execution recorded for this batch"
    return row["package_name"], row["error_code"], row["message"] or ""


def runDqRuleEngine(cf, ctx):
    return quality.runRuleEngine(
        cf, ctx["batchId"], ctx.get("packageExecutionId"), ruleGroupCode=ctx.get("parameters", {}).get("RuleGroupCode") or None
    )


def runDqThresholdGate(cf, ctx):
    return quality.thresholdGate(cf, ctx["batchId"], ctx.get("packageExecutionId"))


def runDqReferentialScreen(cf, ctx):
    return quality.referentialScreen(
        cf,
        ctx["batchId"],
        ctx.get("packageExecutionId"),
        orphanWarnThreshold=_int(ctx, "OrphanWarnThreshold", 0),
        orphanFailThreshold=_int(ctx, "OrphanFailThreshold", 1000),
    )


def runDqRejectReprocess(cf, ctx):
    return quality.rejectReprocess(cf, ctx["batchId"], ctx.get("packageExecutionId"))


def runDqFileScreen(cf, ctx):
    return quality.fileScreen(cf, ctx["batchId"], ctx.get("packageExecutionId"))


def runQuarantineMalformed(cf, ctx):
    return quality.quarantineSweep(cf, ctx["batchId"], ctx.get("packageExecutionId"), _fileOps(cf, ctx))


def runHandlePackageFailure(cf, ctx):
    params = ctx.get("parameters", {})
    package, code, message = _lastFailedExecution(cf, ctx["batchId"])
    return errors.handlePackageFailure(
        cf,
        ctx["batchId"],
        failedPackage=params.get("FailedPackageName") or package,
        failureErrorCode=int(params["FailureErrorCode"]) if params.get("FailureErrorCode") else code,
        failureMessage=params.get("FailureMessage") or message,
        failBatchOnPermanent=_bool(ctx, "FailBatchOnPermanent", True),
    )


def runNotifyOperations(cf, ctx):
    return errors.notifyOperations(cf, ctx["batchId"], notifyOnWarnings=_bool(ctx, "NotifyOnWarnings", False))


def runQuarantineBadFiles(cf, ctx):
    return errors.quarantineBadFiles(cf, ctx["batchId"], _fileOps(cf, ctx), deleteZeroLengthFiles=_bool(ctx, "DeleteZeroLengthFiles", False))


def runReconcileRowCounts(cf, ctx):
    return errors.reconcileRowCounts(
        cf, ctx["batchId"], defaultTolerancePercent=_int(ctx, "DefaultTolerancePercent", 0), raiseOnFailure=_bool(ctx, "RaiseOnFailure", True)
    )


def runRetryFailedSteps(cf, ctx):
    return errors.retryFailedSteps(
        cf, ctx["batchId"], maxRetryAttempts=_int(ctx, "MaxRetryAttempts", 3), backoffBaseSeconds=_int(ctx, "BackoffBaseSeconds", 30)
    )


def runRouteRejectedRows(cf, ctx):
    return errors.routeRejectedRows(cf, ctx["batchId"], _fileOps(cf, ctx), rejectEscalationDays=_int(ctx, "RejectEscalationDays", 5))


def runArchiveProcessedFiles(cf, ctx):
    return maintenance.archiveProcessedFiles(
        cf, ctx.get("batchId"), _fileOps(cf, ctx), archiveRetentionDays=_int(ctx, "ArchiveRetentionDays", 730),
        minimumFileAgeHours=_int(ctx, "MinimumFileAgeHours", 6),
    )


def runCheckDiskSpace(cf, ctx):
    growth = ctx.get("parameters", {}).get("ProjectedGrowthGb")
    return maintenance.checkDiskSpace(
        cf, ctx.get("batchId"), _fileOps(cf, ctx), minimumFreePercent=_int(ctx, "MinimumFreePercent", 15),
        holdBatchOnShortfall=_bool(ctx, "HoldBatchOnShortfall", True), projectedGrowthGb=Decimal(growth) if growth else None,
    )


def runPurgeControlHistory(cf, ctx):
    return maintenance.purgeControlHistory(
        cf, ctx.get("batchId"), batchRetentionDays=_int(ctx, "BatchRetentionDays", 400),
        errorRetentionDays=_int(ctx, "ErrorRetentionDays", 180), rejectRetentionDays=_int(ctx, "RejectRetentionDays", 90),
        dryRun=_bool(ctx, "DryRun", False),
    )


def runPurgeStagingHistory(cf, ctx):
    return maintenance.purgeStagingHistory(cf, ctx.get("batchId"), defaultRetentionDays=_int(ctx, "DefaultRetentionDays", 90), dryRun=_bool(ctx, "DryRun", False))


def runRebuildIndexes(cf, ctx):
    return maintenance.rebuildIndexes(
        cf, ctx.get("batchId"), reorganiseThresholdPercent=_int(ctx, "ReorganiseThresholdPercent", 10),
        rebuildThresholdPercent=_int(ctx, "RebuildThresholdPercent", 30),
    )


def runUpdateStatistics(cf, ctx):
    return maintenance.updateStatistics(cf, ctx.get("batchId"), modificationThresholdRows=_int(ctx, "ModificationThresholdRows", 5000))


def runValidateConfiguration(cf, ctx):
    return maintenance.validateConfiguration(cf, ctx.get("batchId"), failOnMissingKey=_bool(ctx, "FailOnMissingKey", True))


PACKAGE_RUNNERS: dict[str, Runner] = {
    "DQ_Rule_Engine": runDqRuleEngine,
    "DQ_Threshold_Gate": runDqThresholdGate,
    "DQ_Referential_Screen": runDqReferentialScreen,
    "DQ_Reject_Reprocess": runDqRejectReprocess,
    "DQ_File_Screen": runDqFileScreen,
    "ING_FILE_QuarantineMalformed": runQuarantineMalformed,
    "ERR_Handle_PackageFailure": runHandlePackageFailure,
    "ERR_Notify_Operations": runNotifyOperations,
    "ERR_Quarantine_BadFiles": runQuarantineBadFiles,
    "ERR_Reconcile_RowCounts": runReconcileRowCounts,
    "ERR_Retry_FailedSteps": runRetryFailedSteps,
    "ERR_Route_RejectedRows": runRouteRejectedRows,
    "MNT_Archive_ProcessedFiles": runArchiveProcessedFiles,
    "MNT_Check_DiskSpace": runCheckDiskSpace,
    "MNT_Purge_ControlHistory": runPurgeControlHistory,
    "MNT_Purge_StagingHistory": runPurgeStagingHistory,
    "MNT_Rebuild_Indexes": runRebuildIndexes,
    "MNT_Update_Statistics": runUpdateStatistics,
    "MNT_Validate_Configuration": runValidateConfiguration,
}


def runPackage(cf: ControlFramework, package: str, context: dict) -> dict:
    try:
        runner = PACKAGE_RUNNERS[package]
    except KeyError as exc:
        raise KeyError(f"{package} has no Databricks-native runner (masters are jobs, not packages)") from exc
    return runner(cf, context) or {}


def runStandalone(cf: ControlFramework, package: str, parameters: dict | None = None, batchId: int | None = None) -> dict:
    """Run one owned package outside a master: opens/closes an Adhoc batch and a package execution."""
    parameters = {k: v for k, v in (parameters or {}).items() if v not in (None, "")}
    ownBatch = batchId is None
    if ownBatch:
        batchId = cf.startBatch(batchName=package, batchType="Adhoc", notes="standalone package run", jobRunId=parameters.get("job_run_id"))
    executionId = cf.startPackageExecution(batchId, None, package, projectName="WWI_platform_control", jobName=f"ssis_platform_control_{package}")
    context = {"batchId": batchId, "batchStepId": None, "packageExecutionId": executionId, "parameters": parameters, "variables": {}}
    try:
        result = runPackage(cf, package, context)
    except Exception as exc:  # noqa: BLE001
        cf.endPackageExecution(executionId, "Failed", statusDetail=f"{type(exc).__name__}: {exc}"[:4000])
        cf.logError(batchId, str(exc)[:4000], severity="Error", errorCode=getattr(exc, "errorCode", 0) or 0, sourceName=package, packageExecutionId=executionId)
        if ownBatch:
            cf.endBatch(batchId)
        raise
    cf.endPackageExecution(executionId, "Succeeded", rowsRead=result.get("rowsRead"), rowsInserted=result.get("rowsInserted"), rowsRejected=result.get("rowsRejected"), statusDetail=str(result)[:4000])
    if ownBatch:
        result["batch"] = cf.endBatch(batchId)
    result["batchId"] = batchId
    return result
