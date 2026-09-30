"""MNT_* packages mapped to Databricks-native maintenance.

MNT_Rebuild_Indexes / MNT_Update_Statistics -> OPTIMIZE (+ ANALYZE COMPUTE STATISTICS) on our Delta
tables; Predictive Optimization owns this for managed UC tables, so the tasks are audit-logged no-ops
when PO is enabled. MNT_Purge_ControlHistory / MNT_Purge_StagingHistory -> retention DELETE + VACUUM.
MNT_Archive_ProcessedFiles -> volume archive folder moves. MNT_Check_DiskSpace -> volume usage and
table size checks. MNT_Validate_Configuration -> etl_configuration / required key checks.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from platform_control.control import ControlFramework, sqlLiteral, utcNow
from platform_control.files import FileOps
from platform_control.rules import (
    archiveEligible,
    archiveExpired,
    configurationCheckStatus,
    diskSpaceStatus,
    indexMaintenanceAction,
    statisticsRefreshNeeded,
)
from platform_control.tables import CONTROL_TABLES

CONNECTION_PLACEHOLDERS = (
    "ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "SQLSERVER_HOST", "SQLSERVER_PORT",
    "SQLSERVER_USER", "SQLSERVER_OLTP_DB", "SQLSERVER_STAGING_DB", "SQLSERVER_DW_DB",
)

# legacy table -> (our table, timestamp column) for MNT_Purge_ControlHistory
CONTROL_RETENTION = {
    "etl_error_log": "logged_at_utc",
    "etl_data_quality_result": "evaluated_at_utc",
    "etl_row_count_audit": "recorded_at_utc",
    "etl_reconciliation_result": "evaluated_at_utc",
    "etl_operator_notification": "raised_at_utc",
    "etl_purge_audit": "purged_at_utc",
    "etl_maintenance_log": "recorded_at_utc",
    "work_reject_routing_history": "routed_at_utc",
    "work_bad_file_queue": "detected_at_utc",
}


def _log(cf: ControlFramework, batchId: int | None, taskName: str, detail: str) -> None:
    cf.insertRows("etl_maintenance_log", [{"batch_id": batchId, "task_name": taskName, "detail_text": detail[:4000], "recorded_at_utc": utcNow()}])


def _purge(cf: ControlFramework, batchId: int | None, tableName: str, predicate: str, cutoff: date, dryRun: bool) -> int:
    fqn = cf.t(tableName)
    rows = int(cf.scalar(f"SELECT COUNT(*) FROM {fqn} WHERE {predicate}", 0))
    if rows and not dryRun:
        cf.spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")
    cf.insertRows(
        "etl_purge_audit",
        [{"batch_id": batchId, "schema_name": cf.cfg.schema, "table_name": tableName, "cutoff_date": cutoff, "rows_deleted": 0 if dryRun else rows, "purged_at_utc": utcNow()}],
    )
    return rows


def _vacuum(cf: ControlFramework, tableName: str, retentionHours: int) -> None:
    if cf.cfg.isLocal:
        return  # local Delta enforces the 7 day retention check; VACUUM is exercised on the workspace only
    cf.spark.sql(f"VACUUM {cf.t(tableName)} RETAIN {max(retentionHours, 168)} HOURS")


# ------------------------------------------------------------------------------ MNT_Purge_ControlHistory


def purgeControlHistory(cf: ControlFramework, batchId: int | None, batchRetentionDays: int = 400, errorRetentionDays: int = 180, rejectRetentionDays: int = 90, dryRun: bool = False) -> dict:
    now = utcNow()
    batchCutoff = (now - timedelta(days=batchRetentionDays)).date()
    errorCutoff = (now - timedelta(days=errorRetentionDays)).date()
    rejectCutoff = (now - timedelta(days=rejectRetentionDays)).date()
    purged = {}
    # Archive batch summaries before removing them (etl.BatchArchive).
    archiveCandidates = cf.spark.sql(
        f"""SELECT b.batch_id, b.batch_type, b.status, b.started_at_utc, b.completed_at_utc,
                   (SELECT COUNT(*) FROM {cf.t('etl_batch_step')} s WHERE s.batch_id = b.batch_id) AS step_count,
                   (SELECT COUNT(*) FROM {cf.t('etl_batch_step')} s WHERE s.batch_id = b.batch_id AND s.status = 'Failed') AS failed_step_count
            FROM {cf.t('etl_batch')} b WHERE b.status <> 'Running' AND b.started_at_utc < {sqlLiteral(batchCutoff)}
              AND NOT EXISTS (SELECT 1 FROM {cf.t('etl_batch_archive')} a WHERE a.batch_id = b.batch_id)"""
    ).collect()
    if archiveCandidates and not dryRun:
        cf.insertRows(
            "etl_batch_archive",
            [{"batch_id": a["batch_id"], "batch_type": a["batch_type"], "batch_status": a["status"], "started_at_utc": a["started_at_utc"], "ended_at_utc": a["completed_at_utc"],
              "step_count": int(a["step_count"]), "failed_step_count": int(a["failed_step_count"]), "archived_at_utc": now} for a in archiveCandidates],
        )
    purged["etl_batch_archive_added"] = len(archiveCandidates)
    for tableName, column in CONTROL_RETENTION.items():
        purged[tableName] = _purge(cf, batchId, tableName, f"{column} < {sqlLiteral(errorCutoff)}", errorCutoff, dryRun)
    purged["etl_rejected_record"] = _purge(cf, batchId, "etl_rejected_record", f"is_reprocessed AND logged_at_utc < {sqlLiteral(rejectCutoff)}", rejectCutoff, dryRun)
    purged["err_rejected_lookup_failure"] = _purge(cf, batchId, "err_rejected_lookup_failure", f"reprocess_status_code IN ('Reprocessed','Abandoned','Exhausted') AND rejected_at_utc < {sqlLiteral(rejectCutoff)}", rejectCutoff, dryRun)
    purged["err_rejected_file_row"] = _purge(cf, batchId, "err_rejected_file_row", f"reprocess_status_code IN ('Reprocessed','NotReplayable') AND rejected_at_utc < {sqlLiteral(rejectCutoff)}", rejectCutoff, dryRun)
    oldBatches = f"status <> 'Running' AND started_at_utc < {sqlLiteral(batchCutoff)}"
    purged["etl_package_execution"] = _purge(cf, batchId, "etl_package_execution", f"batch_id IN (SELECT batch_id FROM {cf.t('etl_batch')} WHERE {oldBatches})", batchCutoff, dryRun)
    purged["etl_batch_step"] = _purge(cf, batchId, "etl_batch_step", f"batch_id IN (SELECT batch_id FROM {cf.t('etl_batch')} WHERE {oldBatches})", batchCutoff, dryRun)
    purged["etl_batch"] = _purge(cf, batchId, "etl_batch", oldBatches, batchCutoff, dryRun)
    if not dryRun:
        for tableName in list(CONTROL_RETENTION) + ["etl_batch", "etl_batch_step", "etl_package_execution", "etl_rejected_record"]:
            _vacuum(cf, tableName, 168)
    _log(cf, batchId, "MNT_Purge_ControlHistory", f"dry_run={dryRun} purged={purged}")
    return {"purged": purged, "totalRows": sum(v for k, v in purged.items() if k != "etl_batch_archive_added")}


# ------------------------------------------------------------------------------ MNT_Purge_StagingHistory


def purgeStagingHistory(cf: ControlFramework, batchId: int | None, defaultRetentionDays: int = 90, dryRun: bool = False, vacuumRetentionHours: int = 168) -> dict:
    """Legacy: purge stg/work tables by LoadDate. We own no stg_* domain tables; the equivalent is
    retention on our own reject/replay/work tables plus VACUUM of every Delta table in the schema."""
    now = utcNow()
    cutoff = (now - timedelta(days=defaultRetentionDays)).date()
    registered = cf.spark.sql(f"SELECT schema_name, table_name, load_date_column, retention_days FROM {cf.t('etl_staging_table_register')} WHERE is_purge_eligible").collect()
    purged = {}
    ownTargets = {"stg_order_line_replay": "replayed_at_utc", "work_row_count_reconciliation": "evaluated_at_utc", "work_reject_escalation": "escalated_at_utc"}
    for tableName, column in ownTargets.items():
        purged[tableName] = _purge(cf, batchId, tableName, f"{column} < {sqlLiteral(cutoff)}", cutoff, dryRun)
    for r in registered:
        candidate = f"{r['schema_name'].lower()}_{ObjectResolverSnake(r['table_name'])}"
        if candidate in CONTROL_TABLES:
            days = int(r["retention_days"] or defaultRetentionDays)
            tableCutoff = (now - timedelta(days=days)).date()
            purged[candidate] = _purge(cf, batchId, candidate, f"{r['load_date_column']} < {sqlLiteral(tableCutoff)}", tableCutoff, dryRun)
    vacuumed = 0
    if not dryRun:
        for tableName in CONTROL_TABLES:
            _vacuum(cf, tableName, vacuumRetentionHours)
            vacuumed += 1
    _log(cf, batchId, "MNT_Purge_StagingHistory", f"dry_run={dryRun} cutoff={cutoff} purged={purged} vacuumed={vacuumed}")
    return {"purged": purged, "registeredLegacyTables": len(registered), "vacuumedTables": vacuumed}


def ObjectResolverSnake(name: str) -> str:  # noqa: N802 - small local helper kept next to its only caller
    from platform_control.quality import ObjectResolver

    return ObjectResolver.snake(name)


# ------------------------------------------------------------------------------ MNT_Rebuild_Indexes / MNT_Update_Statistics


def _tableDetail(cf: ControlFramework, tableName: str) -> dict:
    rows = cf.spark.sql(f"DESCRIBE DETAIL {cf.t(tableName)}").first().asDict()
    return {"numFiles": int(rows.get("numFiles") or 0), "sizeInBytes": int(rows.get("sizeInBytes") or 0), "properties": rows.get("properties") or {}}


def predictiveOptimizationEnabled(cf: ControlFramework) -> bool | None:
    if cf.cfg.isLocal:
        return None
    try:
        row = cf.spark.sql(f"DESCRIBE SCHEMA EXTENDED {cf.cfg.schemaFqn}").filter("database_description_item = 'Predictive Optimization'").first()
        return bool(row and "ENABLE" in str(row[1]).upper() and "DISABLE" not in str(row[1]).upper())
    except Exception:  # noqa: BLE001
        return None


def rebuildIndexes(cf: ControlFramework, batchId: int | None, reorganiseThresholdPercent: int = 10, rebuildThresholdPercent: int = 30, tables: list[str] | None = None) -> dict:
    """Fragmentation -> small-file ratio: files per GB above the thresholds triggers OPTIMIZE (bin-packing)."""
    actions = {}
    poEnabled = predictiveOptimizationEnabled(cf)
    for tableName in tables or list(CONTROL_TABLES):
        detail = _tableDetail(cf, tableName)
        # fragmentation proxy: percentage of files smaller than 16 MB (small files), capped at 100
        smallFileRatio = Decimal(0) if detail["numFiles"] == 0 else min(Decimal(100), Decimal(detail["numFiles"]) * Decimal(16 * 1024 * 1024) * 100 / Decimal(max(detail["sizeInBytes"], 1)) if detail["sizeInBytes"] < detail["numFiles"] * 16 * 1024 * 1024 else Decimal(0))
        action = indexMaintenanceAction(smallFileRatio, reorganiseThresholdPercent, rebuildThresholdPercent)
        if action != "NONE" and detail["numFiles"] > 1 and not poEnabled:
            cf.spark.sql(f"OPTIMIZE {cf.t(tableName)}")
        actions[tableName] = {"action": action, "numFiles": detail["numFiles"], "smallFilePercent": str(smallFileRatio), "optimized": action != "NONE" and detail["numFiles"] > 1 and not poEnabled}
    _log(cf, batchId, "MNT_Rebuild_Indexes", f"predictive_optimization={poEnabled} actions={actions}")
    return {"predictiveOptimization": poEnabled, "actions": actions, "optimizedCount": sum(1 for a in actions.values() if a["optimized"])}


def updateStatistics(cf: ControlFramework, batchId: int | None, modificationThresholdRows: int = 5000, tables: list[str] | None = None) -> dict:
    """Rows written since the last ANALYZE (from Delta history) decide whether statistics are recomputed."""
    refreshed, skipped = [], []
    for tableName in tables or list(CONTROL_TABLES):
        history = cf.spark.sql(f"DESCRIBE HISTORY {cf.t(tableName)}").select("operation", "operationMetrics").collect()
        modified = 0
        for h in history:
            if h["operation"] in ("ANALYZE", "COMPUTE STATISTICS"):
                break
            metrics = h["operationMetrics"] or {}
            modified += int(metrics.get("numOutputRows", metrics.get("numUpdatedRows", metrics.get("numDeletedRows", 0))) or 0)
        if statisticsRefreshNeeded(modified, modificationThresholdRows):
            try:
                cf.spark.sql(f"ANALYZE TABLE {cf.t(tableName)} COMPUTE STATISTICS")
            except Exception as exc:  # noqa: BLE001 - local Delta (v2 catalog) has no ANALYZE; UC on Databricks does
                if "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE" not in str(exc):
                    raise
                cf.spark.table(cf.t(tableName)).count()
            refreshed.append(tableName)
        else:
            skipped.append(tableName)
    _log(cf, batchId, "MNT_Update_Statistics", f"threshold={modificationThresholdRows} refreshed={refreshed} skipped={len(skipped)}")
    return {"refreshed": refreshed, "skippedCount": len(skipped)}


# ------------------------------------------------------------------------------ MNT_Archive_ProcessedFiles


def archiveProcessedFiles(cf: ControlFramework, batchId: int | None, fileOps: FileOps, archiveRetentionDays: int = 730, minimumFileAgeHours: int = 6, processedFolder: str = "processed", archiveFolder: str = "archive") -> dict:
    now = utcNow()
    archived, tooYoung = 0, 0
    for info in fileOps.listFiles(fileOps.join(processedFolder), recursive=True):
        if not archiveEligible(info.modifiedAtUtc, now, minimumFileAgeHours):
            tooYoung += 1
            continue
        target = fileOps.join(archiveFolder, info.modifiedAtUtc.strftime("%Y%m"))
        destination = fileOps.move(info.path, target)
        cf.insertRows("etl_archive_expiry_list", [{"file_name": info.name, "archive_path": destination, "archived_at_utc": now, "listed_at_utc": now, "is_deleted": False}])
        cf.spark.sql(
            f"UPDATE {cf.t('etl_inbound_file_register')} SET is_archived = true, archive_path = {sqlLiteral(destination)}, archived_at_utc = {sqlLiteral(now)} "
            f"WHERE file_name = {sqlLiteral(info.name)} AND NOT COALESCE(is_archived, false)"
        )
        archived += 1
    expired = cf.spark.sql(f"SELECT archive_expiry_list_id, archive_path, archived_at_utc FROM {cf.t('etl_archive_expiry_list')} WHERE NOT is_deleted").collect()
    deleted = 0
    for e in expired:
        if archiveExpired(e["archived_at_utc"], now, archiveRetentionDays):
            fileOps.delete(e["archive_path"])
            cf.update("etl_archive_expiry_list", {"is_deleted": True, "deleted_at_utc": now}, f"archive_expiry_list_id = {e['archive_expiry_list_id']}")
            deleted += 1
    _log(cf, batchId, "MNT_Archive_ProcessedFiles", f"archived={archived} too_young={tooYoung} expired_deleted={deleted}")
    return {"archivedCount": archived, "tooYoungCount": tooYoung, "expiredDeletedCount": deleted}


# ------------------------------------------------------------------------------ MNT_Check_DiskSpace


def checkDiskSpace(cf: ControlFramework, batchId: int | None, fileOps: FileOps, minimumFreePercent: int = 15, holdBatchOnShortfall: bool = True, projectedGrowthGb: Decimal | None = None) -> dict:
    total, free = fileOps.usage(fileOps.root)
    freePercent = Decimal(free) * 100 / Decimal(max(total, 1))
    availableGb = Decimal(free) / Decimal(1024**3)
    if projectedGrowthGb is None:
        tableBytes = sum(_tableDetail(cf, t)["sizeInBytes"] for t in CONTROL_TABLES)
        history = cf.spark.sql(f"SELECT COUNT(*) FROM {cf.t('etl_batch')} WHERE started_at_utc >= date_sub(current_timestamp(), 7)").first()[0]
        projectedGrowthGb = Decimal(tableBytes) * Decimal(max(int(history), 1)) / Decimal(1024**3)
    status = diskSpaceStatus(freePercent.quantize(Decimal("0.01")), availableGb, projectedGrowthGb, minimumFreePercent)
    cf.insertRows(
        "etl_load_volume_history",
        [{"volume_mount_point": fileOps.root, "load_date": utcNow().date(), "bytes_written": total - free, "free_percent": freePercent.quantize(Decimal("0.0001"))}],
    )
    cf.insertRows("etl_preflight_result", [{"batch_id": batchId, "check_name": "DISK_SPACE", "check_status": status, "detail_text": f"free={freePercent:.2f}% available_gb={availableGb:.2f} projected_gb={projectedGrowthGb:.2f}", "checked_at_utc": utcNow()}])
    held = False
    if status != "OK":
        cf.raiseNotification(batchId, "DISK_SPACE", status, f"Storage volume {status}", f"{fileOps.root}: free={freePercent:.2f}% (minimum {minimumFreePercent}%)")
        if holdBatchOnShortfall and status == "CRITICAL":
            cf.insertRows("etl_batch_hold", [{"hold_reason_code": "DISK_SPACE", "hold_detail": f"free={freePercent:.2f}%", "batch_type": "ALL", "raised_at_utc": utcNow(), "is_cleared": False}])
            held = True
    _log(cf, batchId, "MNT_Check_DiskSpace", f"status={status} free_percent={freePercent:.2f} held={held}")
    return {"status": status, "freePercent": freePercent.quantize(Decimal("0.01")), "availableGb": availableGb.quantize(Decimal("0.01")), "batchHeld": held}


# ------------------------------------------------------------------------------ MNT_Validate_Configuration


def validateConfiguration(cf: ControlFramework, batchId: int | None, environmentCode: str | None = None, failOnMissingKey: bool = True) -> dict:
    env = environmentCode or cf.cfg.environmentCode
    now = utcNow()
    cf.spark.sql(f"DELETE FROM {cf.t('etl_configuration_check')} WHERE environment_code = {sqlLiteral(env)}")
    required = cf.spark.sql(
        f"""SELECT r.configuration_key, c.configuration_value FROM {cf.t('etl_required_configuration_key')} r
            LEFT JOIN {cf.t('etl_configuration')} c ON c.configuration_key = r.configuration_key AND c.environment_code = {sqlLiteral(env)}
            WHERE r.is_mandatory AND (r.environment_code IS NULL OR r.environment_code IN ({sqlLiteral(env)}, 'ALL'))"""
    ).collect()
    checks = [
        {"batch_id": batchId, "configuration_key": r["configuration_key"], "environment_code": env, "check_type_code": "REQUIRED",
         "check_status": configurationCheckStatus(r["configuration_value"]), "detail_text": None if r["configuration_value"] is not None else "Key is not defined for this environment", "checked_at_utc": now}
        for r in required
    ]
    configured = {r["configuration_key"]: r["configuration_value"] for r in cf.spark.sql(f"SELECT configuration_key, configuration_value FROM {cf.t('etl_configuration')} WHERE environment_code = {sqlLiteral(env)}").collect()}
    for placeholder in CONNECTION_PLACEHOLDERS:
        checks.append({"batch_id": batchId, "configuration_key": placeholder, "environment_code": env, "check_type_code": "PLACEHOLDER",
                       "check_status": "OK" if placeholder in configured else "MISSING", "detail_text": "Connection placeholder resolved from the environment, never stored inline", "checked_at_utc": now})
    # Plausibility: the Lakehouse Federation catalogs replace the connection strings; they must resolve.
    for catalog in (cf.cfg.legacyStagingCatalog, cf.cfg.legacyDwCatalog):
        try:
            cf.spark.sql(f"SELECT 1 FROM {catalog}.information_schema.schemata LIMIT 1").collect()
            status, detail = "OK", f"Foreign catalog {catalog} reachable"
        except Exception as exc:  # noqa: BLE001
            status, detail = ("SUSPECT" if not cf.cfg.isLocal else "SKIPPED"), f"Foreign catalog {catalog}: {str(exc)[:300]}"
        checks.append({"batch_id": batchId, "configuration_key": f"FEDERATION_{catalog.upper()}", "environment_code": env, "check_type_code": "PLAUSIBILITY", "check_status": status, "detail_text": detail, "checked_at_utc": now})
    cf.insertRows("etl_configuration_check", checks)
    missing = sum(1 for c in checks if c["check_type_code"] == "REQUIRED" and c["check_status"] in ("MISSING", "EMPTY"))
    suspect = sum(1 for c in checks if c["check_status"] == "SUSPECT")
    cf.insertRows("etl_preflight_result", [{"batch_id": batchId, "check_name": "CONFIGURATION", "check_status": "Failed" if missing else ("Warning" if suspect else "Passed"), "detail_text": f"missing={missing} suspect={suspect} checks={len(checks)}", "checked_at_utc": now}])
    _log(cf, batchId, "MNT_Validate_Configuration", f"env={env} missing={missing} suspect={suspect}")
    if missing and failOnMissingKey:
        cf.logError(batchId, f"{missing} mandatory configuration key(s) missing for {env}", sourceName="MNT_Validate_Configuration")
        raise RuntimeError(f"MNT_Validate_Configuration: {missing} mandatory key(s) missing for {env}")
    return {"checks": len(checks), "missingKeyCount": missing, "suspectCount": suspect}
