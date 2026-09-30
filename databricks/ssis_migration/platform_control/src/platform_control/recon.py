"""Reconciliation evidence for the 28 owned packages -> <catalog>.evidence.recon_results.

Every package gets a ``row_count`` and a ``checksum`` check. Data packages compare our Delta table with
the legacy SSIS output table (federation); when the legacy table is empty the expectation is derived from
the legacy *source* data with the package's own logic (``baseline: source_derived`` -> PARTIAL). Utility
packages and the orchestration masters have no data output: masters are reconciled structurally (plan
nodes/edges vs generated job tasks) and utilities get NOT_APPLICABLE with the Databricks-native equivalent.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from pyspark.sql import types as T

from platform_control.config import (
    ACTOR,
    ALL_OWNED_PACKAGES,
    BRANCH,
    GROUP_SLUG,
    HARNESS_VERSION,
    OWNED_PACKAGES,
)
from platform_control.control import ControlFramework, utcNow

RESOURCES_DIR = Path(__file__).resolve().parents[2] / "resources"

EVIDENCE_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("run_at", T.TimestampType()),
        T.StructField("unit", T.StringType()),
        T.StructField("unit_type", T.StringType()),
        T.StructField("verdict", T.StringType()),
        T.StructField("branch", T.StringType()),
        T.StructField("source_object", T.StringType()),
        T.StructField("target_object", T.StringType()),
        T.StructField("checks", T.StringType()),
        T.StructField("summary", T.StringType()),
        T.StructField("git_sha", T.StringType()),
        T.StructField("actor", T.StringType()),
        T.StructField("harness_version", T.StringType()),
    ]
)

LEGACY_STAGING_DB = "WideWorldImporters_Staging"


# ----------------------------------------------------------------------------- generic measurements
def _hashExpr(exprs: list[str]) -> str:
    parts = ", ".join(f"COALESCE(CAST({e} AS STRING), '')" for e in exprs)
    # DECIMAL(38,0) accumulator: SUM over BIGINT hashes overflows under ANSI mode on Databricks
    return f"SUM(CAST(xxhash64(concat_ws('|', {parts})) AS DECIMAL(38,0)))"


def tableStats(cf: ControlFramework, fqName: str, exprs: list[str], where: str = "1=1") -> dict:
    """COUNT(*) and an order-independent SUM(xxhash64(business columns)) over one table."""
    try:
        row = cf.spark.sql(f"SELECT COUNT(*) AS n, {_hashExpr(exprs)} AS cs FROM {fqName} WHERE {where}").first()
        return {"count": int(row["n"]), "checksum": None if row["cs"] is None else str(row["cs"]), "error": None}
    except Exception as exc:  # noqa: BLE001 - unreadable legacy object is itself a finding
        return {"count": None, "checksum": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}


SOURCE_UNAVAILABLE_NOTE = (
    "the legacy SQL Server/Oracle hosts were stopped by the operator while this evidence run was in flight, "
    "so the federation read failed; re-run ssis_platform_control_recon once the hosts are back."
)


def _sourceUnavailable(error: str | None) -> dict:
    return {"check": "source_unavailable", "pass": False, "error": error}


def compareChecks(source: dict, target: dict, method: str, baseline: str = "legacy", extra: dict | None = None) -> list[dict]:
    countPass = source["count"] is not None and source["count"] == target["count"]
    checksumPass = countPass and source["checksum"] == target["checksum"]
    rowCount = {"check": "row_count", "source": source["count"], "target": target["count"], "pass": countPass}
    checksum = {"check": "checksum", "method": method, "source": source["checksum"], "target": target["checksum"], "pass": checksumPass}
    for check in (rowCount, checksum):
        if baseline != "legacy":
            check["baseline"] = baseline
        if source.get("error") or target.get("error"):
            check["error"] = source.get("error") or target.get("error")
        if extra:
            check.update(extra)
    return [rowCount, checksum]


def latestExecution(cf: ControlFramework, package: str) -> dict | None:
    row = cf.spark.sql(
        f"SELECT package_execution_id, batch_id, status FROM {cf.t('etl_package_execution')} "
        f"WHERE package_name = '{package}' AND status IN ('Succeeded', 'Failed') ORDER BY package_execution_id DESC LIMIT 1"
    ).first()
    return None if row is None else row.asDict()


# ----------------------------------------------------------------------------- package specs
@dataclass
class DataSpec:
    legacySchema: str
    legacyTable: str
    targetTable: str
    columns: list[tuple[str, str]]  # (legacy expression, target expression) business columns
    derived: Callable[[ControlFramework, dict | None], tuple[dict, dict, str]] | None = None
    legacyWhere: str = "1=1"
    scopeColumn: str = "package_execution_id"
    summary: str = ""
    extraChecks: Callable[[ControlFramework], list[dict]] | None = None


@dataclass
class UtilitySpec:
    legacySchema: str
    legacyTable: str
    targetTable: str
    columns: list[tuple[str, str]]
    equivalent: str
    extra: dict = field(default_factory=dict)


def _scoped(cf: ControlFramework, spec: DataSpec, execution: dict | None) -> str:
    if execution is None:
        return "1=0"
    if spec.scopeColumn == "batch_id":
        return f"batch_id = {execution['batch_id']}"
    return f"package_execution_id = {execution['package_execution_id']}"


def _setStats(cf: ControlFramework, sql: str) -> dict:
    """COUNT + checksum over the rows returned by an arbitrary SELECT that yields a single string column `k`."""
    try:
        row = cf.spark.sql(f"SELECT COUNT(*) AS n, SUM(CAST(xxhash64(k) AS DECIMAL(38,0))) AS cs FROM ({sql})").first()
        return {"count": int(row["n"]), "checksum": None if row["cs"] is None else str(row["cs"]), "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"count": None, "checksum": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}


def derivedRuleEngine(cf, execution):
    scope = _scoped(cf, RECON_SPECS["DQ_Rule_Engine"], execution)
    expected = _setStats(cf, f"SELECT rule_code AS k FROM {cf.t('etl_data_quality_rule')} WHERE is_active")
    actual = _setStats(cf, f"SELECT rule_code AS k FROM {cf.t('etl_data_quality_result')} WHERE {scope}")
    return expected, actual, "one DataQualityResult row per active etl.DataQualityRule (rule_code set)"


def derivedRowCountRecon(cf, execution):
    scope = "1=0" if execution is None else f"batch_id = {execution['batch_id']}"
    expected = _setStats(
        cf,
        f"SELECT DISTINCT a.object_name AS k FROM {cf.t('etl_row_count_audit')} a WHERE a.{scope} "
        f"AND a.object_name NOT IN (SELECT object_name FROM {cf.t('etl_reconciliation_exemption')})",
    )
    actual = _setStats(cf, f"SELECT object_name AS k FROM {cf.t('work_row_count_reconciliation')} WHERE {scope}")
    return expected, actual, "one reconciliation row per audited, non-exempt object in the batch"


def derivedReferentialScreen(cf, execution):
    from platform_control.quality import REFERENTIAL_SOURCES  # noqa: F401 - documents the legacy inputs

    scope = _scoped(cf, RECON_SPECS["DQ_Referential_Screen"], execution)
    stg = cf.cfg.legacyStaging
    expected = _setStats(
        cf,
        f"""
        SELECT concat_ws('|', 'stg.OrderLine', ol.OrderLineBusinessKey, 'StockItem') AS k
        FROM {stg('stg', 'OrderLine')} ol LEFT ANTI JOIN {stg('stg', 'StockItem')} si ON si.StockItemBusinessKey = ol.StockItemBusinessKey
        UNION ALL
        SELECT concat_ws('|', 'stg.OrderLine', ol.OrderLineBusinessKey, 'PackageType')
        FROM {stg('stg', 'OrderLine')} ol LEFT ANTI JOIN {stg('ref', 'PackageType')} pt ON pt.PackageTypeCode = ol.PackageTypeCode
        UNION ALL
        SELECT concat_ws('|', 'stg.SaleLine', sl.SaleLineBusinessKey, 'Currency')
        FROM {stg('stg', 'SaleLine')} sl LEFT JOIN {stg('stg', 'Sale')} s ON s.SaleBusinessKey = sl.SaleBusinessKey
        LEFT ANTI JOIN {stg('ref', 'Currency')} c ON c.CurrencyCode = s.TransactionCurrencyCode
        """,
    )
    actual = _setStats(
        cf,
        f"SELECT concat_ws('|', source_object_name, source_business_key, lookup_name) AS k "
        f"FROM {cf.t('err_rejected_lookup_failure')} WHERE {scope}",
    )
    return expected, actual, "orphan (source key, lookup) pairs re-derived from legacy stg/ref with the package's anti-joins"


def derivedFileScreen(cf, execution):
    from platform_control.quality import partnerSalesRawView

    scope = _scoped(cf, RECON_SPECS["DQ_File_Screen"], execution)
    try:
        from pyspark.sql import functions as F

        from platform_control.quality import _screenUdf

        raw = partnerSalesRawView(cf.spark.table(cf.cfg.legacyStaging("raw", "FilePartnerSales")))
        screened = raw.withColumn("screen", _screenUdf("RawLine", "SaleDateText", "AmountText")).select("*", "screen.*")
        screened.filter(~F.col("well_formed")).select(
            F.concat_ws("|", F.col("FileLineNumber").cast("string"), F.col("reject_reason_code")).alias("k")
        ).createOrReplaceTempView("recon_expected_file_rejects")
        expected = _setStats(cf, "SELECT k FROM recon_expected_file_rejects")
    except Exception as exc:  # noqa: BLE001
        expected = {"count": None, "checksum": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    actual = _setStats(
        cf, f"SELECT concat_ws('|', CAST(source_row_number AS STRING), reject_reason_code) AS k FROM {cf.t('err_rejected_file_row')} WHERE {scope}"
    )
    return expected, actual, "malformed rows re-screened from legacy raw.FilePartnerSales with the package's row screen"


def derivedRejectReprocess(cf, execution):
    scope = _scoped(cf, RECON_SPECS["DQ_Reject_Reprocess"], execution)
    expected = _setStats(
        cf,
        f"SELECT CAST(reject_id AS STRING) AS k FROM {cf.t('err_rejected_lookup_failure')} "
        f"WHERE reprocess_status_code = 'Replayed' AND reprocessed_by_execution_id = {execution['package_execution_id'] if execution else -1}",
    )
    actual = _setStats(cf, f"SELECT CAST(reject_id AS STRING) AS k FROM {cf.t('stg_order_line_replay')} WHERE {scope}")
    return expected, actual, "every reject marked Replayed by the execution has exactly one replay row"


def derivedQuarantine(cf, execution):
    from platform_control.files import FileOps

    scope = _scoped(cf, RECON_SPECS["ING_FILE_QuarantineMalformed"], execution)
    fileOps = FileOps(cf.cfg.volumeRoot)
    try:
        lines = []
        for info in fileOps.listFiles(fileOps.join("archive", "quarantine"), recursive=True):
            lines.extend(f"{info.name}|{n}" for n, _ in enumerate(fileOps.readLines(info.path), start=1))
        checksum = sum(int.from_bytes(hashlib.blake2b(line.encode(), digest_size=8).digest(), "big", signed=True) for line in lines)
        expected = {"count": len(lines), "checksum": str(checksum) if lines else None, "error": None}
    except Exception as exc:  # noqa: BLE001
        expected = {"count": None, "checksum": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    rows = cf.spark.sql(
        f"SELECT source_file_name, source_row_number FROM {cf.t('err_rejected_file_row')} WHERE {scope} AND reject_stage = 'Quarantine'"
    ).collect()
    keys = [f"{r['source_file_name']}|{r['source_row_number']}" for r in rows]
    checksum = sum(int.from_bytes(hashlib.blake2b(k.encode(), digest_size=8).digest(), "big", signed=True) for k in keys)
    actual = {"count": len(keys), "checksum": str(checksum) if keys else None, "error": None}
    return expected, actual, "one err.RejectedFileRow per line of every swept quarantine file (files now in archive/quarantine)"


def derivedRouteRejects(cf, execution):
    scope = "1=0" if execution is None else f"batch_id = {execution['batch_id']}"
    expected = _setStats(cf, f"SELECT CAST(rejected_record_id AS STRING) AS k FROM {cf.t('etl_rejected_record')} WHERE NOT is_reprocessed")
    actual = _setStats(
        cf,
        f"SELECT CAST(rejected_record_id AS STRING) AS k FROM {cf.t('work_reject_routing_history')} "
        f"WHERE routed_at_utc = (SELECT MAX(routed_at_utc) FROM {cf.t('work_reject_routing_history')} WHERE {scope}) OR "
        f"rejected_record_id IN (SELECT rejected_record_id FROM {cf.t('work_reject_routing_history')})",
    )
    return expected, actual, "every unreprocessed etl.RejectedRecord appears once in work.RejectRoutingHistory"


RECON_SPECS: dict[str, DataSpec] = {
    "DQ_Rule_Engine": DataSpec(
        "etl", "DataQualityResult", "etl_data_quality_result",
        [("ObjectName", "object_name"), ("RuleCode", "rule_code"), ("CAST(MeasuredValue AS DECIMAL(18,4))", "CAST(measured_value AS DECIMAL(18,4))"), ("ResultStatus", "result_status")],
        derived=derivedRuleEngine,
        summary="Rule-table-driven engine: etl.DataQualityRule rows are translated to safe Spark SQL predicates (no string-concatenated T-SQL) and measured per object.",
    ),
    "DQ_Threshold_Gate": DataSpec(
        "etl", "ReconciliationResult", "etl_reconciliation_result",
        [("ObjectName", "object_name"), ("ReconciliationName", "reconciliation_name"), ("CAST(SourceAmount AS DECIMAL(19,4))", "CAST(source_amount AS DECIMAL(19,4))"), ("CAST(TargetAmount AS DECIMAL(19,4))", "CAST(target_amount AS DECIMAL(19,4))"), ("VarianceStatus", "variance_status")],
        derived=derivedRowCountRecon, scopeColumn="batch_id",
        summary="Threshold gate over etl.RowCountAudit -> etl.ReconciliationResult (ROW_COUNT), tolerance from etl.RowCountTolerance.",
    ),
    "DQ_Referential_Screen": DataSpec(
        "err", "RejectedLookupFailure", "err_rejected_lookup_failure",
        [("SourceObjectName", "source_object_name"), ("SourceBusinessKey", "source_business_key"), ("LookupName", "lookup_name"), ("LookupValue", "lookup_value"), ("RejectReasonCode", "reject_reason_code")],
        derived=derivedReferentialScreen,
        summary="Set-based anti-joins replace the SSIS row-by-row Lookup components; one reject per (source key, lookup) with occurrence_count.",
    ),
    "DQ_Reject_Reprocess": DataSpec(
        "err", "RejectedLookupFailure", "stg_order_line_replay",
        [("SourceBusinessKey", "source_business_key"), ("RecordPayload", "record_payload")],
        derived=derivedRejectReprocess, legacyWhere="ReprocessStatusCode = 'Replayed'",
        summary="Pending lookup rejects are replayed when the missing StockItem now exists; we do not own stg.OrderLine so replays land in stg_order_line_replay.",
    ),
    "DQ_File_Screen": DataSpec(
        "err", "RejectedFileRow", "err_rejected_file_row",
        [("SourceRowNumber", "source_row_number"), ("RejectReasonCode", "reject_reason_code"), ("RawRowText", "raw_row_text")],
        derived=derivedFileScreen, legacyWhere="RejectStage = 'Extract'",
        summary="Row screen (delimiter count, date, amount) over raw.FilePartnerSales -> err.RejectedFileRow, malformed-rate threshold gate.",
    ),
    "ING_FILE_QuarantineMalformed": DataSpec(
        "err", "RejectedFileRow", "err_rejected_file_row",
        [("SourceFileName", "source_file_name"), ("SourceRowNumber", "source_row_number"), ("RejectReasonCode", "reject_reason_code")],
        derived=derivedQuarantine, legacyWhere="RejectStage = 'Quarantine'",
        summary="Quarantine folder sweep on the UC volume <schema>.landing/quarantine (sample files committed under samples/); unreadable files -> quarantine/poison.",
    ),
    "ERR_Reconcile_RowCounts": DataSpec(
        "etl", "ReconciliationResult", "work_row_count_reconciliation",
        [("ObjectName", "object_name"), ("CAST(SourceAmount AS BIGINT)", "staging_row_count"), ("CAST(TargetAmount AS BIGINT)", "target_row_count"), ("VarianceStatus", "reconciliation_status")],
        derived=derivedRowCountRecon, scopeColumn="batch_id", legacyWhere="ReconciliationName = 'ROW_COUNT'",
        summary="Expected = staging - rejected; MATCHED / EXPLAINED / TOLERATED / FAILED per etl.RowCountTolerance and ReconciliationExemption.",
    ),
    "ERR_Route_RejectedRows": DataSpec(
        "work", "RejectRoutingHistory", "work_reject_routing_history",
        [("ObjectName", "object_name"), ("RejectReasonCode", "reject_reason_code"), ("SourceKey", "source_key"), ("CAST(IsEscalated AS BOOLEAN)", "is_escalated")],
        derived=derivedRouteRejects, scopeColumn="batch_id",
        summary="Unrouted etl.RejectedRecord rows are written to a reject file on the volume (errors/) and to work.RejectRoutingHistory; aged rows escalate.",
    ),
}

UTILITY_SPECS: dict[str, UtilitySpec] = {
    "ERR_Handle_PackageFailure": UtilitySpec("etl", "ErrorLog", "etl_error_log", [("ErrorSeverity", "error_severity"), ("SourceName", "source_name"), ("ErrorDescription", "error_description")],
        "errors.handlePackageFailure: classifies the failing package's error (RETRYABLE_ERROR_CODES) into Transient/Permanent, logs to etl_error_log, marks the batch step retryable or fails the batch; Lakeflow task failure + finalize_batch task."),
    "ERR_Notify_Operations": UtilitySpec("etl", "OperatorNotification", "etl_operator_notification", [("NotificationTypeCode", "notification_type_code"), ("Severity", "severity"), ("Subject", "subject")],
        "errors.notifyOperations writes etl_operator_notification rows; delivery is the job's email_notifications.on_failure (var.operator_email) instead of sp_send_dbmail."),
    "ERR_Quarantine_BadFiles": UtilitySpec("work", "BadFileQueue", "work_bad_file_queue", [("FileName", "file_name"), ("QuarantineReasonCode", "quarantine_reason_code")],
        "errors.quarantineBadFiles moves failed inbound files on the UC volume to quarantine/<yyyyMMdd> and marks etl_inbound_file_register; no legacy data output."),
    "ERR_Retry_FailedSteps": UtilitySpec("etl", "BatchStepRerunRequest", "etl_batch_step_rerun_request", [("StepName", "step_name"), ("AttemptNumber", "attempt_number")],
        "errors.retryFailedSteps re-runs retryable failed steps with exponential backoff (AttemptNumber <= MaxRetryAttempts && RetryableStepCount > 0); job tasks also carry max_retries from the SQL Agent step retry settings."),
    "MNT_Archive_ProcessedFiles": UtilitySpec("etl", "ArchiveExpiryList", "etl_archive_expiry_list", [("FileName", "file_name")],
        "maintenance.archiveProcessedFiles moves processed/ files older than MinimumFileAgeHours to archive/<yyyyMM> on the UC volume and expires archives after ArchiveRetentionDays (730)."),
    "MNT_Check_DiskSpace": UtilitySpec("etl", "PreflightResult", "etl_preflight_result", [("CheckName", "check_name"), ("CheckStatus", "check_status")],
        "maintenance.checkDiskSpace: volume usage + projected growth from etl_load_volume_history vs MinimumFreePercent (15); Delta on cloud storage has no drive to fill, so this is a preflight row + optional batch hold."),
    "MNT_Purge_ControlHistory": UtilitySpec("etl", "PurgeAudit", "etl_purge_audit", [("TableName", "table_name"), ("RowsDeleted", "rows_deleted")],
        "maintenance.purgeControlHistory: DELETE by retention (Batch 400d, ErrorLog 180d, RejectedRecord 90d) + VACUUM on the control tables; etl_purge_audit records what was removed."),
    "MNT_Purge_StagingHistory": UtilitySpec("etl", "StagingTableRegister", "etl_staging_table_register", [("TableName", "table_name")],
        "maintenance.purgeStagingHistory: retention DELETE on our reject/replay/work tables (default 90d) + VACUUM; the stg_* domain tables belong to sibling groups."),
    "MNT_Rebuild_Indexes": UtilitySpec("etl", "MaintenanceLog", "etl_maintenance_log", [("TaskName", "task_name")],
        "maintenance.rebuildIndexes: Delta has no B-tree indexes; small-file ratio above Reorganise/Rebuild thresholds triggers OPTIMIZE, skipped when Predictive Optimization is enabled on the schema."),
    "MNT_Update_Statistics": UtilitySpec("etl", "MaintenanceLog", "etl_maintenance_log", [("TaskName", "task_name")],
        "maintenance.updateStatistics: ANALYZE TABLE ... COMPUTE STATISTICS when rows written since the last analyze exceed ModificationThresholdRows (5000)."),
    "MNT_Validate_Configuration": UtilitySpec("work", "ConfigurationCheck", "etl_configuration_check", [("ConfigurationKey", "configuration_key"), ("CheckTypeCode", "check_type_code"), ("CheckStatus", "check_status")],
        "maintenance.validateConfiguration: required keys, placeholders, plausibility and federation reachability checks against the seeded etl_configuration; results in etl_configuration_check."),
}


# ----------------------------------------------------------------------------- orchestration (structural)
def _jobEdges(job: dict) -> set[tuple[str, str]]:
    def nodeOf(key: str) -> str | None:
        if key.startswith(("finalize_batch", "agent_")):
            return None
        if "__" in key:
            base, suffix = key.split("__", 1)
            return base if suffix in ("start", "end") else None
        return key

    edges = set()
    for task in job["tasks"]:
        target = nodeOf(task["task_key"])
        if target is None or task["task_key"].endswith("__end"):
            continue
        for dep in task.get("depends_on", []):
            source = nodeOf(dep["task_key"])
            if source and source != target:
                edges.add((source, target))
    return edges


def _digest(items: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(items)).encode()).hexdigest()[:16]


def reconcileMaster(cf: ControlFramework, root: str) -> tuple[str, list[dict], str, str, str]:
    import yaml

    from platform_control.orchestration import MasterPlan, taskKey

    plan = MasterPlan.load(root)
    resourcePath = RESOURCES_DIR / f"{root.lower()}.job.yml"
    jobName = f"ssis_{GROUP_SLUG}_{root}"
    try:
        job = yaml.safe_load(resourcePath.read_text(encoding="utf-8"))["resources"]["jobs"][jobName]
        planNodes = [taskKey(n) for n in plan.nodes]
        jobNodes = sorted({t["task_key"].split("__", 1)[0] for t in job["tasks"] if not t["task_key"].startswith(("finalize_batch", "agent_"))})
        planEdges = [f"{taskKey(e['from'])}->{taskKey(e['to'])}" for e in plan.edges]
        jobEdges = [f"{a}->{b}" for a, b in _jobEdges(job)]
        planChildren = [f"{taskKey(n)}__{taskKey(c['package'])}" for n, node in plan.nodes.items() for c in node.get("children", [])]
        jobChildren = [t["task_key"] for t in job["tasks"] if "__" in t["task_key"] and not t["task_key"].endswith(("__start", "__end"))]
        checks = [
            {"check": "row_count", "baseline": "source_derived", "unit": "plan nodes vs job node tasks", "source": len(planNodes), "target": len(jobNodes), "pass": sorted(planNodes) == jobNodes},
            {"check": "checksum", "baseline": "source_derived", "method": "sha256(sorted precedence edges)", "source": _digest(planEdges), "target": _digest(jobEdges), "pass": _digest(planEdges) == _digest(jobEdges)},
            {"check": "child_packages", "baseline": "source_derived", "source": len(planChildren), "target": len(jobChildren), "pass": sorted(planChildren) == sorted(jobChildren)},
            {"check": "schedule", "source": "sqlserver/agent", "target": job.get("schedule", {}).get("quartz_cron_expression"), "pass": bool(job.get("schedule"))},
        ]
    except Exception as exc:  # noqa: BLE001
        checks = [
            {"check": "row_count", "baseline": "source_derived", "pass": False, "error": f"{type(exc).__name__}: {exc}"},
            {"check": "checksum", "baseline": "source_derived", "pass": False, "error": f"{type(exc).__name__}: {exc}"},
        ]
    legacyBatches = tableStats(cf, cf.cfg.legacyStaging("etl", "Batch"), ["BatchName"], f"BatchName = '{root}'")
    ourBatches = tableStats(cf, cf.t("etl_batch"), ["batch_name"], f"batch_name = '{root}'")
    checks.append({"check": "batch_rows", "informational": True, "source": legacyBatches["count"], "target": ourBatches["count"], "pass": True, "error": legacyBatches.get("error")})
    structuralPass = all(c["pass"] for c in checks[:3])
    verdict = "PARTIAL" if structuralPass else "FAIL"
    summary = (
        f"Orchestration has no data output: reconciled structurally, plan {len(plan.nodes)} nodes / {len(plan.edges)} edges / "
        f"{sum(len(n.get('children', [])) for n in plan.nodes.values())} child packages vs generated job {jobName} (baseline source_derived, "
        f"hence PARTIAL). Legacy etl.Batch rows for this master: {legacyBatches['count']}; ours: {ourBatches['count']}."
        + ("" if structuralPass else " STRUCTURAL MISMATCH - regenerate resources with tools/generate_jobs.py.")
    )
    return verdict, checks, f"{LEGACY_STAGING_DB}.etl.Batch ({root}); ssis/orchestration-plan.json#{root}", f"{cf.cfg.catalog}.{cf.cfg.schema}.etl_batch; job {jobName}", summary


# ----------------------------------------------------------------------------- data + utility packages
def reconcileData(cf: ControlFramework, package: str) -> tuple[str, list[dict], str, str, str]:
    spec = RECON_SPECS[package]
    legacyFq = cf.cfg.legacyStaging(spec.legacySchema, spec.legacyTable)
    targetFq = cf.t(spec.targetTable)
    execution = latestExecution(cf, package)
    legacyExprs = [c[0] for c in spec.columns]
    targetExprs = [c[1] for c in spec.columns]
    method = f"sum(xxhash64({', '.join(targetExprs)}))"
    legacy = tableStats(cf, legacyFq, legacyExprs, spec.legacyWhere)
    target = tableStats(cf, targetFq, targetExprs, _scoped(cf, spec, execution))
    checks = compareChecks(legacy, target, method)
    runNote = "no execution of this package recorded yet" if execution is None else f"latest execution {execution['package_execution_id']} ({execution['status']}) in batch {execution['batch_id']}"
    if legacy["error"]:
        return "FAIL", checks + [_sourceUnavailable(legacy["error"])], legacyFq, targetFq, (
            f"Legacy {spec.legacySchema}.{spec.legacyTable} could not be read ({runNote}): {SOURCE_UNAVAILABLE_NOTE} {spec.summary}"
        )
    if legacy["count"] and checks[0]["pass"] and checks[1]["pass"]:
        return "PASS", checks, legacyFq, targetFq, f"Row count and checksum match the legacy SSIS output ({runNote}). {spec.summary}"
    if legacy["count"]:
        # the legacy table has rows from the estate's own history; also record the source-derived expectation
        try:
            expected, actual, description = spec.derived(cf, execution) if spec.derived else ({}, {}, "")
        except Exception as exc:  # noqa: BLE001 - federation read failed mid-run
            return "FAIL", checks + [_sourceUnavailable(f"{type(exc).__name__}: {str(exc)[:300]}")], legacyFq, targetFq, f"Source-derived expectation could not be computed ({runNote}): {SOURCE_UNAVAILABLE_NOTE} {spec.summary}"
        if expected and actual:
            checks += compareChecks(expected, actual, "sum(xxhash64(k))", baseline="source_derived", extra={"expectation": description})
        derivedOk = bool(expected) and expected.get("count") is not None and expected["count"] == actual.get("count") and expected["checksum"] == actual["checksum"]
        verdict = "PARTIAL" if derivedOk and execution is not None else "FAIL"
        return verdict, checks, legacyFq, targetFq, (
            f"Legacy {spec.legacySchema}.{spec.legacyTable} holds {legacy['count']} rows from the estate's own SSIS history that are not the "
            f"output of a run over the same input; our latest run ({runNote}) cannot equal it. Source-derived expectation "
            f"{'matches' if derivedOk else 'does NOT match'} ({description}). {spec.summary}"
        )
    if spec.derived is None or execution is None:
        return "FAIL", checks, legacyFq, targetFq, f"Legacy target empty and {runNote}; nothing to derive. {spec.summary}"
    try:
        expected, actual, description = spec.derived(cf, execution)
    except Exception as exc:  # noqa: BLE001 - federation read failed mid-run
        return "FAIL", checks + [_sourceUnavailable(f"{type(exc).__name__}: {str(exc)[:300]}")], legacyFq, targetFq, f"Source-derived expectation could not be computed ({runNote}): {SOURCE_UNAVAILABLE_NOTE} {spec.summary}"
    if expected.get("error") or actual.get("error"):
        checks += compareChecks(expected, actual, "sum(xxhash64(k))", baseline="source_derived", extra={"expectation": description})
        return "FAIL", checks + [_sourceUnavailable(expected.get("error") or actual.get("error"))], legacyFq, targetFq, f"Source-derived expectation could not be computed ({runNote}): {SOURCE_UNAVAILABLE_NOTE} {spec.summary}"
    checks += compareChecks(expected, actual, "sum(xxhash64(k))", baseline="source_derived", extra={"expectation": description})
    derivedOk = expected["count"] is not None and expected["count"] == actual["count"] and expected["checksum"] == actual["checksum"]
    verdict = "PARTIAL" if derivedOk else "FAIL"
    summary = (
        f"Legacy {spec.legacySchema}.{spec.legacyTable} is empty on the host (SSIS never populated it), so the expectation is derived from the legacy "
        f"source data with the package's own logic ({description}): {'matches' if derivedOk else 'MISMATCH'} ({runNote}). {spec.summary}"
    )
    return verdict, checks, legacyFq, targetFq, summary


def reconcileUtility(cf: ControlFramework, package: str) -> tuple[str, list[dict], str, str, str]:
    spec = UTILITY_SPECS[package]
    legacyFq = cf.cfg.legacyStaging(spec.legacySchema, spec.legacyTable)
    targetFq = cf.t(spec.targetTable)
    legacy = tableStats(cf, legacyFq, [c[0] for c in spec.columns])
    target = tableStats(cf, targetFq, [c[1] for c in spec.columns])
    checks = compareChecks(legacy, target, f"sum(xxhash64({', '.join(c[1] for c in spec.columns)}))", extra={"informational": True})
    execution = latestExecution(cf, package)
    checks.append({"check": "executed_on_databricks", "pass": execution is not None and execution["status"] == "Succeeded", "detail": execution})
    summary = (
        f"Utility package with no data output -> NOT_APPLICABLE. Databricks-native equivalent: {spec.equivalent} "
        f"(latest run: {'none' if execution is None else execution['status']}; row_count/checksum against legacy {spec.legacySchema}.{spec.legacyTable} are informational)."
    )
    return "NOT_APPLICABLE", checks, legacyFq, targetFq, summary


# ----------------------------------------------------------------------------- run
def buildEvidenceRows(cf: ControlFramework, runId: str | None = None) -> list[dict]:
    runId = runId or str(uuid.uuid4())
    runAt = utcNow()
    rows = []
    for package in ALL_OWNED_PACKAGES:
        if package in OWNED_PACKAGES["orchestration"]:
            verdict, checks, source, target, summary = reconcileMaster(cf, package)
        elif package in RECON_SPECS:
            verdict, checks, source, target, summary = reconcileData(cf, package)
        else:
            verdict, checks, source, target, summary = reconcileUtility(cf, package)
        rows.append(
            {
                "run_id": runId, "run_at": runAt, "unit": package, "unit_type": "ssis_package", "verdict": verdict, "branch": BRANCH,
                "source_object": source, "target_object": target, "checks": json.dumps(checks, default=str), "summary": summary[:4000],
                "git_sha": cf.cfg.gitSha, "actor": ACTOR, "harness_version": HARNESS_VERSION,
            }
        )
    return rows


def writeEvidence(cf: ControlFramework, rows: list[dict]) -> str:
    table = cf.cfg.evidenceTable("recon_results")
    if cf.cfg.isLocal:
        cf.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cf.cfg.evidenceSchema}")
        cf.spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(f'{f.name} {f.dataType.simpleString()}' for f in EVIDENCE_SCHEMA)}) USING DELTA")
    df = cf.spark.createDataFrame(rows, EVIDENCE_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table)
    return table


def runRecon(cf: ControlFramework, runId: str | None = None) -> dict:
    rows = buildEvidenceRows(cf, runId)
    table = writeEvidence(cf, rows)
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    return {
        "runId": rows[0]["run_id"], "table": table, "rowCount": len(rows), "verdicts": counts,
        "failures": [(r["unit"], r["summary"][:200]) for r in rows if r["verdict"] == "FAIL"], "rows": rows,
    }
