"""Delta control tables: the snake_case equivalents of the legacy etl.* / err.* / work.* tables.

Every table is declared once here so DDL, tests and the recon task share a single catalog.
Surrogate identity columns (BIGINT IDENTITY in SQL Server) are allocated by
``ControlFramework.nextId`` as time-ordered BIGINTs (OSS Delta 3.2 has no identity columns).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from platform_control.config import PlatformConfig

TS = "TIMESTAMP"
STR = "STRING"
BIG = "BIGINT"
INT = "INT"
BOOL = "BOOLEAN"
DEC = "DECIMAL(18,4)"

# name -> ((column, type), ...), legacy_object
CONTROL_TABLES: dict[str, tuple[tuple[tuple[str, str], ...], str]] = {
    "etl_batch": (
        (
            ("batch_id", BIG), ("batch_name", STR), ("batch_type", STR), ("business_date", "DATE"),
            ("environment_code", STR), ("started_at_utc", TS), ("completed_at_utc", TS), ("status", STR),
            ("restart_from_step", STR), ("initiated_by", STR), ("notes", STR), ("job_run_id", STR),
        ),
        "etl.Batch",
    ),
    "etl_batch_step": (
        (
            ("batch_step_id", BIG), ("batch_id", BIG), ("step_name", STR), ("step_sequence", INT),
            ("step_group", STR), ("started_at_utc", TS), ("completed_at_utc", TS), ("status", STR),
            ("attempt_number", INT), ("package_name", STR), ("is_retryable", BOOL), ("error_message", STR),
            ("criticality", STR),
        ),
        "etl.BatchStep",
    ),
    "etl_package_execution": (
        (
            ("package_execution_id", BIG), ("batch_id", BIG), ("batch_step_id", BIG), ("package_name", STR),
            ("project_name", STR), ("machine_name", STR), ("executed_by", STR), ("started_at_utc", TS),
            ("completed_at_utc", TS), ("duration_seconds", BIG), ("status", STR), ("rows_read", BIG),
            ("rows_inserted", BIG), ("rows_updated", BIG), ("rows_deleted", BIG), ("rows_rejected", BIG),
            ("watermark_from", STR), ("watermark_to", STR), ("attempt_number", INT), ("job_name", STR),
            ("job_run_id", STR), ("status_detail", STR),
        ),
        "etl.PackageExecution",
    ),
    "etl_watermark": (
        (
            ("watermark_id", BIG), ("source_system_code", STR), ("object_name", STR), ("watermark_type", STR),
            ("last_value", STR), ("previous_value", STR), ("last_loaded_at_utc", TS),
            ("last_package_execution_id", BIG), ("lookback_minutes", INT), ("is_locked", BOOL),
        ),
        "etl.Watermark",
    ),
    "etl_row_count_audit": (
        (
            ("row_count_audit_id", BIG), ("package_execution_id", BIG), ("batch_id", BIG), ("object_name", STR),
            ("count_stage", STR), ("source_row_count", BIG), ("target_row_count", BIG), ("insert_row_count", BIG),
            ("update_row_count", BIG), ("delete_row_count", BIG), ("reject_row_count", BIG),
            ("variance_row_count", BIG), ("recorded_at_utc", TS),
        ),
        "etl.RowCountAudit",
    ),
    "etl_error_log": (
        (
            ("error_log_id", BIG), ("package_execution_id", BIG), ("batch_id", BIG), ("error_severity", STR),
            ("error_code", INT), ("error_number", INT), ("error_state", INT), ("error_line", INT),
            ("source_name", STR), ("source_component", STR), ("procedure_name", STR),
            ("error_description", STR), ("logged_at_utc", TS),
        ),
        "etl.ErrorLog",
    ),
    "etl_rejected_record": (
        (
            ("rejected_record_id", BIG), ("package_execution_id", BIG), ("batch_id", BIG),
            ("source_system_code", STR), ("object_name", STR), ("business_key", STR), ("reject_reason_code", STR),
            ("reject_reason", STR), ("reject_stage", STR), ("ssis_error_code", INT), ("ssis_error_column", INT),
            ("record_payload", STR), ("is_reprocessed", BOOL), ("reprocessed_at_utc", TS), ("logged_at_utc", TS),
        ),
        "etl.RejectedRecord",
    ),
    "etl_configuration": (
        (
            ("configuration_id", BIG), ("configuration_key", STR), ("environment_code", STR),
            ("configuration_value", STR), ("value_data_type", STR), ("description", STR), ("is_sensitive", BOOL),
            ("modified_at_utc", TS),
        ),
        "etl.Configuration",
    ),
    "etl_required_configuration_key": (
        (
            ("required_configuration_key_id", BIG), ("configuration_key", STR), ("environment_code", STR),
            ("is_mandatory", BOOL), ("description", STR),
        ),
        "etl.RequiredConfigurationKey",
    ),
    "etl_configuration_check": (
        (
            ("configuration_check_id", BIG), ("batch_id", BIG), ("configuration_key", STR), ("environment_code", STR),
            ("check_type_code", STR), ("check_status", STR), ("detail_text", STR), ("checked_at_utc", TS),
        ),
        "work.ConfigurationCheck",
    ),
    "etl_reconciliation_exemption": (
        (("exemption_id", BIG), ("object_name", STR), ("reason", STR)),
        "etl.ReconciliationExemption",
    ),
    "etl_row_count_tolerance": (
        (
            ("row_count_tolerance_id", BIG), ("object_name", STR), ("tolerance_percent", "DECIMAL(9,4)"),
            ("absolute_tolerance", BIG), ("explanation_code", STR), ("approved_by", STR),
        ),
        "etl.RowCountTolerance",
    ),
    "etl_data_quality_rule": (
        (
            ("data_quality_rule_id", BIG), ("rule_code", STR), ("rule_group_code", STR), ("object_name", STR),
            ("rule_name", STR), ("rule_expression", STR), ("spark_expression", STR), ("dimension_code", STR),
            ("severity_code", STR), ("threshold_value", DEC), ("region_code", STR), ("source_system_code", STR),
            ("is_active", BOOL), ("owner_name", STR), ("notes", STR), ("created_at_utc", TS), ("updated_at_utc", TS),
        ),
        "etl.DataQualityRule",
    ),
    "etl_data_quality_rule_exception": (
        (
            ("rule_exception_id", BIG), ("rule_code", STR), ("object_name", STR), ("region_code", STR),
            ("effective_from", "DATE"), ("effective_to", "DATE"), ("reason", STR), ("approved_by", STR),
            ("created_at_utc", TS),
        ),
        "etl.DataQualityRuleException",
    ),
    "etl_data_quality_result": (
        (
            ("data_quality_result_id", BIG), ("batch_id", BIG), ("package_execution_id", BIG), ("object_name", STR),
            ("rule_code", STR), ("measured_value", DEC), ("threshold_value", DEC), ("rows_evaluated", BIG),
            ("result_status", STR), ("region_code", STR), ("detail_text", STR), ("evaluated_at_utc", TS),
        ),
        "etl.DataQualityResult",
    ),
    "etl_reconciliation_result": (
        (
            ("reconciliation_result_id", BIG), ("batch_id", BIG), ("reconciliation_name", STR), ("object_name", STR),
            ("source_key", STR), ("ledger_code", STR), ("accounting_period", STR), ("account_code", STR),
            ("region_code", STR), ("source_amount", "DECIMAL(19,4)"), ("target_amount", "DECIMAL(19,4)"),
            ("variance_amount", "DECIMAL(19,4)"), ("variance_status", STR), ("explanation_code", STR),
            ("evaluated_at_utc", TS),
        ),
        "etl.ReconciliationResult",
    ),
    "etl_operator_notification": (
        (
            ("operator_notification_id", BIG), ("batch_id", BIG), ("notification_type_code", STR), ("severity", STR),
            ("subject", STR), ("body", STR), ("object_name", STR), ("raised_at_utc", TS), ("is_acknowledged", BOOL),
            ("acknowledged_by", STR), ("acknowledged_at_utc", TS),
        ),
        "etl.OperatorNotification",
    ),
    "etl_batch_hold": (
        (
            ("batch_hold_id", BIG), ("hold_reason_code", STR), ("hold_detail", STR), ("batch_type", STR),
            ("raised_at_utc", TS), ("is_cleared", BOOL), ("cleared_by", STR), ("cleared_at_utc", TS),
        ),
        "etl.BatchHold",
    ),
    "etl_batch_step_rerun_request": (
        (
            ("rerun_request_id", BIG), ("batch_id", BIG), ("batch_step_id", BIG), ("package_name", STR),
            ("attempt_number", INT), ("requested_at_utc", TS), ("request_status", STR), ("completed_at_utc", TS),
            ("result_status", STR),
        ),
        "etl.BatchStepRerunRequest",
    ),
    "etl_batch_archive": (
        (
            ("batch_archive_id", BIG), ("batch_id", BIG), ("batch_type", STR), ("batch_status", STR),
            ("started_at_utc", TS), ("ended_at_utc", TS), ("step_count", INT), ("failed_step_count", INT),
            ("archived_at_utc", TS),
        ),
        "etl.BatchArchive",
    ),
    "etl_maintenance_log": (
        (("maintenance_log_id", BIG), ("batch_id", BIG), ("task_name", STR), ("detail_text", STR), ("recorded_at_utc", TS)),
        "etl.MaintenanceLog",
    ),
    "etl_purge_audit": (
        (
            ("purge_audit_id", BIG), ("batch_id", BIG), ("schema_name", STR), ("table_name", STR),
            ("cutoff_date", "DATE"), ("rows_deleted", BIG), ("purged_at_utc", TS),
        ),
        "etl.PurgeAudit",
    ),
    "etl_preflight_result": (
        (
            ("preflight_result_id", BIG), ("batch_id", BIG), ("check_name", STR), ("check_status", STR),
            ("detail_text", STR), ("checked_at_utc", TS),
        ),
        "etl.PreflightResult",
    ),
    "etl_load_volume_history": (
        (
            ("load_volume_history_id", BIG), ("volume_mount_point", STR), ("load_date", "DATE"),
            ("bytes_written", BIG), ("free_percent", "DECIMAL(9,4)"),
        ),
        "etl.LoadVolumeHistory",
    ),
    "etl_inbound_file_register": (
        (
            ("inbound_file_id", BIG), ("feed_code", STR), ("file_name", STR), ("file_path", STR),
            ("file_size_bytes", BIG), ("file_hash", STR), ("received_at_utc", TS), ("structural_check_status", STR),
            ("bad_file_count", INT), ("processing_status", STR), ("processed_at_utc", TS), ("is_quarantined", BOOL),
            ("quarantined_at_utc", TS), ("quarantine_reason", STR), ("is_archived", BOOL), ("archive_path", STR),
            ("archived_at_utc", TS),
        ),
        "etl.InboundFileRegister",
    ),
    "etl_archive_expiry_list": (
        (
            ("archive_expiry_list_id", BIG), ("file_name", STR), ("archive_path", STR), ("archived_at_utc", TS),
            ("listed_at_utc", TS), ("is_deleted", BOOL), ("deleted_at_utc", TS),
        ),
        "etl.ArchiveExpiryList",
    ),
    "etl_staging_table_register": (
        (
            ("staging_table_id", BIG), ("schema_name", STR), ("table_name", STR), ("load_date_column", STR),
            ("retention_days", INT), ("is_purge_eligible", BOOL), ("approximate_row_count", BIG),
        ),
        "etl.StagingTableRegister",
    ),
    "etl_orchestration_variable": (
        (("job_run_id", STR), ("variable_name", STR), ("variable_value", STR), ("updated_at_utc", TS)),
        "(SSIS package variables)",
    ),
    "err_rejected_file_row": (
        (
            ("reject_id", BIG), ("batch_id", BIG), ("package_execution_id", BIG), ("source_system_code", STR),
            ("source_file_name", STR), ("file_format_version", STR), ("source_row_number", BIG), ("raw_row_text", STR),
            ("expected_column_count", INT), ("actual_column_count", INT), ("delimiter_used", STR),
            ("decimal_separator_used", STR), ("date_format_assumed", STR), ("reject_reason_code", STR),
            ("reject_reason", STR), ("reject_stage", STR), ("reprocess_status_code", STR),
            ("reprocess_attempt_count", INT), ("reprocessed_in_batch_id", BIG), ("rejected_at_utc", TS),
        ),
        "err.RejectedFileRow",
    ),
    "err_rejected_lookup_failure": (
        (
            ("reject_id", BIG), ("batch_id", BIG), ("package_execution_id", BIG), ("source_object_name", STR),
            ("source_business_key", STR), ("lookup_name", STR), ("lookup_column_name", STR), ("lookup_value", STR),
            ("source_system_code", STR), ("reject_reason_code", STR), ("reject_reason", STR), ("reject_stage", STR),
            ("routed_to_unknown_member", BOOL), ("queued_for_late_arrival", BOOL), ("occurrence_count", INT),
            ("record_payload", STR), ("reprocess_status_code", STR), ("reprocess_attempt_count", INT),
            ("reprocessed_at_utc", TS), ("reprocessed_by_execution_id", BIG), ("rejected_at_utc", TS),
        ),
        "err.RejectedLookupFailure",
    ),
    "err_rejected_constraint_violation": (
        (
            ("reject_id", BIG), ("batch_id", BIG), ("package_execution_id", BIG), ("target_object_name", STR),
            ("constraint_name", STR), ("constraint_type_code", STR), ("violating_business_key", STR),
            ("violating_column_name", STR), ("violating_value", STR), ("sql_error_number", INT),
            ("sql_error_message", STR), ("reject_reason_code", STR), ("reject_reason", STR), ("reject_stage", STR),
            ("record_payload", STR), ("reprocess_status_code", STR), ("rejected_at_utc", TS),
        ),
        "err.RejectedConstraintViolation",
    ),
    "stg_order_line_replay": (
        (
            ("reject_id", BIG), ("batch_id", BIG), ("package_execution_id", BIG), ("source_business_key", STR),
            ("stock_item_id", INT), ("record_payload", STR), ("replayed_at_utc", TS),
        ),
        "stg.OrderLine (replay rows)",
    ),
    "work_reject_routing_history": (
        (
            ("rejected_record_id", BIG), ("batch_id", BIG), ("object_name", STR), ("reject_stage", STR),
            ("reject_reason_code", STR), ("reject_reason_description", STR), ("source_key", STR),
            ("rejected_at_utc", TS), ("age_days", INT), ("is_escalated", BOOL), ("reject_file_line", STR),
            ("reject_file_name", STR), ("routed_at_utc", TS),
        ),
        "work.RejectRoutingHistory",
    ),
    "work_reject_escalation": (
        (
            ("rejected_record_id", BIG), ("batch_id", BIG), ("object_name", STR), ("reject_reason_code", STR),
            ("source_key", STR), ("age_days", INT), ("escalated_at_utc", TS),
        ),
        "work.RejectEscalation",
    ),
    "work_bad_file_queue": (
        (
            ("bad_file_queue_id", BIG), ("batch_id", BIG), ("file_name", STR), ("file_path", STR),
            ("quarantine_reason_code", STR), ("detected_at_utc", TS), ("is_moved", BOOL), ("moved_at_utc", TS),
            ("quarantine_path", STR),
        ),
        "work.BadFileQueue",
    ),
    "work_row_count_reconciliation": (
        (
            ("batch_id", BIG), ("object_name", STR), ("source_row_count", BIG), ("staging_row_count", BIG),
            ("target_row_count", BIG), ("rejected_row_count", BIG), ("tolerance_percent", "DECIMAL(9,4)"),
            ("explanation_code", STR), ("expected_target_row_count", BIG), ("difference_row_count", BIG),
            ("difference_percent", "DECIMAL(18,2)"), ("reconciliation_status", STR), ("evaluated_at_utc", TS),
        ),
        "work.RowCountReconciliation",
    ),
}

ID_COLUMNS = {
    name: cols[0][0]
    for name, (cols, _legacy) in CONTROL_TABLES.items()
    if cols[0][0].endswith("_id") and cols[0][1] == BIG and name not in ("work_row_count_reconciliation",)
}

TABLE_PROPERTIES = (
    "'delta.autoOptimize.optimizeWrite' = 'true', "
    "'delta.autoOptimize.autoCompact' = 'true', "
    "'delta.logRetentionDuration' = 'interval 400 days'"
)


def ddlFor(cfg: PlatformConfig, name: str) -> str:
    cols, legacy = CONTROL_TABLES[name]
    columnSql = ", ".join(f"{c} {t}" for c, t in cols)
    comment = legacy.replace("'", "")
    return (
        f"CREATE TABLE IF NOT EXISTS {cfg.table(name)} ({columnSql}) USING DELTA "
        f"COMMENT 'platform_control equivalent of {comment}' TBLPROPERTIES ({TABLE_PROPERTIES})"
    )


def ensureSchema(spark: SparkSession, cfg: PlatformConfig) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schemaFqn}")


def ensureControlTables(spark: SparkSession, cfg: PlatformConfig, names: list[str] | None = None) -> list[str]:
    ensureSchema(spark, cfg)
    created = []
    for name in names or CONTROL_TABLES:
        spark.sql(ddlFor(cfg, name))
        created.append(cfg.table(name))
    return created


def ensureLandingVolume(spark: SparkSession, cfg: PlatformConfig) -> str | None:
    """Unity Catalog volume inside our own schema for landing / quarantine / archive / poison paths."""
    if cfg.isLocal:
        return None
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.{cfg.schema}.{cfg.volumeName} "
        "COMMENT 'File-share equivalent for the ING_FILE_*/ERR_Quarantine_BadFiles/MNT_Archive packages'"
    )
    return cfg.volumeRoot


def columnNames(name: str) -> list[str]:
    return [c for c, _t in CONTROL_TABLES[name][0]]
