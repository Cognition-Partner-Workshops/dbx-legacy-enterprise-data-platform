"""Declarative definition of the ``etl`` control schema.

One :class:`Table` per table in ``sqlserver/control/02_tables_control_framework.sql``,
``04_tables_data_quality.sql``, ``06_tables_reconciliation.sql``,
``07_tables_operations.sql`` plus the two tables created inline by the legacy
procedures (``etl.RejectedRecordStaging``, ``etl.ControlPurgeAudit``).

Table names are snake_case; column names keep the legacy PascalCase so the
SQL Server baseline queries port with minimal edits. Legacy ``IDENTITY``
columns become Delta ``GENERATED ALWAYS AS IDENTITY``; computed columns become
Delta generated columns; ``CHECK`` constraints become Delta ``CHECK``
constraints; defaults are preserved as Delta column defaults. Primary keys,
unique constraints, foreign keys and indexes have no enforcement in Delta and
are recorded in the table comment.

The same specification drives :mod:`dbx_etl_common.bootstrap` (DeltaTableBuilder,
runs identically on Databricks and on local delta-spark) and the rendered
``databricks/common/sql/00_etl_control_tables.sql``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

CONTROL_SCHEMA = "etl"

NOW = "current_timestamp()"
CURRENT_USER = "current_user()"


@dataclass(frozen=True)
class Column:
    name: str
    dataType: str
    nullable: bool = True
    identity: bool = False
    default: Optional[str] = None
    generatedAs: Optional[str] = None
    comment: Optional[str] = None


@dataclass(frozen=True)
class Table:
    name: str
    legacyName: str
    columns: Tuple[Column, ...]
    checks: Dict[str, str] = field(default_factory=dict)
    comment: str = ""

    @property
    def identityColumn(self) -> Optional[str]:
        for c in self.columns:
            if c.identity:
                return c.name
        return None

    @property
    def writableColumns(self) -> List[Column]:
        return [c for c in self.columns if not c.identity and c.generatedAs is None]


def _id(name: str, dataType: str = "BIGINT") -> Column:
    """Identity column. Delta identity columns must be BIGINT, so legacy INT IDENTITY keys widen to BIGINT."""
    return Column(name, "BIGINT", nullable=False, identity=True)


def _c(name: str, dataType: str, nullable: bool = True, default: Optional[str] = None,
       comment: Optional[str] = None) -> Column:
    return Column(name, dataType, nullable=nullable, default=default, comment=comment)


def _ts(name: str, nullable: bool = False) -> Column:
    return Column(name, "TIMESTAMP", nullable=nullable, default=None if nullable else NOW)


def _inList(values: List[str]) -> str:
    return ", ".join("'" + v + "'" for v in values)


BATCH_STATUSES = ["Running", "Succeeded", "Failed", "Cancelled", "SucceededWithWarnings"]
BATCH_STEP_STATUSES = ["Running", "Succeeded", "Failed", "Skipped"]
PACKAGE_STATUSES = ["Running", "Succeeded", "Failed", "Cancelled"]
ERROR_SEVERITIES = ["Information", "Warning", "Error", "Critical"]
WATERMARK_TYPES = ["Timestamp", "NumericKey", "DateWindow"]
CONFIG_DATA_TYPES = ["String", "Int", "Decimal", "Boolean", "Date"]
DQ_SEVERITIES = ["INFO", "WARN", "FAIL"]
DQ_RESULT_STATUSES = ["Passed", "Warned", "Failed", "NotEvaluated"]

TABLES: Tuple[Table, ...] = (
    # ------------------------------------------------------------------ 02_tables_control_framework.sql
    Table("source_system", "etl.SourceSystem", (
        _id("SourceSystemId", "INT"),
        _c("SourceSystemCode", "STRING", False),
        _c("SourceSystemName", "STRING", False),
        _c("Platform", "STRING", False),
        _c("RegionCode", "STRING"),
        _c("ConnectionParameter", "STRING"),
        _c("DefaultTimeZone", "STRING", False, "'UTC'"),
        _c("IsActive", "BOOLEAN", False, "true"),
        _ts("ValidFrom"),
    ), comment="Legacy etl.SourceSystem. PK (SourceSystemId); UQ_SourceSystem_Code UNIQUE (SourceSystemCode)."),
    Table("batch", "etl.Batch", (
        _id("BatchId"),
        _c("BatchName", "STRING", False),
        _c("BatchType", "STRING", False),
        _c("BusinessDate", "DATE", False),
        _c("EnvironmentCode", "STRING", False),
        _ts("StartedAtUtc"),
        _ts("CompletedAtUtc", True),
        _c("Status", "STRING", False, "'Running'"),
        _c("RestartFromStep", "STRING"),
        _c("InitiatedBy", "STRING", False, CURRENT_USER),
        _c("Notes", "STRING"),
    ), checks={"CK_Batch_Status": "Status IN (" + _inList(BATCH_STATUSES) + ")"},
        comment="Legacy etl.Batch. PK (BatchId)."),
    Table("batch_step", "etl.BatchStep", (
        _id("BatchStepId"),
        _c("BatchId", "BIGINT", False),
        _c("StepName", "STRING", False),
        _c("StepSequence", "INT", False),
        _c("StepGroup", "STRING", comment="Extract, Stage, Dimension, Fact, Aggregate, Maintenance"),
        _ts("StartedAtUtc"),
        _ts("CompletedAtUtc", True),
        _c("Status", "STRING", False, "'Running'"),
        _c("AttemptNumber", "INT", False, "1"),
    ), checks={"CK_BatchStep_Status": "Status IN (" + _inList(BATCH_STEP_STATUSES) + ")"},
        comment="Legacy etl.BatchStep. PK (BatchStepId); FK_BatchStep_Batch (BatchId) -> etl.batch; "
                "IX_BatchStep_BatchId (BatchId, StepSequence)."),
    Table("package_execution", "etl.PackageExecution", (
        _id("PackageExecutionId"),
        _c("BatchId", "BIGINT"),
        _c("BatchStepId", "BIGINT"),
        _c("PackageName", "STRING", False),
        _c("ProjectName", "STRING"),
        _c("MachineName", "STRING", False, "'databricks'"),
        _c("ExecutedBy", "STRING", False, CURRENT_USER),
        _ts("StartedAtUtc"),
        _ts("CompletedAtUtc", True),
        Column("DurationSeconds", "BIGINT",
               generatedAs="unix_timestamp(CompletedAtUtc) - unix_timestamp(StartedAtUtc)"),
        _c("Status", "STRING", False, "'Running'"),
        _c("RowsRead", "BIGINT"),
        _c("RowsInserted", "BIGINT"),
        _c("RowsUpdated", "BIGINT"),
        _c("RowsDeleted", "BIGINT"),
        _c("RowsRejected", "BIGINT"),
        _c("WatermarkFrom", "STRING"),
        _c("WatermarkTo", "STRING"),
        _c("AttemptNumber", "INT", False, "1"),
    ), checks={"CK_PackageExecution_Status": "Status IN (" + _inList(PACKAGE_STATUSES) + ")"},
        comment="Legacy etl.PackageExecution. PK (PackageExecutionId); FK_PackageExecution_Batch (BatchId) -> etl.batch; "
                "FK_PackageExecution_BatchStep (BatchStepId) -> etl.batch_step; "
                "IX_PackageExecution_Batch (BatchId, PackageName) INCLUDE (Status, RowsInserted); "
                "IX_PackageExecution_Package (PackageName, StartedAtUtc DESC)."),
    Table("watermark", "etl.Watermark", (
        _id("WatermarkId", "INT"),
        _c("SourceSystemCode", "STRING", False),
        _c("ObjectName", "STRING", False),
        _c("WatermarkType", "STRING", False, comment="Timestamp, NumericKey, DateWindow"),
        _c("LastValue", "STRING"),
        _c("PreviousValue", "STRING"),
        _ts("LastLoadedAtUtc", True),
        _c("LastPackageExecutionId", "BIGINT"),
        _c("LookbackMinutes", "INT", False, "0"),
        _c("IsLocked", "BOOLEAN", False, "false"),
    ), checks={"CK_Watermark_Type": "WatermarkType IN (" + _inList(WATERMARK_TYPES) + ")"},
        comment="Legacy etl.Watermark. PK (WatermarkId); UQ_Watermark_Object UNIQUE (SourceSystemCode, ObjectName)."),
    Table("row_count_audit", "etl.RowCountAudit", (
        _id("RowCountAuditId"),
        _c("PackageExecutionId", "BIGINT", False),
        _c("ObjectName", "STRING", False),
        _c("SourceRowCount", "BIGINT"),
        _c("TargetRowCount", "BIGINT"),
        _c("InsertRowCount", "BIGINT"),
        _c("UpdateRowCount", "BIGINT"),
        _c("DeleteRowCount", "BIGINT"),
        _c("RejectRowCount", "BIGINT"),
        Column("VarianceRowCount", "BIGINT",
               generatedAs="coalesce(SourceRowCount, 0) - coalesce(TargetRowCount, 0) - coalesce(RejectRowCount, 0)"),
        _ts("RecordedAtUtc"),
    ), comment="Legacy etl.RowCountAudit (also exposed as view etl.row_count_log). PK (RowCountAuditId); "
               "FK_RowCountAudit_PackageExecution (PackageExecutionId) -> etl.package_execution; "
               "IX_RowCountAudit_Object (ObjectName, RecordedAtUtc DESC)."),
    Table("error_log", "etl.ErrorLog", (
        _id("ErrorLogId"),
        _c("PackageExecutionId", "BIGINT"),
        _c("BatchId", "BIGINT"),
        _c("ErrorSeverity", "STRING", False, "'Error'"),
        _c("ErrorCode", "INT"),
        _c("ErrorNumber", "INT"),
        _c("ErrorState", "INT"),
        _c("ErrorLine", "INT"),
        _c("SourceName", "STRING"),
        _c("SourceComponent", "STRING"),
        _c("ProcedureName", "STRING"),
        _c("ErrorDescription", "STRING"),
        _ts("LoggedAtUtc"),
    ), checks={"CK_ErrorLog_Severity": "ErrorSeverity IN (" + _inList(ERROR_SEVERITIES) + ")"},
        comment="Legacy etl.ErrorLog. PK (ErrorLogId); IX_ErrorLog_Batch (BatchId, LoggedAtUtc DESC)."),
    Table("rejected_record", "etl.RejectedRecord", (
        _id("RejectedRecordId"),
        _c("PackageExecutionId", "BIGINT"),
        _c("BatchId", "BIGINT"),
        _c("SourceSystemCode", "STRING"),
        _c("ObjectName", "STRING", False),
        _c("BusinessKey", "STRING"),
        _c("RejectReasonCode", "STRING", False),
        _c("RejectReason", "STRING"),
        _c("RejectStage", "STRING", False, comment="Extract, Stage, Screen, Dimension, Fact"),
        _c("SsisErrorCode", "INT"),
        _c("SsisErrorColumn", "INT"),
        _c("RecordPayload", "STRING", comment="delimited or JSON copy of the offending row"),
        _c("IsReprocessed", "BOOLEAN", False, "false"),
        _ts("ReprocessedAtUtc", True),
        _ts("LoggedAtUtc"),
    ), comment="Legacy etl.RejectedRecord. PK (RejectedRecordId); "
               "IX_RejectedRecord_Object (ObjectName, LoggedAtUtc DESC) INCLUDE (RejectReasonCode, IsReprocessed)."),
    Table("configuration", "etl.Configuration", (
        _id("ConfigurationId", "INT"),
        _c("ConfigurationKey", "STRING", False),
        _c("EnvironmentCode", "STRING", False, "'ALL'"),
        _c("ConfigurationValue", "STRING", False),
        _c("ValueDataType", "STRING", False, "'String'"),
        _c("Description", "STRING"),
        _c("IsSensitive", "BOOLEAN", False, "false"),
        _ts("ModifiedAtUtc"),
    ), checks={"CK_Configuration_DataType": "ValueDataType IN (" + _inList(CONFIG_DATA_TYPES) + ")"},
        comment="Legacy etl.Configuration. PK (ConfigurationId); UQ_Configuration_Key UNIQUE (ConfigurationKey, EnvironmentCode)."),
    Table("reconciliation_exemption", "etl.ReconciliationExemption", (
        _id("ExemptionId", "INT"),
        _c("ObjectName", "STRING", False),
        _c("Reason", "STRING", False),
    ), comment="Legacy etl.ReconciliationExemption. PK (ExemptionId); UQ_ReconciliationExemption UNIQUE (ObjectName)."),
    Table("package_dependency", "etl.PackageDependency", (
        _id("PackageDependencyId", "INT"),
        _c("PackageName", "STRING", False),
        _c("DependsOnPackage", "STRING", False),
        _c("DependencyType", "STRING", False, "'Hard'"),
    ), checks={"CK_PackageDependency_Type": "DependencyType IN ('Hard', 'Soft')"},
        comment="Legacy etl.PackageDependency. PK (PackageDependencyId); UQ_PackageDependency UNIQUE (PackageName, DependsOnPackage)."),
    # ------------------------------------------------------------------ created inline by procedures
    Table("rejected_record_staging", "etl.RejectedRecordStaging", (
        _id("RejectedRecordStagingId"),
        _c("LoadTag", "STRING", False),
        _c("PackageExecutionId", "BIGINT"),
        _c("BatchId", "BIGINT"),
        _c("SourceSystemCode", "STRING"),
        _c("ObjectName", "STRING", False),
        _c("BusinessKey", "STRING"),
        _c("RejectReasonCode", "STRING", False),
        _c("RejectReason", "STRING"),
        _c("RejectStage", "STRING"),
        _c("SsisErrorCode", "INT"),
        _c("SsisErrorColumn", "INT"),
        _c("RecordPayload", "STRING"),
        _ts("LandedAtUtc"),
    ), comment="Legacy etl.RejectedRecordStaging (created by etl.usp_LogRejectedRecordSet). PK (RejectedRecordStagingId); "
               "IX_RejectedRecordStaging_LoadTag (LoadTag)."),
    Table("control_purge_audit", "etl.ControlPurgeAudit", (
        _id("ControlPurgeAuditId"),
        _ts("PurgeRunAtUtc"),
        _c("TableName", "STRING", False),
        _ts("CutoffUtc", True),
        _c("RowsDeleted", "BIGINT", False),
        _c("DurationSeconds", "INT"),
    ), comment="Legacy etl.ControlPurgeAudit (created by etl.usp_PurgeControlHistory). PK (ControlPurgeAuditId)."),
    # ------------------------------------------------------------------ 04_tables_data_quality.sql
    Table("data_quality_rule", "etl.DataQualityRule", (
        _id("DataQualityRuleId", "INT"),
        _c("RuleCode", "STRING", False),
        _c("RuleGroupCode", "STRING", False),
        _c("ObjectName", "STRING", False),
        _c("RuleName", "STRING", False),
        _c("RuleExpression", "STRING", False, comment="WHERE clause selecting the offending rows"),
        _c("DimensionCode", "STRING", False, "'Validity'"),
        _c("SeverityCode", "STRING", False, "'WARN'"),
        _c("ThresholdValue", "DECIMAL(18,4)", False, "0"),
        _c("RegionCode", "STRING"),
        _c("SourceSystemCode", "STRING"),
        _c("IsActive", "BOOLEAN", False, "true"),
        _c("OwnerName", "STRING"),
        _c("Notes", "STRING"),
        _ts("CreatedAtUtc"),
        _ts("UpdatedAtUtc", True),
    ), checks={"CK_DataQualityRule_Severity": "SeverityCode IN (" + _inList(DQ_SEVERITIES) + ")",
               "CK_DataQualityRule_Threshold": "ThresholdValue >= 0"},
        comment="Legacy etl.DataQualityRule. PK (DataQualityRuleId); UQ_DataQualityRule_RuleCode UNIQUE (RuleCode); "
                "IX_DataQualityRule_Group (RuleGroupCode, IsActive); IX_DataQualityRule_Object (ObjectName, IsActive)."),
    Table("data_quality_result", "etl.DataQualityResult", (
        _id("DataQualityResultId"),
        _c("BatchId", "BIGINT"),
        _c("PackageExecutionId", "BIGINT"),
        _c("ObjectName", "STRING", False),
        _c("RuleCode", "STRING", False),
        _c("MeasuredValue", "DECIMAL(18,4)"),
        _c("ThresholdValue", "DECIMAL(18,4)"),
        _c("RowsEvaluated", "BIGINT"),
        _c("ResultStatus", "STRING"),
        _c("RegionCode", "STRING"),
        _c("DetailText", "STRING"),
        _ts("EvaluatedAtUtc"),
    ), checks={"CK_DataQualityResult_Status": "ResultStatus IS NULL OR ResultStatus IN (" + _inList(DQ_RESULT_STATUSES) + ")"},
        comment="Legacy etl.DataQualityResult. PK (DataQualityResultId); IX_DataQualityResult_Batch (BatchId, RuleCode); "
                "IX_DataQualityResult_Object (ObjectName, EvaluatedAtUtc DESC)."),
    Table("data_quality_rule_exception", "etl.DataQualityRuleException", (
        _id("RuleExceptionId", "INT"),
        _c("RuleCode", "STRING", False),
        _c("ObjectName", "STRING"),
        _c("RegionCode", "STRING"),
        _c("EffectiveFrom", "DATE", False),
        _c("EffectiveTo", "DATE", False),
        _c("Reason", "STRING", False),
        _c("ApprovedBy", "STRING", False),
        _ts("CreatedAtUtc"),
    ), checks={"CK_DataQualityRuleException_Window": "EffectiveTo >= EffectiveFrom"},
        comment="Legacy etl.DataQualityRuleException. PK (RuleExceptionId); "
                "IX_DataQualityRuleException_Rule (RuleCode, EffectiveFrom, EffectiveTo)."),
    # ------------------------------------------------------------------ 06_tables_reconciliation.sql
    Table("reconciliation_result", "etl.ReconciliationResult", (
        _id("ReconciliationResultId"),
        _c("BatchId", "BIGINT"),
        _c("ReconciliationName", "STRING", False),
        _c("ObjectName", "STRING"),
        _c("SourceKey", "STRING"),
        _c("LedgerCode", "STRING"),
        _c("AccountingPeriod", "STRING"),
        _c("AccountCode", "STRING"),
        _c("RegionCode", "STRING"),
        _c("SourceAmount", "DECIMAL(19,4)"),
        _c("TargetAmount", "DECIMAL(19,4)"),
        _c("VarianceAmount", "DECIMAL(19,4)"),
        _c("VarianceStatus", "STRING"),
        _c("ExplanationCode", "STRING"),
        _ts("EvaluatedAtUtc"),
    ), comment="Legacy etl.ReconciliationResult. PK (ReconciliationResultId); "
               "IX_ReconciliationResult_Batch (BatchId, VarianceStatus); IX_ReconciliationResult_Period (AccountingPeriod, VarianceStatus)."),
    # ------------------------------------------------------------------ 07_tables_operations.sql
    Table("operator_notification", "etl.OperatorNotification", (
        _id("OperatorNotificationId"),
        _c("BatchId", "BIGINT"),
        _c("NotificationTypeCode", "STRING", False),
        _c("Severity", "STRING", False, "'INFO'"),
        _c("Subject", "STRING", False),
        _c("Body", "STRING"),
        _c("ObjectName", "STRING"),
        _ts("RaisedAtUtc"),
        _c("IsAcknowledged", "BOOLEAN", False, "false"),
        _c("AcknowledgedBy", "STRING"),
        _ts("AcknowledgedAtUtc", True),
    ), checks={"CK_OperatorNotification_Severity": "Severity IN ('INFO', 'WARNING', 'CRITICAL')"},
        comment="Legacy etl.OperatorNotification. PK (OperatorNotificationId); IX_OperatorNotification_Open (IsAcknowledged, RaisedAtUtc DESC)."),
    Table("batch_hold", "etl.BatchHold", (
        _id("BatchHoldId", "INT"),
        _c("HoldReasonCode", "STRING", False),
        _c("HoldDetail", "STRING"),
        _c("BatchType", "STRING"),
        _ts("RaisedAtUtc"),
        _c("IsCleared", "BOOLEAN", False, "false"),
        _c("ClearedBy", "STRING"),
        _ts("ClearedAtUtc", True),
    ), comment="Legacy etl.BatchHold. PK (BatchHoldId); IX_BatchHold_Open (IsCleared, RaisedAtUtc DESC)."),
    Table("batch_step_rerun_request", "etl.BatchStepRerunRequest", (
        _id("RerunRequestId"),
        _c("BatchId", "BIGINT", False),
        _c("BatchStepId", "BIGINT"),
        _c("PackageName", "STRING", False),
        _c("AttemptNumber", "INT", False, "1"),
        _ts("RequestedAtUtc"),
        _c("RequestStatus", "STRING", False, "'Requested'"),
        _ts("CompletedAtUtc", True),
        _c("ResultStatus", "STRING"),
    ), checks={"CK_BatchStepRerunRequest_Status": "RequestStatus IN ('Requested', 'Running', 'Completed', 'Abandoned')"},
        comment="Legacy etl.BatchStepRerunRequest. PK (RerunRequestId); IX_BatchStepRerunRequest_Batch (BatchId, RequestStatus)."),
    Table("batch_archive", "etl.BatchArchive", (
        _id("BatchArchiveId"),
        _c("BatchId", "BIGINT", False),
        _c("BatchType", "STRING"),
        _c("BatchStatus", "STRING"),
        _ts("StartedAtUtc", True),
        _ts("EndedAtUtc", True),
        _c("StepCount", "INT"),
        _c("FailedStepCount", "INT"),
        _ts("ArchivedAtUtc"),
    ), comment="Legacy etl.BatchArchive. PK (BatchArchiveId); UQ_BatchArchive_Batch UNIQUE (BatchId)."),
    Table("maintenance_log", "etl.MaintenanceLog", (
        _id("MaintenanceLogId"),
        _c("TaskName", "STRING", False),
        _c("DetailText", "STRING"),
        _ts("RecordedAtUtc"),
    ), comment="Legacy etl.MaintenanceLog. PK (MaintenanceLogId); IX_MaintenanceLog_Task (TaskName, RecordedAtUtc DESC)."),
    Table("purge_audit", "etl.PurgeAudit", (
        _id("PurgeAuditId"),
        _c("BatchId", "BIGINT"),
        _c("SchemaName", "STRING", False),
        _c("TableName", "STRING", False),
        _c("CutoffDate", "DATE"),
        _c("RowsDeleted", "BIGINT", False, "0"),
        _ts("PurgedAtUtc"),
    ), comment="Legacy etl.PurgeAudit. PK (PurgeAuditId)."),
    Table("preflight_result", "etl.PreflightResult", (
        _id("PreflightResultId"),
        _c("BatchId", "BIGINT"),
        _c("CheckName", "STRING", False),
        _c("CheckStatus", "STRING", False),
        _c("DetailText", "STRING"),
        _ts("CheckedAtUtc"),
    ), checks={"CK_PreflightResult_Status": "CheckStatus IN ('OK', 'WARNING', 'FAILED')"},
        comment="Legacy etl.PreflightResult. PK (PreflightResultId); IX_PreflightResult_Check (CheckName, CheckedAtUtc DESC)."),
    Table("load_volume_history", "etl.LoadVolumeHistory", (
        _id("LoadVolumeHistoryId"),
        _c("VolumeMountPoint", "STRING", False),
        _c("LoadDate", "DATE", False),
        _c("BytesWritten", "BIGINT"),
        _c("FreePercent", "DECIMAL(9,4)"),
    ), comment="Legacy etl.LoadVolumeHistory. PK (LoadVolumeHistoryId); UQ_LoadVolumeHistory UNIQUE (VolumeMountPoint, LoadDate)."),
    Table("inbound_file_register", "etl.InboundFileRegister", (
        _id("InboundFileId"),
        _c("FeedCode", "STRING", False),
        _c("FileName", "STRING", False),
        _c("FilePath", "STRING", False),
        _c("FileSizeBytes", "BIGINT"),
        _c("FileHash", "STRING"),
        _ts("ReceivedAtUtc"),
        _c("StructuralCheckStatus", "STRING"),
        _c("BadFileCount", "INT", False, "0"),
        _c("ProcessingStatus", "STRING", False, "'Received'"),
        _ts("ProcessedAtUtc", True),
        _c("IsQuarantined", "BOOLEAN", False, "false"),
        _ts("QuarantinedAtUtc", True),
        _c("QuarantineReason", "STRING"),
        _c("IsArchived", "BOOLEAN", False, "false"),
        _c("ArchivePath", "STRING"),
        _ts("ArchivedAtUtc", True),
    ), checks={"CK_InboundFileRegister_Status": "ProcessingStatus IN ('Received', 'Screened', 'Loaded', 'Quarantined', 'Failed')"},
        comment="Legacy etl.InboundFileRegister. PK (InboundFileId); IX_InboundFileRegister_Feed (FeedCode, ReceivedAtUtc DESC); "
                "IX_InboundFileRegister_Archive (IsArchived, ArchivedAtUtc)."),
    Table("file_ingestion_log", "etl.FileIngestionLog", (
        _id("FileIngestionLogId"),
        _c("PackageExecutionId", "BIGINT"),
        _c("ObjectName", "STRING", False),
        _c("FileName", "STRING", False),
        _c("FilePath", "STRING"),
        _ts("ReceivedAtUtc"),
        _ts("CompletedAtUtc", True),
        _c("DetailRowCount", "BIGINT"),
        _c("RejectRowCount", "BIGINT"),
        _c("Status", "STRING", False, "'Received'"),
    ), comment="Legacy etl.FileIngestionLog. PK (FileIngestionLogId); IX_FileIngestionLog_File (FileName, ReceivedAtUtc DESC)."),
    Table("file_control_total", "etl.FileControlTotal", (
        _id("FileControlTotalId"),
        _c("FileName", "STRING", False),
        _c("FeedCode", "STRING"),
        _c("ExpectedRowCount", "BIGINT"),
        _c("ExpectedAmountTotal", "DECIMAL(19,4)"),
        _c("BusinessDate", "DATE"),
        _ts("ReceivedAtUtc"),
    ), comment="Legacy etl.FileControlTotal. PK (FileControlTotalId); UQ_FileControlTotal_File UNIQUE (FileName)."),
    Table("archive_expiry_list", "etl.ArchiveExpiryList", (
        _id("ArchiveExpiryListId"),
        _c("FileName", "STRING", False),
        _c("ArchivePath", "STRING", False),
        _ts("ArchivedAtUtc", True),
        _ts("ListedAtUtc"),
        _c("IsDeleted", "BOOLEAN", False, "false"),
        _ts("DeletedAtUtc", True),
    ), comment="Legacy etl.ArchiveExpiryList. PK (ArchiveExpiryListId); UQ_ArchiveExpiryList_Path UNIQUE (ArchivePath)."),
    Table("staging_table_register", "etl.StagingTableRegister", (
        _id("StagingTableId", "INT"),
        _c("SchemaName", "STRING", False),
        _c("TableName", "STRING", False),
        _c("LoadDateColumn", "STRING"),
        _c("RetentionDays", "INT"),
        _c("IsPurgeEligible", "BOOLEAN", False, "true"),
        _c("ApproximateRowCount", "BIGINT"),
    ), comment="Legacy etl.StagingTableRegister. PK (StagingTableId); UQ_StagingTableRegister UNIQUE (SchemaName, TableName)."),
    Table("row_count_tolerance", "etl.RowCountTolerance", (
        _id("RowCountToleranceId", "INT"),
        _c("ObjectName", "STRING", False),
        _c("TolerancePercent", "DECIMAL(9,4)", False, "0"),
        _c("AbsoluteTolerance", "BIGINT"),
        _c("ExplanationCode", "STRING"),
        _c("ApprovedBy", "STRING"),
    ), comment="Legacy etl.RowCountTolerance. PK (RowCountToleranceId); UQ_RowCountTolerance_Object UNIQUE (ObjectName)."),
    Table("required_configuration_key", "etl.RequiredConfigurationKey", (
        _id("RequiredConfigurationKeyId", "INT"),
        _c("ConfigurationKey", "STRING", False),
        _c("EnvironmentCode", "STRING"),
        _c("IsMandatory", "BOOLEAN", False, "true"),
        _c("Description", "STRING"),
    ), comment="Legacy etl.RequiredConfigurationKey. PK (RequiredConfigurationKeyId); UQ_RequiredConfigurationKey UNIQUE (ConfigurationKey, EnvironmentCode)."),
    Table("region_period_status", "etl.RegionPeriodStatus", (
        _id("RegionPeriodStatusId", "INT"),
        _c("RegionCode", "STRING", False),
        _c("FiscalCalendarCode", "STRING", False),
        _c("OpenPeriodKey", "STRING", False),
        _c("VatRegimeCode", "STRING"),
        _c("LastClosedPeriodKey", "STRING"),
        _ts("UpdatedAtUtc"),
    ), comment="Legacy etl.RegionPeriodStatus. PK (RegionPeriodStatusId); UQ_RegionPeriodStatus_Region UNIQUE (RegionCode)."),
    Table("fx_rate_availability", "etl.FxRateAvailability", (
        _id("FxRateAvailabilityId"),
        _c("RateDate", "DATE", False),
        _c("RegionCode", "STRING"),
        _c("RateCount", "INT", False, "0"),
        _c("IsComplete", "BOOLEAN", False, "false"),
        _ts("CheckedAtUtc"),
    ), comment="Legacy etl.FxRateAvailability. PK (FxRateAvailabilityId); UQ_FxRateAvailability UNIQUE (RateDate, RegionCode)."),
    Table("supplier_scoring_weight", "etl.SupplierScoringWeight", (
        _id("SupplierScoringWeightId", "INT"),
        _c("RegionCode", "STRING", False),
        _c("OnTimeWeight", "DECIMAL(9,4)", False),
        _c("AccuracyWeight", "DECIMAL(9,4)", False),
        _c("PriceWeight", "DECIMAL(9,4)", False),
        _c("QualityWeight", "DECIMAL(9,4)", False),
        _c("EffectiveFrom", "DATE", False, "DATE'2013-01-01'"),
    ), comment="Legacy etl.SupplierScoringWeight. PK (SupplierScoringWeightId); UQ_SupplierScoringWeight_Region UNIQUE (RegionCode)."),
    Table("fact_rekey_queue", "etl.FactRekeyQueue", (
        _id("FactRekeyQueueId"),
        _c("BatchId", "BIGINT"),
        _c("FactTableName", "STRING", False),
        _c("DimensionName", "STRING", False),
        _c("BusinessKey", "STRING", False),
        _c("InferredKey", "INT"),
        _c("ResolvedKey", "INT"),
        _c("AttemptCount", "INT", False, "0"),
        _ts("QueuedAtUtc"),
        _ts("ResolvedAtUtc", True),
        _c("QueueStatus", "STRING", False, "'Queued'"),
    ), checks={"CK_FactRekeyQueue_Status": "QueueStatus IN ('Queued', 'Resolved', 'Abandoned')"},
        comment="Legacy etl.FactRekeyQueue. PK (FactRekeyQueueId); IX_FactRekeyQueue_Open (QueueStatus, DimensionName)."),
)

TABLES_BY_NAME: Dict[str, Table] = {t.name: t for t in TABLES}

# Alias views: names used in the shared migration contract that differ from the legacy table name.
TABLE_ALIASES: Dict[str, str] = {"row_count_log": "row_count_audit"}


def tableFor(legacyName: str) -> Table:
    """Return the table spec for a legacy ``etl.X`` name (case-insensitive)."""
    wanted = legacyName.lower()
    for t in TABLES:
        if t.legacyName.lower() == wanted:
            return t
    raise KeyError(legacyName)
