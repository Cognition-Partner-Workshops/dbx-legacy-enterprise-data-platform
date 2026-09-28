-- Delta DDL for the WWI ETL control framework (schema etl).
-- GENERATED from dbx_etl_common.schema by databricks/common/tools/render_sql.py - do not edit by hand.
-- Legacy sources: sqlserver/control/02_tables_control_framework.sql, 04_tables_data_quality.sql,
--                 06_tables_reconciliation.sql, 07_tables_operations.sql (+ tables created inline by procedures).
-- ${catalog} is the bundle variable (default wwi_${bundle.target}); PK/UQ/FK/index intent is in the table comments.

CREATE SCHEMA IF NOT EXISTS ${catalog}.etl;

CREATE TABLE IF NOT EXISTS ${catalog}.etl.source_system
(
    SourceSystemId BIGINT GENERATED ALWAYS AS IDENTITY,
    SourceSystemCode STRING NOT NULL,
    SourceSystemName STRING NOT NULL,
    Platform STRING NOT NULL,
    RegionCode STRING,
    ConnectionParameter STRING,
    DefaultTimeZone STRING NOT NULL DEFAULT 'UTC',
    IsActive BOOLEAN NOT NULL DEFAULT true,
    ValidFrom TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.SourceSystem. PK (SourceSystemId); UQ_SourceSystem_Code UNIQUE (SourceSystemCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.batch
(
    BatchId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchName STRING NOT NULL,
    BatchType STRING NOT NULL,
    BusinessDate DATE NOT NULL,
    EnvironmentCode STRING NOT NULL,
    StartedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CompletedAtUtc TIMESTAMP,
    Status STRING NOT NULL DEFAULT 'Running',
    RestartFromStep STRING,
    InitiatedBy STRING NOT NULL DEFAULT current_user(),
    Notes STRING,
    CONSTRAINT CK_Batch_Status CHECK (Status IN ('Running', 'Succeeded', 'Failed', 'Cancelled', 'SucceededWithWarnings'))
)
USING DELTA
COMMENT 'Legacy etl.Batch. PK (BatchId).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.batch_step
(
    BatchStepId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT NOT NULL,
    StepName STRING NOT NULL,
    StepSequence INT NOT NULL,
    StepGroup STRING COMMENT 'Extract, Stage, Dimension, Fact, Aggregate, Maintenance',
    StartedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CompletedAtUtc TIMESTAMP,
    Status STRING NOT NULL DEFAULT 'Running',
    AttemptNumber INT NOT NULL DEFAULT 1,
    CONSTRAINT CK_BatchStep_Status CHECK (Status IN ('Running', 'Succeeded', 'Failed', 'Skipped'))
)
USING DELTA
COMMENT 'Legacy etl.BatchStep. PK (BatchStepId); FK_BatchStep_Batch (BatchId) -> etl.batch; IX_BatchStep_BatchId (BatchId, StepSequence).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.package_execution
(
    PackageExecutionId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    BatchStepId BIGINT,
    PackageName STRING NOT NULL,
    ProjectName STRING,
    MachineName STRING NOT NULL DEFAULT 'databricks',
    ExecutedBy STRING NOT NULL DEFAULT current_user(),
    StartedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CompletedAtUtc TIMESTAMP,
    DurationSeconds BIGINT GENERATED ALWAYS AS (unix_timestamp(CompletedAtUtc) - unix_timestamp(StartedAtUtc)),
    Status STRING NOT NULL DEFAULT 'Running',
    RowsRead BIGINT,
    RowsInserted BIGINT,
    RowsUpdated BIGINT,
    RowsDeleted BIGINT,
    RowsRejected BIGINT,
    WatermarkFrom STRING,
    WatermarkTo STRING,
    AttemptNumber INT NOT NULL DEFAULT 1,
    CONSTRAINT CK_PackageExecution_Status CHECK (Status IN ('Running', 'Succeeded', 'Failed', 'Cancelled'))
)
USING DELTA
COMMENT 'Legacy etl.PackageExecution. PK (PackageExecutionId); FK_PackageExecution_Batch (BatchId) -> etl.batch; FK_PackageExecution_BatchStep (BatchStepId) -> etl.batch_step; IX_PackageExecution_Batch (BatchId, PackageName) INCLUDE (Status, RowsInserted); IX_PackageExecution_Package (PackageName, StartedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.watermark
(
    WatermarkId BIGINT GENERATED ALWAYS AS IDENTITY,
    SourceSystemCode STRING NOT NULL,
    ObjectName STRING NOT NULL,
    WatermarkType STRING NOT NULL COMMENT 'Timestamp, NumericKey, DateWindow',
    LastValue STRING,
    PreviousValue STRING,
    LastLoadedAtUtc TIMESTAMP,
    LastPackageExecutionId BIGINT,
    LookbackMinutes INT NOT NULL DEFAULT 0,
    IsLocked BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT CK_Watermark_Type CHECK (WatermarkType IN ('Timestamp', 'NumericKey', 'DateWindow'))
)
USING DELTA
COMMENT 'Legacy etl.Watermark. PK (WatermarkId); UQ_Watermark_Object UNIQUE (SourceSystemCode, ObjectName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.row_count_audit
(
    RowCountAuditId BIGINT GENERATED ALWAYS AS IDENTITY,
    PackageExecutionId BIGINT NOT NULL,
    ObjectName STRING NOT NULL,
    SourceRowCount BIGINT,
    TargetRowCount BIGINT,
    InsertRowCount BIGINT,
    UpdateRowCount BIGINT,
    DeleteRowCount BIGINT,
    RejectRowCount BIGINT,
    VarianceRowCount BIGINT GENERATED ALWAYS AS (coalesce(SourceRowCount, 0) - coalesce(TargetRowCount, 0) - coalesce(RejectRowCount, 0)),
    RecordedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.RowCountAudit (also exposed as view etl.row_count_log). PK (RowCountAuditId); FK_RowCountAudit_PackageExecution (PackageExecutionId) -> etl.package_execution; IX_RowCountAudit_Object (ObjectName, RecordedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.error_log
(
    ErrorLogId BIGINT GENERATED ALWAYS AS IDENTITY,
    PackageExecutionId BIGINT,
    BatchId BIGINT,
    ErrorSeverity STRING NOT NULL DEFAULT 'Error',
    ErrorCode INT,
    ErrorNumber INT,
    ErrorState INT,
    ErrorLine INT,
    SourceName STRING,
    SourceComponent STRING,
    ProcedureName STRING,
    ErrorDescription STRING,
    LoggedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT CK_ErrorLog_Severity CHECK (ErrorSeverity IN ('Information', 'Warning', 'Error', 'Critical'))
)
USING DELTA
COMMENT 'Legacy etl.ErrorLog. PK (ErrorLogId); IX_ErrorLog_Batch (BatchId, LoggedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.rejected_record
(
    RejectedRecordId BIGINT GENERATED ALWAYS AS IDENTITY,
    PackageExecutionId BIGINT,
    BatchId BIGINT,
    SourceSystemCode STRING,
    ObjectName STRING NOT NULL,
    BusinessKey STRING,
    RejectReasonCode STRING NOT NULL,
    RejectReason STRING,
    RejectStage STRING NOT NULL COMMENT 'Extract, Stage, Screen, Dimension, Fact',
    SsisErrorCode INT,
    SsisErrorColumn INT,
    RecordPayload STRING COMMENT 'delimited or JSON copy of the offending row',
    IsReprocessed BOOLEAN NOT NULL DEFAULT false,
    ReprocessedAtUtc TIMESTAMP,
    LoggedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.RejectedRecord. PK (RejectedRecordId); IX_RejectedRecord_Object (ObjectName, LoggedAtUtc DESC) INCLUDE (RejectReasonCode, IsReprocessed).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.configuration
(
    ConfigurationId BIGINT GENERATED ALWAYS AS IDENTITY,
    ConfigurationKey STRING NOT NULL,
    EnvironmentCode STRING NOT NULL DEFAULT 'ALL',
    ConfigurationValue STRING NOT NULL,
    ValueDataType STRING NOT NULL DEFAULT 'String',
    Description STRING,
    IsSensitive BOOLEAN NOT NULL DEFAULT false,
    ModifiedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT CK_Configuration_DataType CHECK (ValueDataType IN ('String', 'Int', 'Decimal', 'Boolean', 'Date'))
)
USING DELTA
COMMENT 'Legacy etl.Configuration. PK (ConfigurationId); UQ_Configuration_Key UNIQUE (ConfigurationKey, EnvironmentCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.reconciliation_exemption
(
    ExemptionId BIGINT GENERATED ALWAYS AS IDENTITY,
    ObjectName STRING NOT NULL,
    Reason STRING NOT NULL
)
USING DELTA
COMMENT 'Legacy etl.ReconciliationExemption. PK (ExemptionId); UQ_ReconciliationExemption UNIQUE (ObjectName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.package_dependency
(
    PackageDependencyId BIGINT GENERATED ALWAYS AS IDENTITY,
    PackageName STRING NOT NULL,
    DependsOnPackage STRING NOT NULL,
    DependencyType STRING NOT NULL DEFAULT 'Hard',
    CONSTRAINT CK_PackageDependency_Type CHECK (DependencyType IN ('Hard', 'Soft'))
)
USING DELTA
COMMENT 'Legacy etl.PackageDependency. PK (PackageDependencyId); UQ_PackageDependency UNIQUE (PackageName, DependsOnPackage).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.rejected_record_staging
(
    RejectedRecordStagingId BIGINT GENERATED ALWAYS AS IDENTITY,
    LoadTag STRING NOT NULL,
    PackageExecutionId BIGINT,
    BatchId BIGINT,
    SourceSystemCode STRING,
    ObjectName STRING NOT NULL,
    BusinessKey STRING,
    RejectReasonCode STRING NOT NULL,
    RejectReason STRING,
    RejectStage STRING,
    SsisErrorCode INT,
    SsisErrorColumn INT,
    RecordPayload STRING,
    LandedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.RejectedRecordStaging (created by etl.usp_LogRejectedRecordSet). PK (RejectedRecordStagingId); IX_RejectedRecordStaging_LoadTag (LoadTag).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.control_purge_audit
(
    ControlPurgeAuditId BIGINT GENERATED ALWAYS AS IDENTITY,
    PurgeRunAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    TableName STRING NOT NULL,
    CutoffUtc TIMESTAMP,
    RowsDeleted BIGINT NOT NULL,
    DurationSeconds INT
)
USING DELTA
COMMENT 'Legacy etl.ControlPurgeAudit (created by etl.usp_PurgeControlHistory). PK (ControlPurgeAuditId).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.data_quality_rule
(
    DataQualityRuleId BIGINT GENERATED ALWAYS AS IDENTITY,
    RuleCode STRING NOT NULL,
    RuleGroupCode STRING NOT NULL,
    ObjectName STRING NOT NULL,
    RuleName STRING NOT NULL,
    RuleExpression STRING NOT NULL COMMENT 'WHERE clause selecting the offending rows',
    DimensionCode STRING NOT NULL DEFAULT 'Validity',
    SeverityCode STRING NOT NULL DEFAULT 'WARN',
    ThresholdValue DECIMAL(18,4) NOT NULL DEFAULT 0,
    RegionCode STRING,
    SourceSystemCode STRING,
    IsActive BOOLEAN NOT NULL DEFAULT true,
    OwnerName STRING,
    Notes STRING,
    CreatedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    UpdatedAtUtc TIMESTAMP,
    CONSTRAINT CK_DataQualityRule_Severity CHECK (SeverityCode IN ('INFO', 'WARN', 'FAIL')),
    CONSTRAINT CK_DataQualityRule_Threshold CHECK (ThresholdValue >= 0)
)
USING DELTA
COMMENT 'Legacy etl.DataQualityRule. PK (DataQualityRuleId); UQ_DataQualityRule_RuleCode UNIQUE (RuleCode); IX_DataQualityRule_Group (RuleGroupCode, IsActive); IX_DataQualityRule_Object (ObjectName, IsActive).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.data_quality_result
(
    DataQualityResultId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    PackageExecutionId BIGINT,
    ObjectName STRING NOT NULL,
    RuleCode STRING NOT NULL,
    MeasuredValue DECIMAL(18,4),
    ThresholdValue DECIMAL(18,4),
    RowsEvaluated BIGINT,
    ResultStatus STRING,
    RegionCode STRING,
    DetailText STRING,
    EvaluatedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT CK_DataQualityResult_Status CHECK (ResultStatus IS NULL OR ResultStatus IN ('Passed', 'Warned', 'Failed', 'NotEvaluated'))
)
USING DELTA
COMMENT 'Legacy etl.DataQualityResult. PK (DataQualityResultId); IX_DataQualityResult_Batch (BatchId, RuleCode); IX_DataQualityResult_Object (ObjectName, EvaluatedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.data_quality_rule_exception
(
    RuleExceptionId BIGINT GENERATED ALWAYS AS IDENTITY,
    RuleCode STRING NOT NULL,
    ObjectName STRING,
    RegionCode STRING,
    EffectiveFrom DATE NOT NULL,
    EffectiveTo DATE NOT NULL,
    Reason STRING NOT NULL,
    ApprovedBy STRING NOT NULL,
    CreatedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT CK_DataQualityRuleException_Window CHECK (EffectiveTo >= EffectiveFrom)
)
USING DELTA
COMMENT 'Legacy etl.DataQualityRuleException. PK (RuleExceptionId); IX_DataQualityRuleException_Rule (RuleCode, EffectiveFrom, EffectiveTo).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.reconciliation_result
(
    ReconciliationResultId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    ReconciliationName STRING NOT NULL,
    ObjectName STRING,
    SourceKey STRING,
    LedgerCode STRING,
    AccountingPeriod STRING,
    AccountCode STRING,
    RegionCode STRING,
    SourceAmount DECIMAL(19,4),
    TargetAmount DECIMAL(19,4),
    VarianceAmount DECIMAL(19,4),
    VarianceStatus STRING,
    ExplanationCode STRING,
    EvaluatedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.ReconciliationResult. PK (ReconciliationResultId); IX_ReconciliationResult_Batch (BatchId, VarianceStatus); IX_ReconciliationResult_Period (AccountingPeriod, VarianceStatus).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.operator_notification
(
    OperatorNotificationId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    NotificationTypeCode STRING NOT NULL,
    Severity STRING NOT NULL DEFAULT 'INFO',
    Subject STRING NOT NULL,
    Body STRING,
    ObjectName STRING,
    RaisedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    IsAcknowledged BOOLEAN NOT NULL DEFAULT false,
    AcknowledgedBy STRING,
    AcknowledgedAtUtc TIMESTAMP,
    CONSTRAINT CK_OperatorNotification_Severity CHECK (Severity IN ('INFO', 'WARNING', 'CRITICAL'))
)
USING DELTA
COMMENT 'Legacy etl.OperatorNotification. PK (OperatorNotificationId); IX_OperatorNotification_Open (IsAcknowledged, RaisedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.batch_hold
(
    BatchHoldId BIGINT GENERATED ALWAYS AS IDENTITY,
    HoldReasonCode STRING NOT NULL,
    HoldDetail STRING,
    BatchType STRING,
    RaisedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    IsCleared BOOLEAN NOT NULL DEFAULT false,
    ClearedBy STRING,
    ClearedAtUtc TIMESTAMP
)
USING DELTA
COMMENT 'Legacy etl.BatchHold. PK (BatchHoldId); IX_BatchHold_Open (IsCleared, RaisedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.batch_step_rerun_request
(
    RerunRequestId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT NOT NULL,
    BatchStepId BIGINT,
    PackageName STRING NOT NULL,
    AttemptNumber INT NOT NULL DEFAULT 1,
    RequestedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    RequestStatus STRING NOT NULL DEFAULT 'Requested',
    CompletedAtUtc TIMESTAMP,
    ResultStatus STRING,
    CONSTRAINT CK_BatchStepRerunRequest_Status CHECK (RequestStatus IN ('Requested', 'Running', 'Completed', 'Abandoned'))
)
USING DELTA
COMMENT 'Legacy etl.BatchStepRerunRequest. PK (RerunRequestId); IX_BatchStepRerunRequest_Batch (BatchId, RequestStatus).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.batch_archive
(
    BatchArchiveId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT NOT NULL,
    BatchType STRING,
    BatchStatus STRING,
    StartedAtUtc TIMESTAMP,
    EndedAtUtc TIMESTAMP,
    StepCount INT,
    FailedStepCount INT,
    ArchivedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.BatchArchive. PK (BatchArchiveId); UQ_BatchArchive_Batch UNIQUE (BatchId).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.maintenance_log
(
    MaintenanceLogId BIGINT GENERATED ALWAYS AS IDENTITY,
    TaskName STRING NOT NULL,
    DetailText STRING,
    RecordedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.MaintenanceLog. PK (MaintenanceLogId); IX_MaintenanceLog_Task (TaskName, RecordedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.purge_audit
(
    PurgeAuditId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    SchemaName STRING NOT NULL,
    TableName STRING NOT NULL,
    CutoffDate DATE,
    RowsDeleted BIGINT NOT NULL DEFAULT 0,
    PurgedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.PurgeAudit. PK (PurgeAuditId).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.preflight_result
(
    PreflightResultId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    CheckName STRING NOT NULL,
    CheckStatus STRING NOT NULL,
    DetailText STRING,
    CheckedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CONSTRAINT CK_PreflightResult_Status CHECK (CheckStatus IN ('OK', 'WARNING', 'FAILED'))
)
USING DELTA
COMMENT 'Legacy etl.PreflightResult. PK (PreflightResultId); IX_PreflightResult_Check (CheckName, CheckedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.load_volume_history
(
    LoadVolumeHistoryId BIGINT GENERATED ALWAYS AS IDENTITY,
    VolumeMountPoint STRING NOT NULL,
    LoadDate DATE NOT NULL,
    BytesWritten BIGINT,
    FreePercent DECIMAL(9,4)
)
USING DELTA
COMMENT 'Legacy etl.LoadVolumeHistory. PK (LoadVolumeHistoryId); UQ_LoadVolumeHistory UNIQUE (VolumeMountPoint, LoadDate).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.inbound_file_register
(
    InboundFileId BIGINT GENERATED ALWAYS AS IDENTITY,
    FeedCode STRING NOT NULL,
    FileName STRING NOT NULL,
    FilePath STRING NOT NULL,
    FileSizeBytes BIGINT,
    FileHash STRING,
    ReceivedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    StructuralCheckStatus STRING,
    BadFileCount INT NOT NULL DEFAULT 0,
    ProcessingStatus STRING NOT NULL DEFAULT 'Received',
    ProcessedAtUtc TIMESTAMP,
    IsQuarantined BOOLEAN NOT NULL DEFAULT false,
    QuarantinedAtUtc TIMESTAMP,
    QuarantineReason STRING,
    IsArchived BOOLEAN NOT NULL DEFAULT false,
    ArchivePath STRING,
    ArchivedAtUtc TIMESTAMP,
    CONSTRAINT CK_InboundFileRegister_Status CHECK (ProcessingStatus IN ('Received', 'Screened', 'Loaded', 'Quarantined', 'Failed'))
)
USING DELTA
COMMENT 'Legacy etl.InboundFileRegister. PK (InboundFileId); IX_InboundFileRegister_Feed (FeedCode, ReceivedAtUtc DESC); IX_InboundFileRegister_Archive (IsArchived, ArchivedAtUtc).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.file_ingestion_log
(
    FileIngestionLogId BIGINT GENERATED ALWAYS AS IDENTITY,
    PackageExecutionId BIGINT,
    ObjectName STRING NOT NULL,
    FileName STRING NOT NULL,
    FilePath STRING,
    ReceivedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    CompletedAtUtc TIMESTAMP,
    DetailRowCount BIGINT,
    RejectRowCount BIGINT,
    Status STRING NOT NULL DEFAULT 'Received'
)
USING DELTA
COMMENT 'Legacy etl.FileIngestionLog. PK (FileIngestionLogId); IX_FileIngestionLog_File (FileName, ReceivedAtUtc DESC).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.file_control_total
(
    FileControlTotalId BIGINT GENERATED ALWAYS AS IDENTITY,
    FileName STRING NOT NULL,
    FeedCode STRING,
    ExpectedRowCount BIGINT,
    ExpectedAmountTotal DECIMAL(19,4),
    BusinessDate DATE,
    ReceivedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.FileControlTotal. PK (FileControlTotalId); UQ_FileControlTotal_File UNIQUE (FileName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.archive_expiry_list
(
    ArchiveExpiryListId BIGINT GENERATED ALWAYS AS IDENTITY,
    FileName STRING NOT NULL,
    ArchivePath STRING NOT NULL,
    ArchivedAtUtc TIMESTAMP,
    ListedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    IsDeleted BOOLEAN NOT NULL DEFAULT false,
    DeletedAtUtc TIMESTAMP
)
USING DELTA
COMMENT 'Legacy etl.ArchiveExpiryList. PK (ArchiveExpiryListId); UQ_ArchiveExpiryList_Path UNIQUE (ArchivePath).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.staging_table_register
(
    StagingTableId BIGINT GENERATED ALWAYS AS IDENTITY,
    SchemaName STRING NOT NULL,
    TableName STRING NOT NULL,
    LoadDateColumn STRING,
    RetentionDays INT,
    IsPurgeEligible BOOLEAN NOT NULL DEFAULT true,
    ApproximateRowCount BIGINT
)
USING DELTA
COMMENT 'Legacy etl.StagingTableRegister. PK (StagingTableId); UQ_StagingTableRegister UNIQUE (SchemaName, TableName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.row_count_tolerance
(
    RowCountToleranceId BIGINT GENERATED ALWAYS AS IDENTITY,
    ObjectName STRING NOT NULL,
    TolerancePercent DECIMAL(9,4) NOT NULL DEFAULT 0,
    AbsoluteTolerance BIGINT,
    ExplanationCode STRING,
    ApprovedBy STRING
)
USING DELTA
COMMENT 'Legacy etl.RowCountTolerance. PK (RowCountToleranceId); UQ_RowCountTolerance_Object UNIQUE (ObjectName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.required_configuration_key
(
    RequiredConfigurationKeyId BIGINT GENERATED ALWAYS AS IDENTITY,
    ConfigurationKey STRING NOT NULL,
    EnvironmentCode STRING,
    IsMandatory BOOLEAN NOT NULL DEFAULT true,
    Description STRING
)
USING DELTA
COMMENT 'Legacy etl.RequiredConfigurationKey. PK (RequiredConfigurationKeyId); UQ_RequiredConfigurationKey UNIQUE (ConfigurationKey, EnvironmentCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.region_period_status
(
    RegionPeriodStatusId BIGINT GENERATED ALWAYS AS IDENTITY,
    RegionCode STRING NOT NULL,
    FiscalCalendarCode STRING NOT NULL,
    OpenPeriodKey STRING NOT NULL,
    VatRegimeCode STRING,
    LastClosedPeriodKey STRING,
    UpdatedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.RegionPeriodStatus. PK (RegionPeriodStatusId); UQ_RegionPeriodStatus_Region UNIQUE (RegionCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.fx_rate_availability
(
    FxRateAvailabilityId BIGINT GENERATED ALWAYS AS IDENTITY,
    RateDate DATE NOT NULL,
    RegionCode STRING,
    RateCount INT NOT NULL DEFAULT 0,
    IsComplete BOOLEAN NOT NULL DEFAULT false,
    CheckedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Legacy etl.FxRateAvailability. PK (FxRateAvailabilityId); UQ_FxRateAvailability UNIQUE (RateDate, RegionCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.supplier_scoring_weight
(
    SupplierScoringWeightId BIGINT GENERATED ALWAYS AS IDENTITY,
    RegionCode STRING NOT NULL,
    OnTimeWeight DECIMAL(9,4) NOT NULL,
    AccuracyWeight DECIMAL(9,4) NOT NULL,
    PriceWeight DECIMAL(9,4) NOT NULL,
    QualityWeight DECIMAL(9,4) NOT NULL,
    EffectiveFrom DATE NOT NULL DEFAULT DATE'2013-01-01'
)
USING DELTA
COMMENT 'Legacy etl.SupplierScoringWeight. PK (SupplierScoringWeightId); UQ_SupplierScoringWeight_Region UNIQUE (RegionCode).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS ${catalog}.etl.fact_rekey_queue
(
    FactRekeyQueueId BIGINT GENERATED ALWAYS AS IDENTITY,
    BatchId BIGINT,
    FactTableName STRING NOT NULL,
    DimensionName STRING NOT NULL,
    BusinessKey STRING NOT NULL,
    InferredKey INT,
    ResolvedKey INT,
    AttemptCount INT NOT NULL DEFAULT 0,
    QueuedAtUtc TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    ResolvedAtUtc TIMESTAMP,
    QueueStatus STRING NOT NULL DEFAULT 'Queued',
    CONSTRAINT CK_FactRekeyQueue_Status CHECK (QueueStatus IN ('Queued', 'Resolved', 'Abandoned'))
)
USING DELTA
COMMENT 'Legacy etl.FactRekeyQueue. PK (FactRekeyQueueId); IX_FactRekeyQueue_Open (QueueStatus, DimensionName).'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
