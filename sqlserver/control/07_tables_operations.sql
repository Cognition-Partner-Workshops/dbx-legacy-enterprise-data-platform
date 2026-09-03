/*
    Objects       : the operational half of the etl control schema -
                    etl.OperatorNotification, etl.BatchHold,
                    etl.BatchStepRerunRequest, etl.BatchArchive,
                    etl.MaintenanceLog, etl.PurgeAudit, etl.PreflightResult,
                    etl.InboundFileRegister, etl.FileIngestionLog,
                    etl.FileControlTotal, etl.ArchiveExpiryList,
                    etl.LoadVolumeHistory, etl.StagingTableRegister,
                    etl.RowCountTolerance, etl.RequiredConfigurationKey,
                    etl.RegionPeriodStatus, etl.FxRateAvailability,
                    etl.SupplierScoringWeight, etl.FactRekeyQueue
    Deploy target : WWI_Staging (etl.FactRekeyQueue is also created in
                    WideWorldImportersDW, where the rekey procedure reads it)
    Deploy order  : after 02_tables_control_framework.sql
    Written by    : the 15_error_handling, 99_maintenance and 03_file_ingestion
                    packages, and the SQL Agent operational jobs
    Read by       : the same packages, the operational views, and the morning
                    operations report

    These grew one incident at a time rather than from a design: the notification
    table exists because nobody saw the 2013 failures until lunchtime, the hold
    table because a batch ran a volume out of space, the file register because a
    supplier sent the same file twice. They are grouped here rather than in
    02_tables_control_framework.sql so the core batch/audit tables stay legible.
*/

SET NOCOUNT ON;
GO

/* --------------------------------------------------------------------------
   Operations: notifications, holds, reruns
   -------------------------------------------------------------------------- */

IF OBJECT_ID(N'etl.OperatorNotification', N'U') IS NULL
BEGIN
    CREATE TABLE etl.OperatorNotification
    (
        OperatorNotificationId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId                 BIGINT                          NULL,
        NotificationTypeCode    NVARCHAR(40)                    NOT NULL,
        Severity                NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_OperatorNotification_Severity DEFAULT (N'INFO'),
        Subject                 NVARCHAR(400)                   NOT NULL,
        Body                    NVARCHAR(MAX)                   NULL,
        ObjectName              NVARCHAR(200)                   NULL,
        RaisedAtUtc             DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_OperatorNotification_RaisedAtUtc DEFAULT (SYSUTCDATETIME()),
        IsAcknowledged          BIT                             NOT NULL
            CONSTRAINT DF_OperatorNotification_IsAcknowledged DEFAULT (0),
        AcknowledgedBy          NVARCHAR(128)                   NULL,
        AcknowledgedAtUtc       DATETIME2(3)                    NULL,
        CONSTRAINT PK_OperatorNotification PRIMARY KEY CLUSTERED (OperatorNotificationId),
        CONSTRAINT CK_OperatorNotification_Severity
            CHECK (Severity IN (N'INFO', N'WARNING', N'CRITICAL'))
    );

    CREATE NONCLUSTERED INDEX IX_OperatorNotification_Open
        ON etl.OperatorNotification (IsAcknowledged, RaisedAtUtc DESC)
        INCLUDE (Severity, NotificationTypeCode, Subject);
END
GO

/*
    A hold stops the next batch from starting. It is cleared by an operator, not
    by the ETL, which is the whole point: the condition that raised it - a full
    volume, a source system mid-restore - is not something a retry fixes.
*/
IF OBJECT_ID(N'etl.BatchHold', N'U') IS NULL
BEGIN
    CREATE TABLE etl.BatchHold
    (
        BatchHoldId     INT             IDENTITY(1, 1)  NOT NULL,
        HoldReasonCode  NVARCHAR(40)                    NOT NULL,
        HoldDetail      NVARCHAR(1000)                  NULL,
        BatchType       NVARCHAR(30)                    NULL,
        RaisedAtUtc     DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_BatchHold_RaisedAtUtc DEFAULT (SYSUTCDATETIME()),
        IsCleared       BIT                             NOT NULL
            CONSTRAINT DF_BatchHold_IsCleared DEFAULT (0),
        ClearedBy       NVARCHAR(128)                   NULL,
        ClearedAtUtc    DATETIME2(3)                    NULL,
        CONSTRAINT PK_BatchHold PRIMARY KEY CLUSTERED (BatchHoldId)
    );

    CREATE NONCLUSTERED INDEX IX_BatchHold_Open ON etl.BatchHold (IsCleared, RaisedAtUtc DESC);
END
GO

IF OBJECT_ID(N'etl.BatchStepRerunRequest', N'U') IS NULL
BEGIN
    CREATE TABLE etl.BatchStepRerunRequest
    (
        RerunRequestId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId         BIGINT                          NOT NULL,
        BatchStepId     BIGINT                          NULL,
        PackageName     NVARCHAR(200)                   NOT NULL,
        AttemptNumber   INT                             NOT NULL
            CONSTRAINT DF_BatchStepRerunRequest_AttemptNumber DEFAULT (1),
        RequestedAtUtc  DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_BatchStepRerunRequest_RequestedAtUtc DEFAULT (SYSUTCDATETIME()),
        RequestStatus   NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_BatchStepRerunRequest_RequestStatus DEFAULT (N'Requested'),
        CompletedAtUtc  DATETIME2(3)                    NULL,
        ResultStatus    NVARCHAR(20)                    NULL,
        CONSTRAINT PK_BatchStepRerunRequest PRIMARY KEY CLUSTERED (RerunRequestId),
        CONSTRAINT CK_BatchStepRerunRequest_Status
            CHECK (RequestStatus IN (N'Requested', N'Running', N'Completed', N'Abandoned'))
    );

    CREATE NONCLUSTERED INDEX IX_BatchStepRerunRequest_Batch
        ON etl.BatchStepRerunRequest (BatchId, RequestStatus);
END
GO

/* --------------------------------------------------------------------------
   Maintenance
   -------------------------------------------------------------------------- */

IF OBJECT_ID(N'etl.BatchArchive', N'U') IS NULL
BEGIN
    CREATE TABLE etl.BatchArchive
    (
        BatchArchiveId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId         BIGINT                          NOT NULL,
        BatchType       NVARCHAR(30)                    NULL,
        BatchStatus     NVARCHAR(20)                    NULL,
        StartedAtUtc    DATETIME2(3)                    NULL,
        EndedAtUtc      DATETIME2(3)                    NULL,
        StepCount       INT                             NULL,
        FailedStepCount INT                             NULL,
        ArchivedAtUtc   DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_BatchArchive_ArchivedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_BatchArchive PRIMARY KEY CLUSTERED (BatchArchiveId),
        CONSTRAINT UQ_BatchArchive_Batch UNIQUE (BatchId)
    );
END
GO

IF OBJECT_ID(N'etl.MaintenanceLog', N'U') IS NULL
BEGIN
    CREATE TABLE etl.MaintenanceLog
    (
        MaintenanceLogId    BIGINT          IDENTITY(1, 1)  NOT NULL,
        TaskName            NVARCHAR(100)                   NOT NULL,
        DetailText          NVARCHAR(MAX)                   NULL,
        RecordedAtUtc       DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_MaintenanceLog_RecordedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_MaintenanceLog PRIMARY KEY CLUSTERED (MaintenanceLogId)
    );

    CREATE NONCLUSTERED INDEX IX_MaintenanceLog_Task
        ON etl.MaintenanceLog (TaskName, RecordedAtUtc DESC);
END
GO

IF OBJECT_ID(N'etl.PurgeAudit', N'U') IS NULL
BEGIN
    CREATE TABLE etl.PurgeAudit
    (
        PurgeAuditId    BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId         BIGINT                          NULL,
        SchemaName      SYSNAME                         NOT NULL,
        TableName       SYSNAME                         NOT NULL,
        CutoffDate      DATE                            NULL,
        RowsDeleted     BIGINT                          NOT NULL
            CONSTRAINT DF_PurgeAudit_RowsDeleted DEFAULT (0),
        PurgedAtUtc     DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_PurgeAudit_PurgedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_PurgeAudit PRIMARY KEY CLUSTERED (PurgeAuditId)
    );
END
GO

IF OBJECT_ID(N'etl.PreflightResult', N'U') IS NULL
BEGIN
    CREATE TABLE etl.PreflightResult
    (
        PreflightResultId   BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId             BIGINT                          NULL,
        CheckName           NVARCHAR(100)                   NOT NULL,
        CheckStatus         NVARCHAR(20)                    NOT NULL,
        DetailText          NVARCHAR(2000)                  NULL,
        CheckedAtUtc        DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_PreflightResult_CheckedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_PreflightResult PRIMARY KEY CLUSTERED (PreflightResultId),
        CONSTRAINT CK_PreflightResult_Status
            CHECK (CheckStatus IN (N'OK', N'WARNING', N'FAILED'))
    );

    CREATE NONCLUSTERED INDEX IX_PreflightResult_Check
        ON etl.PreflightResult (CheckName, CheckedAtUtc DESC);
END
GO

IF OBJECT_ID(N'etl.LoadVolumeHistory', N'U') IS NULL
BEGIN
    CREATE TABLE etl.LoadVolumeHistory
    (
        LoadVolumeHistoryId BIGINT          IDENTITY(1, 1)  NOT NULL,
        VolumeMountPoint    NVARCHAR(200)                   NOT NULL,
        LoadDate            DATE                            NOT NULL,
        BytesWritten        BIGINT                          NULL,
        FreePercent         DECIMAL(9, 4)                   NULL,
        CONSTRAINT PK_LoadVolumeHistory PRIMARY KEY CLUSTERED (LoadVolumeHistoryId),
        CONSTRAINT UQ_LoadVolumeHistory UNIQUE (VolumeMountPoint, LoadDate)
    );
END
GO

/* --------------------------------------------------------------------------
   File ingestion
   -------------------------------------------------------------------------- */

/*
    One row per inbound file ever seen, keyed on the path so the same file
    arriving twice is detectable. The quarantine and archive columns are here
    rather than in a second table because the file's whole life is one row and
    the operations report reads it that way.
*/
IF OBJECT_ID(N'etl.InboundFileRegister', N'U') IS NULL
BEGIN
    CREATE TABLE etl.InboundFileRegister
    (
        InboundFileId           BIGINT          IDENTITY(1, 1)  NOT NULL,
        FeedCode                NVARCHAR(40)                    NOT NULL,
        FileName                NVARCHAR(260)                   NOT NULL,
        FilePath                NVARCHAR(500)                   NOT NULL,
        FileSizeBytes           BIGINT                          NULL,
        FileHash                CHAR(64)                        NULL,
        ReceivedAtUtc           DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_InboundFileRegister_ReceivedAtUtc DEFAULT (SYSUTCDATETIME()),
        StructuralCheckStatus   NVARCHAR(20)                    NULL,
        BadFileCount            INT                             NOT NULL
            CONSTRAINT DF_InboundFileRegister_BadFileCount DEFAULT (0),
        ProcessingStatus        NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_InboundFileRegister_ProcessingStatus DEFAULT (N'Received'),
        ProcessedAtUtc          DATETIME2(3)                    NULL,
        IsQuarantined           BIT                             NOT NULL
            CONSTRAINT DF_InboundFileRegister_IsQuarantined DEFAULT (0),
        QuarantinedAtUtc        DATETIME2(3)                    NULL,
        QuarantineReason        NVARCHAR(500)                   NULL,
        IsArchived              BIT                             NOT NULL
            CONSTRAINT DF_InboundFileRegister_IsArchived DEFAULT (0),
        ArchivePath             NVARCHAR(500)                   NULL,
        ArchivedAtUtc           DATETIME2(3)                    NULL,
        CONSTRAINT PK_InboundFileRegister PRIMARY KEY CLUSTERED (InboundFileId),
        CONSTRAINT CK_InboundFileRegister_Status
            CHECK (ProcessingStatus IN (N'Received', N'Screened', N'Loaded', N'Quarantined', N'Failed'))
    );

    CREATE NONCLUSTERED INDEX IX_InboundFileRegister_Feed
        ON etl.InboundFileRegister (FeedCode, ReceivedAtUtc DESC)
        INCLUDE (ProcessingStatus, IsQuarantined, IsArchived);

    CREATE NONCLUSTERED INDEX IX_InboundFileRegister_Archive
        ON etl.InboundFileRegister (IsArchived, ArchivedAtUtc);
END
GO

IF OBJECT_ID(N'etl.FileIngestionLog', N'U') IS NULL
BEGIN
    CREATE TABLE etl.FileIngestionLog
    (
        FileIngestionLogId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        PackageExecutionId  BIGINT                          NULL,
        ObjectName          NVARCHAR(200)                   NOT NULL,
        FileName            NVARCHAR(260)                   NOT NULL,
        FilePath            NVARCHAR(500)                   NULL,
        ReceivedAtUtc       DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_FileIngestionLog_ReceivedAtUtc DEFAULT (SYSUTCDATETIME()),
        CompletedAtUtc      DATETIME2(3)                    NULL,
        DetailRowCount      BIGINT                          NULL,
        RejectRowCount      BIGINT                          NULL,
        Status              NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_FileIngestionLog_Status DEFAULT (N'Received'),
        CONSTRAINT PK_FileIngestionLog PRIMARY KEY CLUSTERED (FileIngestionLogId)
    );

    CREATE NONCLUSTERED INDEX IX_FileIngestionLog_File
        ON etl.FileIngestionLog (FileName, ReceivedAtUtc DESC);
END
GO

/*
    Control totals from the sidecar file the feeds ship alongside the data. When
    a feed does not send one the row is absent and the load falls back to its
    variance rule, which is why nothing here is NOT NULL beyond the file name.
*/
IF OBJECT_ID(N'etl.FileControlTotal', N'U') IS NULL
BEGIN
    CREATE TABLE etl.FileControlTotal
    (
        FileControlTotalId  BIGINT          IDENTITY(1, 1)  NOT NULL,
        FileName            NVARCHAR(260)                   NOT NULL,
        FeedCode            NVARCHAR(40)                    NULL,
        ExpectedRowCount    BIGINT                          NULL,
        ExpectedAmountTotal DECIMAL(19, 4)                  NULL,
        BusinessDate        DATE                            NULL,
        ReceivedAtUtc       DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_FileControlTotal_ReceivedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_FileControlTotal PRIMARY KEY CLUSTERED (FileControlTotalId),
        CONSTRAINT UQ_FileControlTotal_File UNIQUE (FileName)
    );
END
GO

IF OBJECT_ID(N'etl.ArchiveExpiryList', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ArchiveExpiryList
    (
        ArchiveExpiryListId BIGINT          IDENTITY(1, 1)  NOT NULL,
        FileName            NVARCHAR(260)                   NOT NULL,
        ArchivePath         NVARCHAR(500)                   NOT NULL,
        ArchivedAtUtc       DATETIME2(3)                    NULL,
        ListedAtUtc         DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_ArchiveExpiryList_ListedAtUtc DEFAULT (SYSUTCDATETIME()),
        IsDeleted           BIT                             NOT NULL
            CONSTRAINT DF_ArchiveExpiryList_IsDeleted DEFAULT (0),
        DeletedAtUtc        DATETIME2(3)                    NULL,
        CONSTRAINT PK_ArchiveExpiryList PRIMARY KEY CLUSTERED (ArchiveExpiryListId),
        CONSTRAINT UQ_ArchiveExpiryList_Path UNIQUE (ArchivePath)
    );
END
GO

/* --------------------------------------------------------------------------
   Registers the packages read from
   -------------------------------------------------------------------------- */

IF OBJECT_ID(N'etl.StagingTableRegister', N'U') IS NULL
BEGIN
    CREATE TABLE etl.StagingTableRegister
    (
        StagingTableId      INT             IDENTITY(1, 1)  NOT NULL,
        SchemaName          SYSNAME                         NOT NULL,
        TableName           SYSNAME                         NOT NULL,
        LoadDateColumn      SYSNAME                         NULL,
        RetentionDays       INT                             NULL,
        IsPurgeEligible     BIT                             NOT NULL
            CONSTRAINT DF_StagingTableRegister_IsPurgeEligible DEFAULT (1),
        ApproximateRowCount BIGINT                          NULL,
        CONSTRAINT PK_StagingTableRegister PRIMARY KEY CLUSTERED (StagingTableId),
        CONSTRAINT UQ_StagingTableRegister UNIQUE (SchemaName, TableName)
    );
END
GO

/*
    Per-object reconciliation tolerance. etl.ReconciliationExemption switches a
    check off entirely; this widens it, which is the answer most of the time -
    the shipping feed has always been a few rows out overnight and nobody wants
    to stop checking it.
*/
IF OBJECT_ID(N'etl.RowCountTolerance', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RowCountTolerance
    (
        RowCountToleranceId INT             IDENTITY(1, 1)  NOT NULL,
        ObjectName          NVARCHAR(200)                   NOT NULL,
        TolerancePercent    DECIMAL(9, 4)                   NOT NULL
            CONSTRAINT DF_RowCountTolerance_TolerancePercent DEFAULT (0),
        AbsoluteTolerance   BIGINT                          NULL,
        ExplanationCode     NVARCHAR(50)                    NULL,
        ApprovedBy          NVARCHAR(100)                   NULL,
        CONSTRAINT PK_RowCountTolerance PRIMARY KEY CLUSTERED (RowCountToleranceId),
        CONSTRAINT UQ_RowCountTolerance_Object UNIQUE (ObjectName)
    );
END
GO

IF OBJECT_ID(N'etl.RequiredConfigurationKey', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RequiredConfigurationKey
    (
        RequiredConfigurationKeyId  INT             IDENTITY(1, 1)  NOT NULL,
        ConfigurationKey            NVARCHAR(100)                   NOT NULL,
        EnvironmentCode             NVARCHAR(10)                    NULL,
        IsMandatory                 BIT                             NOT NULL
            CONSTRAINT DF_RequiredConfigurationKey_IsMandatory DEFAULT (1),
        Description                 NVARCHAR(500)                   NULL,
        CONSTRAINT PK_RequiredConfigurationKey PRIMARY KEY CLUSTERED (RequiredConfigurationKeyId),
        CONSTRAINT UQ_RequiredConfigurationKey UNIQUE (ConfigurationKey, EnvironmentCode)
    );
END
GO

/*
    The regions do not close on the same calendar and do not all charge VAT, so
    the finance packages read the open period per region rather than assuming a
    single corporate period.
*/
IF OBJECT_ID(N'etl.RegionPeriodStatus', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RegionPeriodStatus
    (
        RegionPeriodStatusId    INT             IDENTITY(1, 1)  NOT NULL,
        RegionCode              NVARCHAR(10)                    NOT NULL,
        FiscalCalendarCode      NVARCHAR(20)                    NOT NULL,
        OpenPeriodKey           NVARCHAR(10)                    NOT NULL,
        VatRegimeCode           NVARCHAR(20)                    NULL,
        LastClosedPeriodKey     NVARCHAR(10)                    NULL,
        UpdatedAtUtc            DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_RegionPeriodStatus_UpdatedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RegionPeriodStatus PRIMARY KEY CLUSTERED (RegionPeriodStatusId),
        CONSTRAINT UQ_RegionPeriodStatus_Region UNIQUE (RegionCode)
    );
END
GO

IF OBJECT_ID(N'etl.FxRateAvailability', N'U') IS NULL
BEGIN
    CREATE TABLE etl.FxRateAvailability
    (
        FxRateAvailabilityId    BIGINT          IDENTITY(1, 1)  NOT NULL,
        RateDate                DATE                            NOT NULL,
        RegionCode              NVARCHAR(10)                    NULL,
        RateCount               INT                             NOT NULL
            CONSTRAINT DF_FxRateAvailability_RateCount DEFAULT (0),
        IsComplete              BIT                             NOT NULL
            CONSTRAINT DF_FxRateAvailability_IsComplete DEFAULT (0),
        CheckedAtUtc            DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_FxRateAvailability_CheckedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_FxRateAvailability PRIMARY KEY CLUSTERED (FxRateAvailabilityId),
        CONSTRAINT UQ_FxRateAvailability UNIQUE (RateDate, RegionCode)
    );
END
GO

/*
    Supplier scorecard weights differ by region because the regions buy
    differently: NA weights on-time delivery hardest, EU weights invoice
    accuracy hardest, APAC weights quality hardest.
*/
IF OBJECT_ID(N'etl.SupplierScoringWeight', N'U') IS NULL
BEGIN
    CREATE TABLE etl.SupplierScoringWeight
    (
        SupplierScoringWeightId INT             IDENTITY(1, 1)  NOT NULL,
        RegionCode              NVARCHAR(10)                    NOT NULL,
        OnTimeWeight            DECIMAL(9, 4)                   NOT NULL,
        AccuracyWeight          DECIMAL(9, 4)                   NOT NULL,
        PriceWeight             DECIMAL(9, 4)                   NOT NULL,
        QualityWeight           DECIMAL(9, 4)                   NOT NULL,
        EffectiveFrom           DATE                            NOT NULL
            CONSTRAINT DF_SupplierScoringWeight_EffectiveFrom DEFAULT ('2013-01-01'),
        CONSTRAINT PK_SupplierScoringWeight PRIMARY KEY CLUSTERED (SupplierScoringWeightId),
        CONSTRAINT UQ_SupplierScoringWeight_Region UNIQUE (RegionCode)
    );
END
GO

/*
    Facts that were loaded against an inferred dimension member and must be
    repointed once the real member arrives. Deployed to the warehouse as well as
    to staging, because Integration.usp_RekeyLateArrivingDimensions reads it
    from the warehouse side.
*/
IF OBJECT_ID(N'etl.FactRekeyQueue', N'U') IS NULL
BEGIN
    CREATE TABLE etl.FactRekeyQueue
    (
        FactRekeyQueueId    BIGINT          IDENTITY(1, 1)  NOT NULL,
        BatchId             BIGINT                          NULL,
        FactTableName       NVARCHAR(200)                   NOT NULL,
        DimensionName       NVARCHAR(200)                   NOT NULL,
        BusinessKey         NVARCHAR(200)                   NOT NULL,
        InferredKey         INT                             NULL,
        ResolvedKey         INT                             NULL,
        AttemptCount        INT                             NOT NULL
            CONSTRAINT DF_FactRekeyQueue_AttemptCount DEFAULT (0),
        QueuedAtUtc         DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_FactRekeyQueue_QueuedAtUtc DEFAULT (SYSUTCDATETIME()),
        ResolvedAtUtc       DATETIME2(3)                    NULL,
        QueueStatus         NVARCHAR(20)                    NOT NULL
            CONSTRAINT DF_FactRekeyQueue_QueueStatus DEFAULT (N'Queued'),
        CONSTRAINT PK_FactRekeyQueue PRIMARY KEY CLUSTERED (FactRekeyQueueId),
        CONSTRAINT CK_FactRekeyQueue_Status
            CHECK (QueueStatus IN (N'Queued', N'Resolved', N'Abandoned'))
    );

    CREATE NONCLUSTERED INDEX IX_FactRekeyQueue_Open
        ON etl.FactRekeyQueue (QueueStatus, DimensionName)
        INCLUDE (FactTableName, BusinessKey, AttemptCount);
END
GO
