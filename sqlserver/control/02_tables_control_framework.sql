/*
    ETL control framework tables.

    Deploy target : WideWorldImportersStaging  (SQLSERVER_STAGING_DB)
    Deploy order  : 02
    Depends on    : 01_schemas.sql

    Every SSIS package in this estate registers itself here. A nightly run is a
    single etl.Batch row; each master-package section is an etl.BatchStep; each
    package execution is an etl.PackageExecution row carrying timings, statuses
    and row counts. Watermarks drive incremental extraction, and rejected rows
    are routed to etl.RejectedRecord rather than failing the pipeline.
*/

IF OBJECT_ID(N'etl.SourceSystem', N'U') IS NULL
BEGIN
    CREATE TABLE etl.SourceSystem
    (
        SourceSystemId      INT             NOT NULL IDENTITY(1,1),
        SourceSystemCode    NVARCHAR(20)    NOT NULL,
        SourceSystemName    NVARCHAR(100)   NOT NULL,
        Platform            NVARCHAR(50)    NOT NULL,   -- Oracle, SQL Server, File, Manual
        RegionCode          NVARCHAR(10)    NULL,       -- NA, EU, APAC, GLOBAL
        ConnectionParameter NVARCHAR(100)   NULL,       -- project parameter that resolves the connection
        DefaultTimeZone     NVARCHAR(50)    NOT NULL CONSTRAINT DF_SourceSystem_TimeZone DEFAULT (N'UTC'),
        IsActive            BIT             NOT NULL CONSTRAINT DF_SourceSystem_IsActive DEFAULT (1),
        ValidFrom           DATETIME2(3)    NOT NULL CONSTRAINT DF_SourceSystem_ValidFrom DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_SourceSystem PRIMARY KEY CLUSTERED (SourceSystemId),
        CONSTRAINT UQ_SourceSystem_Code UNIQUE (SourceSystemCode)
    );
END;
GO

IF OBJECT_ID(N'etl.Batch', N'U') IS NULL
BEGIN
    CREATE TABLE etl.Batch
    (
        BatchId             BIGINT          NOT NULL IDENTITY(1,1),
        BatchName           NVARCHAR(100)   NOT NULL,
        BatchType           NVARCHAR(30)    NOT NULL,   -- Daily, Intraday, Monthly, Recovery, Backfill
        BusinessDate        DATE            NOT NULL,
        EnvironmentCode     NVARCHAR(10)    NOT NULL,
        StartedAtUtc        DATETIME2(3)    NOT NULL CONSTRAINT DF_Batch_StartedAtUtc DEFAULT (SYSUTCDATETIME()),
        CompletedAtUtc      DATETIME2(3)    NULL,
        Status              NVARCHAR(20)    NOT NULL CONSTRAINT DF_Batch_Status DEFAULT (N'Running'),
        RestartFromStep     NVARCHAR(100)   NULL,
        InitiatedBy         NVARCHAR(128)   NOT NULL CONSTRAINT DF_Batch_InitiatedBy DEFAULT (SUSER_SNAME()),
        Notes               NVARCHAR(1000)  NULL,
        CONSTRAINT PK_Batch PRIMARY KEY CLUSTERED (BatchId),
        CONSTRAINT CK_Batch_Status CHECK (Status IN (N'Running', N'Succeeded', N'Failed', N'Cancelled', N'SucceededWithWarnings'))
    );

    CREATE INDEX IX_Batch_BusinessDate ON etl.Batch (BusinessDate, BatchType) INCLUDE (Status);
END;
GO

IF OBJECT_ID(N'etl.BatchStep', N'U') IS NULL
BEGIN
    CREATE TABLE etl.BatchStep
    (
        BatchStepId         BIGINT          NOT NULL IDENTITY(1,1),
        BatchId             BIGINT          NOT NULL,
        StepName            NVARCHAR(100)   NOT NULL,
        StepSequence        INT             NOT NULL,
        StepGroup           NVARCHAR(50)    NULL,       -- Extract, Stage, Dimension, Fact, Aggregate, Maintenance
        StartedAtUtc        DATETIME2(3)    NOT NULL CONSTRAINT DF_BatchStep_StartedAtUtc DEFAULT (SYSUTCDATETIME()),
        CompletedAtUtc      DATETIME2(3)    NULL,
        Status              NVARCHAR(20)    NOT NULL CONSTRAINT DF_BatchStep_Status DEFAULT (N'Running'),
        AttemptNumber       INT             NOT NULL CONSTRAINT DF_BatchStep_Attempt DEFAULT (1),
        CONSTRAINT PK_BatchStep PRIMARY KEY CLUSTERED (BatchStepId),
        CONSTRAINT FK_BatchStep_Batch FOREIGN KEY (BatchId) REFERENCES etl.Batch (BatchId),
        CONSTRAINT CK_BatchStep_Status CHECK (Status IN (N'Running', N'Succeeded', N'Failed', N'Skipped'))
    );

    CREATE INDEX IX_BatchStep_BatchId ON etl.BatchStep (BatchId, StepSequence);
END;
GO

IF OBJECT_ID(N'etl.PackageExecution', N'U') IS NULL
BEGIN
    CREATE TABLE etl.PackageExecution
    (
        PackageExecutionId  BIGINT          NOT NULL IDENTITY(1,1),
        BatchId             BIGINT          NULL,
        BatchStepId         BIGINT          NULL,
        PackageName         NVARCHAR(200)   NOT NULL,
        ProjectName         NVARCHAR(100)   NULL,
        MachineName         NVARCHAR(128)   NOT NULL CONSTRAINT DF_PackageExecution_Machine DEFAULT (HOST_NAME()),
        ExecutedBy          NVARCHAR(128)   NOT NULL CONSTRAINT DF_PackageExecution_User DEFAULT (SUSER_SNAME()),
        StartedAtUtc        DATETIME2(3)    NOT NULL CONSTRAINT DF_PackageExecution_Started DEFAULT (SYSUTCDATETIME()),
        CompletedAtUtc      DATETIME2(3)    NULL,
        DurationSeconds     AS DATEDIFF(SECOND, StartedAtUtc, CompletedAtUtc),
        Status              NVARCHAR(20)    NOT NULL CONSTRAINT DF_PackageExecution_Status DEFAULT (N'Running'),
        RowsRead            BIGINT          NULL,
        RowsInserted        BIGINT          NULL,
        RowsUpdated         BIGINT          NULL,
        RowsDeleted         BIGINT          NULL,
        RowsRejected        BIGINT          NULL,
        WatermarkFrom       NVARCHAR(50)    NULL,
        WatermarkTo         NVARCHAR(50)    NULL,
        AttemptNumber       INT             NOT NULL CONSTRAINT DF_PackageExecution_Attempt DEFAULT (1),
        CONSTRAINT PK_PackageExecution PRIMARY KEY CLUSTERED (PackageExecutionId),
        CONSTRAINT FK_PackageExecution_Batch FOREIGN KEY (BatchId) REFERENCES etl.Batch (BatchId),
        CONSTRAINT FK_PackageExecution_BatchStep FOREIGN KEY (BatchStepId) REFERENCES etl.BatchStep (BatchStepId),
        CONSTRAINT CK_PackageExecution_Status CHECK (Status IN (N'Running', N'Succeeded', N'Failed', N'Cancelled'))
    );

    CREATE INDEX IX_PackageExecution_Batch ON etl.PackageExecution (BatchId, PackageName) INCLUDE (Status, RowsInserted);
    CREATE INDEX IX_PackageExecution_Package ON etl.PackageExecution (PackageName, StartedAtUtc DESC);
END;
GO

IF OBJECT_ID(N'etl.Watermark', N'U') IS NULL
BEGIN
    CREATE TABLE etl.Watermark
    (
        WatermarkId         INT             NOT NULL IDENTITY(1,1),
        SourceSystemCode    NVARCHAR(20)    NOT NULL,
        ObjectName          NVARCHAR(200)   NOT NULL,
        WatermarkType       NVARCHAR(20)    NOT NULL,   -- Timestamp, NumericKey, DateWindow
        LastValue           NVARCHAR(50)    NULL,
        PreviousValue       NVARCHAR(50)    NULL,
        LastLoadedAtUtc     DATETIME2(3)    NULL,
        LastPackageExecutionId BIGINT       NULL,
        LookbackMinutes     INT             NOT NULL CONSTRAINT DF_Watermark_Lookback DEFAULT (0),
        IsLocked            BIT             NOT NULL CONSTRAINT DF_Watermark_IsLocked DEFAULT (0),
        CONSTRAINT PK_Watermark PRIMARY KEY CLUSTERED (WatermarkId),
        CONSTRAINT UQ_Watermark_Object UNIQUE (SourceSystemCode, ObjectName),
        CONSTRAINT CK_Watermark_Type CHECK (WatermarkType IN (N'Timestamp', N'NumericKey', N'DateWindow'))
    );
END;
GO

IF OBJECT_ID(N'etl.RowCountAudit', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RowCountAudit
    (
        RowCountAuditId     BIGINT          NOT NULL IDENTITY(1,1),
        PackageExecutionId  BIGINT          NOT NULL,
        ObjectName          NVARCHAR(200)   NOT NULL,
        SourceRowCount      BIGINT          NULL,
        TargetRowCount      BIGINT          NULL,
        InsertRowCount      BIGINT          NULL,
        UpdateRowCount      BIGINT          NULL,
        DeleteRowCount      BIGINT          NULL,
        RejectRowCount      BIGINT          NULL,
        VarianceRowCount    AS (ISNULL(SourceRowCount, 0) - ISNULL(TargetRowCount, 0) - ISNULL(RejectRowCount, 0)),
        RecordedAtUtc       DATETIME2(3)    NOT NULL CONSTRAINT DF_RowCountAudit_Recorded DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RowCountAudit PRIMARY KEY CLUSTERED (RowCountAuditId),
        CONSTRAINT FK_RowCountAudit_PackageExecution FOREIGN KEY (PackageExecutionId)
            REFERENCES etl.PackageExecution (PackageExecutionId)
    );

    CREATE INDEX IX_RowCountAudit_Object ON etl.RowCountAudit (ObjectName, RecordedAtUtc DESC);
END;
GO

IF OBJECT_ID(N'etl.ErrorLog', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ErrorLog
    (
        ErrorLogId          BIGINT          NOT NULL IDENTITY(1,1),
        PackageExecutionId  BIGINT          NULL,
        BatchId             BIGINT          NULL,
        ErrorSeverity       NVARCHAR(20)    NOT NULL CONSTRAINT DF_ErrorLog_Severity DEFAULT (N'Error'),
        ErrorCode           INT             NULL,
        ErrorNumber         INT             NULL,
        ErrorState          INT             NULL,
        ErrorLine           INT             NULL,
        SourceName          NVARCHAR(200)   NULL,
        SourceComponent     NVARCHAR(200)   NULL,
        ProcedureName       NVARCHAR(200)   NULL,
        ErrorDescription    NVARCHAR(MAX)   NULL,
        LoggedAtUtc         DATETIME2(3)    NOT NULL CONSTRAINT DF_ErrorLog_LoggedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_ErrorLog PRIMARY KEY CLUSTERED (ErrorLogId),
        CONSTRAINT CK_ErrorLog_Severity CHECK (ErrorSeverity IN (N'Information', N'Warning', N'Error', N'Critical'))
    );

    CREATE INDEX IX_ErrorLog_Batch ON etl.ErrorLog (BatchId, LoggedAtUtc DESC);
END;
GO

IF OBJECT_ID(N'etl.RejectedRecord', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RejectedRecord
    (
        RejectedRecordId    BIGINT          NOT NULL IDENTITY(1,1),
        PackageExecutionId  BIGINT          NULL,
        BatchId             BIGINT          NULL,
        SourceSystemCode    NVARCHAR(20)    NULL,
        ObjectName          NVARCHAR(200)   NOT NULL,
        BusinessKey         NVARCHAR(200)   NULL,
        RejectReasonCode    NVARCHAR(50)    NOT NULL,
        RejectReason        NVARCHAR(500)   NULL,
        RejectStage         NVARCHAR(50)    NOT NULL,   -- Extract, Stage, Screen, Dimension, Fact
        SsisErrorCode       INT             NULL,
        SsisErrorColumn     INT             NULL,
        RecordPayload       NVARCHAR(MAX)   NULL,       -- delimited or JSON copy of the offending row
        IsReprocessed       BIT             NOT NULL CONSTRAINT DF_RejectedRecord_Reprocessed DEFAULT (0),
        ReprocessedAtUtc    DATETIME2(3)    NULL,
        LoggedAtUtc         DATETIME2(3)    NOT NULL CONSTRAINT DF_RejectedRecord_LoggedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RejectedRecord PRIMARY KEY CLUSTERED (RejectedRecordId)
    );

    CREATE INDEX IX_RejectedRecord_Object ON etl.RejectedRecord (ObjectName, LoggedAtUtc DESC)
        INCLUDE (RejectReasonCode, IsReprocessed);
END;
GO

IF OBJECT_ID(N'etl.Configuration', N'U') IS NULL
BEGIN
    CREATE TABLE etl.Configuration
    (
        ConfigurationId     INT             NOT NULL IDENTITY(1,1),
        ConfigurationKey    NVARCHAR(100)   NOT NULL,
        EnvironmentCode     NVARCHAR(10)    NOT NULL CONSTRAINT DF_Configuration_Environment DEFAULT (N'ALL'),
        ConfigurationValue  NVARCHAR(500)   NOT NULL,
        ValueDataType       NVARCHAR(20)    NOT NULL CONSTRAINT DF_Configuration_DataType DEFAULT (N'String'),
        Description         NVARCHAR(500)   NULL,
        IsSensitive         BIT             NOT NULL CONSTRAINT DF_Configuration_Sensitive DEFAULT (0),
        ModifiedAtUtc       DATETIME2(3)    NOT NULL CONSTRAINT DF_Configuration_Modified DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Configuration PRIMARY KEY CLUSTERED (ConfigurationId),
        CONSTRAINT UQ_Configuration_Key UNIQUE (ConfigurationKey, EnvironmentCode),
        CONSTRAINT CK_Configuration_DataType CHECK (ValueDataType IN (N'String', N'Int', N'Decimal', N'Boolean', N'Date'))
    );
END;
GO

IF OBJECT_ID(N'etl.PackageDependency', N'U') IS NULL
BEGIN
    CREATE TABLE etl.PackageDependency
    (
        PackageDependencyId INT             NOT NULL IDENTITY(1,1),
        PackageName         NVARCHAR(200)   NOT NULL,
        DependsOnPackage    NVARCHAR(200)   NOT NULL,
        DependencyType      NVARCHAR(20)    NOT NULL CONSTRAINT DF_PackageDependency_Type DEFAULT (N'Hard'),
        CONSTRAINT PK_PackageDependency PRIMARY KEY CLUSTERED (PackageDependencyId),
        CONSTRAINT UQ_PackageDependency UNIQUE (PackageName, DependsOnPackage),
        CONSTRAINT CK_PackageDependency_Type CHECK (DependencyType IN (N'Hard', N'Soft'))
    );
END;
GO
