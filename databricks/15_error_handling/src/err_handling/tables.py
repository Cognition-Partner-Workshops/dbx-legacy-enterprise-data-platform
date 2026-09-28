"""Delta tables owned by the WWI_ErrorHandling project (legacy ``work.*`` scratch tables).

The legacy packages reference work.BadFileQueue, work.RejectRoutingSet,
work.RejectRoutingHistory, work.RejectEscalation, work.RowCountReconciliation and
work.RowCountFailure, none of which have DDL under sqlserver/staging/tables/. They
are created here as ``silver.work_*`` tables from the column lists the packages
use. ``silver.work_step_failure`` carries the IsRetryable / ErrorMessage /
FailureClass attributes the legacy SQL wrote to columns that do not exist on
etl.BatchStep. Control tables (``etl.*``) are owned by dbx_etl_common and are
never created here.
"""

WORK_TABLES = {
    "work_step_failure": """
        WorkStepFailureId   BIGINT GENERATED ALWAYS AS IDENTITY,
        BatchId             BIGINT NOT NULL,
        BatchStepId         BIGINT,
        PackageExecutionId  BIGINT,
        PackageName         STRING NOT NULL,
        FailureClass        STRING NOT NULL,
        IsRetryable         INT NOT NULL,
        ErrorCode           INT,
        ErrorMessage        STRING,
        ClassifiedAtUtc     TIMESTAMP NOT NULL
    """,
    "work_reject_routing_set": """
        RejectedRecordId        BIGINT NOT NULL,
        BatchId                 BIGINT,
        ObjectName              STRING,
        RejectStage             STRING,
        RejectReasonCode        STRING,
        RejectReasonDescription STRING,
        SourceKey               STRING,
        RejectedAtUtc           TIMESTAMP,
        AgeDays                 INT,
        RoutedInBatchId         BIGINT NOT NULL
    """,
    "work_reject_routing_history": """
        RejectedRecordId        BIGINT NOT NULL,
        BatchId                 BIGINT,
        ObjectName              STRING,
        RejectStage             STRING,
        RejectReasonCode        STRING,
        RejectReasonDescription STRING,
        SourceKey               STRING,
        RejectedAtUtc           TIMESTAMP,
        AgeDays                 INT,
        IsEscalated             BOOLEAN NOT NULL,
        RejectFileLine          STRING,
        RejectFileName          STRING,
        RoutingDestination      STRING NOT NULL,
        RoutedInBatchId         BIGINT NOT NULL,
        RoutedAtUtc             TIMESTAMP NOT NULL
    """,
    "work_reject_escalation": """
        RejectedRecordId        BIGINT NOT NULL,
        BatchId                 BIGINT,
        ObjectName              STRING,
        RejectStage             STRING,
        RejectReasonCode        STRING,
        RejectReasonDescription STRING,
        SourceKey               STRING,
        RejectedAtUtc           TIMESTAMP,
        AgeDays                 INT,
        RejectFileLine          STRING,
        RoutedInBatchId         BIGINT NOT NULL,
        EscalatedAtUtc          TIMESTAMP NOT NULL
    """,
    "work_err_reject_registration": """
        ErrTableName            STRING NOT NULL,
        RejectId                BIGINT NOT NULL,
        BatchId                 BIGINT,
        RoutedInBatchId         BIGINT NOT NULL,
        RegisteredAtUtc         TIMESTAMP NOT NULL
    """,
    "work_bad_file_queue": """
        BadFileQueueId          BIGINT GENERATED ALWAYS AS IDENTITY,
        BatchId                 BIGINT NOT NULL,
        InboundFileId           BIGINT,
        FileName                STRING NOT NULL,
        FilePath                STRING NOT NULL,
        QuarantineReasonCode    STRING NOT NULL,
        DetectedAtUtc           TIMESTAMP NOT NULL,
        IsMoved                 INT NOT NULL,
        MovedAtUtc              TIMESTAMP,
        QuarantinePath          STRING,
        MoveError               STRING
    """,
    "work_row_count_reconciliation": """
        BatchId                 BIGINT NOT NULL,
        ObjectName              STRING NOT NULL,
        SourceRowCount          BIGINT,
        StagingRowCount         BIGINT,
        TargetRowCount          BIGINT,
        RejectedRowCount        BIGINT,
        TolerancePercent        DECIMAL(9,4),
        ExplanationCode         STRING
    """,
    "work_row_count_failure": """
        BatchId                 BIGINT NOT NULL,
        ObjectName              STRING NOT NULL,
        SourceRowCount          BIGINT,
        StagingRowCount         BIGINT,
        TargetRowCount          BIGINT,
        RejectedRowCount        BIGINT,
        TolerancePercent        DECIMAL(9,4),
        ExplanationCode         STRING,
        ExpectedTargetRowCount  BIGINT,
        DifferenceRowCount      BIGINT,
        DifferencePercent       DECIMAL(18,2),
        ReconciliationStatus    STRING NOT NULL,
        EvaluatedAtUtc          TIMESTAMP NOT NULL
    """,
}


def ensureWorkTable(spark, naming, catalog, tableName):
    fullName = naming.table(catalog, "silver", tableName)
    spark.sql("CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA" % (fullName, WORK_TABLES[tableName]))
    return fullName


def clearForBatch(spark, fullName, batchId, column="RoutedInBatchId"):
    """Legacy TRUNCATE of a scratch table, narrowed to the current batch so reruns are idempotent."""
    spark.sql("DELETE FROM %s WHERE %s = %d" % (fullName, column, int(batchId)))


def scalar(spark, sql):
    row = spark.sql(sql).first()
    return None if row is None else row[0]


def sqlLiteral(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
