/*
    Object        : [etl].[usp_PurgeControlHistory]
    Deploy target : WWI_Staging and WideWorldImportersDW
    Deploy order  : after 04_tables_data_quality.sql and the control procedures
    Depends on    : etl.Batch, etl.BatchStep, etl.PackageExecution,
                    etl.RowCountAudit, etl.ErrorLog, etl.RejectedRecord,
                    etl.RejectedRecordStaging, etl.DataQualityResult,
                    etl.ControlPurgeAudit
    Called by     : the WWI - Control History Purge Agent job (once against the
                    staging copy of the control schema and once against the
                    warehouse copy, with different retentions) and
                    MNT_Purge_ControlHistory

    Retention is per table, not per batch, because the finance audit trail is
    read from the error log long after the executions that produced it have been
    aged out. Deletes are chunked and run outside an explicit transaction: this
    job runs in the maintenance window against tables the ETL is not writing, and
    a single-statement delete of a year of audit rows has taken the log with it
    before.

    Order matters - children before parents - and etl.Batch rows are only removed
    once nothing references them, so an interrupted purge leaves orphan-free
    history rather than a broken chain.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.ControlPurgeAudit', N'U') IS NULL
BEGIN
    CREATE TABLE etl.ControlPurgeAudit
    (
        ControlPurgeAuditId BIGINT          IDENTITY(1, 1)  NOT NULL,
        PurgeRunAtUtc       DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_ControlPurgeAudit_RunAtUtc DEFAULT (SYSUTCDATETIME()),
        TableName           NVARCHAR(200)                   NOT NULL,
        CutoffUtc           DATETIME2(3)                    NULL,
        RowsDeleted         BIGINT                          NOT NULL,
        DurationSeconds     INT                             NULL,
        CONSTRAINT PK_ControlPurgeAudit PRIMARY KEY CLUSTERED (ControlPurgeAuditId)
    );
END
GO

IF OBJECT_ID(N'etl.usp_PurgeControlHistory', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_PurgeControlHistory;
GO

CREATE PROCEDURE etl.usp_PurgeControlHistory
(
    @RetentionDays          INT = NULL,
    @ExecutionHistoryMonths INT = 13,
    @ErrorHistoryMonths     INT = 24,
    @RejectHistoryMonths    INT = 12,
    @QualityHistoryMonths   INT = 13,
    @ChunkSize              INT = 25000,
    @WhatIf                 BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    /*
        MNT_Purge_ControlHistory passes a single @RetentionDays for every table;
        the Agent job passes per-table months. A day count wins when supplied.
    */
    DECLARE @ExecutionCutoff DATETIME2(3),
            @ErrorCutoff     DATETIME2(3),
            @RejectCutoff    DATETIME2(3),
            @QualityCutoff   DATETIME2(3),
            @Now             DATETIME2(3) = SYSUTCDATETIME();

    IF @RetentionDays IS NOT NULL
    BEGIN
        SET @ExecutionCutoff = DATEADD(DAY, -@RetentionDays, @Now);
        SET @ErrorCutoff     = @ExecutionCutoff;
        SET @RejectCutoff    = @ExecutionCutoff;
        SET @QualityCutoff   = @ExecutionCutoff;
    END
    ELSE
    BEGIN
        SET @ExecutionCutoff = DATEADD(MONTH, -ISNULL(@ExecutionHistoryMonths, 13), @Now);
        SET @ErrorCutoff     = DATEADD(MONTH, -ISNULL(@ErrorHistoryMonths, 24), @Now);
        SET @RejectCutoff    = DATEADD(MONTH, -ISNULL(@RejectHistoryMonths, 12), @Now);
        SET @QualityCutoff   = DATEADD(MONTH, -ISNULL(@QualityHistoryMonths, 13), @Now);
    END

    SET @ChunkSize = CASE WHEN ISNULL(@ChunkSize, 0) BETWEEN 1000 AND 200000 THEN @ChunkSize ELSE 25000 END;

    IF @WhatIf = 1
    BEGIN
        SELECT N'etl.RowCountAudit'  AS TableName, @ExecutionCutoff AS CutoffUtc,
               COUNT_BIG(*) AS RowsThatWouldBeDeleted
        FROM   etl.RowCountAudit AS a
               INNER JOIN etl.PackageExecution AS pe ON pe.PackageExecutionId = a.PackageExecutionId
        WHERE  pe.StartedAtUtc < @ExecutionCutoff
        UNION ALL
        SELECT N'etl.PackageExecution', @ExecutionCutoff, COUNT_BIG(*)
        FROM   etl.PackageExecution WHERE StartedAtUtc < @ExecutionCutoff
        UNION ALL
        SELECT N'etl.ErrorLog', @ErrorCutoff, COUNT_BIG(*)
        FROM   etl.ErrorLog WHERE LoggedAtUtc < @ErrorCutoff
        UNION ALL
        SELECT N'etl.RejectedRecord', @RejectCutoff, COUNT_BIG(*)
        FROM   etl.RejectedRecord WHERE LoggedAtUtc < @RejectCutoff
        UNION ALL
        SELECT N'etl.DataQualityResult', @QualityCutoff, COUNT_BIG(*)
        FROM   etl.DataQualityResult WHERE EvaluatedAtUtc < @QualityCutoff;

        RETURN 0;
    END

    DECLARE @Deleted BIGINT, @Total BIGINT, @StartedAt DATETIME2(3);

    /* 1. Row count audit - child of package execution. */
    SET @Total = 0;
    SET @StartedAt = SYSUTCDATETIME();
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@ChunkSize) a
        FROM   etl.RowCountAudit AS a
               INNER JOIN etl.PackageExecution AS pe ON pe.PackageExecutionId = a.PackageExecutionId
        WHERE  pe.StartedAtUtc < @ExecutionCutoff;

        SET @Deleted = @@ROWCOUNT;
        SET @Total = @Total + @Deleted;
    END
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.RowCountAudit', @ExecutionCutoff, @Total, DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()));

    /* 2. Data quality results. */
    SET @Total = 0;
    SET @StartedAt = SYSUTCDATETIME();
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@ChunkSize) FROM etl.DataQualityResult WHERE EvaluatedAtUtc < @QualityCutoff;
        SET @Deleted = @@ROWCOUNT;
        SET @Total = @Total + @Deleted;
    END
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.DataQualityResult', @QualityCutoff, @Total, DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()));

    /*
        3. Rejected records. Anything never reprocessed is kept regardless of
        age: an unreprocessed reject is an open item, and the stewardship report
        is the only place it is visible.
    */
    SET @Total = 0;
    SET @StartedAt = SYSUTCDATETIME();
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@ChunkSize) FROM etl.RejectedRecord
        WHERE  LoggedAtUtc < @RejectCutoff AND IsReprocessed = 1;
        SET @Deleted = @@ROWCOUNT;
        SET @Total = @Total + @Deleted;
    END
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.RejectedRecord', @RejectCutoff, @Total, DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()));

    /* 4. Reject staging is scratch; anything older than a week is abandoned. */
    DELETE FROM etl.RejectedRecordStaging WHERE LandedAtUtc < DATEADD(DAY, -7, @Now);
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.RejectedRecordStaging', DATEADD(DAY, -7, @Now), @@ROWCOUNT, 0);

    /* 5. Error log. */
    SET @Total = 0;
    SET @StartedAt = SYSUTCDATETIME();
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@ChunkSize) FROM etl.ErrorLog WHERE LoggedAtUtc < @ErrorCutoff;
        SET @Deleted = @@ROWCOUNT;
        SET @Total = @Total + @Deleted;
    END
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.ErrorLog', @ErrorCutoff, @Total, DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()));

    /* 6. Package executions, once their audit children are gone. */
    SET @Total = 0;
    SET @StartedAt = SYSUTCDATETIME();
    SET @Deleted = 1;
    WHILE @Deleted > 0
    BEGIN
        DELETE TOP (@ChunkSize) pe
        FROM   etl.PackageExecution AS pe
        WHERE  pe.StartedAtUtc < @ExecutionCutoff
               AND NOT EXISTS (SELECT 1 FROM etl.RowCountAudit AS a
                               WHERE a.PackageExecutionId = pe.PackageExecutionId)
               AND NOT EXISTS (SELECT 1 FROM etl.ErrorLog AS e
                               WHERE e.PackageExecutionId = pe.PackageExecutionId)
               AND NOT EXISTS (SELECT 1 FROM etl.RejectedRecord AS r
                               WHERE r.PackageExecutionId = pe.PackageExecutionId);
        SET @Deleted = @@ROWCOUNT;
        SET @Total = @Total + @Deleted;
    END
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.PackageExecution', @ExecutionCutoff, @Total, DATEDIFF(SECOND, @StartedAt, SYSUTCDATETIME()));

    /* 7. Batch steps and batches, once nothing points at them. */
    DELETE bs
    FROM   etl.BatchStep AS bs
           INNER JOIN etl.Batch AS b ON b.BatchId = bs.BatchId
    WHERE  b.StartedAtUtc < @ExecutionCutoff
           AND NOT EXISTS (SELECT 1 FROM etl.PackageExecution AS pe WHERE pe.BatchId = b.BatchId);
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.BatchStep', @ExecutionCutoff, @@ROWCOUNT, 0);

    DELETE b
    FROM   etl.Batch AS b
    WHERE  b.StartedAtUtc < @ExecutionCutoff
           AND NOT EXISTS (SELECT 1 FROM etl.PackageExecution AS pe WHERE pe.BatchId = b.BatchId)
           AND NOT EXISTS (SELECT 1 FROM etl.BatchStep AS bs WHERE bs.BatchId = b.BatchId)
           AND NOT EXISTS (SELECT 1 FROM etl.ErrorLog AS e WHERE e.BatchId = b.BatchId)
           AND NOT EXISTS (SELECT 1 FROM etl.RejectedRecord AS r WHERE r.BatchId = b.BatchId);
    INSERT INTO etl.ControlPurgeAudit (TableName, CutoffUtc, RowsDeleted, DurationSeconds)
    VALUES (N'etl.Batch', @ExecutionCutoff, @@ROWCOUNT, 0);

    SELECT  TableName,
            CutoffUtc,
            RowsDeleted,
            DurationSeconds
    FROM    etl.ControlPurgeAudit
    WHERE   PurgeRunAtUtc >= @Now
    ORDER BY ControlPurgeAuditId;

    RETURN 0;
END
GO
