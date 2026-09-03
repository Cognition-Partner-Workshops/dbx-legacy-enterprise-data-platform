/*
    Object        : [etl].[usp_LogRejectedRecordSet]
    Deploy target : WWI_Staging and WideWorldImportersDW
    Deploy order  : after etl.usp_LogRejectedRecord.sql
    Depends on    : etl.RejectedRecord, etl.RejectedRecordStaging
    Called by     : the staging and fact load procedures that already hold their
                    rejects in an err.* table, and DQ_Reject_Reprocess

    The set-based counterpart to etl.usp_LogRejectedRecord. A loader that has
    already isolated its bad rows into an err.* table should register them in one
    statement instead of a cursor: on a bad file day the row-at-a-time path has
    been observed to be the slowest part of the load.

    Two ways in:
      - @SourceTable / @SourceFilter: name an err.* table and register everything
        that matches the filter. The table must expose ObjectName-compatible
        columns (BusinessKey, RejectReasonCode, RejectReason, RecordPayload);
        missing columns are registered as NULL.
      - etl.RejectedRecordStaging: bulk-insert into the staging table with the
        same @LoadTag and call this with that tag. This is the path the SSIS
        reject destinations use, because a data flow cannot pass a table.
*/

SET NOCOUNT ON;
GO

IF OBJECT_ID(N'etl.RejectedRecordStaging', N'U') IS NULL
BEGIN
    CREATE TABLE etl.RejectedRecordStaging
    (
        RejectedRecordStagingId BIGINT          IDENTITY(1, 1)  NOT NULL,
        LoadTag                 NVARCHAR(100)                   NOT NULL,
        PackageExecutionId      BIGINT                          NULL,
        BatchId                 BIGINT                          NULL,
        SourceSystemCode        NVARCHAR(20)                    NULL,
        ObjectName              NVARCHAR(200)                   NOT NULL,
        BusinessKey             NVARCHAR(200)                   NULL,
        RejectReasonCode        NVARCHAR(50)                    NOT NULL,
        RejectReason            NVARCHAR(500)                   NULL,
        RejectStage             NVARCHAR(50)                    NULL,
        SsisErrorCode           INT                             NULL,
        SsisErrorColumn         INT                             NULL,
        RecordPayload           NVARCHAR(MAX)                   NULL,
        LandedAtUtc             DATETIME2(3)                    NOT NULL
            CONSTRAINT DF_RejectedRecordStaging_LandedAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RejectedRecordStaging PRIMARY KEY CLUSTERED (RejectedRecordStagingId)
    );

    CREATE NONCLUSTERED INDEX IX_RejectedRecordStaging_LoadTag
        ON etl.RejectedRecordStaging (LoadTag);
END
GO

IF OBJECT_ID(N'etl.usp_LogRejectedRecordSet', N'P') IS NOT NULL
    DROP PROCEDURE etl.usp_LogRejectedRecordSet;
GO

CREATE PROCEDURE etl.usp_LogRejectedRecordSet
(
    @ObjectName         NVARCHAR(200),
    @BatchId            BIGINT          = NULL,
    @PackageExecutionId BIGINT          = NULL,
    @SourceSystemCode   NVARCHAR(20)    = NULL,
    @RejectStage        NVARCHAR(50)    = N'Stage',
    @RejectReasonCode   NVARCHAR(50)    = NULL,
    @SourceTable        NVARCHAR(200)   = NULL,
    @SourceFilter       NVARCHAR(1000)  = NULL,
    @LoadTag            NVARCHAR(100)   = NULL,
    @PurgeStaging       BIT             = 1,
    @RejectedRowCount   BIGINT          = NULL OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @RejectedRowCount = 0;

    IF @SourceTable IS NULL AND @LoadTag IS NULL
    BEGIN
        RAISERROR (N'etl.usp_LogRejectedRecordSet needs either @SourceTable or @LoadTag.', 16, 1);
        RETURN 1;
    END

    IF @BatchId IS NULL AND @PackageExecutionId IS NOT NULL
        SELECT @BatchId = pe.BatchId
        FROM   etl.PackageExecution AS pe
        WHERE  pe.PackageExecutionId = @PackageExecutionId;

    IF @LoadTag IS NOT NULL
    BEGIN
        INSERT INTO etl.RejectedRecord
            (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,
             RejectReasonCode, RejectReason, RejectStage, SsisErrorCode, SsisErrorColumn, RecordPayload)
        SELECT  COALESCE(s.PackageExecutionId, @PackageExecutionId),
                COALESCE(s.BatchId, @BatchId),
                COALESCE(s.SourceSystemCode, @SourceSystemCode),
                COALESCE(s.ObjectName, @ObjectName),
                s.BusinessKey,
                COALESCE(s.RejectReasonCode, @RejectReasonCode, N'UNSPECIFIED'),
                s.RejectReason,
                COALESCE(s.RejectStage, @RejectStage),
                s.SsisErrorCode,
                s.SsisErrorColumn,
                s.RecordPayload
        FROM    etl.RejectedRecordStaging AS s
        WHERE   s.LoadTag = @LoadTag;

        SET @RejectedRowCount = @@ROWCOUNT;

        IF @PurgeStaging = 1
            DELETE FROM etl.RejectedRecordStaging WHERE LoadTag = @LoadTag;

        RETURN 0;
    END

    /*
        The err.* tables do not share a column list - each one carries the shape
        of the load that rejected the row - so the columns this procedure needs
        are looked up and the rest are registered as NULL. Object and column
        names are quoted, and the caller's filter is the only free text, which is
        why @SourceFilter is restricted to callers inside the control framework.
    */
    DECLARE @SchemaName     SYSNAME = PARSENAME(@SourceTable, 2),
            @TableName      SYSNAME = PARSENAME(@SourceTable, 1),
            @Sql            NVARCHAR(MAX),
            @KeyColumn      NVARCHAR(200),
            @ReasonCode     NVARCHAR(200),
            @ReasonText     NVARCHAR(200),
            @PayloadColumn  NVARCHAR(200);

    IF @SchemaName IS NULL OR @TableName IS NULL
    BEGIN
        RAISERROR (N'@SourceTable must be schema qualified.', 16, 1);
        RETURN 1;
    END

    IF OBJECT_ID(QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName), N'U') IS NULL
    BEGIN
        RAISERROR (N'@SourceTable does not exist.', 16, 1);
        RETURN 1;
    END

    SELECT  @KeyColumn = MAX(CASE WHEN c.name = N'BusinessKey'      THEN QUOTENAME(c.name) END),
            @ReasonCode = MAX(CASE WHEN c.name = N'RejectReasonCode' THEN QUOTENAME(c.name) END),
            @ReasonText = MAX(CASE WHEN c.name = N'RejectReason'     THEN QUOTENAME(c.name) END),
            @PayloadColumn = MAX(CASE WHEN c.name = N'RecordPayload' THEN QUOTENAME(c.name) END)
    FROM    sys.columns AS c
    WHERE   c.object_id = OBJECT_ID(QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName));

    SET @Sql =
        N'INSERT INTO etl.RejectedRecord' + NCHAR(10)
        + N'    (PackageExecutionId, BatchId, SourceSystemCode, ObjectName, BusinessKey,' + NCHAR(10)
        + N'     RejectReasonCode, RejectReason, RejectStage, RecordPayload)' + NCHAR(10)
        + N'SELECT @PackageExecutionId, @BatchId, @SourceSystemCode, @ObjectName, '
        + ISNULL(@KeyColumn, N'NULL') + N',' + NCHAR(10)
        + N'       ISNULL(' + ISNULL(@ReasonCode, N'NULL') + N', @RejectReasonCode), '
        + ISNULL(@ReasonText, N'NULL') + N', @RejectStage, ' + ISNULL(@PayloadColumn, N'NULL') + NCHAR(10)
        + N'FROM ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) + NCHAR(10)
        + ISNULL(N'WHERE ' + @SourceFilter, N'') + N';';

    EXEC sp_executesql @Sql,
         N'@PackageExecutionId BIGINT, @BatchId BIGINT, @SourceSystemCode NVARCHAR(20),
           @ObjectName NVARCHAR(200), @RejectReasonCode NVARCHAR(50), @RejectStage NVARCHAR(50)',
         @PackageExecutionId = @PackageExecutionId,
         @BatchId            = @BatchId,
         @SourceSystemCode   = @SourceSystemCode,
         @ObjectName         = @ObjectName,
         @RejectReasonCode   = @RejectReasonCode,
         @RejectStage        = @RejectStage;

    SET @RejectedRowCount = @@ROWCOUNT;

    RETURN 0;
END
GO
