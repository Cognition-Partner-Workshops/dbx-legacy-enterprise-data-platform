/*
    stg.usp_TranslateSourceCodes

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_TRANSLATE_CODES (SSIS), once per staged object
    Reads         : ref.CodeCrosswalk, ref.StatusCode, ref.ReasonCode
    Writes        : the caller's staging table (dynamic), err.RejectedLookupFailure
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    Generic code translation driven by dynamic SQL. One procedure translates any
    <schema>.<table>.<column> through ref.CodeCrosswalk, because thirteen
    near-identical translation procedures were what this replaced in 2015.

    Object and column names are resolved through QUOTENAME against sys.columns
    before they reach the statement, so a caller cannot inject through the
    parameters. Values are always passed as parameters, never concatenated.

    @UnmappedAction:
        LEAVE   keep the source value (the default; how the oldest packages ran)
        NULL    null the column and mark the row WARN
        DEFAULT substitute @DefaultConformedValue
    Unmapped values are always recorded in err.RejectedLookupFailure regardless
    of the action, so the stewards get their weekly list either way.
*/

IF OBJECT_ID(N'stg.usp_TranslateSourceCodes', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TranslateSourceCodes;
GO

CREATE PROCEDURE stg.usp_TranslateSourceCodes
(
    @BatchId                BIGINT,
    @PackageExecutionId     BIGINT = NULL,
    @TargetSchemaName       NVARCHAR(128),
    @TargetTableName        NVARCHAR(128),
    @TargetColumnName       NVARCHAR(128),
    @CodeDomainCode         NVARCHAR(30),
    @SourceSystemCode       NVARCHAR(20),
    @BusinessKeyColumnName  NVARCHAR(128),
    @UnmappedAction         NVARCHAR(10) = N'LEAVE',
    @DefaultConformedValue  NVARCHAR(20) = N'UNKNOWN'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = CONCAT(@TargetSchemaName, N'.', @TargetTableName);
    DECLARE @Sql          NVARCHAR(MAX);
    DECLARE @ParamList    NVARCHAR(1000);
    DECLARE @UpdatedRows  BIGINT = 0;
    DECLARE @UnmappedRows BIGINT = 0;
    DECLARE @QualifiedTarget NVARCHAR(300);
    DECLARE @QuotedColumn    NVARCHAR(130);
    DECLARE @QuotedKeyColumn NVARCHAR(130);

    BEGIN TRY
        --  Resolve every identifier against the catalog. Anything that is not a
        --  real column of a real table stops the procedure here.
        IF OBJECT_ID(QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName), N'U') IS NULL
            THROW 55001, N'stg.usp_TranslateSourceCodes: target table does not exist', 1;

        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName))
              AND c.name IN (@TargetColumnName, @BusinessKeyColumnName)
            GROUP BY c.object_id
            HAVING COUNT(DISTINCT c.name) = 2
        )
            THROW 55002, N'stg.usp_TranslateSourceCodes: target or key column does not exist', 1;

        IF @UnmappedAction NOT IN (N'LEAVE', N'NULL', N'DEFAULT')
            THROW 55003, N'stg.usp_TranslateSourceCodes: unsupported @UnmappedAction', 1;

        SET @QualifiedTarget = QUOTENAME(@TargetSchemaName) + N'.' + QUOTENAME(@TargetTableName);
        SET @QuotedColumn    = QUOTENAME(@TargetColumnName);
        SET @QuotedKeyColumn = QUOTENAME(@BusinessKeyColumnName);

        SET @ParamList = N'@BatchId BIGINT, @PackageExecutionId BIGINT, @CodeDomainCode NVARCHAR(30), '
                       + N'@SourceSystemCode NVARCHAR(20), @ObjectName NVARCHAR(200), '
                       + N'@DefaultConformedValue NVARCHAR(20), @RowsAffected BIGINT OUTPUT';

        --  Record the misses first: once the column is translated the original
        --  source value is gone.
        SET @Sql = N'
            INSERT INTO err.RejectedLookupFailure
            (
                BatchId, PackageExecutionId, SourceObjectName, SourceBusinessKey, LookupName,
                LookupColumnName, LookupValue, SourceSystemCode, RejectReasonCode, RejectReason,
                RejectStage, RoutedToUnknownMember, QueuedForLateArrival, OccurrenceCount, RecordPayload
            )
            SELECT
                @BatchId, @PackageExecutionId, @ObjectName, MIN(t.' + @QuotedKeyColumn + N'),
                N''ref.CodeCrosswalk'', ' + QUOTENAME(@TargetColumnName, '''') + N', t.' + @QuotedColumn + N',
                @SourceSystemCode, N''LOOKUP_MISS'',
                N''no active ref.CodeCrosswalk row for this domain, source system and value'',
                N''Transform'', 0, 0, COUNT_BIG(*), NULL
            FROM ' + @QualifiedTarget + N' AS t
            WHERE t.BatchId = @BatchId
              AND t.' + @QuotedColumn + N' IS NOT NULL
              AND NOT EXISTS
                  (
                      SELECT 1
                      FROM ref.CodeCrosswalk AS x
                      WHERE x.CodeDomainCode   = @CodeDomainCode
                        AND x.SourceSystemCode = @SourceSystemCode
                        AND x.SourceCodeValue  = t.' + @QuotedColumn + N'
                        AND x.EffectiveToDate IS NULL
                  )
            GROUP BY t.' + @QuotedColumn + N';
            SET @RowsAffected = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId               = @BatchId,
            @PackageExecutionId    = @PackageExecutionId,
            @CodeDomainCode        = @CodeDomainCode,
            @SourceSystemCode      = @SourceSystemCode,
            @ObjectName            = @ObjectName,
            @DefaultConformedValue = @DefaultConformedValue,
            @RowsAffected          = @UnmappedRows OUTPUT;

        SET @Sql = N'
            UPDATE t
            SET t.' + @QuotedColumn + N' = x.ConformedCodeValue
            FROM ' + @QualifiedTarget + N' AS t
            INNER JOIN ref.CodeCrosswalk AS x
                ON  x.CodeDomainCode   = @CodeDomainCode
                AND x.SourceSystemCode = @SourceSystemCode
                AND x.SourceCodeValue  = t.' + @QuotedColumn + N'
                AND x.EffectiveToDate IS NULL
            WHERE t.BatchId = @BatchId;
            SET @RowsAffected = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            @ParamList,
            @BatchId               = @BatchId,
            @PackageExecutionId    = @PackageExecutionId,
            @CodeDomainCode        = @CodeDomainCode,
            @SourceSystemCode      = @SourceSystemCode,
            @ObjectName            = @ObjectName,
            @DefaultConformedValue = @DefaultConformedValue,
            @RowsAffected          = @UpdatedRows OUTPUT;

        IF @UnmappedAction IN (N'NULL', N'DEFAULT')
        BEGIN
            SET @Sql = N'
                UPDATE t
                SET t.' + @QuotedColumn + N' = '
                    + CASE @UnmappedAction WHEN N'NULL' THEN N'NULL' ELSE N'@DefaultConformedValue' END + N',
                    t.DqStatusCode = N''WARN''
                FROM ' + @QualifiedTarget + N' AS t
                WHERE t.BatchId = @BatchId
                  AND t.' + @QuotedColumn + N' IS NOT NULL
                  AND NOT EXISTS
                      (
                          SELECT 1
                          FROM ref.CodeCrosswalk AS x
                          WHERE x.CodeDomainCode    = @CodeDomainCode
                            AND x.SourceSystemCode  = @SourceSystemCode
                            AND x.ConformedCodeValue = t.' + @QuotedColumn + N'
                            AND x.EffectiveToDate IS NULL
                      );
                SET @RowsAffected = @@ROWCOUNT;';

            DECLARE @DefaultedRows BIGINT = 0;

            EXEC sys.sp_executesql
                @Sql,
                @ParamList,
                @BatchId               = @BatchId,
                @PackageExecutionId    = @PackageExecutionId,
                @CodeDomainCode        = @CodeDomainCode,
                @SourceSystemCode      = @SourceSystemCode,
                @ObjectName            = @ObjectName,
                @DefaultConformedValue = @DefaultConformedValue,
                @RowsAffected          = @DefaultedRows OUTPUT;

            SET @UpdatedRows = @UpdatedRows + @DefaultedRows;
        END;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @UpdateRowCount     = @UpdatedRows,
            @RejectRowCount     = @UnmappedRows;
    END TRY
    BEGIN CATCH
        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_TRANSLATE_CODES',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TranslateSourceCodes';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
