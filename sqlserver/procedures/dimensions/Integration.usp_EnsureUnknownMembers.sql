/*
    Object        : [Integration].[usp_EnsureUnknownMembers]
    Deploy target : WideWorldImportersDW
    Depends on    : Integration.DimensionKeyRegistry, every Dimension.* table,
                    the etl control framework
    Called by     : DIM_Ensure_UnknownMembers, the first dimension step of the
                    nightly master package

    The runtime equivalent of 90_unknown_members.sql. The deployment script seeds
    the reserved members once; this procedure re-checks them every night, because
    the reserved rows have been deleted by hand more than once (twice by a
    truncate-and-reload of a reference dimension, once by a DBA cleaning up what
    looked like corrupt data with a negative key).

    Reserved keys, per the registry:

        -1  Unknown          -2  Not Applicable   -3  Invalid
        -4  Inferred Pending -9  Error             0  Not Yet Assigned

    Rather than hand-write an insert per dimension, this builds one from
    sys.columns: every NOT NULL column that has no default gets a type-appropriate
    placeholder. It produces uglier rows than the hand-written script does, which
    is the trade the 2016 author made to stop editing this every time a dimension
    arrived. IDENTITY key columns are skipped with IDENTITY_INSERT.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_EnsureUnknownMembers]
    @BatchId            BIGINT,
    @DimensionName      NVARCHAR(100) = NULL,   -- NULL means every registered dimension
    @PackageName        NVARCHAR(200) = N'DIM_Ensure_UnknownMembers',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @ValidFrom          DATETIME2(7) = CONVERT(DATETIME2(7), N'1900-01-01T00:00:00');
    DECLARE @ValidTo            DATETIME2(7) = CONVERT(DATETIME2(7), N'9999-12-31T23:59:59.9999999');
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @CheckedCount       BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    DECLARE @vDimension     NVARCHAR(100);
    DECLARE @vKeyColumn     NVARCHAR(100);
    DECLARE @vObjectId      INT;
    DECLARE @vKeyValue      INT;
    DECLARE @vDescription   NVARCHAR(40);
    DECLARE @vColumnList    NVARCHAR(MAX);
    DECLARE @vValueList     NVARCHAR(MAX);
    DECLARE @vHasIdentity   BIT;
    DECLARE @Sql            NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'ReservedMembers',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#ReservedMember') IS NOT NULL DROP TABLE #ReservedMember;

        CREATE TABLE #ReservedMember ([Key Value] INT NOT NULL, [Description] NVARCHAR(40) NOT NULL);

        INSERT INTO #ReservedMember ([Key Value], [Description])
        VALUES (-1, N'Unknown'), (-2, N'Not Applicable'), (-3, N'Invalid'),
               (-4, N'Inferred Pending'), (-9, N'Error');

        DECLARE curDimension CURSOR LOCAL FAST_FORWARD FOR
            SELECT r.[Dimension Name], r.[Key Column Name]
            FROM [Integration].[DimensionKeyRegistry] AS r
            WHERE (@DimensionName IS NULL OR r.[Dimension Name] = @DimensionName)
              AND r.[SCD Pattern] NOT IN (N'Bridge')     -- a bridge has no member to reserve
              AND OBJECT_ID(r.[Dimension Name], N'U') IS NOT NULL;

        OPEN curDimension;
        FETCH NEXT FROM curDimension INTO @vDimension, @vKeyColumn;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @vObjectId = OBJECT_ID(@vDimension, N'U');

            /* The registry key column name does not always match the table -
               Dimension.Date is registered as 'Date Key' and the column is
               'DateKey'. Fall back to the single-column primary key. */
            IF COL_LENGTH(@vDimension, @vKeyColumn) IS NULL
                SELECT TOP (1) @vKeyColumn = c.[name]
                FROM sys.index_columns AS ic
                INNER JOIN sys.indexes AS i
                    ON  i.[object_id] = ic.[object_id]
                    AND i.[index_id]  = ic.[index_id]
                    AND i.[is_primary_key] = 1
                INNER JOIN sys.columns AS c
                    ON  c.[object_id] = ic.[object_id]
                    AND c.[column_id] = ic.[column_id]
                WHERE ic.[object_id] = @vObjectId
                ORDER BY ic.[key_ordinal];

            IF COL_LENGTH(@vDimension, @vKeyColumn) IS NULL
                GOTO NextDimension;     -- no usable key column; the hand-written script covers it

            SET @vHasIdentity = CASE WHEN EXISTS (SELECT 1 FROM sys.columns
                                                  WHERE [object_id] = @vObjectId
                                                    AND [is_identity] = 1)
                                     THEN 1 ELSE 0 END;

            /* Column list: the key, plus every other NOT NULL column with no
               default, given a placeholder of the right type. */
            SET @vColumnList = NULL;
            SET @vValueList  = NULL;

            SELECT @vColumnList = CONCAT(ISNULL(@vColumnList + N', ', N''), QUOTENAME(c.[name])),
                   @vValueList  = CONCAT(ISNULL(@vValueList + N', ', N''),
                        CASE
                            WHEN c.[name] = N'Lineage Key' THEN N'@LineageParam'
                            WHEN c.[name] IN (N'Valid From', N'Effective From') THEN N'@ValidFromParam'
                            WHEN c.[name] IN (N'Valid To', N'Effective To')     THEN N'@ValidToParam'
                            WHEN c.[name] = N'Is Current Row'                   THEN N'1'
                            WHEN c.[name] = N'Version Number'                   THEN N'1'
                            WHEN t.[name] IN (N'nvarchar', N'varchar', N'nchar', N'char')
                                 THEN N'LEFT(@DescriptionParam, '
                                      + CONVERT(NVARCHAR(10),
                                            CASE WHEN c.[max_length] < 0 THEN 40
                                                 WHEN t.[name] IN (N'nvarchar', N'nchar')
                                                 THEN c.[max_length] / 2 ELSE c.[max_length] END)
                                      + N')'
                            WHEN t.[name] IN (N'date')                          THEN N'CONVERT(DATE, @ValidFromParam)'
                            WHEN t.[name] IN (N'datetime2', N'datetime', N'smalldatetime')
                                                                                THEN N'@ValidFromParam'
                            WHEN t.[name] = N'bit'                              THEN N'0'
                            WHEN t.[name] IN (N'varbinary', N'binary')          THEN N'0x'
                            WHEN t.[name] = N'uniqueidentifier'                 THEN N'0x0'
                            ELSE N'@KeyValueParam'
                        END)
            FROM sys.columns AS c
            INNER JOIN sys.types AS t
                ON t.[user_type_id] = c.[user_type_id]
            WHERE c.[object_id]  = @vObjectId
              AND c.[is_nullable] = 0
              AND c.[is_identity] = 0
              AND c.[is_computed] = 0
              AND c.[name] <> @vKeyColumn
            ORDER BY c.[column_id];

            SET @vColumnList = CONCAT(QUOTENAME(@vKeyColumn),
                                      CASE WHEN @vColumnList IS NULL THEN N'' ELSE N', ' + @vColumnList END);
            SET @vValueList  = CONCAT(N'@KeyValueParam',
                                      CASE WHEN @vValueList IS NULL THEN N'' ELSE N', ' + @vValueList END);

            DECLARE curReserved CURSOR LOCAL FAST_FORWARD FOR
                SELECT [Key Value], [Description] FROM #ReservedMember;

            OPEN curReserved;
            FETCH NEXT FROM curReserved INTO @vKeyValue, @vDescription;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @CheckedCount = @CheckedCount + 1;

                SET @Sql = CASE WHEN @vHasIdentity = 1
                                THEN N'SET IDENTITY_INSERT ' + @vDimension + N' ON; ' ELSE N'' END
                         + N'IF NOT EXISTS (SELECT 1 FROM ' + @vDimension
                         + N' WHERE ' + QUOTENAME(@vKeyColumn) + N' = @KeyValueParam) '
                         + N'INSERT INTO ' + @vDimension + N' (' + @vColumnList + N') '
                         + N'VALUES (' + @vValueList + N');'
                         + CASE WHEN @vHasIdentity = 1
                                THEN N' SET IDENTITY_INSERT ' + @vDimension + N' OFF;' ELSE N'' END;

                BEGIN TRY
                    EXEC sys.sp_executesql
                         @Sql,
                         N'@KeyValueParam INT, @DescriptionParam NVARCHAR(40), @LineageParam INT, @ValidFromParam DATETIME2(7), @ValidToParam DATETIME2(7)',
                         @KeyValueParam    = @vKeyValue,
                         @DescriptionParam = @vDescription,
                         @LineageParam     = @LineageKey,
                         @ValidFromParam   = @ValidFrom,
                         @ValidToParam     = @ValidTo;

                    SET @InsertedCount = @InsertedCount + @@ROWCOUNT;
                END TRY
                BEGIN CATCH
                    /* Some dimensions carry check constraints the generic
                       placeholder cannot satisfy - those are the ones the
                       hand-written 90_unknown_members.sql covers. */
                    SET @ErrorMessage = ERROR_MESSAGE();

                    EXEC [etl].[usp_LogError]
                         @PackageExecutionId = @PackageExecutionId,
                         @BatchId            = @BatchId,
                         @ErrorSeverity      = N'Warning',
                         @SourceName         = @PackageName,
                         @SourceComponent    = @vDimension,
                         @ProcedureName      = N'Integration.usp_EnsureUnknownMembers',
                         @ErrorDescription   = @ErrorMessage;
                END CATCH;

                FETCH NEXT FROM curReserved INTO @vKeyValue, @vDescription;
            END;

            CLOSE curReserved;
            DEALLOCATE curReserved;

NextDimension:
            FETCH NEXT FROM curDimension INTO @vDimension, @vKeyColumn;
        END;

        CLOSE curDimension;
        DEALLOCATE curDimension;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [New Member Count], [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'(reserved members)', N'GLOBAL', @BatchId, @PackageExecutionId,
             @CheckedCount, @InsertedCount, N'ReservedMembers', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'(reserved members)',
             @SourceRowCount     = @CheckedCount,
             @InsertRowCount     = @InsertedCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @CheckedCount,
             @RowsInserted       = @InsertedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        IF CURSOR_STATUS(N'local', N'curReserved') >= 0
        BEGIN
            CLOSE curReserved;
            DEALLOCATE curReserved;
        END;

        IF CURSOR_STATUS(N'local', N'curDimension') >= 0
        BEGIN
            CLOSE curDimension;
            DEALLOCATE curDimension;
        END;

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'(reserved members)',
             @ProcedureName      = N'Integration.usp_EnsureUnknownMembers',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @CheckedCount,
             @RowsInserted       = @InsertedCount;

        THROW;
    END CATCH;
END;
GO
