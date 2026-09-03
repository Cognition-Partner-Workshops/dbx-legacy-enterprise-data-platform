/*
    Integration.usp_RekeyLateArrivingDimensions

    Object        : Integration.usp_RekeyLateArrivingDimensions
    Deploy target : WideWorldImportersDW
    Deploy order  : after all fact loads and Fact.Fact Load Hold.
    Called by     : FACT_Rekey_Late_Arriving (runs at the end of the nightly
                    batch and again on Sunday over a wider window).
    Reads         : Fact.Fact Load Hold, etl.FactRekeyQueue, the dimensions.
    Depends on    : the etl control procedures.

    Two jobs in one procedure, which is how it grew:

    1. Facts parked in Fact.Fact Load Hold because their dimension row had not
       arrived. Each held row is retried; if the dimension now exists the fact
       is replayed into its target fact table by name using dynamic SQL, and
       the hold row is closed. After the regional retry limit the hold row is
       abandoned and rejected.

    2. Facts that were loaded pointing at an INFERRED dimension member which
       has since been replaced by the real member. Those facts are repointed
       at the real surrogate key. This is the only place in the warehouse that
       rewrites a surrogate key on an existing fact row.

    Retry limits differ by region because the source feeds do: NA master data
    lands within a day, EU within three, APAC can take a fortnight when the
    Singapore office is on holiday.
*/
IF OBJECT_ID(N'Integration.usp_RekeyLateArrivingDimensions', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RekeyLateArrivingDimensions;
GO

CREATE PROCEDURE Integration.usp_RekeyLateArrivingDimensions
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @MaxHoldRows        INT = 50000
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution   BIT = 0;
    DECLARE @SourceRowCount  BIGINT = 0;
    DECLARE @UpdateRowCount  BIGINT = 0;
    DECLARE @InsertRowCount  BIGINT = 0;
    DECLARE @RejectRowCount  BIGINT = 0;

    DECLARE @HoldId          BIGINT;
    DECLARE @TargetFact      NVARCHAR(128);
    DECLARE @DimensionName   NVARCHAR(128);
    DECLARE @BusinessKey     NVARCHAR(200);
    DECLARE @RegionCode      NVARCHAR(10);
    DECLARE @RetryCount      INT;
    DECLARE @RetryLimit      INT;
    DECLARE @ResolvedKey     INT;
    DECLARE @Sql             NVARCHAR(MAX);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Rekey_Late_Arriving',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'RekeyLateArrivingDimensions',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        /* ---------- 1. Hold-and-retry ---------- */
        DECLARE hold_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT TOP (@MaxHoldRows)
                   h.[Fact Load Hold Key], h.[Target Fact Name], h.[Missing Dimension Name],
                   h.[Missing Business Key], h.[Region Code], ISNULL(h.[Retry Count], 0)
            FROM Fact.[Fact Load Hold] AS h
            WHERE h.[Hold Status Code] = N'HELD'
            ORDER BY h.[First Held Datetime];

        OPEN hold_cursor;
        FETCH NEXT FROM hold_cursor
            INTO @HoldId, @TargetFact, @DimensionName, @BusinessKey, @RegionCode, @RetryCount;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SourceRowCount = @SourceRowCount + 1;
            SET @ResolvedKey = NULL;

            SET @RetryLimit = CASE @RegionCode
                                  WHEN N'NA'   THEN 3
                                  WHEN N'EU'   THEN 7
                                  WHEN N'APAC' THEN 21
                                  ELSE 5
                              END;

            /* The dimension and its business-key column are both data, so the
               lookup has to be built as a string. */
            SET @Sql = N'SELECT @KeyOut = MAX(d.[' + REPLACE(@DimensionName, N']', N']]')
                     + N' Key]) FROM Dimension.[' + REPLACE(@DimensionName, N']', N']]')
                     + N'] AS d WHERE CONVERT(NVARCHAR(200), d.[Business Key]) = @KeyIn'
                     + N' AND d.[Is Inferred Member] = 0;';

            IF OBJECT_ID(N'Dimension.[' + @DimensionName + N']', N'U') IS NOT NULL
                EXECUTE sp_executesql @Sql,
                    N'@KeyIn NVARCHAR(200), @KeyOut INT OUTPUT',
                    @KeyIn = @BusinessKey, @KeyOut = @ResolvedKey OUTPUT;

            IF @ResolvedKey IS NOT NULL
            BEGIN
                /* Replay the held payload into its fact table. The payload was
                   stored as an INSERT statement at hold time, with the key
                   left as a token. */
                SELECT @Sql = REPLACE(CONVERT(NVARCHAR(MAX), h.[Source Payload]),
                                      N'{RESOLVED_KEY}',
                                      CONVERT(NVARCHAR(20), @ResolvedKey))
                FROM Fact.[Fact Load Hold] AS h
                WHERE h.[Fact Load Hold Key] = @HoldId;

                IF @Sql IS NOT NULL AND LEN(@Sql) > 0
                BEGIN
                    EXECUTE sp_executesql @Sql;
                    SET @InsertRowCount = @InsertRowCount + 1;
                END;

                UPDATE Fact.[Fact Load Hold]
                SET [Hold Status Code]   = N'RELEASED',
                    [Released Fact Key]  = @ResolvedKey,
                    [Released Datetime]  = SYSDATETIME(),
                    [Retry Count]        = @RetryCount + 1,
                    [Last Batch Id]      = @BatchId
                WHERE [Fact Load Hold Key] = @HoldId;
            END
            ELSE IF @RetryCount + 1 >= @RetryLimit
            BEGIN
                UPDATE Fact.[Fact Load Hold]
                SET [Hold Status Code]  = N'ABANDONED',
                    [Retry Count]       = @RetryCount + 1,
                    [Released Datetime] = SYSDATETIME(),
                    [Last Batch Id]     = @BatchId
                WHERE [Fact Load Hold Key] = @HoldId;

                EXECUTE etl.usp_LogRejectedRecord
                    @PackageExecutionId = @PackageExecutionId,
                    @BatchId            = @BatchId,
                    @SourceSystemCode   = N'DW',
                    @ObjectName         = @TargetFact,
                    @BusinessKey        = @BusinessKey,
                    @RejectReasonCode   = N'HOLD_EXPIRED',
                    @RejectReason       = N'Dimension member never arrived within the regional retry limit',
                    @RejectStage        = N'Rekey';

                SET @RejectRowCount = @RejectRowCount + 1;
            END
            ELSE
            BEGIN
                UPDATE Fact.[Fact Load Hold]
                SET [Retry Count]      = @RetryCount + 1,
                    [Last Retry Datetime] = SYSDATETIME(),
                    [Last Batch Id]    = @BatchId
                WHERE [Fact Load Hold Key] = @HoldId;
            END;

            FETCH NEXT FROM hold_cursor
                INTO @HoldId, @TargetFact, @DimensionName, @BusinessKey, @RegionCode, @RetryCount;
        END;

        CLOSE hold_cursor;
        DEALLOCATE hold_cursor;

        /* ---------- 2. Repoint facts off inferred members ---------- */
        UPDATE f
        SET [Customer Key] = real_member.[Customer Key],
            [Batch Id]     = @BatchId
        FROM Fact.[Sale] AS f
        INNER JOIN Dimension.[Customer] AS inferred
            ON inferred.[Customer Key] = f.[Customer Key]
           AND inferred.[Is Inferred Member] = 1
        INNER JOIN Dimension.[Customer] AS real_member
            ON real_member.[WWI Customer ID] = inferred.[WWI Customer ID]
           AND real_member.[Is Inferred Member] = 0
           AND real_member.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31');

        SET @UpdateRowCount = @@ROWCOUNT;

        UPDATE f
        SET [Stock Item Key] = real_member.[Stock Item Key],
            [Batch Id]       = @BatchId
        FROM Fact.[Movement] AS f
        INNER JOIN Dimension.[Stock Item] AS inferred
            ON inferred.[Stock Item Key] = f.[Stock Item Key]
           AND inferred.[Is Inferred Member] = 1
        INNER JOIN Dimension.[Stock Item] AS real_member
            ON real_member.[Stock Item Code] = inferred.[Stock Item Code]
           AND real_member.[Is Inferred Member] = 0
           AND real_member.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31');

        SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT;

        UPDATE f
        SET [Supplier Key] = real_member.[Supplier Key],
            [Batch Id]     = @BatchId
        FROM Fact.[Purchase] AS f
        INNER JOIN Dimension.[Supplier] AS inferred
            ON inferred.[Supplier Key] = f.[Supplier Key]
           AND inferred.[Is Inferred Member] = 1
        INNER JOIN Dimension.[Supplier] AS real_member
            ON real_member.[Supplier Reference] = inferred.[Supplier Reference]
           AND real_member.[Is Inferred Member] = 0
           AND real_member.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31');

        SET @UpdateRowCount = @UpdateRowCount + @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Fact Load Hold',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount,
            @RejectRowCount     = @RejectRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'hold_cursor') >= 0
        BEGIN
            CLOSE hold_cursor;
            DEALLOCATE hold_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Fact Load Hold',
            @SourceComponent    = N'Late arriving rekey',
            @ProcedureName      = N'Integration.usp_RekeyLateArrivingDimensions',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
