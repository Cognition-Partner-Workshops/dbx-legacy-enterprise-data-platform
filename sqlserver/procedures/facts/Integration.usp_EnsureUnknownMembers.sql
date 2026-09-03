/*
    Integration.usp_EnsureUnknownMembers

    Object        : Integration.usp_EnsureUnknownMembers
    Deploy target : WideWorldImportersDW
    Deploy order  : after the Dimension tables, before any fact load.
    Called by     : DIM_Load_Unknown_Members, and defensively by the first fact
                    load of every batch.
    Depends on    : etl.usp_LogPackageStart, etl.usp_LogPackageEnd,
                    etl.usp_LogRowCount, etl.usp_LogError.

    Every dimension carries a member with surrogate key 0 ("Unknown") and a
    member with key -1 ("Not Applicable"). Fact loads fall back to 0 when a
    business key is present but unresolvable, and to -1 when the relationship
    genuinely does not apply (a cash sale has no salesperson, an anonymous web
    session has no customer).

    The two are different on purpose: counting rows against key 0 is the data
    quality measure the stewards chase, counting rows against -1 is not.

    The dimension list is held in a table variable rather than driven off
    sys.tables because three dimensions do not follow the key naming convention
    and were easier to hard-code in 2011 than to fix.
*/
IF OBJECT_ID(N'Integration.usp_EnsureUnknownMembers', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_EnsureUnknownMembers;
GO

CREATE PROCEDURE Integration.usp_EnsureUnknownMembers
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution   BIT = 0;
    DECLARE @InsertRowCount  BIGINT = 0;
    DECLARE @RejectRowCount  BIGINT = 0;

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'DIM_Load_Unknown_Members',
            @ProjectName        = N'WWI_Dimensions',
            @StepName           = N'EnsureUnknownMembers',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DECLARE @Dimensions TABLE
        (
            DimensionName   NVARCHAR(128) NOT NULL,
            KeyColumnName   NVARCHAR(128) NOT NULL,
            LabelColumnName NVARCHAR(128) NOT NULL
        );

        INSERT INTO @Dimensions (DimensionName, KeyColumnName, LabelColumnName)
        VALUES
            (N'Customer',         N'Customer Key',         N'Customer'),
            (N'Customer Segment', N'Customer Segment Key', N'Segment Name'),
            (N'Supplier',         N'Supplier Key',         N'Supplier'),
            (N'Vendor Contract',  N'Vendor Contract Key',  N'Contract Reference'),
            (N'Stock Item',       N'Stock Item Key',       N'Stock Item'),
            (N'Product Category', N'Product Category Key', N'Category Name'),
            (N'Employee',         N'Employee Key',         N'Employee'),
            (N'Salesperson',      N'Salesperson Key',      N'Salesperson'),
            (N'City',             N'City Key',             N'City'),
            (N'Geography',        N'Geography Key',        N'Country Name'),
            (N'Sales Territory',  N'Sales Territory Key',  N'Territory Name'),
            (N'Payment Method',   N'Payment Method Key',   N'Payment Method Name'),
            (N'Transaction Type', N'Transaction Type Key', N'Transaction Type'),
            (N'Currency',         N'Currency Key',         N'Currency Name'),
            (N'Cost Center',      N'Cost Center Key',      N'Cost Center Name'),
            (N'Warehouse Site',   N'Warehouse Site Key',   N'Site Name'),
            (N'Carrier',          N'Carrier Key',          N'Carrier Name'),
            (N'Promotion',        N'Promotion Key',        N'Promotion Name'),
            (N'Sales Channel',    N'Sales Channel Key',    N'Channel Name'),
            (N'Return Reason',    N'Return Reason Key',    N'Reason Description'),
            (N'Loyalty Tier',     N'Loyalty Tier Key',     N'Tier Name'),
            (N'Payment Terms',    N'Payment Terms Key',    N'Terms Description');

        DECLARE @DimensionName   NVARCHAR(128);
        DECLARE @KeyColumnName   NVARCHAR(128);
        DECLARE @LabelColumnName NVARCHAR(128);
        DECLARE @MemberKey       INT;
        DECLARE @MemberLabel     NVARCHAR(50);
        DECLARE @Sql             NVARCHAR(MAX);
        DECLARE @Affected        INT;

        DECLARE dim_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT DimensionName, KeyColumnName, LabelColumnName FROM @Dimensions;

        OPEN dim_cursor;
        FETCH NEXT FROM dim_cursor INTO @DimensionName, @KeyColumnName, @LabelColumnName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF OBJECT_ID(N'Dimension.[' + @DimensionName + N']', N'U') IS NULL
            BEGIN
                SET @RejectRowCount = @RejectRowCount + 1;

                EXECUTE etl.usp_LogRejectedRecord
                    @PackageExecutionId = @PackageExecutionId,
                    @BatchId            = @BatchId,
                    @SourceSystemCode   = N'DW',
                    @ObjectName         = N'Integration.usp_EnsureUnknownMembers',
                    @BusinessKey        = @DimensionName,
                    @RejectReasonCode   = N'DIM_MISSING',
                    @RejectReason       = N'Dimension table declared in the catalog is not deployed',
                    @RejectStage        = N'Dimension';
            END
            ELSE
            BEGIN
                DECLARE @Members TABLE (MemberKey INT, MemberLabel NVARCHAR(50));
                DELETE FROM @Members;
                INSERT INTO @Members (MemberKey, MemberLabel)
                VALUES (0, N'Unknown'), (-1, N'N/A');

                DECLARE member_cursor CURSOR LOCAL FAST_FORWARD FOR
                    SELECT MemberKey, MemberLabel FROM @Members;

                OPEN member_cursor;
                FETCH NEXT FROM member_cursor INTO @MemberKey, @MemberLabel;

                WHILE @@FETCH_STATUS = 0
                BEGIN
                    SET @Sql =
                        N'IF NOT EXISTS (SELECT 1 FROM Dimension.[' + @DimensionName + N']' +
                        N' WHERE [' + @KeyColumnName + N'] = @Key)' + CHAR(13) + CHAR(10) +
                        N'BEGIN' + CHAR(13) + CHAR(10) +
                        N'    SET IDENTITY_INSERT Dimension.[' + @DimensionName + N'] ON;' + CHAR(13) + CHAR(10) +
                        N'    INSERT INTO Dimension.[' + @DimensionName + N'] ([' + @KeyColumnName + N'], [' +
                        @LabelColumnName + N'], [Lineage Key])' + CHAR(13) + CHAR(10) +
                        N'    VALUES (@Key, @Label, 0);' + CHAR(13) + CHAR(10) +
                        N'    SET IDENTITY_INSERT Dimension.[' + @DimensionName + N'] OFF;' + CHAR(13) + CHAR(10) +
                        N'END;';

                    EXECUTE sp_executesql @Sql,
                        N'@Key INT, @Label NVARCHAR(50)',
                        @Key = @MemberKey, @Label = @MemberLabel;

                    SET @Affected = @@ROWCOUNT;
                    SET @InsertRowCount = @InsertRowCount + ISNULL(@Affected, 0);

                    FETCH NEXT FROM member_cursor INTO @MemberKey, @MemberLabel;
                END;

                CLOSE member_cursor;
                DEALLOCATE member_cursor;
            END;

            FETCH NEXT FROM dim_cursor INTO @DimensionName, @KeyColumnName, @LabelColumnName;
        END;

        CLOSE dim_cursor;
        DEALLOCATE dim_cursor;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Dimension.UnknownMembers',
            @SourceRowCount     = NULL,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'member_cursor') >= 0
        BEGIN
            CLOSE member_cursor;
            DEALLOCATE member_cursor;
        END;

        IF CURSOR_STATUS('local', 'dim_cursor') >= 0
        BEGIN
            CLOSE dim_cursor;
            DEALLOCATE dim_cursor;
        END;

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Integration.usp_EnsureUnknownMembers',
            @SourceComponent    = N'Dimension seed',
            @ProcedureName      = N'Integration.usp_EnsureUnknownMembers',
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
