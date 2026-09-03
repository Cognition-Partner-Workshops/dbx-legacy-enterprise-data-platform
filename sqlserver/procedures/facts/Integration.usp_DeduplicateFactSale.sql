/*
    Integration.usp_DeduplicateFactSale

    Object        : Integration.usp_DeduplicateFactSale
    Deploy target : WideWorldImportersDW
    Deploy order  : after Integration.usp_LoadFactSale.
    Called by     : FACT_Deduplicate_Sale, and manually after a re-run of the
                    invoice extract.
    Reads         : Fact.Sale.
    Depends on    : the etl control procedures.

    Fact.Sale is loaded delete-by-window, so a duplicate can only appear when
    the same invoice line arrives in two different windows - which happens
    whenever the source re-dates an invoice across the window boundary. The
    surviving row is the one with the highest [Source Row Version], falling
    back to the highest [Sale Key] where the version is missing (pre-2017 rows
    have no version at all).

    Reversal and restatement rows are excluded from the duplicate check: they
    intentionally share a natural key with the original.

    Removed rows are copied to Fact.Sale Duplicate Archive before deletion,
    because in 2019 this procedure deleted 40,000 good rows and there was no
    way back.
*/
IF OBJECT_ID(N'Integration.usp_DeduplicateFactSale', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_DeduplicateFactSale;
GO

CREATE PROCEDURE Integration.usp_DeduplicateFactSale
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @LoadStartDate      DATE = NULL,
    @LoadEndDate        DATE = NULL,
    @ReportOnly         BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;

    SET @LoadEndDate   = ISNULL(@LoadEndDate, CONVERT(DATE, SYSDATETIME()));
    SET @LoadStartDate = ISNULL(@LoadStartDate, DATEADD(DAY, -90, @LoadEndDate));

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Deduplicate_Sale',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'DeduplicateFactSale',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        SELECT
            s.[Sale Key],
            s.[Invoice Number],
            s.[Invoice Line Number],
            ROW_NUMBER() OVER
            (
                PARTITION BY s.[Natural Key Hash]
                ORDER BY ISNULL(s.[Source Row Version], 0) DESC, s.[Sale Key] DESC
            ) AS DuplicateRank,
            COUNT(*) OVER (PARTITION BY s.[Natural Key Hash]) AS DuplicateCount
        INTO #SaleDuplicate
        FROM Fact.[Sale] AS s
        WHERE s.[Invoice Date Key] BETWEEN @LoadStartDate AND @LoadEndDate
          AND ISNULL(s.[Correction Type Code], N'ORIG') = N'ORIG';

        SELECT @SourceRowCount = COUNT_BIG(*) FROM #SaleDuplicate WHERE DuplicateCount > 1;

        IF @SourceRowCount > 0
        BEGIN
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'DW',
                @ObjectName         = N'Fact.Sale',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'DUPLICATE_INVOICE_LINE',
                @RejectReason       = N'Invoice line present more than once in Fact.Sale',
                @RejectStage        = N'Deduplicate';

            SET @RejectRowCount = 1;
        END;

        IF @ReportOnly = 0 AND @SourceRowCount > 0
        BEGIN
            IF OBJECT_ID(N'Fact.Sale Duplicate Archive', N'U') IS NULL
                SELECT TOP (0) *, CONVERT(BIGINT, NULL) AS [Archive Batch Id],
                       CONVERT(DATETIME2(3), NULL) AS [Archive Datetime]
                INTO Fact.[Sale Duplicate Archive]
                FROM Fact.[Sale];

            INSERT INTO Fact.[Sale Duplicate Archive]
            SELECT s.*, @BatchId, SYSDATETIME()
            FROM Fact.[Sale] AS s
            INNER JOIN #SaleDuplicate AS d
                ON d.[Sale Key] = s.[Sale Key]
            WHERE d.DuplicateRank > 1;

            DELETE s
            FROM Fact.[Sale] AS s
            INNER JOIN #SaleDuplicate AS d
                ON d.[Sale Key] = s.[Sale Key]
            WHERE d.DuplicateRank > 1;

            SET @DeleteRowCount = @@ROWCOUNT;
        END;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Sale',
            @SourceRowCount     = @SourceRowCount,
            @DeleteRowCount     = @DeleteRowCount,
            @RejectRowCount     = @RejectRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsDeleted        = @DeleteRowCount,
                @RowsRejected       = @RejectRowCount;

        DROP TABLE #SaleDuplicate;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Sale',
            @SourceComponent    = N'Deduplicate',
            @ProcedureName      = N'Integration.usp_DeduplicateFactSale',
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
