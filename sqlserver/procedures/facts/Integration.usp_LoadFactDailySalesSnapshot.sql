/*
    Integration.usp_LoadFactDailySalesSnapshot

    Object        : Integration.usp_LoadFactDailySalesSnapshot
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Daily Snapshots, Fact.Sale, Fact.Return.
    Called by     : FACT_Load_Daily_Sales_Snapshot (05:00 daily).
    Reads         : Fact.Sale, Fact.Return, Fact.Credit Note, Dimension.Date.
    Depends on    : the etl control procedures.

    Periodic snapshot at salesperson / territory / channel grain, used by the
    commission run. It is deliberately NOT the same grain as
    Aggregate.Daily Sales Summary (product / territory) - the two disagree on
    total revenue whenever a sale has no salesperson, and finance has a
    reconciliation spreadsheet that explains the difference.

    The fiscal calendar is taken from Dimension.Date per region, which is what
    makes the same calendar day fall in different fiscal periods for NA
    (Jan-Dec), EU (Apr-Mar) and APAC (Jul-Jun).
*/
IF OBJECT_ID(N'Integration.usp_LoadFactDailySalesSnapshot', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactDailySalesSnapshot;
GO

CREATE PROCEDURE Integration.usp_LoadFactDailySalesSnapshot
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SnapshotDate       DATE = NULL,
    @RebuildDays        INT = 3
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @FromDate       DATE;

    SET @SnapshotDate = ISNULL(@SnapshotDate, DATEADD(DAY, -1, CONVERT(DATE, SYSDATETIME())));
    SET @FromDate = DATEADD(DAY, -@RebuildDays, @SnapshotDate);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Daily_Sales_Snapshot',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactDailySalesSnapshot',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Fact.[Daily Sales Snapshot]
        WHERE [Snapshot Date Key] BETWEEN @FromDate AND @SnapshotDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Daily Sales Snapshot]
        (
            [Snapshot Date Key], [Salesperson Key], [Sales Territory Key],
            [Sales Channel Key], [Customer Segment Key], [Region Code],
            [Fiscal Year], [Fiscal Period], [Fiscal Week], [Invoice Count],
            [Line Count], [Distinct Customer Count], [Quantity Sold],
            [Gross Sales Amount], [Discount Amount], [Net Sales Amount],
            [Tax Amount], [Freight Amount], [Cost Of Sales Amount],
            [Gross Margin Amount], [Margin Percent], [Returns Amount],
            [Credit Note Amount], [Net Sales Amount Reporting], [Average Order Value],
            [Commissionable Revenue], [Commission Rate], [Commission Accrued Amount],
            [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            s.[Invoice Date Key],
            s.[Salesperson Key],
            s.[Sales Territory Key],
            s.[Sales Channel Key],
            s.[Customer Segment Key],
            s.[Region Code],
            d.[Fiscal Year],
            d.[Fiscal Month Number],
            DATEPART(ISO_WEEK, s.[Invoice Date Key]),
            COUNT(DISTINCT s.[Invoice Number]),
            COUNT_BIG(*),
            COUNT(DISTINCT s.[Customer Key]),
            SUM(s.[Quantity Base UOM]),
            SUM(s.[Gross Amount]),
            SUM(s.[Line Discount Amount]),
            SUM(s.[Net Amount]),
            SUM(s.[Tax Amount]),
            SUM(ISNULL(s.[Freight Amount], 0)),
            SUM(s.[Cost Of Sale Amount]),
            SUM(s.[Gross Margin Amount]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            ISNULL(ret.ReturnCreditAmount, 0),
            ISNULL(cn.CreditNoteAmount, 0),
            SUM(s.[Net Amount Reporting]),
            CASE WHEN COUNT(DISTINCT s.[Invoice Number]) = 0 THEN 0
                 ELSE ROUND(SUM(s.[Net Amount]) / COUNT(DISTINCT s.[Invoice Number]), 2) END,
            /* Commission excludes freight everywhere, excludes tax everywhere,
               and in EU also excludes anything sold under a distributor
               agreement - the codes are hard-coded because the agreement
               dimension was never built. */
            SUM(CASE WHEN s.[Region Code] = N'EU' AND s.[Sales Channel Key] IN (7, 8, 12)
                     THEN 0 ELSE s.[Net Amount] END),
            NULL,
            NULL,
            0, @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        OUTER APPLY
        (
            SELECT SUM(r.[Net Credit Amount]) AS ReturnCreditAmount
            FROM Fact.[Return] AS r
            WHERE r.[Return Date Key] = s.[Invoice Date Key]
              AND r.[Salesperson Key] = s.[Salesperson Key]
              AND r.[Sales Territory Key] = s.[Sales Territory Key]
        ) AS ret
        OUTER APPLY
        (
            SELECT SUM(c.[Credit Excluding Tax]) AS CreditNoteAmount
            FROM Fact.[Credit Note] AS c
            WHERE c.[Credit Note Date Key] = s.[Invoice Date Key]
              AND c.[Salesperson Key] = s.[Salesperson Key]
              AND c.[Sales Territory Key] = s.[Sales Territory Key]
        ) AS cn
        WHERE s.[Invoice Date Key] BETWEEN @FromDate AND @SnapshotDate
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY
            s.[Invoice Date Key], s.[Salesperson Key], s.[Sales Territory Key],
            s.[Sales Channel Key], s.[Customer Segment Key], s.[Region Code],
            d.[Fiscal Year], d.[Fiscal Month Number],
            ret.ReturnCreditAmount, cn.CreditNoteAmount;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Commission rate is applied afterwards, per region, because the rate
           card is regional and banded and nobody wanted a CASE inside the
           aggregate above. */
        UPDATE Fact.[Daily Sales Snapshot]
        SET [Commission Rate] = CASE
                                    WHEN [Region Code] = N'NA'   AND [Commissionable Revenue] > 50000 THEN 0.030
                                    WHEN [Region Code] = N'NA'                                        THEN 0.020
                                    WHEN [Region Code] = N'EU'   AND [Margin Percent] >= 30           THEN 0.025
                                    WHEN [Region Code] = N'EU'                                        THEN 0.015
                                    WHEN [Region Code] = N'APAC'                                      THEN 0.018
                                    ELSE 0.010
                                END
        WHERE [Snapshot Date Key] BETWEEN @FromDate AND @SnapshotDate;

        UPDATE Fact.[Daily Sales Snapshot]
        SET [Commission Accrued Amount] = ROUND([Commissionable Revenue] * [Commission Rate], 2)
        WHERE [Snapshot Date Key] BETWEEN @FromDate AND @SnapshotDate;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Daily Sales Snapshot',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Daily Sales Snapshot',
            @SourceComponent    = N'Snapshot build',
            @ProcedureName      = N'Integration.usp_LoadFactDailySalesSnapshot',
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
