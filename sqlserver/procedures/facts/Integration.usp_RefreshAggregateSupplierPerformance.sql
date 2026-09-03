/*
    Integration.usp_RefreshAggregateSupplierPerformance

    Object        : Integration.usp_RefreshAggregateSupplierPerformance
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Supplier Performance.
    Called by     : AGG_Refresh_Supplier_Performance (monthly, day 3).
    Reads         : Fact.Purchase, Fact.Purchase Receipt, Fact.Supplier Payment,
                    Fact.Supplier Transaction.
    Depends on    : the etl control procedures.

    Supplier x category x month scorecard. Two different spend figures are
    carried on purpose and procurement reports the one that suits the
    conversation: committed spend is PO value at order date, recognised spend
    is receipt value at receipt date.

    Landed cost is built differently per region and [Landed Cost Basis Code]
    records which build was used:
      NA   'FOB'  - freight in only, duty is negligible on domestic supply.
      EU   'DDP'  - freight plus customs duty; VAT is recoverable so excluded.
      APAC 'CIF'  - freight, insurance and duty, and an uplift for the
                    inland leg that the carrier bills separately.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateSupplierPerformance', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateSupplierPerformance;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateSupplierPerformance
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @CalendarMonth      DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @MonthStart     DATE;
    DECLARE @MonthEnd       DATE;
    DECLARE @YearStart      DATE;

    SET @CalendarMonth = ISNULL(@CalendarMonth,
        DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, SYSDATETIME())),
                      MONTH(DATEADD(MONTH, -1, SYSDATETIME())), 1));
    SET @MonthStart = DATEFROMPARTS(YEAR(@CalendarMonth), MONTH(@CalendarMonth), 1);
    SET @MonthEnd   = EOMONTH(@MonthStart);
    SET @YearStart  = DATEFROMPARTS(YEAR(@MonthStart), 1, 1);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Supplier_Performance',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateSupplierPerformance',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Supplier Performance]
        WHERE [Calendar Month] = @MonthStart;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Supplier Performance]
        (
            [Calendar Month], [Supplier Key], [Product Category Key], [Region Code],
            [Vendor Contract Key], [Purchase Order Count], [Purchase Line Count],
            [Receipt Count], [Committed Spend Reporting], [Recognised Spend Reporting],
            [Year To Date Spend Reporting], [Contract Covered Spend], [Maverick Spend Reporting],
            [Freight In Reporting], [Customs Duty Reporting], [Landed Cost Reporting],
            [Landed Cost Basis Code], [On Time Receipt Count], [On Time Percent],
            [In Full Percent], [On Time In Full Percent], [Average Days Late],
            [Average Lead Time Days], [Lead Time Variability Days], [Rejected Quantity],
            [Quality Reject Rate Percent], [Match Exception Count], [Price Variance Reporting],
            [Discount Captured Reporting], [Discount Lost Reporting],
            [Average Days Beyond Terms], [Scorecard Rating Code], [Rank In Category By Spend],
            [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            @MonthStart,
            p.[Supplier Key],
            ISNULL(item.[Product Category Key], 0),
            p.[Region Code],
            ISNULL(p.[Vendor Contract Key], -1),
            COUNT(DISTINCT p.[Purchase Order Number]),
            COUNT_BIG(*),
            ISNULL(rec.ReceiptCount, 0),
            SUM(p.[Landed Cost Reporting]),
            ISNULL(rec.RecognisedSpend, 0),
            ISNULL(ytd.YearToDateSpend, 0),
            SUM(CASE WHEN p.[Vendor Contract Key] > 0
                     THEN p.[Landed Cost Reporting] ELSE 0 END),
            SUM(CASE WHEN ISNULL(p.[Vendor Contract Key], -1) <= 0
                     THEN p.[Landed Cost Reporting] ELSE 0 END),
            ISNULL(rec.FreightIn, 0),
            ISNULL(rec.CustomsDuty, 0),
            CASE p.[Region Code]
                WHEN N'NA' THEN ISNULL(rec.RecognisedSpend, 0) + ISNULL(rec.FreightIn, 0)
                WHEN N'EU' THEN ISNULL(rec.RecognisedSpend, 0) + ISNULL(rec.FreightIn, 0)
                                + ISNULL(rec.CustomsDuty, 0)
                ELSE ROUND((ISNULL(rec.RecognisedSpend, 0) + ISNULL(rec.FreightIn, 0)
                            + ISNULL(rec.CustomsDuty, 0)) * 1.035, 2)
            END,
            CASE p.[Region Code] WHEN N'NA' THEN N'FOB'
                                 WHEN N'EU' THEN N'DDP'
                                 ELSE N'CIF' END,
            ISNULL(rec.OnTimeCount, 0),
            CASE WHEN ISNULL(rec.ReceiptCount, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * rec.OnTimeCount / rec.ReceiptCount, 2) END,
            CASE WHEN ISNULL(rec.ReceiptCount, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * rec.InFullCount / rec.ReceiptCount, 2) END,
            CASE WHEN ISNULL(rec.ReceiptCount, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * rec.OtifCount / rec.ReceiptCount, 2) END,
            rec.AverageDaysLate,
            rec.AverageLeadTime,
            rec.LeadTimeStdev,
            ISNULL(rec.RejectedQuantity, 0),
            CASE WHEN ISNULL(rec.ReceivedQuantity, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * rec.RejectedQuantity / rec.ReceivedQuantity, 2) END,
            ISNULL(pay.MatchExceptionCount, 0),
            ISNULL(rec.PriceVariance, 0),
            ISNULL(pay.DiscountCaptured, 0),
            ISNULL(pay.DiscountLost, 0),
            pay.AverageDaysBeyondTerms,
            NULL, NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[Purchase] AS p
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = p.[Stock Item Key]
        OUTER APPLY
        (
            SELECT COUNT_BIG(*) AS ReceiptCount,
                   SUM(r.[Receipt Value Reporting]) AS RecognisedSpend,
                   SUM(ISNULL(r.[Freight In Amount], 0)) AS FreightIn,
                   SUM(ISNULL(r.[Customs Duty Amount], 0)) AS CustomsDuty,
                   SUM(CASE WHEN r.[On Time Flag] = 1 THEN 1 ELSE 0 END) AS OnTimeCount,
                   SUM(CASE WHEN r.[In Full Flag] = 1 THEN 1 ELSE 0 END) AS InFullCount,
                   SUM(CASE WHEN r.[On Time Flag] = 1 AND r.[In Full Flag] = 1
                            THEN 1 ELSE 0 END) AS OtifCount,
                   AVG(CONVERT(DECIMAL(18, 2), r.[Days Late Versus Promise])) AS AverageDaysLate,
                   AVG(CONVERT(DECIMAL(18, 2), r.[Lead Time Days])) AS AverageLeadTime,
                   STDEV(r.[Lead Time Days]) AS LeadTimeStdev,
                   SUM(ISNULL(r.[Quantity Rejected Base UOM], 0)) AS RejectedQuantity,
                   SUM(r.[Quantity Received Base UOM]) AS ReceivedQuantity,
                   SUM(ISNULL(r.[Price Variance Amount], 0)) AS PriceVariance
            FROM Fact.[Purchase Receipt] AS r
            WHERE r.[Supplier Key] = p.[Supplier Key]
              AND r.[Receipt Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS rec
        OUTER APPLY
        (
            SELECT SUM(CASE WHEN sp.[Match Status Code] IS NOT NULL THEN 1 ELSE 0 END)
                       AS MatchExceptionCount,
                   SUM(ISNULL(sp.[Early Settlement Discount], 0)) AS DiscountCaptured,
                   SUM(ISNULL(sp.[Discount Lost Amount], 0)) AS DiscountLost,
                   AVG(CONVERT(DECIMAL(18, 2), sp.[Days Paid Early Or Late])) AS AverageDaysBeyondTerms
            FROM Fact.[Supplier Payment] AS sp
            WHERE sp.[Supplier Key] = p.[Supplier Key]
              AND sp.[Payment Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS pay
        OUTER APPLY
        (
            SELECT SUM(y.[Landed Cost Reporting]) AS YearToDateSpend
            FROM Fact.[Purchase] AS y
            WHERE y.[Supplier Key] = p.[Supplier Key]
              AND y.[Date Key] BETWEEN @YearStart AND @MonthEnd
        ) AS ytd
        WHERE p.[Date Key] BETWEEN @MonthStart AND @MonthEnd
        GROUP BY p.[Supplier Key], item.[Product Category Key], p.[Region Code],
                 p.[Vendor Contract Key], rec.ReceiptCount, rec.RecognisedSpend,
                 rec.FreightIn, rec.CustomsDuty, rec.OnTimeCount, rec.InFullCount,
                 rec.OtifCount, rec.AverageDaysLate, rec.AverageLeadTime,
                 rec.LeadTimeStdev, rec.RejectedQuantity, rec.ReceivedQuantity,
                 rec.PriceVariance, pay.MatchExceptionCount, pay.DiscountCaptured,
                 pay.DiscountLost, pay.AverageDaysBeyondTerms, ytd.YearToDateSpend;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Scorecard letter and spend rank. The thresholds are the 2011
           procurement policy and have never been revisited. */
        UPDATE Aggregate.[Supplier Performance]
        SET [Scorecard Rating Code] =
                CASE
                    WHEN [On Time In Full Percent] >= 95 AND ISNULL([Quality Reject Rate Percent], 0) <= 1
                        THEN N'A1'
                    WHEN [On Time In Full Percent] >= 90 AND ISNULL([Quality Reject Rate Percent], 0) <= 3
                        THEN N'B2'
                    WHEN [On Time In Full Percent] >= 80 THEN N'C3'
                    WHEN [On Time In Full Percent] IS NULL THEN N'NR'
                    ELSE N'D4'
                END
        WHERE [Calendar Month] = @MonthStart;

        WITH spend_rank AS
        (
            SELECT [Supplier Performance Key],
                   ROW_NUMBER() OVER (PARTITION BY [Product Category Key], [Region Code]
                                      ORDER BY [Recognised Spend Reporting] DESC) AS SpendRank
            FROM Aggregate.[Supplier Performance]
            WHERE [Calendar Month] = @MonthStart
        )
        UPDATE agg
        SET [Rank In Category By Spend] = spend_rank.SpendRank
        FROM Aggregate.[Supplier Performance] AS agg
        INNER JOIN spend_rank
            ON spend_rank.[Supplier Performance Key] = agg.[Supplier Performance Key];

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Supplier Performance',
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
            @SourceName         = N'Aggregate.Supplier Performance',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateSupplierPerformance',
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
