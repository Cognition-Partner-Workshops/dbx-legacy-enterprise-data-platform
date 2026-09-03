/*
    Integration.usp_RefreshAggregateRegionalSales

    Object        : Integration.usp_RefreshAggregateRegionalSales
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Regional Sales Performance.
    Called by     : AGG_Refresh_Regional_Sales (monthly, day 2).
    Reads         : Fact.Sale, Fact.Order, stg.SalesBudget, stg.FxRateMonthly.
    Depends on    : the etl control procedures.

    This is where the three tax regimes actually diverge in the reporting
    layer, and the procedure is written as three separate INSERT statements
    rather than one with a CASE, because that is how it was extended each time
    a region was added and nobody dared merge them:

      NA   - sales tax collected per state, revenue is exclusive of tax.
      EU   - VAT output tax and reverse-charge amounts tracked separately;
             intra-community supplies carry no output tax but must still be
             reported.
      APAC - GST collected plus GST-free sales; Singapore and Hong Kong rows
             are GST-free entirely and land in [Gst Free Sales].

    Translation is also regional: NA and EU translate at the monthly average
    rate, APAC at the daily rate, and [Translation Difference] carries the gap
    between the two so group finance can see it.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateRegionalSales', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateRegionalSales;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateRegionalSales
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
            @PackageName        = N'AGG_Refresh_Regional_Sales',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateRegionalSales',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Regional Sales Performance]
        WHERE [Calendar Month] = @MonthStart;

        SET @DeleteRowCount = @@ROWCOUNT;

        /* ---------------- NA ---------------- */
        INSERT INTO Aggregate.[Regional Sales Performance]
        (
            [Fiscal Year], [Fiscal Period], [Calendar Month], [Region Code],
            [Sales Territory Key], [Sales Channel Key], [Fiscal Calendar Code],
            [Local Currency Code], [Order Count], [Invoice Count], [Active Customer Count],
            [Active Salesperson Count], [Net Sales Local], [Net Sales Daily Rate],
            [Net Sales Monthly Average Rate], [Translation Difference],
            [Gross Margin Reporting], [Margin Percent], [Sales Tax Collected],
            [Vat Output Amount], [Vat Reverse Charge Amount], [Gst Collected],
            [Gst Free Sales], [Budget Net Sales Reporting], [Budget Variance Reporting],
            [Budget Attainment Percent], [Prior Year Net Sales], [Year Over Year Percent],
            [Year To Date Net Sales], [Rank In Region By Sales], [Refresh Batch Id],
            [Refreshed Datetime]
        )
        SELECT
            MAX(d.[Fiscal Year]),
            MAX(d.[Fiscal Month Number]),
            @MonthStart,
            N'NA',
            s.[Sales Territory Key],
            s.[Sales Channel Key],
            N'CY12',
            N'USD',
            COUNT(DISTINCT s.[Order Number]),
            COUNT(DISTINCT s.[Invoice Number]),
            COUNT(DISTINCT s.[Customer Key]),
            COUNT(DISTINCT s.[Salesperson Key]),
            SUM(s.[Net Amount]),
            SUM(s.[Net Amount Reporting]),
            SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Net Amount Reporting])
                - SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            SUM(s.[Tax Amount]),
            0, 0, 0, 0,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        LEFT JOIN stg.FxRateMonthly AS fx
            ON fx.CurrencyCode = s.[Transaction Currency Code]
           AND fx.RateMonth = @MonthStart
        WHERE s.[Region Code] = N'NA'
          AND s.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY s.[Sales Territory Key], s.[Sales Channel Key];

        SET @InsertRowCount = @@ROWCOUNT;

        /* ---------------- EU ---------------- */
        INSERT INTO Aggregate.[Regional Sales Performance]
        (
            [Fiscal Year], [Fiscal Period], [Calendar Month], [Region Code],
            [Sales Territory Key], [Sales Channel Key], [Fiscal Calendar Code],
            [Local Currency Code], [Order Count], [Invoice Count], [Active Customer Count],
            [Active Salesperson Count], [Net Sales Local], [Net Sales Daily Rate],
            [Net Sales Monthly Average Rate], [Translation Difference],
            [Gross Margin Reporting], [Margin Percent], [Sales Tax Collected],
            [Vat Output Amount], [Vat Reverse Charge Amount], [Gst Collected],
            [Gst Free Sales], [Budget Net Sales Reporting], [Budget Variance Reporting],
            [Budget Attainment Percent], [Prior Year Net Sales], [Year Over Year Percent],
            [Year To Date Net Sales], [Rank In Region By Sales], [Refresh Batch Id],
            [Refreshed Datetime]
        )
        SELECT
            MAX(d.[Eu Fiscal Year]),
            MAX(d.[Eu Fiscal Period]),
            @MonthStart,
            N'EU',
            s.[Sales Territory Key],
            s.[Sales Channel Key],
            N'APR12',
            N'EUR',
            COUNT(DISTINCT s.[Order Number]),
            COUNT(DISTINCT s.[Invoice Number]),
            COUNT(DISTINCT s.[Customer Key]),
            COUNT(DISTINCT s.[Salesperson Key]),
            SUM(s.[Net Amount]),
            SUM(s.[Net Amount Reporting]),
            SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Net Amount Reporting])
                - SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            0,
            SUM(CASE WHEN ISNULL(s.[Tax Regime Code], N'STD') = N'STD'
                     THEN s.[Tax Amount] ELSE 0 END),
            SUM(CASE WHEN s.[Tax Regime Code] = N'RC'
                     THEN s.[Tax Amount] ELSE 0 END),
            0, 0,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        LEFT JOIN stg.FxRateMonthly AS fx
            ON fx.CurrencyCode = s.[Transaction Currency Code]
           AND fx.RateMonth = @MonthStart
        WHERE s.[Region Code] = N'EU'
          AND s.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY s.[Sales Territory Key], s.[Sales Channel Key];

        SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;

        /* ---------------- APAC ---------------- */
        INSERT INTO Aggregate.[Regional Sales Performance]
        (
            [Fiscal Year], [Fiscal Period], [Calendar Month], [Region Code],
            [Sales Territory Key], [Sales Channel Key], [Fiscal Calendar Code],
            [Local Currency Code], [Order Count], [Invoice Count], [Active Customer Count],
            [Active Salesperson Count], [Net Sales Local], [Net Sales Daily Rate],
            [Net Sales Monthly Average Rate], [Translation Difference],
            [Gross Margin Reporting], [Margin Percent], [Sales Tax Collected],
            [Vat Output Amount], [Vat Reverse Charge Amount], [Gst Collected],
            [Gst Free Sales], [Budget Net Sales Reporting], [Budget Variance Reporting],
            [Budget Attainment Percent], [Prior Year Net Sales], [Year Over Year Percent],
            [Year To Date Net Sales], [Rank In Region By Sales], [Refresh Batch Id],
            [Refreshed Datetime]
        )
        SELECT
            MAX(d.[Apac Fiscal Year]),
            MAX(d.[Apac Fiscal Period]),
            @MonthStart,
            N'APAC',
            s.[Sales Territory Key],
            s.[Sales Channel Key],
            N'JUL13',
            N'AUD',
            COUNT(DISTINCT s.[Order Number]),
            COUNT(DISTINCT s.[Invoice Number]),
            COUNT(DISTINCT s.[Customer Key]),
            COUNT(DISTINCT s.[Salesperson Key]),
            SUM(s.[Net Amount]),
            SUM(s.[Net Amount Reporting]),
            SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Net Amount Reporting])
                - SUM(s.[Net Amount] * ISNULL(fx.MonthlyAverageRate, 1.0)),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            0, 0, 0,
            SUM(CASE WHEN ISNULL(s.[Tax Regime Code], N'GST') <> N'GSTFREE'
                     THEN s.[Tax Amount] ELSE 0 END),
            SUM(CASE WHEN s.[Tax Regime Code] = N'GSTFREE'
                     THEN s.[Net Amount] ELSE 0 END),
            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        INNER JOIN Dimension.[Date] AS d
            ON d.[Date] = s.[Invoice Date Key]
        LEFT JOIN stg.FxRateMonthly AS fx
            ON fx.CurrencyCode = s.[Transaction Currency Code]
           AND fx.RateMonth = @MonthStart
        WHERE s.[Region Code] = N'APAC'
          AND s.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY s.[Sales Territory Key], s.[Sales Channel Key];

        SET @InsertRowCount = @InsertRowCount + @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Budget, prior year, year to date and rank. */
        UPDATE agg
        SET [Budget Net Sales Reporting] = bud.BudgetAmount,
            [Budget Variance Reporting] = agg.[Net Sales Daily Rate] - bud.BudgetAmount,
            [Budget Attainment Percent] =
                CASE WHEN ISNULL(bud.BudgetAmount, 0) = 0 THEN NULL
                     ELSE ROUND(100.0 * agg.[Net Sales Daily Rate] / bud.BudgetAmount, 2) END,
            [Prior Year Net Sales] = py.[Net Sales Daily Rate],
            [Year Over Year Percent] =
                CASE WHEN ISNULL(py.[Net Sales Daily Rate], 0) = 0 THEN NULL
                     ELSE ROUND(100.0 * (agg.[Net Sales Daily Rate] - py.[Net Sales Daily Rate])
                                / py.[Net Sales Daily Rate], 2) END,
            [Year To Date Net Sales] = ytd.YearToDateAmount
        FROM Aggregate.[Regional Sales Performance] AS agg
        LEFT JOIN stg.SalesBudget AS bud
            ON bud.SalesTerritoryKey = agg.[Sales Territory Key]
           AND bud.BudgetMonth = agg.[Calendar Month]
        LEFT JOIN Aggregate.[Regional Sales Performance] AS py
            ON py.[Sales Territory Key] = agg.[Sales Territory Key]
           AND py.[Sales Channel Key] = agg.[Sales Channel Key]
           AND py.[Calendar Month] = DATEADD(YEAR, -1, agg.[Calendar Month])
        OUTER APPLY
        (
            SELECT SUM(y.[Net Sales Daily Rate]) AS YearToDateAmount
            FROM Aggregate.[Regional Sales Performance] AS y
            WHERE y.[Sales Territory Key] = agg.[Sales Territory Key]
              AND y.[Sales Channel Key] = agg.[Sales Channel Key]
              AND y.[Calendar Month] BETWEEN @YearStart AND agg.[Calendar Month]
        ) AS ytd
        WHERE agg.[Calendar Month] = @MonthStart;

        WITH region_rank AS
        (
            SELECT [Regional Sales Perf Key],
                   ROW_NUMBER() OVER (PARTITION BY [Region Code]
                                      ORDER BY [Net Sales Daily Rate] DESC) AS SalesRank
            FROM Aggregate.[Regional Sales Performance]
            WHERE [Calendar Month] = @MonthStart
        )
        UPDATE agg
        SET [Rank In Region By Sales] = region_rank.SalesRank
        FROM Aggregate.[Regional Sales Performance] AS agg
        INNER JOIN region_rank
            ON region_rank.[Regional Sales Perf Key] = agg.[Regional Sales Perf Key];

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Regional Sales Performance',
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
            @SourceName         = N'Aggregate.Regional Sales Performance',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateRegionalSales',
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
