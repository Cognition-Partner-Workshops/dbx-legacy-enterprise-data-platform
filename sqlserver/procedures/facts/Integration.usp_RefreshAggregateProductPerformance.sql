/*
    Integration.usp_RefreshAggregateProductPerformance

    Object        : Integration.usp_RefreshAggregateProductPerformance
    Deploy target : WideWorldImportersDW
    Deploy order  : after Aggregate.Product Performance.
    Called by     : AGG_Refresh_Product_Performance (monthly, day 1).
    Reads         : Fact.Sale, Fact.Return, Fact.Daily Inventory Snapshot.
    Depends on    : the etl control procedures.

    Product x month. ABC classification is recomputed every month on cumulative
    revenue within category (A to 80%, B to 95%, C thereafter) and the previous
    month's class is carried so that merchandising can see the movers. XYZ is
    on demand variability - coefficient of variation of monthly units over the
    trailing year.

    Lost sales are estimated, not measured: stockout days multiplied by the
    average daily sell rate when the item was in stock. It has been called a
    fiction in three separate steering meetings and is still in the pack.
*/
IF OBJECT_ID(N'Integration.usp_RefreshAggregateProductPerformance', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_RefreshAggregateProductPerformance;
GO

CREATE PROCEDURE Integration.usp_RefreshAggregateProductPerformance
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

    SET @CalendarMonth = ISNULL(@CalendarMonth,
        DATEFROMPARTS(YEAR(DATEADD(MONTH, -1, SYSDATETIME())),
                      MONTH(DATEADD(MONTH, -1, SYSDATETIME())), 1));
    SET @MonthStart = DATEFROMPARTS(YEAR(@CalendarMonth), MONTH(@CalendarMonth), 1);
    SET @MonthEnd   = EOMONTH(@MonthStart);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'AGG_Refresh_Product_Performance',
            @ProjectName        = N'WWI_Aggregates',
            @StepName           = N'RefreshAggregateProductPerformance',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        DELETE FROM Aggregate.[Product Performance]
        WHERE [Calendar Month] = @MonthStart;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Aggregate.[Product Performance]
        (
            [Calendar Month], [Stock Item Key], [Product Category Key], [Region Code],
            [Primary Supplier Key], [Units Sold Base UOM], [Net Revenue Reporting],
            [Gross Margin Reporting], [Margin Percent], [Discount Depth Percent],
            [Units Returned], [Return Rate Percent], [Average Selling Price],
            [Average Unit Cost], [Average Stock Value Reporting], [Inventory Turns],
            [Days Inventory Outstanding], [Stockout Days], [Lost Sales Estimate Reporting],
            [Sell Through Percent], [Distinct Customer Count], [Abc Class], [Xyz Class],
            [Prior Month Abc Class], [Rank In Category By Revenue], [Rank In Category By Margin],
            [New Product Flag], [Discontinued Flag], [Refresh Batch Id], [Refreshed Datetime]
        )
        SELECT
            @MonthStart,
            s.[Stock Item Key],
            ISNULL(item.[Product Category Key], 0),
            s.[Region Code],
            ISNULL(item.[Primary Supplier Key], -1),
            SUM(s.[Quantity Base UOM]),
            SUM(s.[Net Amount Reporting]),
            SUM(s.[Gross Margin Amount] * s.[FX Rate To Reporting]),
            CASE WHEN SUM(s.[Net Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Gross Margin Amount]) / SUM(s.[Net Amount]), 2) END,
            CASE WHEN SUM(s.[Gross Amount]) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Line Discount Amount])
                            / SUM(s.[Gross Amount]), 2) END,
            ISNULL(ret.UnitsReturned, 0),
            CASE WHEN SUM(s.[Quantity Base UOM]) = 0 THEN NULL
                 ELSE ROUND(100.0 * ISNULL(ret.UnitsReturned, 0)
                            / SUM(s.[Quantity Base UOM]), 2) END,
            CASE WHEN SUM(s.[Quantity Base UOM]) = 0 THEN NULL
                 ELSE ROUND(SUM(s.[Net Amount]) / SUM(s.[Quantity Base UOM]), 4) END,
            CASE WHEN SUM(s.[Quantity Base UOM]) = 0 THEN NULL
                 ELSE ROUND(SUM(s.[Cost Of Sale Amount]) / SUM(s.[Quantity Base UOM]), 4) END,
            inv.AverageStockValue,
            CASE WHEN ISNULL(inv.AverageStockValue, 0) = 0 THEN NULL
                 ELSE ROUND(12.0 * SUM(s.[Cost Of Sale Amount] * s.[FX Rate To Reporting])
                            / inv.AverageStockValue, 2) END,
            CASE WHEN SUM(s.[Cost Of Sale Amount] * s.[FX Rate To Reporting]) = 0 THEN NULL
                 ELSE ROUND(DAY(@MonthEnd) * ISNULL(inv.AverageStockValue, 0)
                            / SUM(s.[Cost Of Sale Amount] * s.[FX Rate To Reporting]), 1) END,
            ISNULL(inv.StockoutDays, 0),
            ROUND(ISNULL(inv.StockoutDays, 0)
                  * SUM(s.[Net Amount Reporting])
                  / NULLIF(DAY(@MonthEnd) - ISNULL(inv.StockoutDays, 0), 0), 2),
            CASE WHEN ISNULL(inv.OpeningQuantity, 0) = 0 THEN NULL
                 ELSE ROUND(100.0 * SUM(s.[Quantity Base UOM])
                            / inv.OpeningQuantity, 2) END,
            COUNT(DISTINCT s.[Customer Key]),
            NULL, NULL, NULL, NULL, NULL,
            CASE WHEN item.[Valid From] > DATEADD(MONTH, -6, @MonthStart) THEN 1 ELSE 0 END,
            CASE WHEN ISNULL(item.[Discontinued Flag], 0) = 1 THEN 1 ELSE 0 END,
            @BatchId, SYSDATETIME()
        FROM Fact.[Sale] AS s
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Key] = s.[Stock Item Key]
        OUTER APPLY
        (
            SELECT SUM(ABS(r.[Quantity Returned])) AS UnitsReturned
            FROM Fact.[Return] AS r
            WHERE r.[Stock Item Key] = s.[Stock Item Key]
              AND r.[Return Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS ret
        OUTER APPLY
        (
            SELECT AVG(snap.[Stock Value Reporting]) AS AverageStockValue,
                   SUM(CONVERT(INT, snap.[Stockout Flag])) AS StockoutDays,
                   MIN(snap.[Opening Quantity]) AS OpeningQuantity
            FROM Fact.[Daily Inventory Snapshot] AS snap
            WHERE snap.[Stock Item Key] = s.[Stock Item Key]
              AND snap.[Snapshot Date Key] BETWEEN @MonthStart AND @MonthEnd
        ) AS inv
        WHERE s.[Invoice Date Key] BETWEEN @MonthStart AND @MonthEnd
          AND ISNULL(s.[Correction Type Code], N'ORIG') <> N'REV'
        GROUP BY s.[Stock Item Key], item.[Product Category Key], s.[Region Code],
                 item.[Primary Supplier Key], item.[Valid From], item.[Discontinued Flag],
                 ret.UnitsReturned, inv.AverageStockValue, inv.StockoutDays,
                 inv.OpeningQuantity;

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Ranks within category. */
        WITH ranked AS
        (
            SELECT [Product Performance Key],
                   ROW_NUMBER() OVER (PARTITION BY [Product Category Key], [Region Code]
                                      ORDER BY [Net Revenue Reporting] DESC) AS RevenueRank,
                   ROW_NUMBER() OVER (PARTITION BY [Product Category Key], [Region Code]
                                      ORDER BY [Gross Margin Reporting] DESC) AS MarginRank,
                   SUM([Net Revenue Reporting]) OVER
                       (PARTITION BY [Product Category Key], [Region Code]) AS CategoryRevenue,
                   SUM([Net Revenue Reporting]) OVER
                       (PARTITION BY [Product Category Key], [Region Code]
                        ORDER BY [Net Revenue Reporting] DESC
                        ROWS UNBOUNDED PRECEDING) AS CumulativeRevenue
            FROM Aggregate.[Product Performance]
            WHERE [Calendar Month] = @MonthStart
        )
        UPDATE agg
        SET [Rank In Category By Revenue] = ranked.RevenueRank,
            [Rank In Category By Margin]  = ranked.MarginRank,
            [Abc Class] = CASE WHEN ranked.CategoryRevenue = 0 THEN N'C'
                              WHEN ranked.CumulativeRevenue
                                   <= 0.80 * ranked.CategoryRevenue THEN N'A'
                              WHEN ranked.CumulativeRevenue
                                   <= 0.95 * ranked.CategoryRevenue THEN N'B'
                              ELSE N'C' END
        FROM Aggregate.[Product Performance] AS agg
        INNER JOIN ranked
            ON ranked.[Product Performance Key] = agg.[Product Performance Key];

        /* Prior-month class and demand variability. */
        UPDATE agg
        SET [Prior Month Abc Class] = prior.[Abc Class],
            [Xyz Class] = CASE
                             WHEN cv.MeanUnits IS NULL OR cv.MeanUnits = 0 THEN N'Z'
                             WHEN cv.StdUnits / cv.MeanUnits <= 0.25 THEN N'X'
                             WHEN cv.StdUnits / cv.MeanUnits <= 0.60 THEN N'Y'
                             ELSE N'Z'
                         END
        FROM Aggregate.[Product Performance] AS agg
        LEFT JOIN Aggregate.[Product Performance] AS prior
            ON prior.[Stock Item Key] = agg.[Stock Item Key]
           AND prior.[Region Code] = agg.[Region Code]
           AND prior.[Calendar Month] = DATEADD(MONTH, -1, agg.[Calendar Month])
        OUTER APPLY
        (
            SELECT AVG(CONVERT(DECIMAL(18, 4), h.[Units Sold Base UOM])) AS MeanUnits,
                   STDEV(h.[Units Sold Base UOM]) AS StdUnits
            FROM Aggregate.[Product Performance] AS h
            WHERE h.[Stock Item Key] = agg.[Stock Item Key]
              AND h.[Region Code] = agg.[Region Code]
              AND h.[Calendar Month] > DATEADD(MONTH, -12, agg.[Calendar Month])
              AND h.[Calendar Month] <= agg.[Calendar Month]
        ) AS cv
        WHERE agg.[Calendar Month] = @MonthStart;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Aggregate.Product Performance',
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

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Aggregate.Product Performance',
            @SourceComponent    = N'Aggregate refresh',
            @ProcedureName      = N'Integration.usp_RefreshAggregateProductPerformance',
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
