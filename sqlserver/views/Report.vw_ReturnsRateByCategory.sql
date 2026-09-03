/*
    Report.vw_ReturnsRateByCategory

    Object        : Report.vw_ReturnsRateByCategory
    Deploy target : WideWorldImportersDW
    Reads         : Fact.Return, Dimension.Stock Item,
                    Dimension.Product Category, Aggregate.Product Performance.

    Returns rate by category and month. The denominator is taken from the
    product performance aggregate rather than from Fact.Sale so the rate
    matches the revenue reported elsewhere in the pack, even though that means
    returns against a sale made outside the window still land in the numerator.

    Statutory return windows differ by region (NA 30 days, EU 14 days from
    delivery under distance-selling rules, APAC by local scheme) so the
    out-of-window split is reported alongside the headline rate.
*/
IF OBJECT_ID(N'Report.vw_ReturnsRateByCategory', N'V') IS NOT NULL
    DROP VIEW Report.vw_ReturnsRateByCategory;
GO

CREATE VIEW Report.vw_ReturnsRateByCategory
AS
WITH returns_by_month AS
(
    SELECT
        DATEFROMPARTS(YEAR(r.[Return Date Key]), MONTH(r.[Return Date Key]), 1)
                                                        AS [Calendar Month],
        r.[Region Code]                                 AS [Region Code],
        si.[Product Category Key]                       AS [Product Category Key],
        COUNT_BIG(*)                                    AS [Return Line Count],
        COUNT(DISTINCT r.[RMA Number])                  AS [RMA Count],
        SUM(r.[Quantity Returned])                      AS [Quantity Returned],
        SUM(r.[Quantity Restocked])                     AS [Quantity Restocked],
        SUM(r.[Quantity Scrapped])                      AS [Quantity Scrapped],
        SUM(r.[Net Credit Amount Reporting])            AS [Credit Value],
        SUM(r.[Restocking Fee Amount])                  AS [Restocking Fees],
        SUM(r.[Margin Reversed])                        AS [Margin Reversed],
        SUM(CASE WHEN r.[Faulty Goods Flag] = 1 THEN r.[Net Credit Amount Reporting]
                 ELSE 0 END)                            AS [Faulty Goods Credit],
        SUM(CASE WHEN r.[Within Statutory Window Flag] = 0
                 THEN r.[Net Credit Amount Reporting] ELSE 0 END)
                                                        AS [Out Of Window Credit],
        AVG(CONVERT(DECIMAL (9, 2), r.[Days Since Invoice]))
                                                        AS [Average Days To Return]
    FROM Fact.[Return] AS r
    INNER JOIN Dimension.[Stock Item] AS si
        ON si.[Stock Item Key] = r.[Stock Item Key]
    GROUP BY DATEFROMPARTS(YEAR(r.[Return Date Key]), MONTH(r.[Return Date Key]), 1),
             r.[Region Code],
             si.[Product Category Key]
),
sales_by_month AS
(
    SELECT
        m.[Calendar Month]                              AS [Calendar Month],
        m.[Region Code]                                 AS [Region Code],
        m.[Product Category Key]                        AS [Product Category Key],
        SUM(m.[Net Revenue Reporting])                  AS [Net Revenue],
        SUM(m.[Units Sold Base UOM])                    AS [Quantity Sold]
    FROM Aggregate.[Product Performance] AS m
    GROUP BY m.[Calendar Month], m.[Region Code], m.[Product Category Key]
)
SELECT
    rb.[Calendar Month]                                 AS [Calendar Month],
    rb.[Region Code]                                    AS [Region],
    cat.[Product Category]                              AS [Category],
    rb.[Return Line Count]                              AS [Return Lines],
    rb.[RMA Count]                                      AS [RMA Count],
    rb.[Quantity Returned]                              AS [Quantity Returned],
    rb.[Quantity Restocked]                             AS [Quantity Restocked],
    rb.[Quantity Scrapped]                              AS [Quantity Scrapped],
    rb.[Credit Value]                                   AS [Credit Value],
    rb.[Restocking Fees]                                AS [Restocking Fees],
    rb.[Margin Reversed]                                AS [Margin Reversed],
    rb.[Faulty Goods Credit]                            AS [Faulty Goods Credit],
    rb.[Out Of Window Credit]                           AS [Out Of Window Credit],
    rb.[Average Days To Return]                         AS [Average Days To Return],
    sb.[Net Revenue]                                    AS [Net Revenue],
    sb.[Quantity Sold]                                  AS [Quantity Sold],
    ROUND(100.0 * rb.[Credit Value] / NULLIF(sb.[Net Revenue], 0), 2)
                                                        AS [Returns Rate % (Value)],
    ROUND(100.0 * rb.[Quantity Returned] / NULLIF(sb.[Quantity Sold], 0), 2)
                                                        AS [Returns Rate % (Units)],
    LAG(ROUND(100.0 * rb.[Credit Value] / NULLIF(sb.[Net Revenue], 0), 2), 1)
        OVER (PARTITION BY rb.[Region Code], rb.[Product Category Key]
              ORDER BY rb.[Calendar Month])             AS [Prior Month Returns Rate %],
    LAG(ROUND(100.0 * rb.[Credit Value] / NULLIF(sb.[Net Revenue], 0), 2), 12)
        OVER (PARTITION BY rb.[Region Code], rb.[Product Category Key]
              ORDER BY rb.[Calendar Month])             AS [Prior Year Returns Rate %],
    ROUND(AVG(100.0 * rb.[Credit Value] / NULLIF(sb.[Net Revenue], 0)) OVER
          (PARTITION BY rb.[Region Code], rb.[Product Category Key]
           ORDER BY rb.[Calendar Month]
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                                                        AS [Three Month Average Rate %],
    ROUND(100.0 * rb.[Quantity Scrapped]
          / NULLIF(rb.[Quantity Returned], 0), 2)       AS [Scrap Rate %],
    RANK() OVER (PARTITION BY rb.[Calendar Month], rb.[Region Code]
                 ORDER BY rb.[Credit Value] DESC)       AS [Rank By Credit Value]
FROM returns_by_month AS rb
LEFT JOIN sales_by_month AS sb
    ON sb.[Calendar Month] = rb.[Calendar Month]
   AND sb.[Region Code] = rb.[Region Code]
   AND sb.[Product Category Key] = rb.[Product Category Key]
LEFT JOIN Dimension.[Product Category] AS cat
    ON cat.[Product Category Key] = rb.[Product Category Key];
GO
