/*
    Report.vw_DailySalesTrend

    Object        : Report.vw_DailySalesTrend
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Daily Sales Summary, Dimension.Sales Territory,
                    Dimension.Sales Channel.

    The daily trend the sales dashboard refreshes every fifteen minutes. It
    rolls the product-grain daily aggregate up to territory and channel at
    query time, which is exactly the work that should have been materialised;
    the aggregate is kept at item grain for the merchandising team and nobody
    has ever been given time to add a second summary table.

    Weekday numbering is left as SQL Server's DATEPART default rather than
    being normalised per region, so the [Day Of Week] column is only
    meaningful when read together with [Region].
*/
IF OBJECT_ID(N'Report.vw_DailySalesTrend', N'V') IS NOT NULL
    DROP VIEW Report.vw_DailySalesTrend;
GO

CREATE VIEW Report.vw_DailySalesTrend
AS
WITH rolled AS
(
    SELECT
        d.[Sales Date]                                  AS [Sales Date],
        d.[Region Code]                                 AS [Region Code],
        d.[Sales Territory Key]                         AS [Sales Territory Key],
        d.[Sales Channel Key]                           AS [Sales Channel Key],
        MAX(d.[Fiscal Year])                            AS [Fiscal Year],
        MAX(d.[Fiscal Period])                          AS [Fiscal Period],
        SUM(d.[Invoice Count])                          AS [Invoice Count],
        SUM(d.[Line Count])                             AS [Line Count],
        SUM(d.[Distinct Customer Count])                AS [Customer Count Sum Of Items],
        SUM(d.[Quantity Sold Base UOM])                 AS [Quantity Sold],
        SUM(d.[Gross Sales Amount])                     AS [Gross Sales],
        SUM(d.[Line Discount Amount])                   AS [Line Discount],
        SUM(d.[Promotion Discount Amount])              AS [Promotion Discount],
        SUM(d.[Net Sales Amount])                       AS [Net Sales],
        SUM(d.[Tax Amount])                             AS [Tax],
        SUM(d.[Freight Amount])                         AS [Freight],
        SUM(d.[Cost Of Sales Amount])                   AS [Cost Of Sales],
        SUM(d.[Gross Margin Amount])                    AS [Gross Margin],
        SUM(d.[Returns Amount])                         AS [Returns],
        SUM(d.[Net Sales Amount Reporting])             AS [Net Sales Reporting],
        SUM(d.[Prior Year Net Sales])                   AS [Prior Year Net Sales]
    FROM Aggregate.[Daily Sales Summary] AS d
    GROUP BY d.[Sales Date],
             d.[Region Code],
             d.[Sales Territory Key],
             d.[Sales Channel Key]
)
SELECT
    r.[Sales Date]                                      AS [Sales Date],
    DATEPART(WEEKDAY, r.[Sales Date])                   AS [Day Of Week],
    r.[Fiscal Year]                                     AS [Fiscal Year],
    r.[Fiscal Period]                                   AS [Fiscal Period],
    r.[Region Code]                                     AS [Region],
    terr.[Sales Territory]                              AS [Territory],
    ch.[Sales Channel]                                  AS [Channel],
    r.[Invoice Count]                                   AS [Invoices],
    r.[Line Count]                                      AS [Lines],
    r.[Quantity Sold]                                   AS [Quantity Sold],
    r.[Gross Sales]                                     AS [Gross Sales],
    r.[Line Discount] + ISNULL(r.[Promotion Discount], 0)
                                                        AS [Total Discount],
    r.[Net Sales]                                       AS [Net Sales],
    r.[Net Sales Reporting]                             AS [Net Sales (Reporting Currency)],
    r.[Tax]                                             AS [Indirect Tax],
    r.[Freight]                                         AS [Freight],
    r.[Cost Of Sales]                                   AS [Cost Of Sales],
    r.[Gross Margin]                                    AS [Gross Margin],
    ROUND(100.0 * r.[Gross Margin] / NULLIF(r.[Net Sales], 0), 2)
                                                        AS [Margin %],
    r.[Returns]                                         AS [Returns],
    r.[Prior Year Net Sales]                            AS [Prior Year Net Sales],
    ROUND(100.0 * (r.[Net Sales] - r.[Prior Year Net Sales])
          / NULLIF(r.[Prior Year Net Sales], 0), 2)     AS [Prior Year Growth %],
    LAG(r.[Net Sales], 7) OVER
    (
        PARTITION BY r.[Sales Territory Key], r.[Sales Channel Key]
        ORDER BY r.[Sales Date]
    )                                                   AS [Net Sales Same Day Last Week],
    AVG(r.[Net Sales]) OVER
    (
        PARTITION BY r.[Sales Territory Key], r.[Sales Channel Key]
        ORDER BY r.[Sales Date]
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                                   AS [Seven Day Moving Average],
    SUM(r.[Net Sales Reporting]) OVER
    (
        PARTITION BY r.[Sales Territory Key], r.[Sales Channel Key], r.[Fiscal Year]
        ORDER BY r.[Sales Date]
        ROWS UNBOUNDED PRECEDING
    )                                                   AS [Fiscal Year To Date Revenue],
    RANK() OVER (PARTITION BY r.[Sales Date], r.[Region Code]
                 ORDER BY r.[Net Sales Reporting] DESC) AS [Rank In Region That Day]
FROM rolled AS r
LEFT JOIN Dimension.[Sales Territory] AS terr
    ON terr.[Sales Territory Key] = r.[Sales Territory Key]
LEFT JOIN Dimension.[Sales Channel] AS ch
    ON ch.[Sales Channel Key] = r.[Sales Channel Key];
GO
