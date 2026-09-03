/*
    Report.vw_SalesByProductMonth

    Object        : Report.vw_SalesByProductMonth
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Product Performance, Dimension.Stock Item,
                    Dimension.Product Category.

    Product x month with the ABC/XYZ classification and the class movement
    that merchandising review weekly. Only the top 500 products per region and
    month by revenue are ranked into [Top 500 Flag]; below that the tail is
    reported in total elsewhere.
*/
IF OBJECT_ID(N'Report.vw_SalesByProductMonth', N'V') IS NOT NULL
    DROP VIEW Report.vw_SalesByProductMonth;
GO

CREATE VIEW Report.vw_SalesByProductMonth
AS
SELECT
    p.[Calendar Month]                                    AS [Calendar Month],
    p.[Stock Item Key]                                     AS [Stock Item Key],
    si.[Stock Item]                                      AS [Product],
    si.[Brand]                                           AS [Brand],
    si.[Size]                                            AS [Size],
    cat.[Product Category]                               AS [Category],
    p.[Region Code]                                       AS [Region],
    p.[Units Sold Base UOM]                                 AS [Units Sold],
    p.[Net Revenue Reporting]                              AS [Net Revenue],
    p.[Gross Margin Reporting]                             AS [Gross Margin],
    p.[Margin Percent]                                    AS [Margin %],
    p.[Discount Depth Percent]                             AS [Discount Depth %],
    p.[Average Selling Price]                              AS [Average Selling Price],
    p.[Average Unit Cost]                                  AS [Average Unit Cost],
    p.[Units Returned]                                    AS [Units Returned],
    p.[Return Rate Percent]                                AS [Return Rate %],
    p.[Inventory Turns]                                   AS [Inventory Turns],
    p.[Stockout Days]                                     AS [Stockout Days],
    p.[Lost Sales Estimate Reporting]                       AS [Estimated Lost Sales],
    p.[Sell Through Percent]                               AS [Sell Through %],
    p.[Distinct Customer Count]                            AS [Buying Customers],
    p.[Abc Class]                                         AS [ABC Class],
    p.[Xyz Class]                                         AS [XYZ Class],
    p.[Abc Class] + N'/' + ISNULL(p.[Xyz Class], N'?')     AS [ABC XYZ],
    p.[Prior Month Abc Class]                               AS [Prior Month ABC],
    CASE WHEN p.[Prior Month Abc Class] IS NULL THEN N'New'
         WHEN p.[Abc Class] < p.[Prior Month Abc Class] THEN N'Promoted'
         WHEN p.[Abc Class] > p.[Prior Month Abc Class] THEN N'Demoted'
         ELSE N'Stable' END                              AS [Class Movement],
    p.[Rank In Category By Revenue]                          AS [Category Revenue Rank],
    p.[Rank In Category By Margin]                           AS [Category Margin Rank],
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY p.[Region Code], p.[Calendar Month]
                                 ORDER BY p.[Net Revenue Reporting] DESC) <= 500
         THEN 1 ELSE 0 END                               AS [Top 500 Flag],
    prev.[Net Revenue Reporting]                           AS [Prior Month Net Revenue],
    CASE WHEN ISNULL(prev.[Net Revenue Reporting], 0) = 0 THEN NULL
         ELSE ROUND(100.0 * (p.[Net Revenue Reporting] - prev.[Net Revenue Reporting])
                    / prev.[Net Revenue Reporting], 2) END AS [Month On Month %],
    p.[New Product Flag]                                   AS [New Product Flag],
    p.[Discontinued Flag]                                 AS [Discontinued Flag],
    p.[Refreshed Datetime]                                AS [Data As Of]
FROM Aggregate.[Product Performance] AS p
LEFT JOIN Dimension.[Stock Item] AS si
    ON si.[Stock Item Key] = p.[Stock Item Key]
LEFT JOIN Dimension.[Product Category] AS cat
    ON cat.[Product Category Key] = p.[Product Category Key]
LEFT JOIN Aggregate.[Product Performance] AS prev
    ON prev.[Stock Item Key] = p.[Stock Item Key]
   AND prev.[Region Code] = p.[Region Code]
   AND prev.[Calendar Month] = DATEADD(MONTH, -1, p.[Calendar Month]);
GO
