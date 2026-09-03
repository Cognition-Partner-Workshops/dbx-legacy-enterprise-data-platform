/*
    Report.vw_InventoryHealthCurrent

    Object        : Report.vw_InventoryHealthCurrent
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Daily Inventory Health, Dimension.Warehouse Site,
                    Dimension.Product Category.

    Latest snapshot per warehouse site and category, with the movement against
    the same day last week so the operations call can see direction as well as
    level. The latest date is resolved per site rather than globally because
    the APAC sites snapshot on their own clock and are routinely a day behind
    when the group looks at this before lunch.
*/
IF OBJECT_ID(N'Report.vw_InventoryHealthCurrent', N'V') IS NOT NULL
    DROP VIEW Report.vw_InventoryHealthCurrent;
GO

CREATE VIEW Report.vw_InventoryHealthCurrent
AS
SELECT
    h.[Snapshot Date]                                    AS [Snapshot Date],
    h.[Region Code]                                      AS [Region],
    ws.[Warehouse Site]                                 AS [Warehouse Site],
    cat.[Product Category]                              AS [Category],
    h.[Sku Count]                                        AS [SKUs],
    h.[Sku Stocked Count]                                 AS [SKUs In Stock],
    h.[Stockout Sku Count]                                AS [SKUs Out Of Stock],
    h.[Below Reorder Sku Count]                            AS [SKUs Below Reorder],
    h.[Excess Sku Count]                                  AS [SKUs In Excess],
    h.[Slow Moving Sku Count]                              AS [SKUs Slow Moving],
    h.[Quarantined Sku Count]                             AS [SKUs Quarantined],
    h.[Total Quantity On Hand]                             AS [Quantity On Hand],
    h.[Total Stock Value Reporting]                        AS [Stock Value],
    h.[Excess Stock Value Reporting]                       AS [Excess Stock Value],
    h.[Obsolescence Provision Amount]                     AS [Obsolescence Provision],
    CASE WHEN ISNULL(h.[Total Stock Value Reporting], 0) = 0 THEN NULL
         ELSE ROUND(100.0 * h.[Obsolescence Provision Amount]
                    / h.[Total Stock Value Reporting], 2) END
                                                        AS [Provision % Of Stock],
    h.[Average Days Of Cover]                              AS [Days Of Cover],
    h.[Service Level Percent]                             AS [Service Level %],
    h.[Inventory Turns Annualised]                        AS [Inventory Turns],
    h.[Days Inventory Outstanding]                        AS [Days Inventory Outstanding],
    h.[Stockout Rate Percent]                             AS [Stockout Rate %],
    lw.[Stockout Rate Percent]                            AS [Stockout Rate % Last Week],
    ROUND(h.[Stockout Rate Percent]
          - ISNULL(lw.[Stockout Rate Percent], h.[Stockout Rate Percent]), 2)
                                                        AS [Stockout Rate Movement],
    ROUND(h.[Total Stock Value Reporting]
          - ISNULL(lw.[Total Stock Value Reporting], h.[Total Stock Value Reporting]), 2)
                                                        AS [Stock Value Movement],
    CASE WHEN h.[Stockout Rate Percent] > 5 THEN N'RED'
         WHEN h.[Stockout Rate Percent] > 2 THEN N'AMBER'
         ELSE N'GREEN' END                              AS [Availability Rag],
    CASE WHEN h.[Average Days Of Cover] > 180 THEN N'Overstocked'
         WHEN h.[Average Days Of Cover] < 14 THEN N'Exposed'
         ELSE N'Balanced' END                           AS [Cover Assessment],
    h.[Intraday Refresh Count]                            AS [Intraday Refreshes],
    h.[Refreshed Datetime]                               AS [Data As Of]
FROM Aggregate.[Daily Inventory Health] AS h
INNER JOIN
(
    SELECT [Warehouse Site Key], MAX([Snapshot Date]) AS LatestSnapshot
    FROM Aggregate.[Daily Inventory Health]
    GROUP BY [Warehouse Site Key]
) AS latest
    ON latest.[Warehouse Site Key] = h.[Warehouse Site Key]
   AND latest.LatestSnapshot = h.[Snapshot Date]
LEFT JOIN Aggregate.[Daily Inventory Health] AS lw
    ON lw.[Warehouse Site Key] = h.[Warehouse Site Key]
   AND lw.[Product Category Key] = h.[Product Category Key]
   AND lw.[Snapshot Date] = DATEADD(DAY, -7, h.[Snapshot Date])
LEFT JOIN Dimension.[Warehouse Site] AS ws
    ON ws.[Warehouse Site Key] = h.[Warehouse Site Key]
LEFT JOIN Dimension.[Product Category] AS cat
    ON cat.[Product Category Key] = h.[Product Category Key];
GO
