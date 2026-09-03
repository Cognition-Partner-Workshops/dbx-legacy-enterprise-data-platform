/*
    Report.vw_OrderToCashCycle

    Object        : Report.vw_OrderToCashCycle
    Deploy target : WideWorldImportersDW
    Reads         : Fact.Order Fulfilment, Dimension.Customer,
                    Dimension.Sales Territory, Dimension.Warehouse Site.

    Line-level order-to-cash pipeline off the accumulating snapshot. Open
    lines are included with NULL downstream milestones so the pipeline view
    and the completed-cycle view are the same object; consumers filter on
    [Cycle Complete Flag].

    This is one of the heavy views: the cohort median is recomputed for every
    row through the correlated aggregate rather than being materialised, which
    is why the finance pack extract runs for several minutes.
*/
IF OBJECT_ID(N'Report.vw_OrderToCashCycle', N'V') IS NOT NULL
    DROP VIEW Report.vw_OrderToCashCycle;
GO

CREATE VIEW Report.vw_OrderToCashCycle
AS
SELECT
    f.[Order Number]                                    AS [Order Number],
    f.[Order Line Number]                               AS [Order Line],
    f.[Invoice Number]                                  AS [Invoice Number],
    f.[Despatch Note Number]                            AS [Despatch Note],
    f.[Region Code]                                     AS [Region],
    cust.[Customer]                                     AS [Customer],
    terr.[Sales Territory]                              AS [Territory],
    site.[Warehouse Site]                               AS [Warehouse Site],
    f.[Order Date Key]                                  AS [Order Date],
    f.[Allocation Date Key]                             AS [Allocated],
    f.[Pick Date Key]                                   AS [Picked],
    f.[Pack Date Key]                                   AS [Packed],
    f.[Despatch Date Key]                               AS [Despatched],
    f.[Delivery Date Key]                               AS [Delivered],
    f.[Invoice Date Key]                                AS [Invoiced],
    f.[Cash Applied Date Key]                           AS [Cash Applied],
    f.[Order To Pick Lag Days]                          AS [Order To Pick Days],
    f.[Pick To Despatch Lag Days]                       AS [Pick To Despatch Days],
    f.[Despatch To Delivery Lag Days]                   AS [Despatch To Delivery Days],
    f.[Delivery To Invoice Lag Days]                    AS [Delivery To Invoice Days],
    f.[Invoice To Cash Lag Days]                        AS [Invoice To Cash Days],
    f.[Order To Cash Cycle Days]                        AS [Order To Cash Days],
    f.[Service Target Days]                             AS [Service Target Days],
    f.[Quantity Ordered]                                AS [Quantity Ordered],
    f.[Quantity Despatched]                             AS [Quantity Despatched],
    f.[Quantity Invoiced]                               AS [Quantity Invoiced],
    f.[Order Value Reporting]                           AS [Order Value],
    f.[Invoiced Value Reporting]                        AS [Invoiced Value],
    f.[Cash Applied Reporting]                          AS [Cash Applied Value],
    f.[Order Value Reporting] - ISNULL(f.[Cash Applied Reporting], 0)
                                                        AS [Value Still In Pipeline],
    f.[Pipeline Status Code]                            AS [Pipeline Status],
    f.[Open Milestone Count]                            AS [Open Milestones],
    f.[Pick SLA Breach Flag]                            AS [Pick SLA Breach],
    f.[Delivery SLA Breach Flag]                        AS [Delivery SLA Breach],
    f.[Perfect Order Flag]                              AS [Perfect Order Flag],
    f.[Cancelled Flag]                                  AS [Cancelled Flag],
    f.[Cycle Complete Flag]                             AS [Cycle Complete Flag],
    cohort.MedianCycleDays                              AS [Cohort Median Cycle Days],
    f.[Order To Cash Cycle Days] - cohort.MedianCycleDays
                                                        AS [Variance To Cohort Median],
    CASE WHEN f.[Cycle Complete Flag] <> 1 THEN N'In Flight'
         WHEN f.[Order To Cash Cycle Days] <= f.[Service Target Days] THEN N'Within Target'
         WHEN f.[Order To Cash Cycle Days] <= f.[Service Target Days] + 10 THEN N'Marginal'
         ELSE N'Breached' END                           AS [Cycle Assessment],
    f.[Last Milestone Update]                           AS [Last Milestone Update]
FROM Fact.[Order Fulfilment] AS f
LEFT JOIN Dimension.[Customer] AS cust
    ON cust.[Customer Key] = f.[Customer Key]
LEFT JOIN Dimension.[Sales Territory] AS terr
    ON terr.[Sales Territory Key] = f.[Sales Territory Key]
LEFT JOIN Dimension.[Warehouse Site] AS site
    ON site.[Warehouse Site Key] = f.[Warehouse Site Key]
OUTER APPLY
(
    SELECT DISTINCT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CONVERT(FLOAT, c.[Order To Cash Cycle Days]))
            OVER (PARTITION BY c.[Region Code]) AS MedianCycleDays
    FROM Fact.[Order Fulfilment] AS c
    WHERE c.[Region Code] = f.[Region Code]
      AND c.[Cycle Complete Flag] = 1
      AND c.[Order Date Key] >= DATEADD(MONTH, -6, f.[Order Date Key])
      AND c.[Order Date Key] <= f.[Order Date Key]
) AS cohort;
GO
