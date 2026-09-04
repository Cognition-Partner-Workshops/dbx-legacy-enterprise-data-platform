/*
    Report.vw_SupplierSpendYtd

    Object        : Report.vw_SupplierSpendYtd
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Supplier Performance, Dimension.Supplier.

    Year-to-date supplier spend rolled up from the monthly scorecard, with the
    contract coverage split procurement is measured on. The year is the
    calendar year for NA and APAC and the April year for EU, so the same
    supplier trading in two regions appears twice with different year
    boundaries; that is intentional and finance reconciles it manually.

    Pareto position is calculated across the whole region, not per category,
    because the "top suppliers by spend" list has always been a regional list.
*/
IF OBJECT_ID(N'Report.vw_SupplierSpendYtd', N'V') IS NOT NULL
    DROP VIEW Report.vw_SupplierSpendYtd;
GO

CREATE VIEW Report.vw_SupplierSpendYtd
AS
WITH ytd AS
(
    SELECT
        sp.[Supplier Key],
        sp.[Region Code],
        CASE sp.[Region Code]
            WHEN N'EU' THEN CASE WHEN MONTH(sp.[Calendar Month]) >= 4
                                 THEN YEAR(sp.[Calendar Month])
                                 ELSE YEAR(sp.[Calendar Month]) - 1 END
            ELSE YEAR(sp.[Calendar Month])
        END                                             AS SpendYear,
        SUM(sp.[Committed Spend Reporting])               AS CommittedSpend,
        SUM(sp.[Recognised Spend Reporting])              AS RecognisedSpend,
        SUM(sp.[Contract Covered Spend])                  AS ContractCoveredSpend,
        SUM(sp.[Maverick Spend Reporting])                AS MaverickSpend,
        SUM(sp.[Landed Cost Reporting])                   AS LandedCost,
        SUM(sp.[Freight In Reporting])                    AS FreightIn,
        SUM(sp.[Customs Duty Reporting])                  AS CustomsDuty,
        SUM(sp.[Price Variance Reporting])                AS PriceVariance,
        SUM(sp.[Discount Captured Reporting])             AS DiscountCaptured,
        SUM(sp.[Discount Lost Reporting])                 AS DiscountLost,
        SUM(sp.[Purchase Order Count])                    AS PurchaseOrders,
        SUM(sp.[Match Exception Count])                   AS MatchExceptions,
        MAX(sp.[Landed Cost Basis Code])                   AS LandedCostBasis,
        MAX(sp.[Calendar Month])                         AS LatestMonth,
        MAX(sp.[Refreshed Datetime])                     AS RefreshedDatetime
    FROM Aggregate.[Supplier Performance] AS sp
    GROUP BY
        sp.[Supplier Key],
        sp.[Region Code],
        CASE sp.[Region Code]
            WHEN N'EU' THEN CASE WHEN MONTH(sp.[Calendar Month]) >= 4
                                 THEN YEAR(sp.[Calendar Month])
                                 ELSE YEAR(sp.[Calendar Month]) - 1 END
            ELSE YEAR(sp.[Calendar Month])
        END
)
SELECT
    ytd.SpendYear                                       AS [Spend Year],
    ytd.[Region Code]                                    AS [Region],
    CASE ytd.[Region Code] WHEN N'EU' THEN N'April Year' ELSE N'Calendar Year' END
                                                        AS [Year Basis],
    ytd.[Supplier Key]                                   AS [Supplier Key],
    s.[Supplier]                                        AS [Supplier],
    s.[Category]                                        AS [Supplier Category],
    s.[Payment Days]                                    AS [Payment Terms Days],
    ytd.CommittedSpend                                  AS [Committed Spend YTD],
    ytd.RecognisedSpend                                 AS [Recognised Spend YTD],
    ytd.LandedCost                                      AS [Landed Cost YTD],
    ytd.LandedCostBasis                                 AS [Landed Cost Basis],
    ytd.FreightIn                                       AS [Freight In YTD],
    ytd.CustomsDuty                                     AS [Customs Duty YTD],
    ytd.ContractCoveredSpend                            AS [Contract Covered Spend],
    ytd.MaverickSpend                                   AS [Maverick Spend],
    CASE WHEN ISNULL(ytd.CommittedSpend, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * ytd.ContractCoveredSpend / ytd.CommittedSpend, 2) END
                                                        AS [Contract Coverage %],
    ytd.PriceVariance                                   AS [Purchase Price Variance],
    ytd.DiscountCaptured                                AS [Early Settlement Captured],
    ytd.DiscountLost                                    AS [Early Settlement Lost],
    ytd.PurchaseOrders                                  AS [Purchase Orders],
    ytd.MatchExceptions                                 AS [Match Exceptions],
    RANK() OVER (PARTITION BY ytd.[Region Code], ytd.SpendYear
                 ORDER BY ytd.RecognisedSpend DESC)     AS [Spend Rank],
    SUM(ytd.RecognisedSpend) OVER
    (
        PARTITION BY ytd.[Region Code], ytd.SpendYear
        ORDER BY ytd.RecognisedSpend DESC
        ROWS UNBOUNDED PRECEDING
    )                                                   AS [Cumulative Spend],
    ROUND(100.0 * SUM(ytd.RecognisedSpend) OVER
    (
        PARTITION BY ytd.[Region Code], ytd.SpendYear
        ORDER BY ytd.RecognisedSpend DESC
        ROWS UNBOUNDED PRECEDING
    ) / NULLIF(SUM(ytd.RecognisedSpend) OVER
    (
        PARTITION BY ytd.[Region Code], ytd.SpendYear
    ), 0), 2)                                           AS [Cumulative Spend %],
    ytd.LatestMonth                                     AS [Latest Month Included],
    ytd.RefreshedDatetime                               AS [Data As Of]
FROM ytd
LEFT JOIN Dimension.[Supplier] AS s
    ON s.[Supplier Key] = ytd.[Supplier Key];
GO
