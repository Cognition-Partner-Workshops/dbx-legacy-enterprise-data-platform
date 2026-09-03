/*
    Report.vw_MarginByProductCategory

    Object        : Report.vw_MarginByProductCategory
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Monthly Margin Analysis, Dimension.Product
                    Category, Dimension.Sales Territory.

    Category margin bridge. The four effect columns (price, volume, mix, cost)
    are carried from the aggregate and the residual is shown explicitly rather
    than being buried in mix, because the bridge has never fully tied out and
    hiding it caused an argument.

    The cost basis differs by region (weighted average in NA, FIFO in EU,
    standard in APAC) so [Standard Margin %] is only meaningful for APAC and
    is deliberately blanked elsewhere.
*/
IF OBJECT_ID(N'Report.vw_MarginByProductCategory', N'V') IS NOT NULL
    DROP VIEW Report.vw_MarginByProductCategory;
GO

CREATE VIEW Report.vw_MarginByProductCategory
AS
SELECT
    m.[Calendar Month]                                   AS [Calendar Month],
    m.[Fiscal Year]                                      AS [Fiscal Year],
    m.[Fiscal Period]                                    AS [Fiscal Period],
    m.[Region Code]                                      AS [Region],
    cat.[Product Category]                              AS [Category],
    t.[Sales Territory]                                 AS [Territory],
    m.[Cost Basis Code]                                   AS [Cost Basis],
    m.[Quantity Sold Base UOM]                             AS [Units],
    m.[Net Revenue Reporting]                             AS [Net Revenue],
    m.[Cost Of Sales Reporting]                            AS [Cost Of Sales],
    m.[Standard Cost Reporting]                           AS [Standard Cost],
    m.[Purchase Price Variance]                           AS [Purchase Price Variance],
    m.[Freight Cost Reporting]                            AS [Freight],
    m.[Rebate Accrual Reporting]                          AS [Rebate Accrual],
    m.[Gross Margin Reporting]                            AS [Gross Margin],
    m.[Contribution Margin Reporting]                     AS [Contribution Margin],
    m.[Margin Percent]                                   AS [Margin %],
    CASE WHEN m.[Cost Basis Code] = N'STD' THEN m.[Standard Margin Percent] ELSE NULL END
                                                        AS [Standard Margin %],
    m.[Prior Period Margin Percent]                        AS [Prior Period Margin %],
    ROUND(m.[Margin Percent]
          - ISNULL(m.[Prior Period Margin Percent], m.[Margin Percent]), 2)
                                                        AS [Margin Point Movement],
    m.[Price Effect Amount]                               AS [Price Effect],
    m.[Volume Effect Amount]                              AS [Volume Effect],
    m.[Mix Effect Amount]                                 AS [Mix Effect],
    m.[Cost Effect Amount]                                AS [Cost Effect],
    ROUND(m.[Gross Margin Reporting]
          - ISNULL(m.[Price Effect Amount], 0) - ISNULL(m.[Volume Effect Amount], 0)
          - ISNULL(m.[Mix Effect Amount], 0) - ISNULL(m.[Cost Effect Amount], 0), 2)
                                                        AS [Bridge Residual],
    m.[Negative Margin Line Count]                         AS [Negative Margin Lines],
    CASE WHEN m.[Margin Percent] < 0 THEN N'Loss Making'
         WHEN m.[Margin Percent] < 15 THEN N'Thin'
         WHEN m.[Margin Percent] < 35 THEN N'Normal'
         ELSE N'Strong' END                             AS [Margin Band],
    m.[Refreshed Datetime]                               AS [Data As Of]
FROM Aggregate.[Monthly Margin Analysis] AS m
LEFT JOIN Dimension.[Product Category] AS cat
    ON cat.[Product Category Key] = m.[Product Category Key]
LEFT JOIN Dimension.[Sales Territory] AS t
    ON t.[Sales Territory Key] = m.[Sales Territory Key];
GO
