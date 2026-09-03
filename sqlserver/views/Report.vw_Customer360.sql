/*
    Report.vw_Customer360

    Object        : Report.vw_Customer360
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Customer 360, Aggregate.Customer Rolling 12 Month,
                    Dimension.Customer, Dimension.Customer Segment,
                    Dimension.Loyalty Tier.

    The account-management landing view. Contact detail is suppressed for
    anonymised rows rather than filtered out, because the revenue history of
    an anonymised customer is still reportable in every region - only the
    identifying attributes are not.

    The rolling window columns are pulled from the rolling aggregate at
    [Month Offset] = 0, which is the most recently refreshed month.
*/
IF OBJECT_ID(N'Report.vw_Customer360', N'V') IS NOT NULL
    DROP VIEW Report.vw_Customer360;
GO

CREATE VIEW Report.vw_Customer360
AS
SELECT
    c.[Customer Key]                                    AS [Customer Key],
    CASE WHEN c.[Anonymised Flag] = 1 THEN N'(anonymised)'
         ELSE c.[Customer Name] END                     AS [Customer],
    CASE WHEN c.[Anonymised Flag] = 1 OR c.[Marketing Consent Flag] = 0
         THEN NULL ELSE c.[Primary Contact Email] END   AS [Contact Email],
    c.[Region Code]                                     AS [Region],
    seg.[Customer Segment]                              AS [Segment],
    tier.[Loyalty Tier]                                 AS [Loyalty Tier],
    c.[Tenure Months]                                   AS [Tenure Months],
    c.[First Order Date]                                AS [First Order],
    c.[Last Order Date]                                 AS [Last Order],
    c.[Days Since Last Order]                           AS [Days Since Last Order],
    c.[Lifetime Order Count]                            AS [Lifetime Orders],
    c.[Lifetime Net Revenue]                            AS [Lifetime Revenue],
    c.[Lifetime Gross Margin]                           AS [Lifetime Margin],
    CASE WHEN ISNULL(c.[Lifetime Net Revenue], 0) = 0 THEN NULL
         ELSE ROUND(100.0 * c.[Lifetime Gross Margin]
                    / c.[Lifetime Net Revenue], 2) END  AS [Lifetime Margin %],
    c.[Lifetime Returns Amount]                         AS [Lifetime Returns],
    c.[Average Order Value]                             AS [Average Order Value],
    c.[Average Days To Pay]                             AS [Average Days To Pay],
    c.[Current Balance Reporting]                       AS [Current Balance],
    c.[Overdue Balance Reporting]                       AS [Overdue Balance],
    c.[Credit Limit Reporting]                          AS [Credit Limit],
    c.[Credit Utilisation Percent]                      AS [Credit Utilisation %],
    c.[Loyalty Point Balance]                           AS [Loyalty Points],
    c.[Web Session Count 90 Day]                        AS [Web Sessions (90 Day)],
    c.[Rfm Score]                                       AS [RFM Score],
    c.[Churn Risk Score]                                AS [Churn Risk Score],
    c.[Churn Risk Band]                                 AS [Churn Risk Band],
    roll.[Rolling 12 Month Revenue]                     AS [Rolling 12 Month Revenue],
    roll.[Rolling 12 Month Margin]                      AS [Rolling 12 Month Margin],
    roll.[Rolling 3 Month Revenue]                      AS [Rolling 3 Month Revenue],
    roll.[Revenue Trend Percent]                        AS [Revenue Trend %],
    roll.[Consecutive Inactive Months]                  AS [Consecutive Inactive Months],
    RANK() OVER (PARTITION BY c.[Region Code]
                 ORDER BY c.[Lifetime Net Revenue] DESC)
                                                        AS [Rank By Lifetime Revenue],
    NTILE(10) OVER (PARTITION BY c.[Region Code]
                    ORDER BY roll.[Rolling 12 Month Revenue] DESC)
                                                        AS [Revenue Decile],
    c.[Marketing Consent Flag]                          AS [Marketing Consent Flag],
    c.[Retention Expiry Date]                           AS [Retention Expiry],
    c.[Anonymised Flag]                                 AS [Anonymised Flag],
    c.[Refreshed Datetime]                              AS [Data As Of]
FROM Aggregate.[Customer 360] AS c
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS roll
    ON roll.[Customer Key] = c.[Customer Key]
   AND roll.[Month Offset] = 0
LEFT JOIN Dimension.[Customer Segment] AS seg
    ON seg.[Customer Segment Key] = c.[Customer Segment Key]
LEFT JOIN Dimension.[Loyalty Tier] AS tier
    ON tier.[Loyalty Tier Key] = c.[Loyalty Tier Key];
GO
