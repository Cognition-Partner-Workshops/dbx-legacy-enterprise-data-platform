/*
    Report.vw_PromotionRoi

    Object        : Report.vw_PromotionRoi
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Promotion Effectiveness, Dimension.Promotion,
                    Dimension.Product Category, Dimension.Sales Channel.

    Promotion payback view for the trade-spend review. ROI is reported both
    as the aggregate calculated it (incremental margin over discount cost) and
    as the older "gross" definition marketing still quote, because the two
    numbers have been in circulation since 2012 and neither side will drop
    theirs.

    Rows flagged [Consent Restricted Flag] have an incomplete eligible
    population in EU - the eligibility fact only carries consented customers
    there - so the take-up rate is not comparable across regions.
*/
IF OBJECT_ID(N'Report.vw_PromotionRoi', N'V') IS NOT NULL
    DROP VIEW Report.vw_PromotionRoi;
GO

CREATE VIEW Report.vw_PromotionRoi
AS
SELECT
    p.[Promotion Code]                                  AS [Promotion Code],
    promo.[Promotion Name]                              AS [Promotion],
    p.[Region Code]                                     AS [Region],
    cat.[Product Category]                              AS [Category],
    ch.[Sales Channel]                                  AS [Channel],
    p.[Promotion Start Date]                            AS [Start Date],
    p.[Promotion End Date]                              AS [End Date],
    DATEDIFF(DAY, p.[Promotion Start Date], p.[Promotion End Date]) + 1
                                                        AS [Promotion Days],
    p.[Baseline Start Date]                             AS [Baseline Start],
    p.[Baseline End Date]                               AS [Baseline End],
    p.[Eligible Customer Count]                         AS [Eligible Customers],
    p.[Participating Customer Count]                    AS [Participating Customers],
    p.[Eligible Not Purchased Count]                    AS [Eligible Not Purchased],
    p.[Take Up Rate Percent]                            AS [Take Up %],
    p.[New Customer Count]                              AS [New Customers],
    p.[Reactivated Customer Count]                      AS [Reactivated Customers],
    p.[Promoted Units Sold]                             AS [Promoted Units],
    p.[Baseline Revenue Reporting]                      AS [Baseline Revenue],
    p.[Promotion Revenue Reporting]                     AS [Promotion Revenue],
    p.[Incremental Revenue Reporting]                   AS [Incremental Revenue],
    p.[Cannibalised Revenue]                            AS [Cannibalised Revenue],
    p.[Discount Cost Reporting]                         AS [Discount Cost],
    p.[Baseline Margin Reporting]                       AS [Baseline Margin],
    p.[Promotion Margin Reporting]                      AS [Promotion Margin],
    p.[Incremental Margin Reporting]                    AS [Incremental Margin],
    p.[Loyalty Points Issued]                           AS [Loyalty Points Issued],
    p.[Return Rate Percent]                             AS [Return Rate %],
    p.[Roi Percent]                                     AS [ROI % (Incremental Margin)],
    CASE WHEN ISNULL(p.[Discount Cost Reporting], 0) = 0 THEN NULL
         ELSE ROUND(100.0 * p.[Promotion Revenue Reporting]
                    / p.[Discount Cost Reporting], 2) END
                                                        AS [ROI % (Gross, 2012 Basis)],
    CASE WHEN ISNULL(p.[Participating Customer Count], 0) = 0 THEN NULL
         ELSE ROUND(p.[Discount Cost Reporting]
                    / p.[Participating Customer Count], 2) END
                                                        AS [Cost Per Participant],
    p.[Payback Achieved Flag]                           AS [Payback Achieved Flag],
    p.[Consent Restricted Flag]                         AS [Consent Restricted Flag],
    RANK() OVER (PARTITION BY p.[Region Code]
                 ORDER BY p.[Incremental Margin Reporting] DESC)
                                                        AS [Rank By Incremental Margin],
    RANK() OVER (PARTITION BY p.[Region Code]
                 ORDER BY p.[Roi Percent] DESC)         AS [Rank By ROI],
    CASE WHEN p.[Incremental Margin Reporting] < 0 THEN N'Value Destroying'
         WHEN p.[Payback Achieved Flag] = 1 THEN N'Repeatable'
         ELSE N'Review Before Repeat' END               AS [Recommendation],
    p.[Refreshed Datetime]                              AS [Data As Of]
FROM Aggregate.[Promotion Effectiveness] AS p
LEFT JOIN Dimension.[Promotion] AS promo
    ON promo.[Promotion Key] = p.[Promotion Key]
LEFT JOIN Dimension.[Product Category] AS cat
    ON cat.[Product Category Key] = p.[Product Category Key]
LEFT JOIN Dimension.[Sales Channel] AS ch
    ON ch.[Sales Channel Key] = p.[Sales Channel Key];
GO
