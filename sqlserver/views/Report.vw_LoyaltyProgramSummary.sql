/*
    Report.vw_LoyaltyProgramSummary

    Object        : Report.vw_LoyaltyProgramSummary
    Deploy target : WideWorldImportersDW
    Reads         : Fact.Loyalty Points, Dimension.Loyalty Tier.

    Monthly loyalty scheme position: points issued, redeemed, expired and the
    outstanding liability, by tier and region.

    Regional divergence carried through from the fact:
      NA   - liability held at full face value, breakage recognised annually.
      EU   - liability held net of the modelled breakage rate and points from
             customers with [Opt Out Flag] = 1 are excluded from the member
             counts (they may still hold a balance, which stays in liability).
      APAC - points expire on a rolling 24 month basis and the scheme is run
             in local currency, so only the reporting-currency liability is
             comparable across regions.
*/
IF OBJECT_ID(N'Report.vw_LoyaltyProgramSummary', N'V') IS NOT NULL
    DROP VIEW Report.vw_LoyaltyProgramSummary;
GO

CREATE VIEW Report.vw_LoyaltyProgramSummary
AS
WITH movement_month AS
(
    SELECT
        DATEFROMPARTS(YEAR(lp.[Movement Date Key]), MONTH(lp.[Movement Date Key]), 1)
                                                        AS [Calendar Month],
        lp.[Region Code]                                AS [Region Code],
        lp.[Loyalty Tier Key]                           AS [Loyalty Tier Key],
        COUNT_BIG(*)                                    AS [Movement Count],
        COUNT(DISTINCT lp.[Loyalty Account Number])     AS [Active Accounts],
        COUNT(DISTINCT CASE WHEN lp.[Opt Out Flag] = 1
                            THEN lp.[Loyalty Account Number] END)
                                                        AS [Opted Out Accounts],
        SUM(CASE WHEN lp.[Movement Type Code] IN (N'EARN', N'BONUS')
                 THEN lp.[Points Delta] ELSE 0 END)     AS [Points Issued],
        SUM(CASE WHEN lp.[Movement Type Code] = N'REDEEM'
                 THEN -lp.[Points Delta] ELSE 0 END)    AS [Points Redeemed],
        SUM(CASE WHEN lp.[Movement Type Code] = N'EXPIRE'
                 THEN -lp.[Points Delta] ELSE 0 END)    AS [Points Expired],
        SUM(CASE WHEN lp.[Movement Type Code] = N'ADJUST'
                 THEN lp.[Points Delta] ELSE 0 END)     AS [Points Adjusted],
        SUM(lp.[Points Delta])                          AS [Net Points Movement],
        SUM(lp.[Point Liability Reporting])             AS [Liability Movement],
        SUM(lp.[Redemption Value Amount])               AS [Redemption Value],
        SUM(lp.[Breakage Amount])                       AS [Breakage Recognised],
        SUM(lp.[Qualifying Spend Amount])               AS [Qualifying Spend],
        SUM(CASE WHEN lp.[Tier Change Flag] = 1 THEN 1 ELSE 0 END)
                                                        AS [Tier Changes]
    FROM Fact.[Loyalty Points] AS lp
    GROUP BY DATEFROMPARTS(YEAR(lp.[Movement Date Key]), MONTH(lp.[Movement Date Key]), 1),
             lp.[Region Code],
             lp.[Loyalty Tier Key]
)
SELECT
    mm.[Calendar Month]                                 AS [Calendar Month],
    mm.[Region Code]                                    AS [Region],
    tier.[Loyalty Tier]                                 AS [Loyalty Tier],
    mm.[Movement Count]                                 AS [Movements],
    CASE WHEN mm.[Region Code] = N'EU'
         THEN mm.[Active Accounts] - mm.[Opted Out Accounts]
         ELSE mm.[Active Accounts] END                  AS [Reportable Members],
    mm.[Opted Out Accounts]                             AS [Opted Out Members],
    mm.[Points Issued]                                  AS [Points Issued],
    mm.[Points Redeemed]                                AS [Points Redeemed],
    mm.[Points Expired]                                 AS [Points Expired],
    mm.[Points Adjusted]                                AS [Points Adjusted],
    mm.[Net Points Movement]                            AS [Net Points Movement],
    SUM(mm.[Net Points Movement]) OVER
    (
        PARTITION BY mm.[Region Code], mm.[Loyalty Tier Key]
        ORDER BY mm.[Calendar Month]
        ROWS UNBOUNDED PRECEDING
    )                                                   AS [Closing Points Balance],
    SUM(mm.[Liability Movement]) OVER
    (
        PARTITION BY mm.[Region Code], mm.[Loyalty Tier Key]
        ORDER BY mm.[Calendar Month]
        ROWS UNBOUNDED PRECEDING
    )                                                   AS [Closing Liability],
    mm.[Redemption Value]                               AS [Redemption Value],
    mm.[Breakage Recognised]                            AS [Breakage Recognised],
    mm.[Qualifying Spend]                               AS [Qualifying Spend],
    mm.[Tier Changes]                                   AS [Tier Changes],
    ROUND(100.0 * mm.[Points Redeemed]
          / NULLIF(mm.[Points Issued], 0), 2)           AS [Redemption Rate %],
    ROUND(100.0 * mm.[Points Expired]
          / NULLIF(mm.[Points Issued], 0), 2)           AS [Expiry Rate %],
    LAG(mm.[Points Issued], 12) OVER
    (
        PARTITION BY mm.[Region Code], mm.[Loyalty Tier Key]
        ORDER BY mm.[Calendar Month]
    )                                                   AS [Points Issued Prior Year],
    CASE mm.[Region Code]
        WHEN N'NA' THEN N'Full face value, annual breakage'
        WHEN N'EU' THEN N'Net of modelled breakage'
        ELSE N'Rolling 24 month expiry, local currency scheme'
    END                                                 AS [Liability Basis]
FROM movement_month AS mm
LEFT JOIN Dimension.[Loyalty Tier] AS tier
    ON tier.[Loyalty Tier Key] = mm.[Loyalty Tier Key];
GO
