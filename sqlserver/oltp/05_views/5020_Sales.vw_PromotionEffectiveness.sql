/*
    Sales.vw_PromotionEffectiveness

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5020 - after 5010
    Depends on    : Sales.Promotions, Sales.PromotionRedemptions, Sales.OrderDiscounts
    Called by     : marketing reporting, promotion extract

    Redemption and spend against budget per promotion. The denormalised
    RedemptionCount and RedeemedValue on the promotion row are shown next to
    the counted figures; they are maintained by trigger and drift whenever a
    redemption is reversed outside the application.
*/
CREATE VIEW [Sales].[vw_PromotionEffectiveness]
AS
SELECT
    p.[PromotionID],
    p.[PromotionCode],
    p.[PromotionName],
    p.[RegionCode],
    p.[PromotionType],
    p.[CampaignReference],
    p.[StartDate],
    p.[EndDate],
    p.[PromotionStatus],
    p.[BudgetAmount],
    p.[BudgetCurrencyCode],
    p.[SupplierFundedPercent],
    p.[RedemptionCount]                     AS [CachedRedemptionCount],
    p.[RedeemedValue]                       AS [CachedRedeemedValue],
    ISNULL(r.[CountedRedemptions], 0)       AS [CountedRedemptions],
    ISNULL(r.[CountedRedeemedValue], 0)     AS [CountedRedeemedValue],
    ISNULL(r.[CountedSupplierFunded], 0)    AS [CountedSupplierFundedValue],
    ISNULL(r.[DistinctCustomerCount], 0)    AS [DistinctCustomerCount],
    ISNULL(d.[DiscountLineCount], 0)        AS [DiscountLineCount],
    ISNULL(d.[DiscountValueApplied], 0)     AS [DiscountValueApplied],
    CASE WHEN p.[BudgetAmount] IS NULL OR p.[BudgetAmount] = 0 THEN NULL
         ELSE CONVERT(DECIMAL (9, 4), ISNULL(r.[CountedRedeemedValue], 0) / p.[BudgetAmount] * 100)
    END                                     AS [BudgetConsumedPercent],
    CASE WHEN ABS(ISNULL(p.[RedeemedValue], 0) - ISNULL(r.[CountedRedeemedValue], 0)) > 0.01
         THEN 1 ELSE 0 END                  AS [IsCacheStale]
FROM [Sales].[Promotions] AS p
    OUTER APPLY
    (
        SELECT
            COUNT(*)                            AS [CountedRedemptions],
            SUM(pr.[DiscountValue])             AS [CountedRedeemedValue],
            SUM(pr.[FundedBySupplierValue])     AS [CountedSupplierFunded],
            COUNT(DISTINCT pr.[CustomerID])     AS [DistinctCustomerCount]
        FROM [Sales].[PromotionRedemptions] AS pr
        WHERE pr.[PromotionID] = p.[PromotionID]
            AND pr.[RedemptionStatus] = N'APPLIED'
    ) AS r
    OUTER APPLY
    (
        SELECT
            COUNT(*)                        AS [DiscountLineCount],
            SUM(od.[DiscountAmount])        AS [DiscountValueApplied]
        FROM [Sales].[OrderDiscounts] AS od
        WHERE od.[PromotionID] = p.[PromotionID]
    ) AS d;
GO
