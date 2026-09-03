/*
    08_seed / 8020 - Returns and loyalty reference data

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : after 8010_seed_warehouse_shipping_reference.sql
    Depends on    : Returns.ReturnReasons, Loyalty.LoyaltyPrograms,
                    Loyalty.LoyaltyTiers, Application.People
    Called by     : Returns.usp_AuthorizeReturn, Returns.ufn_RestockingFee,
                    Loyalty.usp_AccruePointsForInvoice,
                    Loyalty.usp_RecalculateMemberTier

    Grouped reference/seed script, guarded by NOT EXISTS on the natural key.

    Regional divergence: the NA change-of-mind window is 30 days with a
    restocking fee, the EU window is the 14 day statutory cooling-off period
    with no fee chargeable, and APAC allows only 7 days and charges the
    highest restocking percentage. Loyalty follows the same split - EU is
    strictly opt-in with a two year retention, APAC is a newer pilot.
*/
INSERT INTO [Returns].[ReturnReasons]
(
    [ReasonCode], [RegionCode], [ReasonDescription], [ReasonCategory], [IsCustomerFault],
    [DefaultRestockingApplies], [RestockingPercent], [RequiresInspection],
    [RequiresPhotoEvidence], [AllowsResale], [ReturnWindowDays], [SupplierRecoverable],
    [IsActive], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'COM',    N'NA  ', N'Change of mind - no fault found',           N'CHANGEOFMIND', 1, 1, 15.00, 1, 0, 1,  30, 0, 1, 1),
    (N'DAMTR',  N'NA  ', N'Damaged in transit',                        N'DAMAGE',       0, 0, NULL,  1, 1, 0,  90, 1, 1, 1),
    (N'PICKER', N'NA  ', N'Warehouse picked the wrong item',           N'PICKERROR',    0, 0, NULL,  1, 0, 1, 120, 0, 1, 1),
    (N'QUAL',   N'NA  ', N'Quality below specification',               N'QUALITY',      0, 0, NULL,  1, 1, 0, 365, 1, 1, 1),
    (N'RECALL', N'NA  ', N'Supplier product recall',                   N'RECALL',       0, 0, NULL,  0, 0, 0, NULL, 1, 1, 1),
    (N'COOL',   N'EU  ', N'Statutory cooling-off cancellation',        N'CHANGEOFMIND', 1, 0, NULL,  0, 0, 1,  14, 0, 1, 1),
    (N'COM',    N'EU  ', N'Change of mind after cooling-off period',   N'CHANGEOFMIND', 1, 1, 10.00, 1, 0, 1,  30, 0, 1, 1),
    (N'DAMTR',  N'EU  ', N'Damaged in transit',                        N'DAMAGE',       0, 0, NULL,  1, 1, 0,  90, 1, 1, 1),
    (N'CONFORM', N'EU  ', N'Goods not in conformity with contract',    N'QUALITY',      0, 0, NULL,  1, 1, 0, 730, 1, 1, 1),
    (N'LATEEU', N'EU  ', N'Delivered outside agreed delivery window',  N'LATE',         0, 0, NULL,  0, 0, 1,  30, 1, 1, 1),
    (N'COM',    N'APAC', N'Change of mind - no fault found',           N'CHANGEOFMIND', 1, 1, 20.00, 1, 0, 1,   7, 0, 1, 1),
    (N'DAMTR',  N'APAC', N'Damaged in transit',                        N'DAMAGE',       0, 0, NULL,  1, 1, 0,  60, 1, 1, 1),
    (N'QUAL',   N'APAC', N'Quality below specification',               N'QUALITY',      0, 0, NULL,  1, 1, 0, 180, 1, 1, 1),
    (N'CUSTAP', N'APAC', N'Rejected at customs or quarantine',         N'OTHER',        0, 0, NULL,  1, 1, 0,  45, 0, 1, 1)
) AS s ([ReasonCode], [RegionCode], [ReasonDescription], [ReasonCategory], [IsCustomerFault],
        [DefaultRestockingApplies], [RestockingPercent], [RequiresInspection],
        [RequiresPhotoEvidence], [AllowsResale], [ReturnWindowDays], [SupplierRecoverable],
        [IsActive], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Returns].[ReturnReasons] AS r
    WHERE r.[ReasonCode] = s.[ReasonCode]
        AND r.[RegionCode] = s.[RegionCode]
);
GO

INSERT INTO [Loyalty].[LoyaltyPrograms]
(
    [ProgramCode], [ProgramName], [RegionCode], [EarnBasis], [PointsPerCurrencyUnit],
    [CurrencyCode], [PointValueInCurrency], [MinimumRedeemPoints], [PointsExpiryMonths],
    [ExpiryBasis], [ConsentBasis], [RetentionMonths], [LaunchedOnDate], [ClosedOnDate],
    [ProgramStatus], [LastEditedBy]
)
SELECT * FROM (VALUES
    (N'WWIREWARD', N'WWI Rewards - North America',  N'NA  ', N'GROSS',  1.0000, N'USD', 0.010000,  500,   24, N'ROLLING',      N'OPTOUT',   120, CONVERT(DATE, '2009-03-01'), NULL, N'LIVE',   1),
    (N'WWICLUBEU', N'WWI Trade Club - Europe',      N'EU  ', N'NET',    0.5000, N'EUR', 0.020000, 1000,   12, N'CALENDARYEAR', N'OPTIN',     24, CONVERT(DATE, '2014-05-01'), NULL, N'LIVE',   1),
    (N'WWIPOINTS', N'WWI Points - Asia Pacific',    N'APAC', N'MARGIN', 2.0000, N'AUD', 0.005000,  250,   18, N'FISCALYEAR',   N'OPTIN',     60, CONVERT(DATE, '2021-07-01'), NULL, N'PILOT',  1),
    (N'WWILEGACY', N'WWI Loyalty (retired scheme)', N'NA  ', N'GROSS',  1.0000, N'USD', 0.008000, 1000, NULL, N'NEVER',        N'CONTRACT', 120, CONVERT(DATE, '2004-01-01'), CONVERT(DATE, '2009-02-28'), N'CLOSED', 1)
) AS s ([ProgramCode], [ProgramName], [RegionCode], [EarnBasis], [PointsPerCurrencyUnit],
        [CurrencyCode], [PointValueInCurrency], [MinimumRedeemPoints], [PointsExpiryMonths],
        [ExpiryBasis], [ConsentBasis], [RetentionMonths], [LaunchedOnDate], [ClosedOnDate],
        [ProgramStatus], [LastEditedBy])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Loyalty].[LoyaltyPrograms] AS p
    WHERE p.[ProgramCode] = s.[ProgramCode]
);
GO

INSERT INTO [Loyalty].[LoyaltyTiers]
(
    [LoyaltyProgramID], [TierCode], [TierName], [TierRank], [QualifyingSpendAmount],
    [QualifyingPoints], [QualifyingWindowMonths], [EarnMultiplier], [FreeFreightThreshold],
    [DiscountPercent], [GracePeriodMonths], [IsInviteOnly], [LastEditedBy]
)
SELECT
    p.[LoyaltyProgramID], s.[TierCode], s.[TierName], s.[TierRank], s.[QualifyingSpendAmount],
    s.[QualifyingPoints], s.[QualifyingWindowMonths], s.[EarnMultiplier], s.[FreeFreightThreshold],
    s.[DiscountPercent], s.[GracePeriodMonths], s.[IsInviteOnly], s.[LastEditedBy]
FROM (VALUES
    (N'WWIREWARD', N'BRONZE', N'Bronze',    1,      0.00,    NULL, 12, 1.00,   500.00, 0.00, 3, 0, 1),
    (N'WWIREWARD', N'SILVER', N'Silver',    2,  25000.00,    NULL, 12, 1.25,   350.00, 1.50, 3, 0, 1),
    (N'WWIREWARD', N'GOLD',   N'Gold',      3, 100000.00,    NULL, 12, 1.50,     0.00, 3.00, 6, 0, 1),
    (N'WWIREWARD', N'CHAIR',  N'Chairman',  4, 400000.00,    NULL, 12, 2.00,     0.00, 5.00, 6, 1, 1),
    (N'WWICLUBEU', N'CLUB',   N'Club',      1,      0.00,    NULL, 12, 1.00,   400.00, 0.00, 6, 0, 1),
    (N'WWICLUBEU', N'CLUBP',  N'Club Plus', 2,  40000.00,    NULL, 12, 1.30,   200.00, 2.00, 6, 0, 1),
    (N'WWICLUBEU', N'CLUBE',  N'Club Elite',3, 150000.00,    NULL, 12, 1.75,     0.00, 4.00, 6, 1, 1),
    (N'WWIPOINTS', N'START',  N'Starter',   1,      0.00,    NULL, 18, 1.00,   750.00, 0.00, 3, 0, 1),
    (N'WWIPOINTS', N'RISE',   N'Riser',     2,      NULL,  50000, 18, 1.40,   500.00, 1.00, 3, 0, 1),
    (N'WWIPOINTS', N'PEAK',   N'Peak',      3,      NULL, 200000, 18, 1.80,     0.00, 2.50, 3, 0, 1)
) AS s ([ProgramCode], [TierCode], [TierName], [TierRank], [QualifyingSpendAmount],
        [QualifyingPoints], [QualifyingWindowMonths], [EarnMultiplier], [FreeFreightThreshold],
        [DiscountPercent], [GracePeriodMonths], [IsInviteOnly], [LastEditedBy])
    INNER JOIN [Loyalty].[LoyaltyPrograms] AS p
        ON p.[ProgramCode] = s.[ProgramCode]
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Loyalty].[LoyaltyTiers] AS t
    WHERE t.[LoyaltyProgramID] = p.[LoyaltyProgramID]
        AND t.[TierCode] = s.[TierCode]
);
GO
