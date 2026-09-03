/*
    Loyalty.vw_LoyaltyBalance

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5120 - after 5110
    Depends on    : Loyalty.LoyaltyMembers, Loyalty.LoyaltyPointsLedger,
                    Loyalty.LoyaltyTiers, Loyalty.LoyaltyPrograms
    Called by     : loyalty portal, Loyalty.usp_RecalculateMemberTier

    Member balances from the ledger next to the cached balance on the member
    row. The cache is maintained by the accrual and redemption procedures;
    expiry is applied by a nightly job that writes ledger rows but does not
    always update the cache, so the two diverge most in the first week of the
    month.
*/
CREATE VIEW [Loyalty].[vw_LoyaltyBalance]
AS
SELECT
    m.[LoyaltyMemberID],
    m.[MemberNumber],
    m.[CustomerID],
    m.[LoyaltyProgramID],
    prog.[ProgramCode],
    prog.[RegionCode],
    prog.[PointValueInCurrency],
    prog.[CurrencyCode],
    m.[MemberStatus],
    m.[EnrolledWhen],
    m.[CurrentTierID],
    tier.[TierCode],
    tier.[TierRank],
    tier.[EarnMultiplier],
    m.[TierAchievedWhen],
    m.[TierReviewDueDate],
    m.[CurrentPointsBalance]                                        AS [CachedPointsBalance],
    ISNULL(led.[LedgerPointsBalance], 0)                            AS [LedgerPointsBalance],
    ISNULL(led.[PointsEarned], 0)                                   AS [LifetimePointsEarnedFromLedger],
    ISNULL(led.[PointsRedeemed], 0)                                 AS [LifetimePointsRedeemed],
    ISNULL(led.[PointsExpired], 0)                                  AS [LifetimePointsExpired],
    ISNULL(led.[PointsExpiringNext90Days], 0)                       AS [PointsExpiringNext90Days],
    CASE WHEN m.[CurrentPointsBalance] <> ISNULL(led.[LedgerPointsBalance], 0)
         THEN 1 ELSE 0 END                                          AS [IsBalanceCacheStale],
    CONVERT(DECIMAL (18, 2), ISNULL(led.[LedgerPointsBalance], 0) * prog.[PointValueInCurrency]) AS [BalanceValueInCurrency],
    m.[MarketingConsentFlag],
    m.[RetentionExpiresOn]
FROM [Loyalty].[LoyaltyMembers] AS m
    INNER JOIN [Loyalty].[LoyaltyPrograms] AS prog
        ON prog.[LoyaltyProgramID] = m.[LoyaltyProgramID]
    LEFT JOIN [Loyalty].[LoyaltyTiers] AS tier
        ON tier.[LoyaltyTierID] = m.[CurrentTierID]
    OUTER APPLY
    (
        SELECT
            SUM(l.[PointsDelta])                                                                AS [LedgerPointsBalance],
            SUM(CASE WHEN l.[EntryTypeCode] = N'EARN' THEN l.[PointsDelta] ELSE 0 END)       AS [PointsEarned],
            SUM(CASE WHEN l.[EntryTypeCode] = N'BURN' THEN -l.[PointsDelta] ELSE 0 END)   AS [PointsRedeemed],
            SUM(CASE WHEN l.[EntryTypeCode] = N'EXPIRE' THEN -l.[PointsDelta] ELSE 0 END)       AS [PointsExpired],
            SUM(CASE WHEN l.[ExpiredWhen] IS NULL
                          AND l.[ExpiresOnDate] IS NOT NULL
                          AND l.[ExpiresOnDate] <= DATEADD(DAY, 90, CONVERT(DATE, SYSDATETIME()))
                     THEN ISNULL(l.[PointsRemaining], 0) ELSE 0 END)                            AS [PointsExpiringNext90Days]
        FROM [Loyalty].[LoyaltyPointsLedger] AS l
        WHERE l.[LoyaltyMemberID] = m.[LoyaltyMemberID]
    ) AS led;
GO
