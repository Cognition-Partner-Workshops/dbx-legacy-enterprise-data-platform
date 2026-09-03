/*
    Loyalty.usp_RecalculateMemberTier

    Catalog entry : sqlserver_oltp.procedures - Loyalty.RecalculateMemberTier
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6180 - after 6170
    Depends on    : Loyalty.LoyaltyMembers, Loyalty.LoyaltyTiers,
                    Loyalty.LoyaltyPointsLedger, Loyalty.vw_LoyaltyBalance

    Reviews tiers for a programme. Qualification is on trailing spend inside
    the tier's qualifying window; the trailing figure on the member row is
    rebuilt here from the ledger rather than trusted.

    Downgrades honour the grace period on the tier the member currently holds,
    not the tier they would drop to, which is generous and was never
    corrected. Invite-only tiers are never awarded by this procedure and never
    removed by it either.
*/
CREATE PROCEDURE [Loyalty].[usp_RecalculateMemberTier]
    @LoyaltyProgramID   INT,
    @RunByPersonID      INT,
    @BatchID            BIGINT = NULL,
    @MembersChanged     INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @MembersChanged = 0;

    CREATE TABLE #TierReview
    (
        [LoyaltyMemberID]   INT     NOT NULL,
        [CurrentTierID]     INT     NULL,
        [QualifyingTierID]  INT     NULL,
        [TrailingSpend]     DECIMAL (18, 2) NULL,
        [TrailingPoints]    INT     NULL
    );

    INSERT INTO #TierReview ([LoyaltyMemberID], [CurrentTierID], [TrailingSpend], [TrailingPoints])
    SELECT
        m.[LoyaltyMemberID],
        m.[CurrentTierID],
        ISNULL(led.[TrailingSpend], 0),
        ISNULL(led.[TrailingPoints], 0)
    FROM [Loyalty].[LoyaltyMembers] AS m
        OUTER APPLY
        (
            SELECT
                SUM(l.[QualifyingAmount])   AS [TrailingSpend],
                SUM(l.[PointsDelta])        AS [TrailingPoints]
            FROM [Loyalty].[LoyaltyPointsLedger] AS l
            WHERE l.[LoyaltyMemberID] = m.[LoyaltyMemberID]
                AND l.[EntryTypeCode] = N'EARN'
                AND l.[EntryWhen] >= DATEADD(MONTH, -12, SYSDATETIME())
        ) AS led
    WHERE m.[LoyaltyProgramID] = @LoyaltyProgramID
        AND m.[MemberStatus] = N'ACTIVE';

    UPDATE r
    SET r.[QualifyingTierID] = t.[LoyaltyTierID]
    FROM #TierReview AS r
        CROSS APPLY
        (
            SELECT TOP (1) tier.[LoyaltyTierID]
            FROM [Loyalty].[LoyaltyTiers] AS tier
            WHERE tier.[LoyaltyProgramID] = @LoyaltyProgramID
                AND tier.[IsInviteOnly] = 0
                AND (tier.[QualifyingSpendAmount] IS NULL OR r.[TrailingSpend] >= tier.[QualifyingSpendAmount])
                AND (tier.[QualifyingPoints] IS NULL OR r.[TrailingPoints] >= tier.[QualifyingPoints])
            ORDER BY tier.[TierRank] DESC
        ) AS t;

    BEGIN TRANSACTION;

    -- Upgrades apply immediately.
    UPDATE m
    SET m.[CurrentTierID] = r.[QualifyingTierID],
        m.[TierAchievedWhen] = SYSDATETIME(),
        m.[TierReviewDueDate] = DATEADD(MONTH, 12, CONVERT(DATE, SYSDATETIME())),
        m.[TrailingSpendAmount] = r.[TrailingSpend],
        m.[LastEditedBy] = @RunByPersonID,
        m.[LastEditedWhen] = SYSDATETIME()
    FROM [Loyalty].[LoyaltyMembers] AS m
        INNER JOIN #TierReview AS r
            ON r.[LoyaltyMemberID] = m.[LoyaltyMemberID]
        INNER JOIN [Loyalty].[LoyaltyTiers] AS newTier
            ON newTier.[LoyaltyTierID] = r.[QualifyingTierID]
        LEFT JOIN [Loyalty].[LoyaltyTiers] AS oldTier
            ON oldTier.[LoyaltyTierID] = r.[CurrentTierID]
    WHERE newTier.[TierRank] > ISNULL(oldTier.[TierRank], 0);

    SET @MembersChanged = @@ROWCOUNT;

    -- Downgrades wait out the grace period held on the tier being left.
    UPDATE m
    SET m.[CurrentTierID] = r.[QualifyingTierID],
        m.[TierAchievedWhen] = SYSDATETIME(),
        m.[TierReviewDueDate] = DATEADD(MONTH, 12, CONVERT(DATE, SYSDATETIME())),
        m.[TrailingSpendAmount] = r.[TrailingSpend],
        m.[LastEditedBy] = @RunByPersonID,
        m.[LastEditedWhen] = SYSDATETIME()
    FROM [Loyalty].[LoyaltyMembers] AS m
        INNER JOIN #TierReview AS r
            ON r.[LoyaltyMemberID] = m.[LoyaltyMemberID]
        INNER JOIN [Loyalty].[LoyaltyTiers] AS oldTier
            ON oldTier.[LoyaltyTierID] = r.[CurrentTierID]
        LEFT JOIN [Loyalty].[LoyaltyTiers] AS newTier
            ON newTier.[LoyaltyTierID] = r.[QualifyingTierID]
    WHERE ISNULL(newTier.[TierRank], 0) < oldTier.[TierRank]
        AND oldTier.[IsInviteOnly] = 0
        AND m.[TierAchievedWhen] < DATEADD(MONTH, -ISNULL(oldTier.[GracePeriodMonths], 0), SYSDATETIME());

    SET @MembersChanged = @MembersChanged + @@ROWCOUNT;

    UPDATE m
    SET m.[TrailingSpendAmount] = r.[TrailingSpend],
        m.[LastEditedWhen] = SYSDATETIME()
    FROM [Loyalty].[LoyaltyMembers] AS m
        INNER JOIN #TierReview AS r
            ON r.[LoyaltyMemberID] = m.[LoyaltyMemberID]
    WHERE m.[TrailingSpendAmount] <> r.[TrailingSpend];

    COMMIT TRANSACTION;

    DROP TABLE #TierReview;
END
GO
