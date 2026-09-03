/*
    Loyalty.tr_LoyaltyPointsLedger_MaintainBalance

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7060
    Depends on    : Loyalty.LoyaltyPointsLedger, Loyalty.LoyaltyMembers
    Fires on      : AFTER INSERT, UPDATE on Loyalty.LoyaltyPointsLedger

    The member balance is denormalised onto Loyalty.LoyaltyMembers because the
    call centre screen could not afford to sum the ledger. Both this trigger
    and Loyalty.usp_AccruePointsForInvoice write CurrentPointsBalance; the
    procedure writes an incremental value and this trigger then overwrites it
    with the full recalculation, so the procedure's arithmetic is effectively
    dead code that nobody has dared remove.

    Expiries are included in the balance the moment the EXPIRE row is written,
    which is why the balance can move overnight with no member activity.
*/
CREATE TRIGGER [Loyalty].[tr_LoyaltyPointsLedger_MaintainBalance]
    ON [Loyalty].[LoyaltyPointsLedger]
    AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Members TABLE ([LoyaltyMemberID] INT NOT NULL PRIMARY KEY);

    INSERT INTO @Members ([LoyaltyMemberID])
    SELECT DISTINCT [LoyaltyMemberID] FROM [inserted]
    UNION
    SELECT DISTINCT [LoyaltyMemberID] FROM [deleted];

    IF NOT EXISTS (SELECT 1 FROM @Members)
        RETURN;

    UPDATE m
    SET m.[CurrentPointsBalance] = ISNULL(l.[BalancePoints], 0),
        m.[LifetimePointsEarned] = ISNULL(l.[EarnedPoints], 0),
        m.[LastAccrualWhen] = ISNULL(l.[LastEarnWhen], m.[LastAccrualWhen]),
        m.[LastEditedWhen] = SYSDATETIME()
    FROM [Loyalty].[LoyaltyMembers] AS m
        INNER JOIN @Members AS t
            ON t.[LoyaltyMemberID] = m.[LoyaltyMemberID]
        OUTER APPLY
        (
            SELECT SUM(pl.[PointsDelta]) AS [BalancePoints],
                   SUM(CASE WHEN pl.[EntryTypeCode] IN (N'EARN', N'GOODWILL')
                            THEN pl.[PointsDelta] ELSE 0 END) AS [EarnedPoints],
                   MAX(CASE WHEN pl.[EntryTypeCode] = N'EARN'
                            THEN pl.[EntryWhen] END) AS [LastEarnWhen]
            FROM [Loyalty].[LoyaltyPointsLedger] AS pl
            WHERE pl.[LoyaltyMemberID] = t.[LoyaltyMemberID]
        ) AS l;

    -- A member who goes negative is suspended rather than corrected; the
    -- goodwill desk clears it manually.
    UPDATE m
    SET m.[MemberStatus] = N'SUSPENDED'
    FROM [Loyalty].[LoyaltyMembers] AS m
        INNER JOIN @Members AS t
            ON t.[LoyaltyMemberID] = m.[LoyaltyMemberID]
    WHERE m.[CurrentPointsBalance] < 0
        AND m.[MemberStatus] = N'ACTIVE';
END
GO
