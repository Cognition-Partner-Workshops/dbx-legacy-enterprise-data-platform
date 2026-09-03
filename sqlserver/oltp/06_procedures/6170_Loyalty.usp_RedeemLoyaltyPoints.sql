/*
    Loyalty.usp_RedeemLoyaltyPoints

    Catalog entry : sqlserver_oltp.procedures - Loyalty.RedeemLoyaltyPoints
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6170 - after 6160
    Depends on    : Loyalty.LoyaltyRedemptions, Loyalty.LoyaltyPointsLedger,
                    Loyalty.LoyaltyMembers, Loyalty.LoyaltyPrograms

    Burns points oldest-first across the accrual rows, writing one burn ledger
    entry for the total and decrementing PointsRemaining on each accrual it
    consumes. The row-by-row consumption is the reason this procedure holds a
    cursor: the ledger has to show which accruals paid for the redemption
    because expiry is per accrual.

    A redemption that cannot be covered in full is not partially applied - the
    request is declined and the reason recorded, which the web front end shows
    as "points pending" rather than a refusal.
*/
CREATE PROCEDURE [Loyalty].[usp_RedeemLoyaltyPoints]
    @LoyaltyMemberID        INT,
    @PointsRequested        INT,
    @RedemptionType         NVARCHAR (12),
    @AppliedToOrderID       INT = NULL,
    @ProcessedByPersonID    INT = NULL,
    @BatchID                BIGINT = NULL,
    @LoyaltyRedemptionID    BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ProgramID          INT;
    DECLARE @Balance            INT;
    DECLARE @MinimumRedeem      INT;
    DECLARE @PointValue         DECIMAL (18, 6);
    DECLARE @CurrencyCode       NCHAR (3);
    DECLARE @CashValue          DECIMAL (18, 2);
    DECLARE @Reference          NVARCHAR (24);
    DECLARE @BurnLedgerID       BIGINT;
    DECLARE @Remaining          INT;
    DECLARE @LedgerID           BIGINT;
    DECLARE @AvailableOnRow     INT;
    DECLARE @TakeFromRow        INT;

    SELECT
        @ProgramID = m.[LoyaltyProgramID],
        @Balance = m.[CurrentPointsBalance]
    FROM [Loyalty].[LoyaltyMembers] AS m
    WHERE m.[LoyaltyMemberID] = @LoyaltyMemberID;

    IF @ProgramID IS NULL
    BEGIN
        RAISERROR (N'Loyalty member %d does not exist.', 16, 1, @LoyaltyMemberID);
        RETURN;
    END

    SELECT
        @MinimumRedeem = p.[MinimumRedeemPoints],
        @PointValue = p.[PointValueInCurrency],
        @CurrencyCode = p.[CurrencyCode]
    FROM [Loyalty].[LoyaltyPrograms] AS p
    WHERE p.[LoyaltyProgramID] = @ProgramID;

    SET @Reference = N'RDM' + RIGHT(N'000000000' + CONVERT(NVARCHAR (12), @LoyaltyMemberID), 9)
                   + N'-' + CONVERT(NVARCHAR (8), CONVERT(INT, RAND() * 99999999));
    SET @CashValue = ROUND(@PointsRequested * @PointValue, 2);

    BEGIN TRANSACTION;

    INSERT INTO [Loyalty].[LoyaltyRedemptions]
    (
        [LoyaltyMemberID], [RedemptionReference], [RequestedWhen], [RedemptionType],
        [PointsRequested], [PointsApplied], [CashValueAmount], [CashCurrencyCode],
        [AppliedToOrderID], [RedemptionStatus], [ProcessedByPersonID]
    )
    VALUES
    (
        @LoyaltyMemberID, @Reference, SYSDATETIME(), @RedemptionType,
        @PointsRequested, 0, @CashValue, @CurrencyCode,
        @AppliedToOrderID, N'REQUESTED', @ProcessedByPersonID
    );

    SET @LoyaltyRedemptionID = SCOPE_IDENTITY();

    IF @PointsRequested < ISNULL(@MinimumRedeem, 0) OR @PointsRequested > ISNULL(@Balance, 0)
    BEGIN
        UPDATE [Loyalty].[LoyaltyRedemptions]
        SET [RedemptionStatus] = N'DECLINED',
            [DeclineReason] = CASE WHEN @PointsRequested < ISNULL(@MinimumRedeem, 0)
                                   THEN N'Below the programme minimum redemption'
                                   ELSE N'Insufficient points balance' END,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [LoyaltyRedemptionID] = @LoyaltyRedemptionID;

        COMMIT TRANSACTION;
        RETURN;
    END

    INSERT INTO [Loyalty].[LoyaltyPointsLedger]
    (
        [LoyaltyMemberID], [EntryWhen], [EntryTypeCode], [PointsDelta], [PointsRemaining],
        [SourceReference], [PostedByPersonID], [Narrative]
    )
    VALUES
    (
        @LoyaltyMemberID, SYSDATETIME(), N'BURN', -@PointsRequested, 0,
        @Reference, @ProcessedByPersonID,
        N'Redemption ' + @Reference + N' of type ' + @RedemptionType
    );

    SET @BurnLedgerID = SCOPE_IDENTITY();
    SET @Remaining = @PointsRequested;

    DECLARE AccrualCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT l.[LoyaltyLedgerID], l.[PointsRemaining]
        FROM [Loyalty].[LoyaltyPointsLedger] AS l
        WHERE l.[LoyaltyMemberID] = @LoyaltyMemberID
            AND l.[EntryTypeCode] = N'EARN'
            AND ISNULL(l.[PointsRemaining], 0) > 0
            AND l.[ExpiredWhen] IS NULL
        ORDER BY ISNULL(l.[ExpiresOnDate], N'9999-12-31') ASC, l.[EntryWhen] ASC;

    OPEN AccrualCursor;
    FETCH NEXT FROM AccrualCursor INTO @LedgerID, @AvailableOnRow;

    WHILE @@FETCH_STATUS = 0 AND @Remaining > 0
    BEGIN
        SET @TakeFromRow = CASE WHEN @AvailableOnRow >= @Remaining THEN @Remaining ELSE @AvailableOnRow END;

        UPDATE [Loyalty].[LoyaltyPointsLedger]
        SET [PointsRemaining] = [PointsRemaining] - @TakeFromRow
        WHERE [LoyaltyLedgerID] = @LedgerID;

        SET @Remaining = @Remaining - @TakeFromRow;

        FETCH NEXT FROM AccrualCursor INTO @LedgerID, @AvailableOnRow;
    END

    CLOSE AccrualCursor;
    DEALLOCATE AccrualCursor;

    UPDATE [Loyalty].[LoyaltyRedemptions]
    SET [PointsApplied] = @PointsRequested,
        [RedemptionStatus] = N'APPLIED',
        [BurnLedgerID] = @BurnLedgerID,
        [VoucherCode] = CASE WHEN @RedemptionType = N'VOUCHER' THEN @Reference ELSE NULL END,
        [VoucherExpiresOn] = CASE WHEN @RedemptionType = N'VOUCHER'
                                  THEN DATEADD(MONTH, 6, CONVERT(DATE, SYSDATETIME())) ELSE NULL END,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [LoyaltyRedemptionID] = @LoyaltyRedemptionID;

    UPDATE [Loyalty].[LoyaltyMembers]
    SET [CurrentPointsBalance] = [CurrentPointsBalance] - @PointsRequested,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [LoyaltyMemberID] = @LoyaltyMemberID;

    COMMIT TRANSACTION;
END
GO
