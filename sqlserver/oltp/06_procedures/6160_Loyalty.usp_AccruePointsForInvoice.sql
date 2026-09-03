/*
    Loyalty.usp_AccruePointsForInvoice

    Catalog entry : sqlserver_oltp.procedures - Loyalty.AccruePointsForInvoice
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6160 - after 6150
    Depends on    : Loyalty.LoyaltyMembers, Loyalty.LoyaltyPointsLedger,
                    Loyalty.LoyaltyTiers, Loyalty.LoyaltyPrograms,
                    Loyalty.ufn_PointsForAmount, Sales.Invoices

    Accrues points for one invoice. Qualifying spend excludes freight
    everywhere and excludes tax only where the programme earns on net; the
    EarnBasis column decides which.

    Expiry is set from the programme's PointsExpiryMonths against the basis
    the programme declares - ROLLING from the accrual date, ANNIVERSARY from
    the member's enrolment month, FIXED to the end of the calendar year. The
    ANNIVERSARY branch ignores leap years and always lands on the 28th for
    February enrolments.
*/
CREATE PROCEDURE [Loyalty].[usp_AccruePointsForInvoice]
    @InvoiceID          INT,
    @PostedByPersonID   INT = NULL,
    @BatchID            BIGINT = NULL,
    @PointsAccrued      INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @LoyaltyMemberID    INT;
    DECLARE @ProgramID          INT;
    DECLARE @EarnBasis          NVARCHAR (12);
    DECLARE @ExpiryBasis        NVARCHAR (12);
    DECLARE @ExpiryMonths       SMALLINT;
    DECLARE @Multiplier         DECIMAL (5, 2);
    DECLARE @QualifyingAmount   DECIMAL (18, 2);
    DECLARE @CurrencyCode       NCHAR (3);
    DECLARE @EnrolledWhen       DATETIME2 (7);
    DECLARE @ExpiresOnDate      DATE;
    DECLARE @MemberStatus       NVARCHAR (10);

    SET @PointsAccrued = 0;

    SELECT
        @LoyaltyMemberID = i.[LoyaltyMemberID],
        @QualifyingAmount = i.[InvoiceTotalExTax],
        @CurrencyCode = ISNULL(i.[CurrencyCode], N'USD')
    FROM [Sales].[Invoices] AS i
    WHERE i.[InvoiceID] = @InvoiceID;

    IF @LoyaltyMemberID IS NULL
        RETURN;

    IF EXISTS (SELECT 1 FROM [Loyalty].[LoyaltyPointsLedger]
               WHERE [SourceInvoiceID] = @InvoiceID
                   AND [EntryTypeCode] = N'EARN'
                   AND [ReversalOfLedgerID] IS NULL)
        RETURN;

    SELECT
        @ProgramID = m.[LoyaltyProgramID],
        @EnrolledWhen = m.[EnrolledWhen],
        @MemberStatus = m.[MemberStatus],
        @Multiplier = ISNULL(t.[EarnMultiplier], 1)
    FROM [Loyalty].[LoyaltyMembers] AS m
        LEFT JOIN [Loyalty].[LoyaltyTiers] AS t
            ON t.[LoyaltyTierID] = m.[CurrentTierID]
    WHERE m.[LoyaltyMemberID] = @LoyaltyMemberID;

    IF @MemberStatus <> N'ACTIVE'
        RETURN;

    SELECT
        @EarnBasis = p.[EarnBasis],
        @ExpiryBasis = p.[ExpiryBasis],
        @ExpiryMonths = p.[PointsExpiryMonths]
    FROM [Loyalty].[LoyaltyPrograms] AS p
    WHERE p.[LoyaltyProgramID] = @ProgramID;

    IF @EarnBasis = N'GROSS'
        SELECT @QualifyingAmount = i.[InvoiceTotalExTax] + ISNULL(i.[InvoiceTaxAmount], 0)
        FROM [Sales].[Invoices] AS i
        WHERE i.[InvoiceID] = @InvoiceID;

    SET @PointsAccrued = [Loyalty].[ufn_PointsForAmount](@ProgramID, @QualifyingAmount, @Multiplier);

    IF @PointsAccrued <= 0
        RETURN;

    SET @ExpiresOnDate =
        CASE @ExpiryBasis
            WHEN N'ROLLING'     THEN DATEADD(MONTH, @ExpiryMonths, CONVERT(DATE, SYSDATETIME()))
            WHEN N'ANNIVERSARY' THEN DATEFROMPARTS(YEAR(SYSDATETIME()) + 1, MONTH(@EnrolledWhen),
                                                   CASE WHEN DAY(@EnrolledWhen) > 28 THEN 28 ELSE DAY(@EnrolledWhen) END)
            WHEN N'FIXED'       THEN DATEFROMPARTS(YEAR(SYSDATETIME()), 12, 31)
            ELSE NULL
        END;

    BEGIN TRANSACTION;

    INSERT INTO [Loyalty].[LoyaltyPointsLedger]
    (
        [LoyaltyMemberID], [EntryWhen], [EntryTypeCode], [PointsDelta], [PointsRemaining],
        [SourceInvoiceID], [SourceReference], [QualifyingAmount], [QualifyingCurrency],
        [EarnMultiplierApplied], [ExpiresOnDate], [PostedByPersonID], [Narrative]
    )
    VALUES
    (
        @LoyaltyMemberID, SYSDATETIME(), N'EARN', @PointsAccrued, @PointsAccrued,
        @InvoiceID, N'INV' + CONVERT(NVARCHAR (12), @InvoiceID), @QualifyingAmount, @CurrencyCode,
        @Multiplier, @ExpiresOnDate, @PostedByPersonID,
        N'Accrual for invoice ' + CONVERT(NVARCHAR (12), @InvoiceID)
    );

    UPDATE [Loyalty].[LoyaltyMembers]
    SET [CurrentPointsBalance] = [CurrentPointsBalance] + @PointsAccrued,
        [LifetimePointsEarned] = [LifetimePointsEarned] + @PointsAccrued,
        [LifetimeSpendAmount] = [LifetimeSpendAmount] + @QualifyingAmount,
        [TrailingSpendAmount] = [TrailingSpendAmount] + @QualifyingAmount,
        [LastAccrualWhen] = SYSDATETIME(),
        [LastEditedWhen] = SYSDATETIME()
    WHERE [LoyaltyMemberID] = @LoyaltyMemberID;

    UPDATE [Sales].[Invoices]
    SET [LoyaltyPointsAccrued] = ISNULL([LoyaltyPointsAccrued], 0) + @PointsAccrued
    WHERE [InvoiceID] = @InvoiceID;

    COMMIT TRANSACTION;
END
GO
