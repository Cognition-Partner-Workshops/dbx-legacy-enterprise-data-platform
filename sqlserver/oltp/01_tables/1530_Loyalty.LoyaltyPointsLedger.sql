/*
    Loyalty.LoyaltyPointsLedger

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1530 - after Loyalty.LoyaltyMembers
    Depends on    : Loyalty.LoyaltyMembers, Sales.Invoices, Application.People
    Called by     : Loyalty.usp_AccruePointsForInvoice, Loyalty.usp_RedeemLoyaltyPoints,
                    Loyalty.vw_LoyaltyBalance

    Append-only points ledger: positive rows earn, negative rows burn, and
    nothing is ever updated except ExpiredWhen when the expiry sweep consumes
    an earning row. Balance is the sum of the ledger; the member row holds a
    cached copy of it.
*/
CREATE TABLE [Loyalty].[LoyaltyPointsLedger] (
    [LoyaltyLedgerID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [LoyaltyMemberID]       INT             NOT NULL,
    [EntryWhen]             DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyPointsLedger_EntryWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [EntryTypeCode]         NVARCHAR (12)   NOT NULL,
    [PointsDelta]           INT             NOT NULL,
    [PointsRemaining]       INT             NULL,
    [SourceInvoiceID]       INT             NULL,
    [SourceReference]       NVARCHAR (40)   NULL,
    [QualifyingAmount]      DECIMAL (18, 2) NULL,
    [QualifyingCurrency]    NCHAR (3)       NULL,
    [EarnMultiplierApplied] DECIMAL (5, 2)  NULL,
    [ExpiresOnDate]         DATE            NULL,
    [ExpiredWhen]           DATETIME2 (7)   NULL,
    [ReversalOfLedgerID]    BIGINT          NULL,
    [PostedByPersonID]      INT             NULL,
    [Narrative]             NVARCHAR (200)  NULL,
    CONSTRAINT [PK_Loyalty_LoyaltyPointsLedger] PRIMARY KEY CLUSTERED ([LoyaltyLedgerID] ASC),
    CONSTRAINT [CK_Loyalty_LoyaltyPointsLedger_Type] CHECK ([EntryTypeCode] IN (N'EARN', N'BURN', N'EXPIRE', N'ADJUST', N'REVERSAL', N'TRANSFER', N'GOODWILL')),
    CONSTRAINT [CK_Loyalty_LoyaltyPointsLedger_Delta] CHECK ([PointsDelta] <> 0),
    CONSTRAINT [CK_Loyalty_LoyaltyPointsLedger_Earn] CHECK ([EntryTypeCode] <> N'EARN' OR [PointsDelta] > 0),
    CONSTRAINT [CK_Loyalty_LoyaltyPointsLedger_Burn] CHECK ([EntryTypeCode] <> N'BURN' OR [PointsDelta] < 0),
    CONSTRAINT [FK_Loyalty_LoyaltyPointsLedger_Members] FOREIGN KEY ([LoyaltyMemberID]) REFERENCES [Loyalty].[LoyaltyMembers] ([LoyaltyMemberID]),
    CONSTRAINT [FK_Loyalty_LoyaltyPointsLedger_Invoices] FOREIGN KEY ([SourceInvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Loyalty_LoyaltyPointsLedger_Reversal] FOREIGN KEY ([ReversalOfLedgerID]) REFERENCES [Loyalty].[LoyaltyPointsLedger] ([LoyaltyLedgerID]),
    CONSTRAINT [FK_Loyalty_LoyaltyPointsLedger_Application_People] FOREIGN KEY ([PostedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Loyalty_LoyaltyPointsLedger_Member_When]
    ON [Loyalty].[LoyaltyPointsLedger] ([LoyaltyMemberID] ASC, [EntryWhen] DESC)
    INCLUDE ([EntryTypeCode], [PointsDelta]);
GO

CREATE NONCLUSTERED INDEX [IX_Loyalty_LoyaltyPointsLedger_Expiring]
    ON [Loyalty].[LoyaltyPointsLedger] ([ExpiresOnDate] ASC)
    INCLUDE ([LoyaltyMemberID], [PointsRemaining])
    WHERE [ExpiredWhen] IS NULL AND [ExpiresOnDate] IS NOT NULL;
GO
