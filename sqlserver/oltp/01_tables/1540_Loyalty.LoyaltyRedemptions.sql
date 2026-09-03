/*
    Loyalty.LoyaltyRedemptions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1540 - after Loyalty.LoyaltyPointsLedger
    Depends on    : Loyalty.LoyaltyMembers, Loyalty.LoyaltyPointsLedger, Sales.Orders,
                    Application.People
    Called by     : Loyalty.usp_RedeemLoyaltyPoints

    Redemption request and its outcome. The burn ledger row is written first
    and referenced here; if the order it funds is later cancelled the
    redemption is marked REVERSED and a compensating ledger row is written
    rather than the original being deleted.
*/
CREATE TABLE [Loyalty].[LoyaltyRedemptions] (
    [LoyaltyRedemptionID]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [LoyaltyMemberID]       INT             NOT NULL,
    [RedemptionReference]   NVARCHAR (24)   NOT NULL,
    [RequestedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyRedemptions_RequestedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RedemptionType]        NVARCHAR (12)   NOT NULL,
    [PointsRequested]       INT             NOT NULL,
    [PointsApplied]         INT             NULL,
    [CashValueAmount]       DECIMAL (18, 2) NULL,
    [CashCurrencyCode]      NCHAR (3)       NULL,
    [AppliedToOrderID]      INT             NULL,
    [VoucherCode]           NVARCHAR (24)   NULL,
    [VoucherExpiresOn]      DATE            NULL,
    [BurnLedgerID]          BIGINT          NULL,
    [RedemptionStatus]      NVARCHAR (12)   CONSTRAINT [DF_Loyalty_LoyaltyRedemptions_RedemptionStatus] DEFAULT (N'REQUESTED') NOT NULL,
    [DeclineReason]         NVARCHAR (200)  NULL,
    [ProcessedByPersonID]   INT             NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyRedemptions_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Loyalty_LoyaltyRedemptions] PRIMARY KEY CLUSTERED ([LoyaltyRedemptionID] ASC),
    CONSTRAINT [UQ_Loyalty_LoyaltyRedemptions_Reference] UNIQUE ([RedemptionReference]),
    CONSTRAINT [CK_Loyalty_LoyaltyRedemptions_Type] CHECK ([RedemptionType] IN (N'ORDERDISCOUNT', N'VOUCHER', N'FREIGHT', N'CHARITY', N'MERCHANDISE')),
    CONSTRAINT [CK_Loyalty_LoyaltyRedemptions_Points] CHECK ([PointsRequested] > 0),
    CONSTRAINT [CK_Loyalty_LoyaltyRedemptions_Status] CHECK ([RedemptionStatus] IN (N'REQUESTED', N'APPROVED', N'APPLIED', N'DECLINED', N'REVERSED', N'EXPIRED')),
    CONSTRAINT [FK_Loyalty_LoyaltyRedemptions_Members] FOREIGN KEY ([LoyaltyMemberID]) REFERENCES [Loyalty].[LoyaltyMembers] ([LoyaltyMemberID]),
    CONSTRAINT [FK_Loyalty_LoyaltyRedemptions_Ledger] FOREIGN KEY ([BurnLedgerID]) REFERENCES [Loyalty].[LoyaltyPointsLedger] ([LoyaltyLedgerID]),
    CONSTRAINT [FK_Loyalty_LoyaltyRedemptions_Orders] FOREIGN KEY ([AppliedToOrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Loyalty_LoyaltyRedemptions_Application_People] FOREIGN KEY ([ProcessedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Loyalty_LoyaltyRedemptions_Member]
    ON [Loyalty].[LoyaltyRedemptions] ([LoyaltyMemberID] ASC, [RequestedWhen] DESC)
    INCLUDE ([RedemptionStatus], [PointsApplied]);
GO
