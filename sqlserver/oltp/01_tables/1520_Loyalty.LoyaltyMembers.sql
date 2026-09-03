/*
    Loyalty.LoyaltyMembers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1520 - after Loyalty.LoyaltyTiers
    Depends on    : Loyalty.LoyaltyPrograms, Loyalty.LoyaltyTiers, Sales.Customers,
                    Application.People
    Called by     : Loyalty.usp_AccruePointsForInvoice, Loyalty.usp_RecalculateMemberTier,
                    Loyalty.vw_LoyaltyBalance

    Member record. CurrentPointsBalance is a denormalised cache maintained by
    the accrual and redemption procedures alongside the points ledger; the two
    drift and Loyalty.vw_LoyaltyBalance exists to show the ledger truth next to
    the cached figure.

    Consent is recorded three ways because three privacy regimes apply, and the
    marketing preference list is a delimited string inherited from the 2009
    campaign tool.
*/
CREATE TABLE [Loyalty].[LoyaltyMembers] (
    [LoyaltyMemberID]       INT             IDENTITY (1, 1) NOT NULL,
    [MemberNumber]          BIGINT          CONSTRAINT [DF_Loyalty_LoyaltyMembers_MemberNumber] DEFAULT (NEXT VALUE FOR [Sequences].[LoyaltyMemberNumber]) NOT NULL,
    [LoyaltyProgramID]      INT             NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [ContactPersonID]       INT             NULL,
    [MemberEmail]           NVARCHAR (256)  NULL,
    [EnrolledWhen]          DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyMembers_EnrolledWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [EnrolmentChannel]      NVARCHAR (12)   NOT NULL,
    [CurrentTierID]         INT             NULL,
    [TierAchievedWhen]      DATETIME2 (7)   NULL,
    [TierReviewDueDate]     DATE            NULL,
    [CurrentPointsBalance]  INT             CONSTRAINT [DF_Loyalty_LoyaltyMembers_CurrentPointsBalance] DEFAULT (0) NOT NULL,
    [LifetimePointsEarned]  INT             CONSTRAINT [DF_Loyalty_LoyaltyMembers_LifetimePointsEarned] DEFAULT (0) NOT NULL,
    [LifetimeSpendAmount]   DECIMAL (18, 2) CONSTRAINT [DF_Loyalty_LoyaltyMembers_LifetimeSpendAmount] DEFAULT (0) NOT NULL,
    [TrailingSpendAmount]   DECIMAL (18, 2) CONSTRAINT [DF_Loyalty_LoyaltyMembers_TrailingSpendAmount] DEFAULT (0) NOT NULL,
    [LastAccrualWhen]       DATETIME2 (7)   NULL,
    [MarketingConsentFlag]  BIT             CONSTRAINT [DF_Loyalty_LoyaltyMembers_MarketingConsentFlag] DEFAULT (0) NOT NULL,
    [ConsentCapturedWhen]   DATETIME2 (7)   NULL,
    [ConsentSourceText]     NVARCHAR (120)  NULL,
    [PreferenceList]        NVARCHAR (200)  NULL,
    [RetentionExpiresOn]    DATE            NULL,
    [MemberStatus]          NVARCHAR (12)   CONSTRAINT [DF_Loyalty_LoyaltyMembers_MemberStatus] DEFAULT (N'ACTIVE') NOT NULL,
    [ClosedWhen]            DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyMembers_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Loyalty_LoyaltyMembers] PRIMARY KEY CLUSTERED ([LoyaltyMemberID] ASC),
    CONSTRAINT [UQ_Loyalty_LoyaltyMembers_Number] UNIQUE ([MemberNumber]),
    CONSTRAINT [UQ_Loyalty_LoyaltyMembers_Customer] UNIQUE ([LoyaltyProgramID], [CustomerID]),
    CONSTRAINT [CK_Loyalty_LoyaltyMembers_Channel] CHECK ([EnrolmentChannel] IN (N'WEB', N'CALLCENTRE', N'REP', N'STORE', N'IMPORT')),
    CONSTRAINT [CK_Loyalty_LoyaltyMembers_Status] CHECK ([MemberStatus] IN (N'ACTIVE', N'DORMANT', N'SUSPENDED', N'CLOSED', N'ERASED')),
    CONSTRAINT [CK_Loyalty_LoyaltyMembers_Consent] CHECK ([MarketingConsentFlag] = 0 OR [ConsentCapturedWhen] IS NOT NULL),
    CONSTRAINT [FK_Loyalty_LoyaltyMembers_Programs] FOREIGN KEY ([LoyaltyProgramID]) REFERENCES [Loyalty].[LoyaltyPrograms] ([LoyaltyProgramID]),
    CONSTRAINT [FK_Loyalty_LoyaltyMembers_Tiers] FOREIGN KEY ([CurrentTierID]) REFERENCES [Loyalty].[LoyaltyTiers] ([LoyaltyTierID]),
    CONSTRAINT [FK_Loyalty_LoyaltyMembers_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Loyalty_LoyaltyMembers_Contact] FOREIGN KEY ([ContactPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Loyalty_LoyaltyMembers_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Loyalty_LoyaltyMembers_Customer]
    ON [Loyalty].[LoyaltyMembers] ([CustomerID] ASC)
    INCLUDE ([LoyaltyProgramID], [CurrentTierID], [CurrentPointsBalance]);
GO

CREATE NONCLUSTERED INDEX [IX_Loyalty_LoyaltyMembers_TierReview]
    ON [Loyalty].[LoyaltyMembers] ([TierReviewDueDate] ASC)
    INCLUDE ([CurrentTierID], [TrailingSpendAmount])
    WHERE [MemberStatus] = N'ACTIVE';
GO
