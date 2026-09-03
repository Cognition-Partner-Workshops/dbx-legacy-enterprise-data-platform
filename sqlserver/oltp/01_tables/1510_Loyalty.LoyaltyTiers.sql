/*
    Loyalty.LoyaltyTiers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1510 - after Loyalty.LoyaltyPrograms
    Depends on    : Loyalty.LoyaltyPrograms, Application.People
    Called by     : Loyalty.usp_RecalculateMemberTier, Loyalty.ufn_PointsForAmount

    Tier bands. Qualification is on trailing twelve-month spend for NA and EU
    and on points earned for APAC, so QualifyingSpendAmount and
    QualifyingPoints are alternately null. The tier multiplier is applied at
    accrual time and is not re-applied if the member is later upgraded, which
    members complain about every January.
*/
CREATE TABLE [Loyalty].[LoyaltyTiers] (
    [LoyaltyTierID]         INT             IDENTITY (1, 1) NOT NULL,
    [LoyaltyProgramID]      INT             NOT NULL,
    [TierCode]              NVARCHAR (10)   NOT NULL,
    [TierName]              NVARCHAR (40)   NOT NULL,
    [TierRank]              SMALLINT        NOT NULL,
    [QualifyingSpendAmount] DECIMAL (18, 2) NULL,
    [QualifyingPoints]      INT             NULL,
    [QualifyingWindowMonths] SMALLINT       CONSTRAINT [DF_Loyalty_LoyaltyTiers_QualifyingWindowMonths] DEFAULT (12) NOT NULL,
    [EarnMultiplier]        DECIMAL (5, 2)  CONSTRAINT [DF_Loyalty_LoyaltyTiers_EarnMultiplier] DEFAULT (1.00) NOT NULL,
    [FreeFreightThreshold]  DECIMAL (18, 2) NULL,
    [DiscountPercent]       DECIMAL (5, 2)  CONSTRAINT [DF_Loyalty_LoyaltyTiers_DiscountPercent] DEFAULT (0) NOT NULL,
    [GracePeriodMonths]     SMALLINT        CONSTRAINT [DF_Loyalty_LoyaltyTiers_GracePeriodMonths] DEFAULT (3) NOT NULL,
    [IsInviteOnly]          BIT             CONSTRAINT [DF_Loyalty_LoyaltyTiers_IsInviteOnly] DEFAULT (0) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyTiers_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Loyalty_LoyaltyTiers] PRIMARY KEY CLUSTERED ([LoyaltyTierID] ASC),
    CONSTRAINT [UQ_Loyalty_LoyaltyTiers_Code] UNIQUE ([LoyaltyProgramID], [TierCode]),
    CONSTRAINT [UQ_Loyalty_LoyaltyTiers_Rank] UNIQUE ([LoyaltyProgramID], [TierRank]),
    CONSTRAINT [CK_Loyalty_LoyaltyTiers_Qualification] CHECK ([QualifyingSpendAmount] IS NOT NULL OR [QualifyingPoints] IS NOT NULL OR [IsInviteOnly] = 1),
    CONSTRAINT [CK_Loyalty_LoyaltyTiers_Multiplier] CHECK ([EarnMultiplier] > 0),
    CONSTRAINT [FK_Loyalty_LoyaltyTiers_Programs] FOREIGN KEY ([LoyaltyProgramID]) REFERENCES [Loyalty].[LoyaltyPrograms] ([LoyaltyProgramID]),
    CONSTRAINT [FK_Loyalty_LoyaltyTiers_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
