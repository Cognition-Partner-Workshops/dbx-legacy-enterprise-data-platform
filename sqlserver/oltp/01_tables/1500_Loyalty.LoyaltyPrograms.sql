/*
    Loyalty.LoyaltyPrograms

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1500 - after 00_schemas
    Depends on    : Application.People
    Called by     : Loyalty.LoyaltyTiers, Loyalty.LoyaltyMembers,
                    Loyalty.ufn_PointsForAmount

    One programme per region, run on different rules. The NA programme earns
    on gross invoice value including sales tax; the EU programme earns on the
    net amount excluding VAT (a 2016 legal opinion, never applied to the other
    two); the APAC programme earns on net and expires points on the June
    financial year end. PointsPerCurrencyUnit is therefore not comparable
    across programmes and no report has ever said so.
*/
CREATE TABLE [Loyalty].[LoyaltyPrograms] (
    [LoyaltyProgramID]      INT             IDENTITY (1, 1) NOT NULL,
    [ProgramCode]           NVARCHAR (12)   NOT NULL,
    [ProgramName]           NVARCHAR (80)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [EarnBasis]             NVARCHAR (12)   NOT NULL,
    [PointsPerCurrencyUnit] DECIMAL (9, 4)  NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [PointValueInCurrency]  DECIMAL (9, 6)  NOT NULL,
    [MinimumRedeemPoints]   INT             CONSTRAINT [DF_Loyalty_LoyaltyPrograms_MinimumRedeemPoints] DEFAULT (500) NOT NULL,
    [PointsExpiryMonths]    SMALLINT        NULL,
    [ExpiryBasis]           NVARCHAR (12)   NOT NULL,
    [ConsentBasis]          NVARCHAR (12)   NOT NULL,
    [RetentionMonths]       SMALLINT        NOT NULL,
    [LaunchedOnDate]        DATE            NOT NULL,
    [ClosedOnDate]          DATE            NULL,
    [ProgramStatus]         NVARCHAR (12)   CONSTRAINT [DF_Loyalty_LoyaltyPrograms_ProgramStatus] DEFAULT (N'LIVE') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Loyalty_LoyaltyPrograms_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Loyalty_LoyaltyPrograms] PRIMARY KEY CLUSTERED ([LoyaltyProgramID] ASC),
    CONSTRAINT [UQ_Loyalty_LoyaltyPrograms_Code] UNIQUE ([ProgramCode]),
    CONSTRAINT [CK_Loyalty_LoyaltyPrograms_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Loyalty_LoyaltyPrograms_EarnBasis] CHECK ([EarnBasis] IN (N'GROSS', N'NET', N'MARGIN')),
    CONSTRAINT [CK_Loyalty_LoyaltyPrograms_ExpiryBasis] CHECK ([ExpiryBasis] IN (N'ROLLING', N'FISCALYEAR', N'CALENDARYEAR', N'NEVER')),
    CONSTRAINT [CK_Loyalty_LoyaltyPrograms_Consent] CHECK ([ConsentBasis] IN (N'OPTIN', N'OPTOUT', N'CONTRACT')),
    CONSTRAINT [CK_Loyalty_LoyaltyPrograms_Status] CHECK ([ProgramStatus] IN (N'PILOT', N'LIVE', N'CLOSED')),
    CONSTRAINT [FK_Loyalty_LoyaltyPrograms_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
