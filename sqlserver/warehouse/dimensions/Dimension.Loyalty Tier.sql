/*
    Object        : [Dimension].[Loyalty Tier]  (SCD Type 1 on the tier definition)
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Region.sql
    Depends on    : Sequences.LoyaltyTierKey
    Called by     : Integration.usp_MigrateStagedLoyaltyTierData

    The tier *definition* is Type 1 - when the qualification threshold is changed
    the new threshold applies everywhere, and the fact that a customer qualified
    under the old one is only recoverable from the loyalty ledger fact. A customer's
    tier *membership* over time is not held here at all; it is a Type 2 attribute
    on Dimension.Customer Segment plus the points ledger.

    The three regional programmes were never merged. NA is a spend-based programme
    with four tiers and points that expire 24 months after earning. EU is a
    points-based programme with three tiers, GDPR-driven opt-in, and points that
    expire at the end of the following calendar year. APAC has separate country
    programmes with locally-set thresholds in local currency, which is why the
    qualification amount carries a currency and why the same tier code can exist
    several times with different thresholds.
*/
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'Dimension.Loyalty Tier', N'U') IS NULL
BEGIN
    /* first created in the 2004 DW build */ CREATE TABLE [Dimension].[Loyalty Tier]
    (
        [Loyalty Tier Key]              INT             CONSTRAINT [DF_Dimension_Loyalty_Tier_Key] DEFAULT (NEXT VALUE FOR [Sequences].[LoyaltyTierKey]) NOT NULL,
        [WWI Loyalty Tier ID]           INT             NULL,
        [Programme Code]                NVARCHAR(20)    NOT NULL,   -- NA_REWARDS / EU_CLUB / APAC_<country>
        [Programme Name]                NVARCHAR(80)    NULL,
        [Loyalty Tier Code]             NVARCHAR(15)    NOT NULL,
        [Loyalty Tier]                  NVARCHAR(60)    NOT NULL,
        [Tier Rank]                     SMALLINT        NULL,
        [Region Code]                   NVARCHAR(10)    NULL,
        [Country Code]                  NVARCHAR(3)     NULL,

        [Qualification Basis Code]      NVARCHAR(10)    NULL,   -- SPEND / POINTS / VISITS / INVITE
        [Qualification Amount]          DECIMAL(18, 2)  NULL,
        [Qualification Currency Code]   NVARCHAR(3)     NULL,
        [Qualification Points]          INT             NULL,
        [Qualification Period Months]   SMALLINT        NULL,
        [Retention Amount]              DECIMAL(18, 2)  NULL,   -- lower bar to keep the tier than to reach it
        [Downgrade Grace Months]        SMALLINT        NULL,

        [Earn Rate Points Per Unit]     DECIMAL(9, 4)   NULL,
        [Redeem Rate Value Per Point]   DECIMAL(18, 6)  NULL,
        [Points Expiry Rule Code]       NVARCHAR(15)    NULL,   -- MONTHS24 (NA) / ENDNEXTYEAR (EU) / NONE / MONTHS12
        [Points Expiry Months]          SMALLINT        NULL,
        [Discount Percentage]           DECIMAL(9, 4)   NULL,
        [Free Shipping Threshold]       DECIMAL(18, 2)  NULL,
        [Has Dedicated Support]         BIT             NULL,
        [Has Early Access]              BIT             NULL,
        [Benefit Summary]               NVARCHAR(400)   NULL,

        [Requires Marketing Consent]    BIT             NULL,   -- EU programme cannot enrol without opt-in
        [Consent Basis Code]            NVARCHAR(20)    NULL,
        [Data Retention Months]         SMALLINT        NULL,

        [Is Active]                     BIT             NULL,
        [Launched On]                   DATE            NULL,
        [Retired On]                    DATE            NULL,
        [Source System Code]            NVARCHAR(20)    NULL,
        [Row Hash Type 1]               VARBINARY(32)   NULL,
        [Valid From]                    DATETIME2(7)    NOT NULL,
        [Valid To]                      DATETIME2(7)    NOT NULL,
        [Lineage Key]                   INT             NOT NULL,
        [Last Load Batch Id]            BIGINT          NULL,
        CONSTRAINT [PK_Dimension_Loyalty_Tier] PRIMARY KEY CLUSTERED ([Loyalty Tier Key] ASC),
        CONSTRAINT [UQ_Dimension_Loyalty_Tier_Code] UNIQUE ([Programme Code], [Loyalty Tier Code])
    );
END;
GO
