/*
    Fact.Loyalty Points

    Object        : [Fact].[Loyalty Points] - transaction fact, one row per
                    loyalty point movement (earn, redeem, expire, adjust,
                    transfer).
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Loyalty Tier, Dimension.Customer,
                    Dimension.Sales Channel, Dimension.Promotion (WP05).
    Called by     : loaded by Integration.usp_LoadFactLoyaltyPoints from the
                    loyalty platform extract.
    Grain         : one point movement line.

    Points earned and points redeemed live in the same signed measure
    ([Points Delta]) but the monetary liability is only meaningful on earn and
    expiry rows, so the two are kept apart rather than derived.

    Consent and retention diverge: EU rows carry a marketing consent flag and an
    anonymise-after date driven by the GDPR retention rule, and the load nulls
    the free-text note for EU customers who withdrew consent; APAC carries a
    jurisdiction-specific consent basis code; NA rows carry an opt-out flag only,
    which is why the same customer can have different privacy state per region.
*/
CREATE TABLE [Fact].[Loyalty Points] (
    [Loyalty Points Key]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [Movement Date Key]             DATE            NOT NULL,
    [Points Expiry Date Key]        DATE            NULL,
    [Customer Key]                  INT             NOT NULL,
    [Loyalty Tier Key]              INT             NOT NULL,
    [Loyalty Scheme Key]            INT             NULL,
    [Sales Channel Key]             INT             NULL,
    [Promotion Key]                 INT             NULL,
    [Stock Item Key]                INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Loyalty Account Number]        NVARCHAR (20)   NOT NULL,
    [Movement Reference]            NVARCHAR (30)   NOT NULL,
    [Movement Type Code]            NVARCHAR (6)    NOT NULL,
    [Invoice Number]                NVARCHAR (20)   NULL,
    [Points Delta]                  DECIMAL (18, 2) NOT NULL,
    [Points Balance After]          DECIMAL (18, 2) NULL,
    [Bonus Multiplier]              DECIMAL (9, 4)  NULL,
    [Qualifying Spend Amount]       DECIMAL (18, 2) NULL,
    [Point Liability Amount]        DECIMAL (18, 2) NULL,
    [Point Liability Reporting]     DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Redemption Value Amount]       DECIMAL (18, 2) NULL,
    [Breakage Amount]               DECIMAL (18, 2) NULL,
    [Tier At Movement Code]         NVARCHAR (6)    NULL,
    [Tier Change Flag]              BIT             NULL,
    [Marketing Consent Flag]        BIT             NULL,
    [Consent Basis Code]            NVARCHAR (10)   NULL,
    [Opt Out Flag]                  BIT             NULL,
    [Anonymise After Date]          DATE            NULL,
    [Movement Note]                 NVARCHAR (200)  NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Inferred Member Flag]          BIT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Loyalty_Points] PRIMARY KEY NONCLUSTERED ([Loyalty Points Key] ASC, [Movement Date Key] ASC) ON [PS_Date] ([Movement Date Key]),
    CONSTRAINT [FK_Fact_Loyalty_Points_Movement_Date_Key_Dimension_Date] FOREIGN KEY ([Movement Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Loyalty_Points_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key]),
    CONSTRAINT [FK_Fact_Loyalty_Points_Loyalty_Tier_Key_Dimension_Loyalty Tier] FOREIGN KEY ([Loyalty Tier Key]) REFERENCES [Dimension].[Loyalty Tier] ([Loyalty Tier Key])
)
ON [PS_Date] ([Movement Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Loyalty_Points_Natural_Key]
    ON [Fact].[Loyalty Points] ([Loyalty Account Number] ASC, [Movement Reference] ASC, [Movement Date Key] ASC)
    ON [PS_Date] ([Movement Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Loyalty_Points_Expiry_Sweep]
    ON [Fact].[Loyalty Points] ([Points Expiry Date Key] ASC, [Movement Type Code] ASC)
    INCLUDE ([Points Delta], [Point Liability Reporting])
    ON [PS_Date] ([Movement Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Loyalty_Points]
    ON [Fact].[Loyalty Points]
    ON [PS_Date] ([Movement Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Loyalty point movement fact with regional consent and retention attributes',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Loyalty Points';
GO
