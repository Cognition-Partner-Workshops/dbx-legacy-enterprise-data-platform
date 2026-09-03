/*
    Fact.Promotion Eligibility

    Object        : [Fact].[Promotion Eligibility] - factless fact table
                    recording which customer/product combinations were eligible
                    for which promotion on which days.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Promotion, Dimension.Customer,
                    Dimension.Stock Item, Dimension.Sales Channel (WP05).
    Called by     : loaded by Integration.usp_RefreshAggregatePromotionEffectiveness
                    before the effectiveness numbers are computed.
    Grain         : promotion x customer x stock item x eligibility date range.

    Factless: there are no additive measures. The table exists so that
    "eligible but did not buy" is answerable - the promotion effectiveness
    aggregate left-joins sales to this table, and the denominator of every
    take-up rate comes from here.

    Eligibility rules diverge: EU promotions cannot be offered to customers who
    withheld marketing consent, so the eligibility rows are simply not written;
    APAC promotions are restricted by country-level pricing approval; NA
    promotions are open to every customer in the territory. The same promotion
    can therefore have wildly different denominators by region.
*/
CREATE TABLE [Fact].[Promotion Eligibility] (
    [Promotion Eligibility Key]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [Eligibility Start Date Key]    DATE            NOT NULL,
    [Eligibility End Date Key]      DATE            NULL,
    [Promotion Key]                 INT             NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Stock Item Key]                INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Sales Territory Key]           INT             NULL,
    [Customer Segment Key]          INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Promotion Code]                NVARCHAR (20)   NOT NULL,
    [Eligibility Rule Code]         NVARCHAR (10)   NOT NULL,
    [Eligibility Source Code]       NVARCHAR (10)   NULL,
    [Minimum Quantity]              DECIMAL (18, 4) NULL,
    [Minimum Spend Reporting]       DECIMAL (18, 2) NULL,
    [Marketing Consent Required]    BIT             NULL,
    [Country Approval Reference]    NVARCHAR (20)   NULL,
    [Opted Out Flag]                BIT             NULL,
    [Notified Flag]                 BIT             NULL,
    [Eligibility Days]              INT             NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Promotion_Eligibility] PRIMARY KEY NONCLUSTERED ([Promotion Eligibility Key] ASC, [Eligibility Start Date Key] ASC) ON [PS_Date] ([Eligibility Start Date Key]),
    CONSTRAINT [FK_Fact_Promotion_Eligibility_Eligibility_Start_Date_Key_Dimension_Date] FOREIGN KEY ([Eligibility Start Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Promotion_Eligibility_Promotion_Key_Dimension_Promotion] FOREIGN KEY ([Promotion Key]) REFERENCES [Dimension].[Promotion] ([Promotion Key]),
    CONSTRAINT [FK_Fact_Promotion_Eligibility_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Eligibility Start Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Promotion_Eligibility_Grain]
    ON [Fact].[Promotion Eligibility] ([Promotion Key] ASC, [Customer Key] ASC, [Stock Item Key] ASC, [Eligibility Start Date Key] ASC)
    ON [PS_Date] ([Eligibility Start Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Promotion_Eligibility_Take_Up]
    ON [Fact].[Promotion Eligibility] ([Promotion Code] ASC, [Region Code] ASC)
    INCLUDE ([Customer Key], [Stock Item Key], [Eligibility End Date Key])
    ON [PS_Date] ([Eligibility Start Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Factless fact: promotion eligibility coverage, denominator for take-up rates',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Promotion Eligibility';
GO
