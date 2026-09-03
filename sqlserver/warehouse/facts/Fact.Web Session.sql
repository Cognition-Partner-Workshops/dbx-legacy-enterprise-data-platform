/*
    Fact.Web Session

    Object        : [Fact].[Web Session] - transaction fact, one row per web
                    session (sessionised clickstream), with the significant page
                    events flattened onto the session row.
    Deploy target : WideWorldImportersDW
    Deploy order  : after Dimension.Customer, Dimension.Sales Channel,
                    Dimension.Geography, Dimension.Promotion (WP05).
    Called by     : loaded by Integration.usp_LoadFactWebSession from the daily
                    clickstream flat-file drop.
    Grain         : one web session.

    The clickstream vendor delivers events, not sessions; the load sessionises
    with a 30 minute inactivity gap in a cursor because the 2013 implementation
    predates window functions being trusted here. Most sessions are anonymous,
    so [Customer Key] resolves to the unknown member far more often than on any
    other fact - the anonymous rate is a data-quality KPI, not a defect.

    Privacy divergence: EU sessions store a truncated IP prefix and drop the
    user agent when cookie consent was not granted; APAC stores the full IP but
    an anonymise-after date; NA stores everything and relies on the opt-out
    list. Same source file, three different retention behaviours.
*/
CREATE TABLE [Fact].[Web Session] (
    [Web Session Key]               BIGINT          IDENTITY (1, 1) NOT NULL,
    [Session Start Date Key]        DATE            NOT NULL,
    [Customer Key]                  INT             NOT NULL,
    [Sales Channel Key]             INT             NULL,
    [Geography Key]                 INT             NULL,
    [Promotion Key]                 INT             NULL,
    [Region Code]                   NVARCHAR (4)    NOT NULL,
    [Session Id]                    NVARCHAR (64)   NOT NULL,
    [Visitor Id]                    NVARCHAR (64)   NULL,
    [Session Start Time]            DATETIME2 (0)   NOT NULL,
    [Session End Time]              DATETIME2 (0)   NULL,
    [Session Duration Seconds]      INT             NULL,
    [Page View Count]               INT             NULL,
    [Product View Count]            INT             NULL,
    [Search Count]                  INT             NULL,
    [Add To Cart Count]             INT             NULL,
    [Cart Abandoned Flag]           BIT             NULL,
    [Checkout Started Flag]         BIT             NULL,
    [Order Placed Flag]             BIT             NULL,
    [Order Number]                  NVARCHAR (20)   NULL,
    [Order Value]                   DECIMAL (18, 2) NULL,
    [Order Value Reporting]         DECIMAL (18, 2) NULL,
    [FX Rate To Reporting]          DECIMAL (19, 9) NULL,
    [Device Type Code]              NVARCHAR (10)   NULL,
    [Browser Family]                NVARCHAR (40)   NULL,
    [User Agent String]             NVARCHAR (400)  NULL,
    [IP Address Prefix]             NVARCHAR (40)   NULL,
    [Traffic Source Code]           NVARCHAR (20)   NULL,
    [Campaign Code]                 NVARCHAR (30)   NULL,
    [Landing Page Path]             NVARCHAR (400)  NULL,
    [Exit Page Path]                NVARCHAR (400)  NULL,
    [Cookie Consent Flag]           BIT             NULL,
    [Anonymous Session Flag]        BIT             NULL,
    [Anonymise After Date]          DATE            NULL,
    [Natural Key Hash]              BINARY (32)     NULL,
    [Lineage Key]                   INT             NOT NULL,
    [Batch Id]                      BIGINT          NULL,
    [Load Datetime]                 DATETIME2 (3)   NULL,
    CONSTRAINT [PK_Fact_Web_Session] PRIMARY KEY NONCLUSTERED ([Web Session Key] ASC, [Session Start Date Key] ASC) ON [PS_Date] ([Session Start Date Key]),
    CONSTRAINT [FK_Fact_Web_Session_Session_Start_Date_Key_Dimension_Date] FOREIGN KEY ([Session Start Date Key]) REFERENCES [Dimension].[Date] ([Date]),
    CONSTRAINT [FK_Fact_Web_Session_Customer_Key_Dimension_Customer] FOREIGN KEY ([Customer Key]) REFERENCES [Dimension].[Customer] ([Customer Key])
)
ON [PS_Date] ([Session Start Date Key]);
GO

CREATE UNIQUE NONCLUSTERED INDEX [UX_Fact_Web_Session_Natural_Key]
    ON [Fact].[Web Session] ([Session Id] ASC, [Session Start Date Key] ASC)
    ON [PS_Date] ([Session Start Date Key]);
GO

CREATE NONCLUSTERED INDEX [IX_Fact_Web_Session_Conversion]
    ON [Fact].[Web Session] ([Session Start Date Key] ASC, [Order Placed Flag] ASC)
    INCLUDE ([Order Value Reporting], [Traffic Source Code], [Device Type Code])
    ON [PS_Date] ([Session Start Date Key]);
GO

CREATE CLUSTERED COLUMNSTORE INDEX [CCX_Fact_Web_Session]
    ON [Fact].[Web Session]
    ON [PS_Date] ([Session Start Date Key]);
GO

EXECUTE sp_addextendedproperty @name = N'Description',
    @value = N'Sessionised clickstream fact with regional privacy handling',
    @level0type = N'SCHEMA', @level0name = N'Fact',
    @level1type = N'TABLE', @level1name = N'Web Session';
GO
