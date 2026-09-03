/*
    Ecommerce.WebSessions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1600 - after 00_schemas
    Depends on    : Sales.Customers, Application.People
    Called by     : Ecommerce.CartHeaders, Ecommerce.vw_WebConversionFunnel

    Web analytics landed straight into the trading database in 2011 and never
    left. Volume is high and nothing prunes it except the regional retention
    sweep, which uses different windows per region: the EU rows are anonymised
    at thirteen months, the APAC rows at two years, and the NA rows are kept
    indefinitely because marketing asked.

    IpAddressText holds the truncated address for EU sessions and the full
    address for the others - the truncation is done by the web tier, so the
    column cannot be trusted to a single format.
*/
CREATE TABLE [Ecommerce].[WebSessions] (
    [WebSessionID]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [SessionGuid]           UNIQUEIDENTIFIER NOT NULL,
    [CustomerID]            INT             NULL,
    [ContactPersonID]       INT             NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [StartedWhen]           DATETIME2 (7)   NOT NULL,
    [EndedWhen]             DATETIME2 (7)   NULL,
    [DurationSeconds]       AS (CASE WHEN [EndedWhen] IS NULL THEN NULL
                                     ELSE DATEDIFF(SECOND, [StartedWhen], [EndedWhen]) END),
    [DeviceCategory]        NVARCHAR (12)   NOT NULL,
    [BrowserFamily]         NVARCHAR (40)   NULL,
    [LandingPageUrl]        NVARCHAR (400)  NULL,
    [ReferrerUrl]           NVARCHAR (400)  NULL,
    [CampaignCode]          NVARCHAR (24)   NULL,
    [SearchTermList]        NVARCHAR (MAX)  NULL,
    [PageViewCount]         INT             CONSTRAINT [DF_Ecommerce_WebSessions_PageViewCount] DEFAULT (0) NOT NULL,
    [IpAddressText]         NVARCHAR (45)   NULL,
    [CountryISO3]           NCHAR (3)       NULL,
    [ConsentStateCode]      NVARCHAR (12)   NOT NULL,
    [IsBotSuspected]        BIT             CONSTRAINT [DF_Ecommerce_WebSessions_IsBotSuspected] DEFAULT (0) NOT NULL,
    [AnonymisedWhen]        DATETIME2 (7)   NULL,
    [RetentionExpiresOn]    DATE            NULL,
    CONSTRAINT [PK_Ecommerce_WebSessions] PRIMARY KEY CLUSTERED ([WebSessionID] ASC),
    CONSTRAINT [UQ_Ecommerce_WebSessions_Guid] UNIQUE ([SessionGuid]),
    CONSTRAINT [CK_Ecommerce_WebSessions_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Ecommerce_WebSessions_Device] CHECK ([DeviceCategory] IN (N'DESKTOP', N'MOBILE', N'TABLET', N'KIOSK', N'UNKNOWN')),
    CONSTRAINT [CK_Ecommerce_WebSessions_Consent] CHECK ([ConsentStateCode] IN (N'GRANTED', N'DENIED', N'NOTASKED', N'IMPLIED', N'WITHDRAWN')),
    CONSTRAINT [FK_Ecommerce_WebSessions_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Ecommerce_WebSessions_Application_People] FOREIGN KEY ([ContactPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_WebSessions_Started]
    ON [Ecommerce].[WebSessions] ([StartedWhen] ASC)
    INCLUDE ([RegionCode], [CustomerID], [CampaignCode], [PageViewCount]);
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_WebSessions_RetentionDue]
    ON [Ecommerce].[WebSessions] ([RetentionExpiresOn] ASC)
    WHERE [AnonymisedWhen] IS NULL AND [RetentionExpiresOn] IS NOT NULL;
GO
