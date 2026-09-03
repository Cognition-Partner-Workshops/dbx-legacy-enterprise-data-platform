/*
    Sales.SalesChannels

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1010 - no dependencies beyond Application.People
    Depends on    : Application.People
    Called by     : Sales.Orders (OrderSourceChannelID, added in 02_extensions),
                    Sales.vw_OrderLineExtract, Ecommerce.WebSessions

    Order intake channels. The table pre-dates the ecommerce project, which is
    why the web channels were bolted on with a ChannelClass of 'DIGITAL' and
    reuse the EDI partner columns for the storefront identifier.

    Legacy pattern: ChannelStatus is overloaded across two processes. The order
    entry application treats anything other than 'CLOSED' as orderable, while
    the nightly commission run treats 'PILOT' as non-commissionable. There is
    no separate IsCommissionable flag and there never was.
*/
CREATE TABLE [Sales].[SalesChannels] (
    [SalesChannelID]            INT             IDENTITY (1, 1) NOT NULL,
    [ChannelCode]               NVARCHAR (10)   NOT NULL,
    [ChannelName]               NVARCHAR (60)   NOT NULL,
    [ChannelClass]              NVARCHAR (12)   NOT NULL,
    [RegionCode]                NCHAR (4)       NOT NULL,
    [ChannelStatus]             NVARCHAR (10)   NOT NULL,
    [PartnerIdentifier]         NVARCHAR (40)   NULL,
    [DefaultPriceListCode]      NVARCHAR (20)   NULL,
    [CommissionModifierPercent] DECIMAL (5, 2)  NOT NULL,
    [RequiresManualApproval]    BIT             NOT NULL,
    [OrderPrefix]               NVARCHAR (6)    NULL,
    [ValidFromDate]             DATE            NOT NULL,
    [ValidToDate]               DATE            NULL,
    [LastEditedBy]              INT             NOT NULL,
    [LastEditedWhen]            DATETIME2 (7)   CONSTRAINT [DF_Sales_SalesChannels_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_SalesChannels] PRIMARY KEY CLUSTERED ([SalesChannelID] ASC),
    CONSTRAINT [UQ_Sales_SalesChannels_ChannelCode] UNIQUE ([ChannelCode], [RegionCode]),
    CONSTRAINT [CK_Sales_SalesChannels_ChannelClass] CHECK ([ChannelClass] IN (N'DIRECT', N'EDI', N'DIGITAL', N'PARTNER', N'CALLCENTRE')),
    CONSTRAINT [CK_Sales_SalesChannels_RegionCode] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Sales_SalesChannels_ChannelStatus] CHECK ([ChannelStatus] IN (N'ACTIVE', N'PILOT', N'SUSPENDED', N'CLOSED')),
    CONSTRAINT [CK_Sales_SalesChannels_Validity] CHECK ([ValidToDate] IS NULL OR [ValidToDate] >= [ValidFromDate]),
    CONSTRAINT [FK_Sales_SalesChannels_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_SalesChannels_Region_Open]
    ON [Sales].[SalesChannels] ([RegionCode] ASC, [ChannelClass] ASC)
    INCLUDE ([ChannelCode], [ChannelName], [CommissionModifierPercent])
    WHERE [ChannelStatus] <> N'CLOSED';
GO
