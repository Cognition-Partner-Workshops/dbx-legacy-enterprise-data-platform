/*
    Sales.QuoteHeaders

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1140 - after Sales.PriceLists, Sales.SalesChannels
    Depends on    : Sales.Customers, Sales.PriceLists, Sales.SalesChannels,
                    Sales.Orders, Application.People
    Called by     : Sales.QuoteLines, Sales.usp_ConvertQuoteToOrder

    Quotations. A quote holds its own FX rate at the moment of quoting; when it
    converts to an order the rate is carried over rather than re-fetched, which
    is why an order can carry a rate several weeks old. EU quotes are quoted
    gross (VAT inclusive) to match the price list, NA and APAC quotes net.
*/
CREATE TABLE [Sales].[QuoteHeaders] (
    [QuoteID]               INT             CONSTRAINT [DF_Sales_QuoteHeaders_QuoteID] DEFAULT (NEXT VALUE FOR [Sequences].[QuoteID]) NOT NULL,
    [QuoteReference]        NVARCHAR (24)   NOT NULL,
    [CustomerID]            INT             NOT NULL,
    [ContactPersonID]       INT             NULL,
    [SalespersonPersonID]   INT             NOT NULL,
    [SalesChannelID]        INT             NULL,
    [PriceListID]           INT             NULL,
    [QuoteDate]             DATE            NOT NULL,
    [ValidUntilDate]        DATE            NOT NULL,
    [CurrencyCode]          NCHAR (3)       NOT NULL,
    [ExchangeRateToUSD]     DECIMAL (18, 8) NULL,
    [TaxTreatment]          NVARCHAR (12)   NOT NULL,
    [QuoteStatus]           NVARCHAR (12)   CONSTRAINT [DF_Sales_QuoteHeaders_QuoteStatus] DEFAULT (N'DRAFT') NOT NULL,
    [RevisionNumber]        SMALLINT        CONSTRAINT [DF_Sales_QuoteHeaders_RevisionNumber] DEFAULT (1) NOT NULL,
    [SupersedesQuoteID]     INT             NULL,
    [ConvertedOrderID]      INT             NULL,
    [ConvertedWhen]         DATETIME2 (7)   NULL,
    [LostReasonCode]        NVARCHAR (10)   NULL,
    [Comments]              NVARCHAR (MAX)  NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_QuoteHeaders_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_QuoteHeaders] PRIMARY KEY CLUSTERED ([QuoteID] ASC),
    CONSTRAINT [UQ_Sales_QuoteHeaders_Reference] UNIQUE ([QuoteReference], [RevisionNumber]),
    CONSTRAINT [CK_Sales_QuoteHeaders_Validity] CHECK ([ValidUntilDate] >= [QuoteDate]),
    CONSTRAINT [CK_Sales_QuoteHeaders_Status] CHECK ([QuoteStatus] IN (N'DRAFT', N'SENT', N'ACCEPTED', N'CONVERTED', N'EXPIRED', N'LOST')),
    CONSTRAINT [CK_Sales_QuoteHeaders_TaxTreatment] CHECK ([TaxTreatment] IN (N'EXCLUSIVE', N'INCLUSIVE')),
    CONSTRAINT [CK_Sales_QuoteHeaders_Converted] CHECK ([QuoteStatus] <> N'CONVERTED' OR [ConvertedOrderID] IS NOT NULL),
    CONSTRAINT [FK_Sales_QuoteHeaders_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Contact] FOREIGN KEY ([ContactPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Salesperson] FOREIGN KEY ([SalespersonPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Channels] FOREIGN KEY ([SalesChannelID]) REFERENCES [Sales].[SalesChannels] ([SalesChannelID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_PriceLists] FOREIGN KEY ([PriceListID]) REFERENCES [Sales].[PriceLists] ([PriceListID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Supersedes] FOREIGN KEY ([SupersedesQuoteID]) REFERENCES [Sales].[QuoteHeaders] ([QuoteID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Orders] FOREIGN KEY ([ConvertedOrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Sales_QuoteHeaders_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_QuoteHeaders_Open]
    ON [Sales].[QuoteHeaders] ([ValidUntilDate] ASC, [CustomerID] ASC)
    INCLUDE ([QuoteReference], [SalespersonPersonID], [CurrencyCode])
    WHERE [QuoteStatus] IN (N'DRAFT', N'SENT', N'ACCEPTED');
GO
