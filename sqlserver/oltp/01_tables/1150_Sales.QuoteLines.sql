/*
    Sales.QuoteLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1150 - after Sales.QuoteHeaders
    Depends on    : Sales.QuoteHeaders, Warehouse.StockItems, Application.People
    Called by     : Sales.usp_ConvertQuoteToOrder, Sales.ufn_LineNetAmount

    Quote detail. The description is snapshotted onto the line because the
    stock item description changes and quotes are legal documents in the EU
    entity. TaxRatePercent is stored per line, not derived, for the same reason.
*/
CREATE TABLE [Sales].[QuoteLines] (
    [QuoteLineID]           BIGINT          IDENTITY (1, 1) NOT NULL,
    [QuoteID]               INT             NOT NULL,
    [LineNumber]            SMALLINT        NOT NULL,
    [StockItemID]           INT             NULL,
    [DescriptionSnapshot]   NVARCHAR (200)  NOT NULL,
    [Quantity]              DECIMAL (18, 3) NOT NULL,
    [UnitPrice]             DECIMAL (18, 2) NOT NULL,
    [DiscountPercent]       DECIMAL (5, 2)  CONSTRAINT [DF_Sales_QuoteLines_DiscountPercent] DEFAULT (0) NOT NULL,
    [TaxRatePercent]        DECIMAL (5, 2)  CONSTRAINT [DF_Sales_QuoteLines_TaxRatePercent] DEFAULT (0) NOT NULL,
    [LineNetAmount]         AS (CONVERT(DECIMAL (18, 2), [Quantity] * [UnitPrice] * (1 - [DiscountPercent] / 100.0))) PERSISTED,
    [PromisedLeadTimeDays]  SMALLINT        NULL,
    [IsOptionalLine]        BIT             CONSTRAINT [DF_Sales_QuoteLines_IsOptionalLine] DEFAULT (0) NOT NULL,
    [LineStatus]            NVARCHAR (12)   CONSTRAINT [DF_Sales_QuoteLines_LineStatus] DEFAULT (N'OPEN') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Sales_QuoteLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Sales_QuoteLines] PRIMARY KEY CLUSTERED ([QuoteLineID] ASC),
    CONSTRAINT [UQ_Sales_QuoteLines_LineNumber] UNIQUE ([QuoteID], [LineNumber]),
    CONSTRAINT [CK_Sales_QuoteLines_Quantity] CHECK ([Quantity] > 0),
    CONSTRAINT [CK_Sales_QuoteLines_Discount] CHECK ([DiscountPercent] BETWEEN 0 AND 100),
    CONSTRAINT [CK_Sales_QuoteLines_Status] CHECK ([LineStatus] IN (N'OPEN', N'ACCEPTED', N'DECLINED', N'CONVERTED')),
    CONSTRAINT [FK_Sales_QuoteLines_QuoteHeaders] FOREIGN KEY ([QuoteID]) REFERENCES [Sales].[QuoteHeaders] ([QuoteID]),
    CONSTRAINT [FK_Sales_QuoteLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_QuoteLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_QuoteLines_StockItem]
    ON [Sales].[QuoteLines] ([StockItemID] ASC)
    INCLUDE ([QuoteID], [Quantity], [UnitPrice]);
GO
