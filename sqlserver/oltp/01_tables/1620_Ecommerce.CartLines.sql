/*
    Ecommerce.CartLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1620 - after Ecommerce.CartHeaders
    Depends on    : Ecommerce.CartHeaders, Warehouse.StockItems
    Called by     : Ecommerce.vw_WebConversionFunnel

    Basket lines, kept after removal rather than deleted (RemovedWhen is set)
    because merchandising wanted to analyse what people take out again. The
    displayed price is snapshotted at add-to-basket time and is not refreshed,
    so a long-lived cart can quote a price the catalogue no longer offers;
    checkout re-prices and the difference is the source of most "the price
    changed" support calls.
*/
CREATE TABLE [Ecommerce].[CartLines] (
    [CartLineID]            BIGINT          IDENTITY (1, 1) NOT NULL,
    [CartID]                BIGINT          NOT NULL,
    [LineSequence]          SMALLINT        NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [AddedWhen]             DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_CartLines_AddedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RemovedWhen]           DATETIME2 (7)   NULL,
    [Quantity]              DECIMAL (18, 3) NOT NULL,
    [DisplayedUnitPrice]    DECIMAL (18, 2) NOT NULL,
    [DisplayedTaxRate]      DECIMAL (18, 3) CONSTRAINT [DF_Ecommerce_CartLines_DisplayedTaxRate] DEFAULT (0) NOT NULL,
    [LineSubtotal]          AS (CONVERT(DECIMAL (18, 2), [Quantity] * [DisplayedUnitPrice])) PERSISTED,
    [AddedFromPageType]     NVARCHAR (16)   NULL,
    [WasBackordered]        BIT             CONSTRAINT [DF_Ecommerce_CartLines_WasBackordered] DEFAULT (0) NOT NULL,
    [PersonalisationText]   NVARCHAR (400)  NULL,
    CONSTRAINT [PK_Ecommerce_CartLines] PRIMARY KEY CLUSTERED ([CartLineID] ASC),
    CONSTRAINT [UQ_Ecommerce_CartLines_Sequence] UNIQUE ([CartID], [LineSequence]),
    CONSTRAINT [CK_Ecommerce_CartLines_Quantity] CHECK ([Quantity] > 0),
    CONSTRAINT [CK_Ecommerce_CartLines_PageType] CHECK ([AddedFromPageType] IS NULL OR [AddedFromPageType] IN (N'SEARCH', N'CATEGORY', N'PRODUCT', N'RECOMMEND', N'REORDER', N'WISHLIST')),
    CONSTRAINT [FK_Ecommerce_CartLines_Headers] FOREIGN KEY ([CartID]) REFERENCES [Ecommerce].[CartHeaders] ([CartID]),
    CONSTRAINT [FK_Ecommerce_CartLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Ecommerce_CartLines_StockItem]
    ON [Ecommerce].[CartLines] ([StockItemID] ASC, [AddedWhen] DESC)
    INCLUDE ([CartID], [Quantity], [RemovedWhen]);
GO
