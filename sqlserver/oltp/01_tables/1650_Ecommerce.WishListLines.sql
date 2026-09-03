/*
    Ecommerce.WishListLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1650 - after Ecommerce.WishLists
    Depends on    : Ecommerce.WishLists, Warehouse.StockItems
    Called by     : reorder-template checkout

    Wish list detail. PriceWhenAdded is kept purely so the site can show a
    "price dropped" badge; it is never refreshed and for registry lists it is
    also what the gift purchaser is charged if the item is bought through the
    registry link, which was not the intention when the column was added.
*/
CREATE TABLE [Ecommerce].[WishListLines] (
    [WishListLineID]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [WishListID]            INT             NOT NULL,
    [LineSequence]          SMALLINT        NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [DesiredQuantity]       DECIMAL (18, 3) CONSTRAINT [DF_Ecommerce_WishListLines_DesiredQuantity] DEFAULT (1) NOT NULL,
    [PurchasedQuantity]     DECIMAL (18, 3) CONSTRAINT [DF_Ecommerce_WishListLines_PurchasedQuantity] DEFAULT (0) NOT NULL,
    [PriceWhenAdded]        DECIMAL (18, 2) NULL,
    [AddedWhen]             DATETIME2 (7)   CONSTRAINT [DF_Ecommerce_WishListLines_AddedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [PriorityRank]          SMALLINT        NULL,
    [NoteText]              NVARCHAR (200)  NULL,
    [IsFulfilled]           AS (CASE WHEN [PurchasedQuantity] >= [DesiredQuantity] THEN CONVERT(BIT, 1) ELSE CONVERT(BIT, 0) END) PERSISTED,
    CONSTRAINT [PK_Ecommerce_WishListLines] PRIMARY KEY CLUSTERED ([WishListLineID] ASC),
    CONSTRAINT [UQ_Ecommerce_WishListLines_Item] UNIQUE ([WishListID], [StockItemID]),
    CONSTRAINT [CK_Ecommerce_WishListLines_Quantity] CHECK ([DesiredQuantity] > 0),
    CONSTRAINT [FK_Ecommerce_WishListLines_Lists] FOREIGN KEY ([WishListID]) REFERENCES [Ecommerce].[WishLists] ([WishListID]),
    CONSTRAINT [FK_Ecommerce_WishListLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID])
);
GO
