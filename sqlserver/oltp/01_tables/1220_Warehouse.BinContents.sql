/*
    Warehouse.BinContents

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1220 - after Warehouse.Bins
    Depends on    : Warehouse.Bins, Warehouse.StockItems, Application.People
    Called by     : Warehouse.usp_PostStockMovement, Warehouse.usp_ReconcileCycleCount,
                    Warehouse.ufn_AvailableToPromise, Warehouse.vw_StockOnHandBySite

    Quantity on hand per bin per item per lot. This is the denormalised
    position that Warehouse.usp_PostStockMovement maintains; the ledger in
    Warehouse.StockMovementDetails is the record of truth and the two are
    reconciled by the cycle count process rather than by a constraint.
*/
CREATE TABLE [Warehouse].[BinContents] (
    [BinContentID]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [BinID]                 INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   CONSTRAINT [DF_Warehouse_BinContents_LotNumber] DEFAULT (N'NOLOT') NOT NULL,
    [ExpiryDate]            DATE            NULL,
    [QuantityOnHand]        DECIMAL (18, 3) CONSTRAINT [DF_Warehouse_BinContents_QuantityOnHand] DEFAULT (0) NOT NULL,
    [QuantityReserved]      DECIMAL (18, 3) CONSTRAINT [DF_Warehouse_BinContents_QuantityReserved] DEFAULT (0) NOT NULL,
    [QuantityAvailable]     AS ([QuantityOnHand] - [QuantityReserved]) PERSISTED,
    [UnitCostAtReceipt]     DECIMAL (18, 4) NULL,
    [ReceivedWhen]          DATETIME2 (7)   NULL,
    [LastMovementWhen]      DATETIME2 (7)   NULL,
    [LastCountedWhen]       DATETIME2 (7)   NULL,
    [ContentStatus]         NVARCHAR (12)   CONSTRAINT [DF_Warehouse_BinContents_ContentStatus] DEFAULT (N'GOOD') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_BinContents_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_BinContents] PRIMARY KEY CLUSTERED ([BinContentID] ASC),
    CONSTRAINT [UQ_Warehouse_BinContents_Bin_Item_Lot] UNIQUE ([BinID], [StockItemID], [LotNumber]),
    CONSTRAINT [CK_Warehouse_BinContents_Reserved] CHECK ([QuantityReserved] >= 0 AND [QuantityReserved] <= [QuantityOnHand] + 0.001),
    CONSTRAINT [CK_Warehouse_BinContents_Status] CHECK ([ContentStatus] IN (N'GOOD', N'HELD', N'DAMAGED', N'EXPIRED', N'QUARANTINE')),
    CONSTRAINT [FK_Warehouse_BinContents_Bins] FOREIGN KEY ([BinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_BinContents_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_BinContents_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_BinContents_Item_Available]
    ON [Warehouse].[BinContents] ([StockItemID] ASC)
    INCLUDE ([BinID], [QuantityOnHand], [QuantityReserved], [LotNumber])
    WHERE [ContentStatus] = N'GOOD';
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_BinContents_Expiry]
    ON [Warehouse].[BinContents] ([ExpiryDate] ASC)
    INCLUDE ([StockItemID], [BinID], [QuantityOnHand])
    WHERE [ExpiryDate] IS NOT NULL;
GO
