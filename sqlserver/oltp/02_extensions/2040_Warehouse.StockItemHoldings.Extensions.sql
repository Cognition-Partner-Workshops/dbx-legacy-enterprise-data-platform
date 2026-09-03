/*
    Warehouse.StockItemHoldings - additive column extensions

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2040 - after 2030
    Depends on    : Warehouse.StockItemHoldings (Microsoft sample),
                    Warehouse.WarehouseSites
    Called by     : Warehouse.usp_PostStockMovement, Warehouse.ufn_AvailableToPromise,
                    Warehouse.vw_StockOnHandBySite

    The sample holds one stock row per item with no site dimension, which was
    true until the second warehouse opened. Rather than restructure it, a
    primary site was stamped on each row and the real per-site position moved
    to Warehouse.BinContents; these columns are the reconciliation between the
    two and QuantityOnHandAllSites is a denormalised roll-up maintained by
    Warehouse.usp_PostStockMovement.
*/
IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'PrimaryWarehouseSiteID') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [PrimaryWarehouseSiteID] INT NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'QuantityOnHandAllSites') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [QuantityOnHandAllSites] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'QuantityReservedAllSites') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [QuantityReservedAllSites] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'QuantityInTransit') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [QuantityInTransit] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'QuantityOnPurchaseOrder') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [QuantityOnPurchaseOrder] DECIMAL (18, 3) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'AbcClass') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [AbcClass] NCHAR (1) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'LastCountedWhen') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [LastCountedWhen] DATETIME2 (7) NULL;
GO

IF COL_LENGTH(N'Warehouse.StockItemHoldings', N'LastMovementWhen') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings] ADD [LastMovementWhen] DATETIME2 (7) NULL;
GO

IF OBJECT_ID(N'Warehouse.CK_Warehouse_StockItemHoldings_AbcClass', N'C') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings]
        ADD CONSTRAINT [CK_Warehouse_StockItemHoldings_AbcClass]
        CHECK ([AbcClass] IS NULL OR [AbcClass] IN (N'A', N'B', N'C'));
GO

IF OBJECT_ID(N'Warehouse.FK_Warehouse_StockItemHoldings_WarehouseSites', N'F') IS NULL
    ALTER TABLE [Warehouse].[StockItemHoldings]
        ADD CONSTRAINT [FK_Warehouse_StockItemHoldings_WarehouseSites]
        FOREIGN KEY ([PrimaryWarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]);
GO
