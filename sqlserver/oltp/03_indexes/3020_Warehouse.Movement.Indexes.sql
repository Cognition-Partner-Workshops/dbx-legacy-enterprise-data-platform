/*
    Additive indexes on the warehouse movement and bin tables

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 03_indexes / 3020 - after 01_tables
    Depends on    : Warehouse.StockMovementDetails, Warehouse.BinContents,
                    Warehouse.CycleCountLines
    Called by     : Warehouse.vw_StockMovementExtract, Warehouse.ufn_AvailableToPromise,
                    the nightly movement extract

    The movement ledger is the largest table in the OLTP and every index on it
    was added under duress. The extract index duplicates the leading column of
    the site/date index because the extract plan kept choosing the wrong one.
*/
IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.StockMovementDetails'), N'IX_Warehouse_StockMovementDetails_Extract', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_Extract]
        ON [Warehouse].[StockMovementDetails] ([LastEditedWhen] ASC)
        INCLUDE ([WarehouseSiteID], [StockItemID], [MovementTypeCode], [Quantity], [UnitCost]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.StockMovementDetails'), N'IX_Warehouse_StockMovementDetails_SiteDate', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_SiteDate]
        ON [Warehouse].[StockMovementDetails] ([WarehouseSiteID] ASC, [MovementWhen] ASC)
        INCLUDE ([StockItemID], [MovementTypeCode], [ReasonCode], [Quantity]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.StockMovementDetails'), N'IX_Warehouse_StockMovementDetails_Reference', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_Reference]
        ON [Warehouse].[StockMovementDetails] ([ReferenceType] ASC, [ReferenceID] ASC)
        WHERE [ReferenceID] IS NOT NULL;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.StockMovementDetails'), N'IX_Warehouse_StockMovementDetails_Reversals', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_Reversals]
        ON [Warehouse].[StockMovementDetails] ([ReversalOfMovementID] ASC)
        WHERE [IsReversal] = 1;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.BinContents'), N'IX_Warehouse_BinContents_ItemAvailable', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_BinContents_ItemAvailable]
        ON [Warehouse].[BinContents] ([StockItemID] ASC, [ContentStatus] ASC)
        INCLUDE ([BinID], [LotNumber], [QuantityOnHand], [QuantityReserved], [ExpiryDate]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.BinContents'), N'IX_Warehouse_BinContents_Expiring', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_BinContents_Expiring]
        ON [Warehouse].[BinContents] ([ExpiryDate] ASC)
        INCLUDE ([StockItemID], [BinID], [QuantityOnHand])
        WHERE [ExpiryDate] IS NOT NULL AND [QuantityOnHand] > 0;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Warehouse.CycleCountLines'), N'IX_Warehouse_CycleCountLines_Variance', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Warehouse_CycleCountLines_Variance]
        ON [Warehouse].[CycleCountLines] ([CycleCountID] ASC, [StockItemID] ASC)
        INCLUDE ([CountedQuantity], [SystemQuantity], [LineStatus]);
GO
