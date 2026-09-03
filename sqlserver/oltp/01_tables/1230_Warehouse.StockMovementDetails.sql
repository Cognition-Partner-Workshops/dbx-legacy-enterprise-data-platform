/*
    Warehouse.StockMovementDetails

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1230 - after Warehouse.Bins, Warehouse.WarehouseSites
    Depends on    : Warehouse.Bins, Warehouse.WarehouseSites, Warehouse.StockItems,
                    Application.People
    Called by     : Warehouse.usp_PostStockMovement, Warehouse.usp_TransferStockBetweenSites,
                    Warehouse.usp_ReconcileCycleCount, Warehouse.vw_StockMovementExtract,
                    EXT_SQL_StockMovement (incremental extract, keyed on StockMovementID)

    The movement ledger: append-only, one row per posted movement of stock.
    Every quantity is signed (negative issues, positive receipts) and the
    ledger is the reconciliation point for Warehouse.BinContents.

    The extract reads this table by StockMovementID watermark, so rows are
    never renumbered and never back-dated.
*/
CREATE TABLE [Warehouse].[StockMovementDetails] (
    [StockMovementID]       BIGINT          CONSTRAINT [DF_Warehouse_StockMovementDetails_StockMovementID] DEFAULT (NEXT VALUE FOR [Sequences].[StockMovementID]) NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [FromBinID]             INT             NULL,
    [ToBinID]               INT             NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [MovementTypeCode]      NVARCHAR (10)   NOT NULL,
    [ReasonCode]            NVARCHAR (10)   NULL,
    [Quantity]              DECIMAL (18, 3) NOT NULL,
    [UnitCost]              DECIMAL (18, 4) NULL,
    [ExtendedCost]          AS (CASE WHEN [UnitCost] IS NULL THEN NULL
                                     ELSE CONVERT(DECIMAL (18, 4), [Quantity] * [UnitCost]) END) PERSISTED,
    [ReferenceType]         NVARCHAR (16)   NULL,
    [ReferenceID]           BIGINT          NULL,
    [MovementWhen]          DATETIME2 (7)   CONSTRAINT [DF_Warehouse_StockMovementDetails_MovementWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [PostedByPersonID]      INT             NOT NULL,
    [IsReversal]            BIT             CONSTRAINT [DF_Warehouse_StockMovementDetails_IsReversal] DEFAULT (0) NOT NULL,
    [ReversalOfMovementID]  BIGINT          NULL,
    [SourceApplication]     NVARCHAR (20)   CONSTRAINT [DF_Warehouse_StockMovementDetails_SourceApplication] DEFAULT (N'WWI-OLTP') NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_StockMovementDetails_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_StockMovementDetails] PRIMARY KEY CLUSTERED ([StockMovementID] ASC),
    CONSTRAINT [CK_Warehouse_StockMovementDetails_Type] CHECK ([MovementTypeCode] IN (N'RECEIPT', N'ISSUE', N'TRANSFER', N'ADJUST', N'COUNT', N'SCRAP', N'RETURN', N'PICK', N'PUTAWAY')),
    CONSTRAINT [CK_Warehouse_StockMovementDetails_Quantity] CHECK ([Quantity] <> 0),
    CONSTRAINT [CK_Warehouse_StockMovementDetails_Bins] CHECK ([FromBinID] IS NOT NULL OR [ToBinID] IS NOT NULL),
    CONSTRAINT [CK_Warehouse_StockMovementDetails_Reversal] CHECK ([IsReversal] = 0 OR [ReversalOfMovementID] IS NOT NULL),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_FromBin] FOREIGN KEY ([FromBinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_ToBin] FOREIGN KEY ([ToBinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_Reversal] FOREIGN KEY ([ReversalOfMovementID]) REFERENCES [Warehouse].[StockMovementDetails] ([StockMovementID]),
    CONSTRAINT [FK_Warehouse_StockMovementDetails_Application_People] FOREIGN KEY ([PostedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_Item_When]
    ON [Warehouse].[StockMovementDetails] ([StockItemID] ASC, [MovementWhen] ASC)
    INCLUDE ([WarehouseSiteID], [Quantity], [MovementTypeCode]);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_StockMovementDetails_Reference]
    ON [Warehouse].[StockMovementDetails] ([ReferenceType] ASC, [ReferenceID] ASC)
    WHERE [ReferenceID] IS NOT NULL;
GO
