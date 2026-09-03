/*
    Warehouse.CycleCountLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1270 - after Warehouse.CycleCounts
    Depends on    : Warehouse.CycleCounts, Warehouse.Bins, Warehouse.StockItems
    Called by     : Warehouse.usp_ReconcileCycleCount, Warehouse.vw_CycleCountVariance

    Blind count detail. SystemQuantity is stamped at the moment the sheet is
    generated, not at reconciliation, so a movement posted mid-count shows up
    as a variance. Everybody knows this and the supervisors count out of hours
    because of it.
*/
CREATE TABLE [Warehouse].[CycleCountLines] (
    [CycleCountLineID]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [CycleCountID]          INT             NOT NULL,
    [BinID]                 INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [SystemQuantity]        DECIMAL (18, 3) NOT NULL,
    [CountedQuantity]       DECIMAL (18, 3) NULL,
    [SecondCountQuantity]   DECIMAL (18, 3) NULL,
    [VarianceQuantity]      AS (ISNULL([CountedQuantity], [SystemQuantity]) - [SystemQuantity]) PERSISTED,
    [UnitCostAtCount]       DECIMAL (18, 4) NULL,
    [VarianceValue]         AS (CASE WHEN [UnitCostAtCount] IS NULL THEN NULL
                                     ELSE CONVERT(DECIMAL (18, 2),
                                          (ISNULL([CountedQuantity], [SystemQuantity]) - [SystemQuantity]) * [UnitCostAtCount]) END),
    [VarianceReasonCode]    NVARCHAR (10)   NULL,
    [AdjustmentMovementID]  BIGINT          NULL,
    [LineStatus]            NVARCHAR (12)   CONSTRAINT [DF_Warehouse_CycleCountLines_LineStatus] DEFAULT (N'PENDING') NOT NULL,
    [CountedWhen]           DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_CycleCountLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_CycleCountLines] PRIMARY KEY CLUSTERED ([CycleCountLineID] ASC),
    CONSTRAINT [UQ_Warehouse_CycleCountLines_Bin_Item] UNIQUE ([CycleCountID], [BinID], [StockItemID], [LotNumber]),
    CONSTRAINT [CK_Warehouse_CycleCountLines_Status] CHECK ([LineStatus] IN (N'PENDING', N'COUNTED', N'VARIANCE', N'ADJUSTED', N'WRITTENOFF')),
    CONSTRAINT [FK_Warehouse_CycleCountLines_Counts] FOREIGN KEY ([CycleCountID]) REFERENCES [Warehouse].[CycleCounts] ([CycleCountID]),
    CONSTRAINT [FK_Warehouse_CycleCountLines_Bins] FOREIGN KEY ([BinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_CycleCountLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_CycleCountLines_Movement] FOREIGN KEY ([AdjustmentMovementID]) REFERENCES [Warehouse].[StockMovementDetails] ([StockMovementID]),
    CONSTRAINT [FK_Warehouse_CycleCountLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_CycleCountLines_Variance]
    ON [Warehouse].[CycleCountLines] ([CycleCountID] ASC)
    INCLUDE ([StockItemID], [BinID], [SystemQuantity], [CountedQuantity])
    WHERE [LineStatus] IN (N'VARIANCE', N'ADJUSTED');
GO
