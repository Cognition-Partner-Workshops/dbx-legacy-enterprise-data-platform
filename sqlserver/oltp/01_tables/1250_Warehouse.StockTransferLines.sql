/*
    Warehouse.StockTransferLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1250 - after Warehouse.StockTransfers
    Depends on    : Warehouse.StockTransfers, Warehouse.StockItems, Warehouse.Bins
    Called by     : Warehouse.usp_TransferStockBetweenSites

    Transfer detail with three quantities: requested, despatched and received.
    They are allowed to disagree; the difference is the in-transit loss that
    the month-end stock reconciliation writes off.
*/
CREATE TABLE [Warehouse].[StockTransferLines] (
    [StockTransferLineID]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [StockTransferID]       INT             NOT NULL,
    [LineNumber]            SMALLINT        NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [FromBinID]             INT             NULL,
    [ToBinID]               INT             NULL,
    [QuantityRequested]     DECIMAL (18, 3) NOT NULL,
    [QuantityDespatched]    DECIMAL (18, 3) NULL,
    [QuantityReceived]      DECIMAL (18, 3) NULL,
    [QuantityVariance]      AS (ISNULL([QuantityReceived], 0) - ISNULL([QuantityDespatched], 0)) PERSISTED,
    [VarianceReasonCode]    NVARCHAR (10)   NULL,
    [UnitCostAtDespatch]    DECIMAL (18, 4) NULL,
    [LineStatus]            NVARCHAR (12)   CONSTRAINT [DF_Warehouse_StockTransferLines_LineStatus] DEFAULT (N'OPEN') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_StockTransferLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_StockTransferLines] PRIMARY KEY CLUSTERED ([StockTransferLineID] ASC),
    CONSTRAINT [UQ_Warehouse_StockTransferLines_LineNumber] UNIQUE ([StockTransferID], [LineNumber]),
    CONSTRAINT [CK_Warehouse_StockTransferLines_Requested] CHECK ([QuantityRequested] > 0),
    CONSTRAINT [CK_Warehouse_StockTransferLines_Status] CHECK ([LineStatus] IN (N'OPEN', N'DESPATCHED', N'RECEIVED', N'SHORT', N'CANCELLED')),
    CONSTRAINT [FK_Warehouse_StockTransferLines_Transfers] FOREIGN KEY ([StockTransferID]) REFERENCES [Warehouse].[StockTransfers] ([StockTransferID]),
    CONSTRAINT [FK_Warehouse_StockTransferLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_StockTransferLines_FromBin] FOREIGN KEY ([FromBinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_StockTransferLines_ToBin] FOREIGN KEY ([ToBinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Warehouse_StockTransferLines_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_StockTransferLines_Variance]
    ON [Warehouse].[StockTransferLines] ([VarianceReasonCode] ASC)
    INCLUDE ([StockTransferID], [StockItemID], [QuantityVariance])
    WHERE [VarianceReasonCode] IS NOT NULL;
GO
