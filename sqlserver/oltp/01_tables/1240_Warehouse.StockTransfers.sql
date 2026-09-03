/*
    Warehouse.StockTransfers

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1240 - after Warehouse.WarehouseSites
    Depends on    : Warehouse.WarehouseSites, Application.People
    Called by     : Warehouse.StockTransferLines, Warehouse.usp_TransferStockBetweenSites

    Inter-site transfer header. Stock in transit belongs to neither site: the
    despatch posts an issue at the origin and the receipt posts a receipt at
    the destination, and anything between the two is only visible through the
    transfer itself. Discrepancies at receipt are not blocked; they raise a
    variance that the goods-in supervisor clears manually.
*/
CREATE TABLE [Warehouse].[StockTransfers] (
    [StockTransferID]       INT             IDENTITY (1, 1) NOT NULL,
    [TransferReference]     NVARCHAR (20)   NOT NULL,
    [FromWarehouseSiteID]   INT             NOT NULL,
    [ToWarehouseSiteID]     INT             NOT NULL,
    [TransferType]          NVARCHAR (12)   NOT NULL,
    [RequestedDate]         DATE            NOT NULL,
    [DespatchedWhen]        DATETIME2 (7)   NULL,
    [ReceivedWhen]          DATETIME2 (7)   NULL,
    [ExpectedTransitDays]   SMALLINT        NULL,
    [CarrierCode]           NVARCHAR (12)   NULL,
    [TransferStatus]        NVARCHAR (12)   CONSTRAINT [DF_Warehouse_StockTransfers_TransferStatus] DEFAULT (N'REQUESTED') NOT NULL,
    [IsCrossBorder]         BIT             CONSTRAINT [DF_Warehouse_StockTransfers_IsCrossBorder] DEFAULT (0) NOT NULL,
    [CustomsDeclarationRef] NVARCHAR (30)   NULL,
    [VarianceNote]          NVARCHAR (400)  NULL,
    [RequestedByPersonID]   INT             NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_StockTransfers_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_StockTransfers] PRIMARY KEY CLUSTERED ([StockTransferID] ASC),
    CONSTRAINT [UQ_Warehouse_StockTransfers_Reference] UNIQUE ([TransferReference]),
    CONSTRAINT [CK_Warehouse_StockTransfers_Sites] CHECK ([FromWarehouseSiteID] <> [ToWarehouseSiteID]),
    CONSTRAINT [CK_Warehouse_StockTransfers_Type] CHECK ([TransferType] IN (N'REPLENISH', N'REBALANCE', N'RETURNSCONS', N'EMERGENCY')),
    CONSTRAINT [CK_Warehouse_StockTransfers_Status] CHECK ([TransferStatus] IN (N'REQUESTED', N'PICKING', N'INTRANSIT', N'RECEIVED', N'CLOSED', N'CANCELLED')),
    CONSTRAINT [CK_Warehouse_StockTransfers_CrossBorder] CHECK ([IsCrossBorder] = 0 OR [CustomsDeclarationRef] IS NOT NULL OR [TransferStatus] IN (N'REQUESTED', N'CANCELLED')),
    CONSTRAINT [FK_Warehouse_StockTransfers_FromSite] FOREIGN KEY ([FromWarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_StockTransfers_ToSite] FOREIGN KEY ([ToWarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_StockTransfers_RequestedBy] FOREIGN KEY ([RequestedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Warehouse_StockTransfers_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_StockTransfers_InTransit]
    ON [Warehouse].[StockTransfers] ([ToWarehouseSiteID] ASC, [DespatchedWhen] ASC)
    INCLUDE ([TransferReference], [FromWarehouseSiteID])
    WHERE [TransferStatus] = N'INTRANSIT';
GO
