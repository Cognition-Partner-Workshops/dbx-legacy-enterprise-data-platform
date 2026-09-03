/*
    Warehouse.ReplenishmentOrders

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1290 - after Warehouse.ReplenishmentRules
    Depends on    : Warehouse.ReplenishmentRules, Warehouse.WarehouseSites,
                    Warehouse.StockItems, Purchasing.PurchaseOrders,
                    Warehouse.StockTransfers
    Called by     : Warehouse.usp_GenerateReplenishmentOrders

    Proposals raised by the nightly replenishment run. A proposal becomes
    either a purchase order (bought in) or a stock transfer (moved from
    another site); exactly one of the two keys is set once it is actioned, and
    neither while it is still a proposal.
*/
CREATE TABLE [Warehouse].[ReplenishmentOrders] (
    [ReplenishmentOrderID]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [ReplenishmentRuleID]   INT             NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [GeneratedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Warehouse_ReplenishmentOrders_GeneratedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [GeneratedByRun]        NVARCHAR (40)   NOT NULL,
    [ProposedQuantity]      DECIMAL (18, 3) NOT NULL,
    [ApprovedQuantity]      DECIMAL (18, 3) NULL,
    [QuantityOnHandAtRun]   DECIMAL (18, 3) NOT NULL,
    [QuantityOnOrderAtRun]  DECIMAL (18, 3) CONSTRAINT [DF_Warehouse_ReplenishmentOrders_QuantityOnOrderAtRun] DEFAULT (0) NOT NULL,
    [RequiredByDate]        DATE            NULL,
    [FulfilmentMethod]      NVARCHAR (12)   NULL,
    [PurchaseOrderID]       INT             NULL,
    [StockTransferID]       INT             NULL,
    [OrderStatus]           NVARCHAR (12)   CONSTRAINT [DF_Warehouse_ReplenishmentOrders_OrderStatus] DEFAULT (N'PROPOSED') NOT NULL,
    [RejectionReason]       NVARCHAR (200)  NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_ReplenishmentOrders_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_ReplenishmentOrders] PRIMARY KEY CLUSTERED ([ReplenishmentOrderID] ASC),
    CONSTRAINT [CK_Warehouse_ReplenishmentOrders_Quantity] CHECK ([ProposedQuantity] > 0),
    CONSTRAINT [CK_Warehouse_ReplenishmentOrders_Status] CHECK ([OrderStatus] IN (N'PROPOSED', N'APPROVED', N'ACTIONED', N'REJECTED', N'EXPIRED')),
    CONSTRAINT [CK_Warehouse_ReplenishmentOrders_Fulfilment] CHECK ([FulfilmentMethod] IS NULL OR [FulfilmentMethod] IN (N'PURCHASE', N'TRANSFER')),
    CONSTRAINT [CK_Warehouse_ReplenishmentOrders_Keys] CHECK ([PurchaseOrderID] IS NULL OR [StockTransferID] IS NULL),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_Rules] FOREIGN KEY ([ReplenishmentRuleID]) REFERENCES [Warehouse].[ReplenishmentRules] ([ReplenishmentRuleID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_PurchaseOrders] FOREIGN KEY ([PurchaseOrderID]) REFERENCES [Purchasing].[PurchaseOrders] ([PurchaseOrderID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_Transfers] FOREIGN KEY ([StockTransferID]) REFERENCES [Warehouse].[StockTransfers] ([StockTransferID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentOrders_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Warehouse_ReplenishmentOrders_Open]
    ON [Warehouse].[ReplenishmentOrders] ([WarehouseSiteID] ASC, [GeneratedWhen] DESC)
    INCLUDE ([StockItemID], [ProposedQuantity])
    WHERE [OrderStatus] IN (N'PROPOSED', N'APPROVED');
GO
