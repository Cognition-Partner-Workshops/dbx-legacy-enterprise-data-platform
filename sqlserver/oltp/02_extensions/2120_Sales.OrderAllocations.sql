/*
    Sales.OrderAllocations

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 02_extensions / 2120 - after 2110
    Depends on    : Sales.OrderLines, Warehouse.WarehouseSites, Warehouse.Bins,
                    Application.People
    Called by     : allocation run, Shipping.usp_CreateShipmentFromOrder,
                    Warehouse.ufn_AvailableToPromise

    Soft and hard reservations against stock. A soft allocation can be stolen
    by a higher-priority order; a hard allocation cannot. Expiry is enforced by
    the nightly sweep, not by the database, so expired soft allocations are
    routinely still present and available-to-promise has to exclude them by
    date.
*/
CREATE TABLE [Sales].[OrderAllocations] (
    [OrderAllocationID]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [OrderLineID]           INT             NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [BinID]                 INT             NULL,
    [StockItemID]           INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [AllocationType]        NVARCHAR (8)    CONSTRAINT [DF_Sales_OrderAllocations_AllocationType] DEFAULT (N'SOFT') NOT NULL,
    [QuantityAllocated]     DECIMAL (18, 3) NOT NULL,
    [QuantityPicked]        DECIMAL (18, 3) CONSTRAINT [DF_Sales_OrderAllocations_QuantityPicked] DEFAULT (0) NOT NULL,
    [QuantityOutstanding]   AS ([QuantityAllocated] - [QuantityPicked]) PERSISTED,
    [AllocatedWhen]         DATETIME2 (7)   CONSTRAINT [DF_Sales_OrderAllocations_AllocatedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [ExpiresWhen]           DATETIME2 (7)   NULL,
    [PriorityRank]          TINYINT         CONSTRAINT [DF_Sales_OrderAllocations_PriorityRank] DEFAULT (5) NOT NULL,
    [AllocationStatus]      NVARCHAR (12)   CONSTRAINT [DF_Sales_OrderAllocations_AllocationStatus] DEFAULT (N'OPEN') NOT NULL,
    [ReleasedWhen]          DATETIME2 (7)   NULL,
    [AllocatedByPersonID]   INT             NULL,
    CONSTRAINT [PK_Sales_OrderAllocations] PRIMARY KEY CLUSTERED ([OrderAllocationID] ASC),
    CONSTRAINT [CK_Sales_OrderAllocations_Type] CHECK ([AllocationType] IN (N'SOFT', N'HARD')),
    CONSTRAINT [CK_Sales_OrderAllocations_Quantity] CHECK ([QuantityAllocated] > 0 AND [QuantityPicked] >= 0),
    CONSTRAINT [CK_Sales_OrderAllocations_Status] CHECK ([AllocationStatus] IN (N'OPEN', N'PICKED', N'RELEASED', N'EXPIRED', N'CANCELLED')),
    CONSTRAINT [FK_Sales_OrderAllocations_OrderLines] FOREIGN KEY ([OrderLineID]) REFERENCES [Sales].[OrderLines] ([OrderLineID]),
    CONSTRAINT [FK_Sales_OrderAllocations_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Sales_OrderAllocations_Bins] FOREIGN KEY ([BinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Sales_OrderAllocations_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Sales_OrderAllocations_Application_People] FOREIGN KEY ([AllocatedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Sales_OrderAllocations_Item_Open]
    ON [Sales].[OrderAllocations] ([StockItemID] ASC, [WarehouseSiteID] ASC)
    INCLUDE ([QuantityOutstanding], [AllocationType], [ExpiresWhen])
    WHERE [AllocationStatus] = N'OPEN';
GO
