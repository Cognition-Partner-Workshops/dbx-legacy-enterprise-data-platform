/*
    Shipping.ShipmentHeaders

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1340 - after Shipping.Carriers, Warehouse.WarehouseSites
    Depends on    : Sales.Orders, Sales.Invoices, Sales.Customers, Shipping.Carriers,
                    Shipping.DeliveryRoutes, Warehouse.WarehouseSites, Application.People
    Called by     : Shipping.ShipmentLines, Shipping.ShipmentEvents,
                    Shipping.usp_CreateShipmentFromOrder, Shipping.usp_RateShipment,
                    Shipping.vw_ShipmentExtract

    Despatch header. One order can produce several shipments (split shipment):
    SplitSequence numbers them and IsFinalShipment marks the last one, set by
    the despatch clerk rather than derived, so orders exist with no final
    shipment and orders exist with two.

    The wave is the picking batch the shipment was released in; waves are not
    a separate table because the WMS owns them, only the reference is kept.
*/
CREATE TABLE [Shipping].[ShipmentHeaders] (
    [ShipmentID]            INT             CONSTRAINT [DF_Shipping_ShipmentHeaders_ShipmentID] DEFAULT (NEXT VALUE FOR [Sequences].[ShipmentID]) NOT NULL,
    [ShipmentReference]     NVARCHAR (24)   NOT NULL,
    [OrderID]               INT             NULL,
    [InvoiceID]             INT             NULL,
    [CustomerID]            INT             NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [CarrierID]             INT             NULL,
    [ServiceLevelCode]      NVARCHAR (12)   NULL,
    [DeliveryRouteID]       INT             NULL,
    [WaveReference]         NVARCHAR (20)   NULL,
    [SplitSequence]         SMALLINT        CONSTRAINT [DF_Shipping_ShipmentHeaders_SplitSequence] DEFAULT (1) NOT NULL,
    [IsFinalShipment]       BIT             CONSTRAINT [DF_Shipping_ShipmentHeaders_IsFinalShipment] DEFAULT (1) NOT NULL,
    [PlannedDespatchDate]   DATE            NULL,
    [PickStartedWhen]       DATETIME2 (7)   NULL,
    [PackCompletedWhen]     DATETIME2 (7)   NULL,
    [DespatchedWhen]        DATETIME2 (7)   NULL,
    [PromisedDeliveryWhen]  DATETIME2 (7)   NULL,
    [DeliveredWhen]         DATETIME2 (7)   NULL,
    [TrackingNumber]        NVARCHAR (40)   NULL,
    [TotalPackages]         SMALLINT        CONSTRAINT [DF_Shipping_ShipmentHeaders_TotalPackages] DEFAULT (0) NOT NULL,
    [TotalGrossWeightKg]    DECIMAL (12, 3) CONSTRAINT [DF_Shipping_ShipmentHeaders_TotalGrossWeightKg] DEFAULT (0) NOT NULL,
    [TotalVolumeM3]         DECIMAL (12, 4) NULL,
    [ChargeableWeightKg]    DECIMAL (12, 3) NULL,
    [FreightChargeAmount]   DECIMAL (18, 2) NULL,
    [FreightCurrencyCode]   NCHAR (3)       NULL,
    [FreightRatedWhen]      DATETIME2 (7)   NULL,
    [IncotermCode]          NCHAR (3)       NULL,
    [ShipmentStatus]        NVARCHAR (12)   CONSTRAINT [DF_Shipping_ShipmentHeaders_ShipmentStatus] DEFAULT (N'PLANNED') NOT NULL,
    [ExceptionCode]         NVARCHAR (10)   NULL,
    [DeliveryInstructions]  NVARCHAR (400)  NULL,
    [PackedByPersonID]      INT             NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_ShipmentHeaders_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_ShipmentHeaders] PRIMARY KEY CLUSTERED ([ShipmentID] ASC),
    CONSTRAINT [UQ_Shipping_ShipmentHeaders_Reference] UNIQUE ([ShipmentReference]),
    CONSTRAINT [CK_Shipping_ShipmentHeaders_Status] CHECK ([ShipmentStatus] IN (N'PLANNED', N'PICKING', N'PACKED', N'DESPATCHED', N'INTRANSIT', N'DELIVERED', N'EXCEPTION', N'CANCELLED')),
    CONSTRAINT [CK_Shipping_ShipmentHeaders_Sequence] CHECK ([SplitSequence] > 0),
    CONSTRAINT [CK_Shipping_ShipmentHeaders_Source] CHECK ([OrderID] IS NOT NULL OR [InvoiceID] IS NOT NULL),
    CONSTRAINT [CK_Shipping_ShipmentHeaders_Delivered] CHECK ([ShipmentStatus] <> N'DELIVERED' OR [DeliveredWhen] IS NOT NULL),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Orders] FOREIGN KEY ([OrderID]) REFERENCES [Sales].[Orders] ([OrderID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Invoices] FOREIGN KEY ([InvoiceID]) REFERENCES [Sales].[Invoices] ([InvoiceID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Carriers] FOREIGN KEY ([CarrierID]) REFERENCES [Shipping].[Carriers] ([CarrierID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Routes] FOREIGN KEY ([DeliveryRouteID]) REFERENCES [Shipping].[DeliveryRoutes] ([DeliveryRouteID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_PackedBy] FOREIGN KEY ([PackedByPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Shipping_ShipmentHeaders_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_Order]
    ON [Shipping].[ShipmentHeaders] ([OrderID] ASC, [SplitSequence] ASC)
    INCLUDE ([ShipmentStatus], [DespatchedWhen]);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_OpenByCarrier]
    ON [Shipping].[ShipmentHeaders] ([CarrierID] ASC, [PlannedDespatchDate] ASC)
    INCLUDE ([ShipmentReference], [CustomerID], [TotalGrossWeightKg])
    WHERE [ShipmentStatus] IN (N'PLANNED', N'PICKING', N'PACKED');
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_Changed]
    ON [Shipping].[ShipmentHeaders] ([LastEditedWhen] ASC)
    INCLUDE ([ShipmentStatus], [CustomerID]);
GO
