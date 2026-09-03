/*
    Shipping.ShipmentLines

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1350 - after Shipping.ShipmentHeaders
    Depends on    : Shipping.ShipmentHeaders, Shipping.PackagingTypes,
                    Sales.OrderLines, Warehouse.StockItems, Warehouse.Bins
    Called by     : Shipping.usp_CreateShipmentFromOrder, Shipping.vw_ShipmentExtract

    One row per item per package. A package with several items repeats the
    PackageNumber; the package weight is therefore carried on the first line of
    the package only and is null on the rest, which every downstream sum has to
    know about.
*/
CREATE TABLE [Shipping].[ShipmentLines] (
    [ShipmentLineID]        BIGINT          CONSTRAINT [DF_Shipping_ShipmentLines_ShipmentLineID] DEFAULT (NEXT VALUE FOR [Sequences].[ShipmentLineID]) NOT NULL,
    [ShipmentID]            INT             NOT NULL,
    [LineNumber]            SMALLINT        NOT NULL,
    [OrderLineID]           INT             NULL,
    [StockItemID]           INT             NOT NULL,
    [LotNumber]             NVARCHAR (30)   NULL,
    [PickedFromBinID]       INT             NULL,
    [QuantityShipped]       DECIMAL (18, 3) NOT NULL,
    [PackageNumber]         SMALLINT        CONSTRAINT [DF_Shipping_ShipmentLines_PackageNumber] DEFAULT (1) NOT NULL,
    [PackagingTypeID]       INT             NULL,
    [PackageGrossWeightKg]  DECIMAL (12, 3) NULL,
    [SerialNumberList]      NVARCHAR (MAX)  NULL,
    [PickedByPersonID]      INT             NULL,
    [PickedWhen]            DATETIME2 (7)   NULL,
    [LineStatus]            NVARCHAR (12)   CONSTRAINT [DF_Shipping_ShipmentLines_LineStatus] DEFAULT (N'ALLOCATED') NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_ShipmentLines_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_ShipmentLines] PRIMARY KEY CLUSTERED ([ShipmentLineID] ASC),
    CONSTRAINT [UQ_Shipping_ShipmentLines_LineNumber] UNIQUE ([ShipmentID], [LineNumber]),
    CONSTRAINT [CK_Shipping_ShipmentLines_Quantity] CHECK ([QuantityShipped] > 0),
    CONSTRAINT [CK_Shipping_ShipmentLines_Status] CHECK ([LineStatus] IN (N'ALLOCATED', N'PICKED', N'PACKED', N'SHORT', N'CANCELLED')),
    CONSTRAINT [FK_Shipping_ShipmentLines_Headers] FOREIGN KEY ([ShipmentID]) REFERENCES [Shipping].[ShipmentHeaders] ([ShipmentID]),
    CONSTRAINT [FK_Shipping_ShipmentLines_OrderLines] FOREIGN KEY ([OrderLineID]) REFERENCES [Sales].[OrderLines] ([OrderLineID]),
    CONSTRAINT [FK_Shipping_ShipmentLines_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Shipping_ShipmentLines_Bins] FOREIGN KEY ([PickedFromBinID]) REFERENCES [Warehouse].[Bins] ([BinID]),
    CONSTRAINT [FK_Shipping_ShipmentLines_PackagingTypes] FOREIGN KEY ([PackagingTypeID]) REFERENCES [Shipping].[PackagingTypes] ([PackagingTypeID]),
    CONSTRAINT [FK_Shipping_ShipmentLines_PickedBy] FOREIGN KEY ([PickedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentLines_OrderLine]
    ON [Shipping].[ShipmentLines] ([OrderLineID] ASC)
    INCLUDE ([ShipmentID], [QuantityShipped], [LineStatus])
    WHERE [OrderLineID] IS NOT NULL;
GO
