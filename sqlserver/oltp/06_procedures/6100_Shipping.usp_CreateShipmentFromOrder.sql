/*
    Shipping.usp_CreateShipmentFromOrder

    Catalog entry : sqlserver_oltp.procedures - Shipping.CreateShipmentFromOrder
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6100 - after 6090
    Depends on    : Shipping.ShipmentHeaders, Shipping.ShipmentLines,
                    Sales.Orders, Sales.OrderLines, Sales.OrderAllocations,
                    Sales.OrderHolds, Shipping.Carriers
    Called by     : despatch screen, wave release job

    Creates one shipment from whatever is allocated and unpicked on an order.
    Split shipments are numbered with SplitSequence; the caller decides
    whether this is the final one, because nothing in the data can tell the
    difference between a short pick and a deliberate split.

    Despatch is blocked by any open hold flagged IsBlockingDespatch. Credit
    holds set on the customer rather than the order are not checked here -
    that check lives in the picking screen and is skipped by the wave job.
*/
CREATE PROCEDURE [Shipping].[usp_CreateShipmentFromOrder]
    @OrderID            INT,
    @WarehouseSiteID    INT,
    @CarrierID          INT,
    @ServiceLevelCode   NVARCHAR (10),
    @WaveReference      NVARCHAR (30) = NULL,
    @IsFinalShipment    BIT = 0,
    @CreatedByPersonID  INT,
    @BatchID            BIGINT = NULL,
    @ShipmentID         INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CustomerID     INT;
    DECLARE @SplitSequence  SMALLINT;
    DECLARE @BlockingHolds  INT;
    DECLARE @Reference      NVARCHAR (30);

    SELECT @CustomerID = o.[CustomerID]
    FROM [Sales].[Orders] AS o
    WHERE o.[OrderID] = @OrderID;

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR (N'Order %d does not exist.', 16, 1, @OrderID);
        RETURN;
    END

    SELECT @BlockingHolds = COUNT(*)
    FROM [Sales].[OrderHolds] AS h
    WHERE h.[OrderID] = @OrderID
        AND h.[ReleasedWhen] IS NULL
        AND h.[IsBlockingDespatch] = 1;

    IF @BlockingHolds > 0
    BEGIN
        RAISERROR (N'Order %d carries a hold that blocks despatch.', 16, 1, @OrderID);
        RETURN;
    END

    IF NOT EXISTS (SELECT 1
                   FROM [Sales].[OrderAllocations] AS a
                       INNER JOIN [Sales].[OrderLines] AS ol
                           ON ol.[OrderLineID] = a.[OrderLineID]
                   WHERE ol.[OrderID] = @OrderID
                       AND a.[WarehouseSiteID] = @WarehouseSiteID
                       AND a.[AllocationStatus] = N'OPEN'
                       AND a.[QuantityOutstanding] > 0)
    BEGIN
        RAISERROR (N'Order %d has nothing allocated and outstanding at this site.', 16, 1, @OrderID);
        RETURN;
    END

    SELECT @SplitSequence = ISNULL(MAX(sh.[SplitSequence]), 0) + 1
    FROM [Shipping].[ShipmentHeaders] AS sh
    WHERE sh.[OrderID] = @OrderID;

    SET @Reference = N'SHP' + RIGHT(N'00000000' + CONVERT(NVARCHAR (12), @OrderID), 8)
                   + N'-' + CONVERT(NVARCHAR (4), @SplitSequence);

    SELECT @ShipmentID = NEXT VALUE FOR [Sequences].[ShipmentID];

    BEGIN TRANSACTION;

    INSERT INTO [Shipping].[ShipmentHeaders]
    (
        [ShipmentID], [ShipmentReference], [OrderID], [CustomerID], [WarehouseSiteID], [CarrierID],
        [ServiceLevelCode], [WaveReference], [SplitSequence], [IsFinalShipment],
        [PlannedDespatchDate], [PickStartedWhen], [ShipmentStatus],
        [IncotermCode], [DeliveryInstructions], [LastEditedBy]
    )
    SELECT
        @ShipmentID,
        @Reference,
        @OrderID,
        @CustomerID,
        @WarehouseSiteID,
        @CarrierID,
        @ServiceLevelCode,
        @WaveReference,
        @SplitSequence,
        @IsFinalShipment,
        CONVERT(DATE, SYSDATETIME()),
        SYSDATETIME(),
        N'PICKING',
        CASE WHEN car.[RegionCode] = N'EU' THEN N'DAP'
             WHEN car.[RegionCode] = N'APAC' THEN N'CIP'
             ELSE N'FOB' END,
        o.[DeliveryInstructions],
        @CreatedByPersonID
    FROM [Sales].[Orders] AS o
        LEFT JOIN [Shipping].[Carriers] AS car
            ON car.[CarrierID] = @CarrierID
    WHERE o.[OrderID] = @OrderID;

    INSERT INTO [Shipping].[ShipmentLines]
    (
        [ShipmentID], [LineNumber], [OrderLineID], [StockItemID], [LotNumber],
        [PickedFromBinID], [QuantityShipped], [PackageNumber], [LineStatus]
    )
    SELECT
        @ShipmentID,
        ROW_NUMBER() OVER (ORDER BY a.[OrderAllocationID] ASC),
        a.[OrderLineID],
        a.[StockItemID],
        a.[LotNumber],
        a.[BinID],
        a.[QuantityOutstanding],
        1,
        N'PLANNED'
    FROM [Sales].[OrderAllocations] AS a
        INNER JOIN [Sales].[OrderLines] AS ol
            ON ol.[OrderLineID] = a.[OrderLineID]
    WHERE ol.[OrderID] = @OrderID
        AND a.[WarehouseSiteID] = @WarehouseSiteID
        AND a.[AllocationStatus] = N'OPEN'
        AND a.[QuantityOutstanding] > 0;

    UPDATE [Sales].[Orders]
    SET [OrderStatusCode] = CASE WHEN @IsFinalShipment = 1 THEN N'SHIPPED' ELSE N'PARTSHIP' END,
        [LastEditedBy] = @CreatedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [OrderID] = @OrderID;

    COMMIT TRANSACTION;
END
GO
