/*
    Warehouse.ufn_AvailableToPromise

    Catalog entry : sqlserver_oltp.functions - Warehouse.ufn_AvailableToPromise
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4030 - after 4020
    Depends on    : Warehouse.BinContents, Sales.OrderAllocations,
                    Warehouse.StockTransferLines, Warehouse.StockTransfers
    Called by     : order entry, Sales.usp_ConvertQuoteToOrder,
                    Warehouse.usp_GenerateReplenishmentOrders

    Inline table-valued function returning the promisable quantity for an item
    at a site. Expired soft allocations are excluded here rather than being
    cleaned up, in-transit transfer stock is counted as available at the
    destination, and quarantined bins are excluded.
*/
CREATE FUNCTION [Warehouse].[ufn_AvailableToPromise]
(
    @StockItemID        INT,
    @WarehouseSiteID    INT,
    @AsAtWhen           DATETIME2 (7)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        @StockItemID            AS [StockItemID],
        @WarehouseSiteID        AS [WarehouseSiteID],
        ISNULL(onhand.[QuantityOnHand], 0)      AS [QuantityOnHand],
        ISNULL(alloc.[QuantityAllocated], 0)    AS [QuantityAllocated],
        ISNULL(intransit.[QuantityInTransit], 0) AS [QuantityInTransit],
        ISNULL(onhand.[QuantityOnHand], 0)
            - ISNULL(alloc.[QuantityAllocated], 0)
            + ISNULL(intransit.[QuantityInTransit], 0)          AS [QuantityAvailableToPromise]
    FROM
    (
        SELECT SUM(bc.[QuantityOnHand]) AS [QuantityOnHand]
        FROM [Warehouse].[BinContents] AS bc
            INNER JOIN [Warehouse].[Bins] AS b
                ON b.[BinID] = bc.[BinID]
        WHERE bc.[StockItemID] = @StockItemID
            AND b.[WarehouseSiteID] = @WarehouseSiteID
            AND b.[BinStatus] = N'AVAILABLE'
    ) AS onhand
    CROSS JOIN
    (
        SELECT SUM(oa.[QuantityOutstanding]) AS [QuantityAllocated]
        FROM [Sales].[OrderAllocations] AS oa
        WHERE oa.[StockItemID] = @StockItemID
            AND oa.[WarehouseSiteID] = @WarehouseSiteID
            AND oa.[AllocationStatus] = N'OPEN'
            AND (oa.[AllocationType] = N'HARD'
                 OR oa.[ExpiresWhen] IS NULL
                 OR oa.[ExpiresWhen] > @AsAtWhen)
    ) AS alloc
    CROSS JOIN
    (
        SELECT SUM(stl.[QuantityDespatched]) AS [QuantityInTransit]
        FROM [Warehouse].[StockTransferLines] AS stl
            INNER JOIN [Warehouse].[StockTransfers] AS st
                ON st.[StockTransferID] = stl.[StockTransferID]
        WHERE stl.[StockItemID] = @StockItemID
            AND st.[ToWarehouseSiteID] = @WarehouseSiteID
            AND st.[TransferStatus] = N'INTRANSIT'
    ) AS intransit
);
GO
