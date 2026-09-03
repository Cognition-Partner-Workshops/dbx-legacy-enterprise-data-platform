/*
    Shipping.vw_ShipmentExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5080 - after 5070
    Depends on    : Shipping.ShipmentHeaders, Shipping.ShipmentLines,
                    Shipping.Carriers, Shipping.CustomsDeclarations,
                    Warehouse.WarehouseSites
    Called by     : SSIS shipment extract

    Shipment headers with their line roll-up and customs status. Split
    shipments carry SplitSequence and IsFinalShipment; downstream treats the
    final shipment as the one that closes the order, which is wrong whenever
    the last split was cancelled after being flagged final.
*/
CREATE VIEW [Shipping].[vw_ShipmentExtract]
AS
SELECT
    sh.[ShipmentID],
    sh.[ShipmentReference],
    sh.[OrderID],
    sh.[InvoiceID],
    sh.[CustomerID],
    sh.[WarehouseSiteID],
    site.[SiteCode],
    site.[RegionCode],
    sh.[CarrierID],
    car.[CarrierCode],
    car.[ScacCode],
    sh.[ServiceLevelCode],
    sh.[WaveReference],
    sh.[SplitSequence],
    sh.[IsFinalShipment],
    sh.[PlannedDespatchDate],
    sh.[PickStartedWhen],
    sh.[PackCompletedWhen],
    sh.[DespatchedWhen],
    sh.[PromisedDeliveryWhen],
    sh.[DeliveredWhen],
    sh.[TrackingNumber],
    sh.[TotalPackages],
    sh.[TotalGrossWeightKg],
    sh.[ChargeableWeightKg],
    sh.[FreightChargeAmount],
    sh.[FreightCurrencyCode],
    sh.[IncotermCode],
    sh.[ShipmentStatus],
    sh.[ExceptionCode],
    ISNULL(lines.[LineCount], 0)                                    AS [LineCount],
    ISNULL(lines.[TotalQuantityShipped], 0)                         AS [TotalQuantityShipped],
    lines.[DistinctStockItemCount],
    cus.[DeclarationRegime],
    cus.[ClearanceStatus],
    cus.[DutyAmount],
    CASE WHEN sh.[DeliveredWhen] IS NULL OR sh.[PromisedDeliveryWhen] IS NULL THEN NULL
         WHEN sh.[DeliveredWhen] <= sh.[PromisedDeliveryWhen] THEN 1
         ELSE 0 END                                                 AS [IsOnTime],
    sh.[LastEditedWhen]                                             AS [ChangedWhen]
FROM [Shipping].[ShipmentHeaders] AS sh
    INNER JOIN [Warehouse].[WarehouseSites] AS site
        ON site.[WarehouseSiteID] = sh.[WarehouseSiteID]
    LEFT JOIN [Shipping].[Carriers] AS car
        ON car.[CarrierID] = sh.[CarrierID]
    LEFT JOIN [Shipping].[CustomsDeclarations] AS cus
        ON cus.[ShipmentID] = sh.[ShipmentID]
    OUTER APPLY
    (
        SELECT
            COUNT(*)                            AS [LineCount],
            SUM(sl.[QuantityShipped])           AS [TotalQuantityShipped],
            COUNT(DISTINCT sl.[StockItemID])    AS [DistinctStockItemCount]
        FROM [Shipping].[ShipmentLines] AS sl
        WHERE sl.[ShipmentID] = sh.[ShipmentID]
            AND sl.[LineStatus] <> N'CANCELLED'
    ) AS lines;
GO
