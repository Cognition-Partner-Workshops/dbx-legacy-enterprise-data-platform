/*
    Shipping.vw_DeliveryPerformance

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5090 - after 5080
    Depends on    : Shipping.ShipmentHeaders, Shipping.ShipmentEvents,
                    Shipping.DeliveryStops, Shipping.Carriers
    Called by     : carrier review reporting

    Carrier performance by month. Event times arrive in carrier local time
    with an offset that some feeds do not send, so the UTC column is null for
    those and the transit calculation silently falls back to the local column,
    which is why APAC transit times look an hour short in half the year.
*/
CREATE VIEW [Shipping].[vw_DeliveryPerformance]
AS
SELECT
    car.[CarrierID],
    car.[CarrierCode],
    car.[CarrierName],
    car.[RegionCode],
    sh.[ServiceLevelCode],
    CONVERT(CHAR (7), sh.[DespatchedWhen], 126)                     AS [DespatchMonth],
    COUNT(*)                                                        AS [ShipmentCount],
    SUM(CASE WHEN sh.[DeliveredWhen] IS NOT NULL THEN 1 ELSE 0 END) AS [DeliveredCount],
    SUM(CASE WHEN sh.[DeliveredWhen] IS NOT NULL
                  AND sh.[PromisedDeliveryWhen] IS NOT NULL
                  AND sh.[DeliveredWhen] <= sh.[PromisedDeliveryWhen]
             THEN 1 ELSE 0 END)                                     AS [OnTimeCount],
    SUM(CASE WHEN sh.[ExceptionCode] IS NOT NULL THEN 1 ELSE 0 END) AS [ExceptionCount],
    SUM(CASE WHEN stop.[FailedStopCount] > 0 THEN 1 ELSE 0 END)     AS [FailedDeliveryCount],
    AVG(CASE WHEN sh.[DeliveredWhen] IS NULL OR sh.[DespatchedWhen] IS NULL THEN NULL
             ELSE DATEDIFF(HOUR, sh.[DespatchedWhen], sh.[DeliveredWhen]) END)  AS [AverageTransitHours],
    AVG(ev.[EventCount])                                            AS [AverageTrackingEventCount],
    car.[OnTimeTargetPercent]
FROM [Shipping].[ShipmentHeaders] AS sh
    INNER JOIN [Shipping].[Carriers] AS car
        ON car.[CarrierID] = sh.[CarrierID]
    OUTER APPLY
    (
        SELECT COUNT(*) AS [EventCount]
        FROM [Shipping].[ShipmentEvents] AS e
        WHERE e.[ShipmentID] = sh.[ShipmentID]
    ) AS ev
    OUTER APPLY
    (
        SELECT COUNT(*) AS [FailedStopCount]
        FROM [Shipping].[DeliveryStops] AS s
        WHERE s.[ShipmentID] = sh.[ShipmentID]
            AND s.[StopStatus] = N'FAILED'
    ) AS stop
WHERE sh.[DespatchedWhen] IS NOT NULL
GROUP BY
    car.[CarrierID],
    car.[CarrierCode],
    car.[CarrierName],
    car.[RegionCode],
    sh.[ServiceLevelCode],
    CONVERT(CHAR (7), sh.[DespatchedWhen], 126),
    car.[OnTimeTargetPercent];
GO
