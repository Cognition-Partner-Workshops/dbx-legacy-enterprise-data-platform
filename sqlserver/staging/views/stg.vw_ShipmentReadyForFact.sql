/*
    stg.vw_ShipmentReadyForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the FACT_Shipment warehouse load

    Shipment header attributes at the shipment-line grain, with the carrier scan
    events folded in from the flat-file feed. The carrier files arrive a day
    behind the shipment itself, so the delivery columns are frequently NULL on
    the first pass; the fact load re-reads this view for the trailing seven days
    on every run, which is why nothing here filters on the current batch alone.

    The scan feed is joined on the shipment reference rather than the tracking
    number: three of the five carriers recycle tracking numbers within a year and
    the 2016 duplicate-delivery incident came from exactly that join.

    Weight is always published in kilograms. NA depots send pounds in the scan
    file, which stg.usp_AppendIncremental_Shipment converts before this point.
*/

IF OBJECT_ID(N'stg.vw_ShipmentReadyForFact', N'V') IS NOT NULL
    DROP VIEW stg.vw_ShipmentReadyForFact;
GO

CREATE VIEW stg.vw_ShipmentReadyForFact
AS
SELECT
    sl.ShipmentLineBusinessKey,
    s.ShipmentBusinessKey,
    s.ShipmentReference,
    s.SaleBusinessKey,
    s.CustomerBusinessKey,
    s.ShipToGeographyBusinessKey,
    s.ShipToCountryCode,
    s.ShipToPostalCodeStandardized,
    sl.SaleLineBusinessKey,
    sl.StockItemBusinessKey,
    s.CarrierCode,
    s.ServiceLevelCode,
    s.DeliveryRouteCode,
    s.ShipFromWarehouseCode,
    s.ShippedDate,
    s.ShippedDateTimeUtc,
    s.PromisedDeliveryUtc,
    COALESCE(s.DeliveredDateTimeUtc, scan.DeliveredAtUtc)   AS DeliveredDateTimeUtc,
    scan.ScanEventCount,
    scan.LastScanEventCode,
    scan.ExceptionScanCount,
    CASE
        WHEN COALESCE(s.DeliveredDateTimeUtc, scan.DeliveredAtUtc) IS NULL THEN NULL
        ELSE DATEDIFF(HOUR, s.PromisedDeliveryUtc, COALESCE(s.DeliveredDateTimeUtc, scan.DeliveredAtUtc))
    END                                                     AS DeliveryVarianceHours,
    CASE
        WHEN COALESCE(s.DeliveredDateTimeUtc, scan.DeliveredAtUtc) IS NULL THEN N'IN_TRANSIT'
        WHEN COALESCE(s.DeliveredDateTimeUtc, scan.DeliveredAtUtc) <= s.PromisedDeliveryUtc THEN N'ON_TIME'
        ELSE N'LATE'
    END                                                     AS DeliveryPerformanceCode,
    s.OnTimeDeliveryFlag,
    s.DeliveryLatencyHours,
    sl.PackageTypeCode,
    sl.ShippedQuantity,
    sl.WeightKg                                             AS ShippedWeightKg,
    sl.SerialNumberCount,
    sl.TemperatureAtLoadC,
    sl.ColdChainBreachFlag,
    s.TotalWeightKg,
    s.TotalVolumeM3,
    s.FreightChargeAmount,
    s.FreightCurrencyCode,
    s.FreightChargeAmountUsd,
    s.CustomsRequiredFlag,
    s.CustomsDeclarationRef,
    s.ShipmentStatusCode,
    sl.LineStatusCode,
    s.RegionCode,
    sl.BatchId,
    sl.PackageExecutionId
FROM stg.ShipmentLine AS sl
INNER JOIN stg.Shipment AS s
    ON  s.ShipmentBusinessKey = sl.ShipmentBusinessKey
    AND s.BatchId             = sl.BatchId
OUTER APPLY
(
    SELECT
        MAX(CASE WHEN r.ScanEventCode IN (N'DL', N'POD', N'DEL')
                 THEN TRY_CONVERT(DATETIME2(3), r.ScanTimestampLocal, 120) END)         AS DeliveredAtUtc,
        COUNT_BIG(*)                                                                    AS ScanEventCount,
        MAX(r.ScanEventCode)                                                            AS LastScanEventCode,
        SUM(CASE WHEN NULLIF(r.ExceptionReasonCode, N'') IS NOT NULL THEN 1 ELSE 0 END) AS ExceptionScanCount
    FROM raw.FileCarrierScan AS r
    WHERE r.ShipmentReference = s.ShipmentReference
      AND r.CarrierCode       = s.CarrierCode
) AS scan
WHERE sl.DqStatusCode IN (N'PASS', N'WARN')
  AND ISNULL(s.ShipmentStatusCode, N'OPEN') <> N'CANCELLED';
GO
