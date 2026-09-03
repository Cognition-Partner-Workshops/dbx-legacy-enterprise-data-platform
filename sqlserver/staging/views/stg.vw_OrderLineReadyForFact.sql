/*
    stg.vw_OrderLineReadyForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the FACT_OrderLine warehouse load

    Publishes only the enriched lines that resolved every dimension lookup. Lines
    with a lookup failure stay behind in work.OrderLineEnriched and are picked up
    by work.usp_QueueLateArrivingDimensions, so the fact load never has to invent
    a member itself.
*/

IF OBJECT_ID(N'stg.vw_OrderLineReadyForFact', N'V') IS NOT NULL
    DROP VIEW stg.vw_OrderLineReadyForFact;
GO

CREATE VIEW stg.vw_OrderLineReadyForFact
AS
SELECT
    e.OrderLineBusinessKey,
    e.OrderBusinessKey,
    e.CustomerBusinessKey,
    e.StockItemBusinessKey,
    e.SalespersonBusinessKey,
    e.PromotionBusinessKey,
    e.GeographyBusinessKey,
    o.SalesTerritoryCode,
    o.SalesChannelCode,
    e.OrderDate,
    o.ExpectedDeliveryDate,
    o.CustomerPurchaseOrderNumber,
    o.IsUndersupplyBackordered,
    l.LineNumber,
    l.PackageTypeCode,
    l.OrderedQuantity,
    l.PickedQuantity,
    l.OutstandingQuantity,
    l.UnitPriceAmount,
    l.LineDiscountAmount,
    l.NetLineAmount,
    e.NetLineAmountUsd,
    e.TaxRegimeCode,
    l.TaxRatePercent,
    e.TaxAmount,
    l.GrossLineAmount,
    l.TransactionCurrencyCode,
    l.LineStatusCode,
    e.RegionCode,
    l.RowHash,
    e.BatchId,
    e.PackageExecutionId
FROM work.OrderLineEnriched AS e
INNER JOIN stg.OrderLine AS l
    ON  l.OrderLineBusinessKey = e.OrderLineBusinessKey
    AND l.BatchId              = e.BatchId
INNER JOIN stg.[Order] AS o
    ON  o.OrderBusinessKey = e.OrderBusinessKey
    AND o.BatchId          = e.BatchId
WHERE e.IsReadyForFact = 1
  AND e.LookupFailureCount = 0
  AND l.DqStatusCode IN (N'PASS', N'WARN');
GO
