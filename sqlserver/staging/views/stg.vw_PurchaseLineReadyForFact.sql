/*
    stg.vw_PurchaseLineReadyForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the FACT_Purchase warehouse load

    Grain is the purchase order line, with the receipt and invoice quantities
    rolled up onto it so the warehouse can measure receipt latency and three-way
    match variance without a second pass. Recoverable tax is carried separately
    because EU input VAT does not belong in spend, while NA sales tax does.
*/

IF OBJECT_ID(N'stg.vw_PurchaseLineReadyForFact', N'V') IS NOT NULL
    DROP VIEW stg.vw_PurchaseLineReadyForFact;
GO

CREATE VIEW stg.vw_PurchaseLineReadyForFact
AS
SELECT
    pl.PurchaseOrderLineBusinessKey,
    pl.PurchaseOrderBusinessKey,
    po.PurchaseOrderNumber,
    po.RevisionNumber,
    e.SupplierBusinessKey,
    e.ProductBusinessKey,
    e.CostCenterCode,
    e.ContractBusinessKey,
    po.BuyerEmployeeKey,
    po.OrderDate,
    pl.NeedByDate,
    po.PromisedDate,
    r.FirstReceiptDate,
    r.ReceivedQuantityTotal,
    r.MaxDaysLateVsPromised,
    pl.LineNumber,
    pl.OrderQuantity,
    pl.OrderQuantityBaseUom,
    pl.UnitPriceAmount,
    pl.ExtendedAmount,
    pl.ExtendedAmountUsd,
    pl.TaxAmount,
    pl.RecoverableTaxAmount,
    CASE
        WHEN po.RegionCode = N'EU' THEN pl.ExtendedAmountUsd
        ELSE pl.ExtendedAmountUsd + ISNULL(pl.TaxAmount, 0) - ISNULL(pl.RecoverableTaxAmount, 0)
    END                                     AS NetSpendAmountUsd,
    pl.BilledQuantity,
    pl.OpenQuantity,
    e.ThreeWayMatchStatusCode,
    e.PriceVariancePercent,
    e.ContractComplianceFlag,
    pl.LineStatusCode,
    po.IncotermCode,
    po.LedgerCode,
    po.RegionCode,
    po.FiscalPeriodLabel,
    pl.BatchId,
    pl.PackageExecutionId
FROM stg.PurchaseOrderLine AS pl
INNER JOIN stg.PurchaseOrder AS po
    ON  po.PurchaseOrderBusinessKey = pl.PurchaseOrderBusinessKey
    AND po.BatchId                  = pl.BatchId
INNER JOIN work.PurchaseLineEnriched AS e
    ON  e.PurchaseOrderLineBusinessKey = pl.PurchaseOrderLineBusinessKey
    AND e.BatchId                      = pl.BatchId
LEFT JOIN
(
    SELECT
        PurchaseOrderLineBusinessKey,
        BatchId,
        MIN(ReceiptDate)            AS FirstReceiptDate,
        SUM(ReceivedQuantityBaseUom) AS ReceivedQuantityTotal,
        MAX(DaysLateVsPromised)     AS MaxDaysLateVsPromised
    FROM stg.Receipt
    GROUP BY PurchaseOrderLineBusinessKey, BatchId
) AS r
    ON  r.PurchaseOrderLineBusinessKey = pl.PurchaseOrderLineBusinessKey
    AND r.BatchId                      = pl.BatchId
WHERE e.IsReadyForFact = 1
  AND pl.DqStatusCode IN (N'PASS', N'WARN')
  AND po.IsCancelled = 0;
GO
