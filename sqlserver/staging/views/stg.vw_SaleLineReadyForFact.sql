/*
    stg.vw_SaleLineReadyForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the FACT_Sale warehouse load

    Two feeds land in the same fact: the OLTP invoice lines and the partner sales
    file. They are unioned here rather than in the package because the partner
    rows need their own defaulting - no salesperson, no profit, a synthetic sale
    key and a match method that tells the warehouse how confident the product
    resolution was.

    Partner rows that never resolved to a stock item are excluded; they sit in
    err.RejectedFileRow until a steward adds the crosswalk entry.
*/

IF OBJECT_ID(N'stg.vw_SaleLineReadyForFact', N'V') IS NOT NULL
    DROP VIEW stg.vw_SaleLineReadyForFact;
GO

CREATE VIEW stg.vw_SaleLineReadyForFact
AS
SELECT
    sl.SaleLineBusinessKey,
    sl.SaleBusinessKey,
    e.CustomerBusinessKey,
    e.BillToCustomerBusinessKey,
    sl.StockItemBusinessKey,
    e.SalespersonBusinessKey,
    e.GeographyBusinessKey,
    sl.PromotionBusinessKey,
    sl.InvoiceDate,
    sl.LineNumber,
    sl.Quantity,
    sl.QuantityBaseUom,
    sl.UnitPriceAmount,
    sl.NetLineAmount,
    sl.TaxAmount,
    sl.GrossLineAmount,
    sl.NetLineAmountUsd,
    sl.LineProfitAmount,
    sl.CommissionAmount,
    sl.TaxRegimeCode,
    sl.TaxRoundingRuleCode,
    sl.TransactionCurrencyCode,
    sl.RegionCode,
    N'OLTP_INVOICE'                     AS SaleOriginCode,
    N'EXACT'                            AS ProductMatchMethodCode,
    e.MarginOutlierFlag,
    sl.BatchId,
    sl.PackageExecutionId
FROM stg.SaleLine AS sl
INNER JOIN work.SaleLineEnriched AS e
    ON  e.SaleLineBusinessKey = sl.SaleLineBusinessKey
    AND e.BatchId             = sl.BatchId
WHERE e.IsReadyForFact = 1
  AND sl.DqStatusCode IN (N'PASS', N'WARN')

UNION ALL

SELECT
    ps.PartnerSaleBusinessKey           AS SaleLineBusinessKey,
    N'PARTNER|' + ps.PartnerCode + N'|' + ISNULL(ps.ReportingPeriodCode, N'000000') AS SaleBusinessKey,
    NULL                                AS CustomerBusinessKey,
    NULL                                AS BillToCustomerBusinessKey,
    ps.StockItemBusinessKey,
    NULL                                AS SalespersonBusinessKey,
    NULL                                AS GeographyBusinessKey,
    NULL                                AS PromotionBusinessKey,
    ps.TransactionDate                  AS InvoiceDate,
    NULL                                AS LineNumber,
    ps.QuantitySold                     AS Quantity,
    ps.QuantityBaseUom,
    CASE WHEN ISNULL(ps.QuantitySold, 0) <> 0 THEN ps.NetAmount / ps.QuantitySold END AS UnitPriceAmount,
    ps.NetAmount                        AS NetLineAmount,
    ps.TaxAmount,
    ps.GrossAmount                      AS GrossLineAmount,
    ps.NetAmountUsd                     AS NetLineAmountUsd,
    NULL                                AS LineProfitAmount,
    NULL                                AS CommissionAmount,
    CASE ps.RegionCode WHEN N'NA' THEN N'SALESTAX' WHEN N'EU' THEN N'VAT' ELSE N'GST' END AS TaxRegimeCode,
    N'INVOICE'                          AS TaxRoundingRuleCode,
    ps.TransactionCurrencyCode,
    ps.RegionCode,
    N'PARTNER_FILE'                     AS SaleOriginCode,
    ps.ProductMatchMethodCode,
    CONVERT(BIT, 0)                     AS MarginOutlierFlag,
    ps.BatchId,
    ps.PackageExecutionId
FROM stg.PartnerSale AS ps
WHERE ps.StockItemBusinessKey IS NOT NULL
  AND ps.DqStatusCode IN (N'PASS', N'WARN');
GO
