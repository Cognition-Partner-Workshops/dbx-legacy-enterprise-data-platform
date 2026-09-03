/*
    stg.vw_SupplierReadyForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the DIM_Supplier warehouse load

    Suppliers on hold are still published - the warehouse needs them to explain
    historical spend - but the on-hold flag and reason travel with the row. The
    three regional tax attributes are folded into one conformed pair
    (TaxIdentifierTypeCode / TaxIdentifier) plus the regime-specific flags, so the
    dimension does not need to know which ERP instance the row came from.
*/

IF OBJECT_ID(N'stg.vw_SupplierReadyForDimension', N'V') IS NOT NULL
    DROP VIEW stg.vw_SupplierReadyForDimension;
GO

CREATE VIEW stg.vw_SupplierReadyForDimension
AS
SELECT
    s.SupplierBusinessKey,
    s.SourceSystemCode,
    s.SourceSupplierId,
    s.ErpSupplierNumber,
    s.SupplierName,
    ISNULL(s.SupplierNameStandardized, s.SupplierName)  AS SupplierNameConformed,
    s.SupplierCategoryCode,
    s.SupplierStatusCode,
    s.DunsNumber,
    s.TaxIdentifier,
    s.TaxIdentifierTypeCode,
    s.WithholdingCode,
    CASE WHEN s.RegionCode = N'NA'   THEN CONVERT(BIT, CASE WHEN s.WithholdingCode IS NOT NULL THEN 1 ELSE 0 END) END AS Is1099ReportableFlag,
    CASE WHEN s.RegionCode = N'EU'   THEN s.VatRecoveryEligibleFlag END AS VatRecoveryEligibleFlag,
    CASE WHEN s.RegionCode = N'APAC' THEN s.GstRegisteredFlag END       AS GstRegisteredFlag,
    s.PaymentTermsCode,
    t.NetDays                                           AS PaymentTermsNetDays,
    t.DiscountPercent                                   AS PaymentTermsDiscountPercent,
    s.PaymentMethodCode,
    s.TransactionCurrencyCode,
    s.DefaultIncotermCode,
    s.LeadTimeDays,
    s.MinimumOrderAmountUsd,
    s.ScorecardRatingCode,
    s.DiversityClassCode,
    s.RegionCode,
    s.LedgerCode,
    s.OnHoldFlag,
    s.HoldReasonCode,
    v.OpenContractCount,
    v.CommittedAmountUsd                                AS OpenContractCommittedUsd,
    s.RowHash,
    s.ChangeHash,
    s.BatchId,
    s.PackageExecutionId,
    s.LoadedAtUtc
FROM stg.Supplier AS s
LEFT JOIN stg.PaymentTerms AS t
    ON  t.PaymentTermsCode = s.PaymentTermsCode
    AND t.BatchId          = s.BatchId
LEFT JOIN
(
    SELECT
        SupplierBusinessKey,
        BatchId,
        COUNT_BIG(*)                AS OpenContractCount,
        SUM(CommittedAmountUsd)     AS CommittedAmountUsd
    FROM stg.VendorContract
    WHERE ContractStatusCode = N'ACTIVE'
    GROUP BY SupplierBusinessKey, BatchId
) AS v
    ON  v.SupplierBusinessKey = s.SupplierBusinessKey
    AND v.BatchId             = s.BatchId
WHERE s.IsSurvivorRow = 1
  AND s.DqStatusCode IN (N'PASS', N'WARN');
GO
