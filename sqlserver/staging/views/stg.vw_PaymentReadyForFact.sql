/*
    stg.vw_PaymentReadyForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the FACT_Payment warehouse load

    A payment can settle several invoices, so the fact grain is the matched
    payment/invoice pair produced by work.usp_MatchPaymentsToInvoices. Unmatched
    payments are published too, with a NULL invoice key and MatchStatusCode of
    'UNMATCHED', because treasury reconciles on total cash out and a dropped
    payment shows up as a variance the next morning.

    Settlement discount and FX gain/loss are computed here so both the matched
    and unmatched branches use identical arithmetic.
*/

IF OBJECT_ID(N'stg.vw_PaymentReadyForFact', N'V') IS NOT NULL
    DROP VIEW stg.vw_PaymentReadyForFact;
GO

CREATE VIEW stg.vw_PaymentReadyForFact
AS
SELECT
    p.PaymentBusinessKey,
    m.ApInvoiceBusinessKey,
    p.SupplierBusinessKey,
    p.PaymentDate,
    p.PaymentMethodCode,
    p.PaymentStatusCode,
    p.BankAccountReference,
    p.TransactionCurrencyCode,
    p.TransactionFxRate,
    p.PaymentAmount,
    p.PaymentAmountUsd,
    m.AppliedAmount,
    m.AppliedAmountUsd,
    m.DiscountTakenAmount,
    m.ResidualAmount,
    m.FxDifferenceUsd                           AS FxGainLossAmountUsd,
    CASE
        WHEN m.PaymentBusinessKey IS NULL           THEN N'UNMATCHED'
        WHEN ISNULL(m.ResidualAmount, 0) = 0        THEN N'MATCHED'
        WHEN ISNULL(m.WithinToleranceFlag, 0) = 1   THEN N'MATCHED_IN_TOLERANCE'
        ELSE N'PART_MATCHED'
    END                                         AS MatchStatusCode,
    m.MatchRuleCode,
    m.MatchPassNumber,
    m.MatchConfidence                           AS MatchConfidencePercent,
    m.UnmatchedReasonCode,
    DATEDIFF(DAY, i.InvoiceDate, p.PaymentDate) AS DaysToPay,
    DATEDIFF(DAY, i.DueDate, p.PaymentDate)     AS DaysPastDue,
    i.WithholdingAmount,
    i.FiscalPeriodLabel,
    p.RealizedFxGainLossUsd,
    p.UnappliedAmount,
    p.LedgerCode,
    p.RegionCode,
    p.RemittanceReference,
    p.BatchId,
    p.PackageExecutionId
FROM stg.Payment AS p
LEFT JOIN work.PaymentMatched AS m
    ON  m.PaymentBusinessKey = p.PaymentBusinessKey
    AND m.BatchId            = p.BatchId
    AND m.IsFinalAllocation  = 1
LEFT JOIN stg.ApInvoice AS i
    ON  i.ApInvoiceBusinessKey = m.ApInvoiceBusinessKey
    AND i.BatchId              = m.BatchId
WHERE p.DqStatusCode IN (N'PASS', N'WARN')
  AND p.VoidDate IS NULL
  AND ISNULL(p.PaymentStatusCode, N'ISSUED') <> N'VOID';
GO
