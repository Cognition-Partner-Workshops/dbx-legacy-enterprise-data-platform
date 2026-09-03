/*
    Sales.usp_RefreshSalesQuotaAttainment

    Catalog entry : sqlserver_oltp.procedures - Sales.RefreshSalesQuotaAttainment
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6050 - after 6040
    Depends on    : Sales.SalesQuotas, Sales.Invoices, Sales.SalesTerritories,
                    Returns.CreditNotes
    Called by     : nightly sales reporting refresh

    Writes attainment back onto the quota row. Regional divergence is in the
    basis, not in the arithmetic:
      * NA counts invoiced value in the invoice currency, credits deducted;
      * EU counts invoiced value converted to the territory reporting
        currency at the rate stored on the invoice, credits deducted;
      * APAC counts invoiced value gross of credits, because credit notes are
        raised centrally and were never wired into the APAC quota feed.
*/
CREATE PROCEDURE [Sales].[usp_RefreshSalesQuotaAttainment]
    @FiscalPeriodLabel  NVARCHAR (20) = NULL,
    @RegionCode         NCHAR (4) = NULL,
    @RunByPersonID      INT,
    @BatchID            BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE q
    SET
        q.[AttainmentAmount] = ISNULL(inv.[InvoicedAmount], 0)
                             - CASE WHEN ter.[RegionCode] = N'APAC' THEN 0 ELSE ISNULL(cr.[CreditedAmount], 0) END,
        q.[AttainmentRefreshedWhen] = SYSDATETIME(),
        q.[LastEditedBy] = @RunByPersonID,
        q.[LastEditedWhen] = SYSDATETIME()
    FROM [Sales].[SalesQuotas] AS q
        INNER JOIN [Sales].[SalesTerritories] AS ter
            ON ter.[SalesTerritoryID] = q.[SalesTerritoryID]
        OUTER APPLY
        (
            SELECT SUM(CASE WHEN ter.[RegionCode] = N'EU'
                            THEN i.[InvoiceTotalExTax] * ISNULL(i.[ExchangeRateToUsd], 1)
                            ELSE i.[InvoiceTotalExTax] END) AS [InvoicedAmount]
            FROM [Sales].[Invoices] AS i
            WHERE i.[SalespersonPersonID] = q.[SalespersonPersonID]
                AND i.[SalesTerritoryID] = q.[SalesTerritoryID]
                AND i.[InvoiceDate] BETWEEN q.[PeriodStartDate] AND q.[PeriodEndDate]
        ) AS inv
        OUTER APPLY
        (
            SELECT SUM(cn.[TotalAmount]) AS [CreditedAmount]
            FROM [Returns].[CreditNotes] AS cn
                INNER JOIN [Sales].[Invoices] AS oi
                    ON oi.[InvoiceID] = cn.[OriginalInvoiceID]
            WHERE oi.[SalespersonPersonID] = q.[SalespersonPersonID]
                AND cn.[IssuedDate] BETWEEN q.[PeriodStartDate] AND q.[PeriodEndDate]
                AND cn.[CreditNoteStatus] <> N'CANCELLED'
        ) AS cr
    WHERE (@FiscalPeriodLabel IS NULL OR q.[FiscalPeriodLabel] = @FiscalPeriodLabel)
        AND (@RegionCode IS NULL OR ter.[RegionCode] = @RegionCode)
        AND q.[QuotaStatus] <> N'LOCKED';

    -- Quotas that have run past their period end and are fully attained are
    -- closed here; anything short is left open for the manual review that
    -- sales operations run at quarter end.
    UPDATE [Sales].[SalesQuotas]
    SET [QuotaStatus] = N'LOCKED',
        [LastEditedBy] = @RunByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [PeriodEndDate] < CONVERT(DATE, SYSDATETIME())
        AND [QuotaStatus] = N'OPEN'
        AND [AttainmentAmount] >= [QuotaAmount]
        AND (@FiscalPeriodLabel IS NULL OR [FiscalPeriodLabel] = @FiscalPeriodLabel);
END
GO
