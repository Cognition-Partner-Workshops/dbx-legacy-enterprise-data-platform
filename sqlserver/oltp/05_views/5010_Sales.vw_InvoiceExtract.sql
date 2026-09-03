/*
    Sales.vw_InvoiceExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5010 - after 5000
    Depends on    : Sales.Invoices, Sales.Customers, Sales.SalesTerritories,
                    Sales.PaymentAllocations, Sales.CustomerDisputes
    Called by     : SSIS extract package for invoices, aged debt reporting

    Invoice header extract with settlement recomputed from the allocation
    ledger alongside the stored AmountOutstanding, so downstream can see the
    two figures disagree rather than silently picking one.
*/
CREATE VIEW [Sales].[vw_InvoiceExtract]
AS
SELECT
    i.[InvoiceID],
    i.[CustomerID],
    c.[RegionCode],
    i.[SalesTerritoryID],
    ter.[TerritoryCode],
    i.[InvoiceDate],
    i.[TaxPointDate],
    i.[PaymentDueDate],
    i.[CurrencyCode],
    i.[ExchangeRateToUsd],
    i.[TaxRegimeCode],
    i.[CustomerTaxNumber],
    i.[InvoiceTotalExTax],
    i.[InvoiceTaxAmount],
    i.[InvoiceTotalExTax] + ISNULL(i.[InvoiceTaxAmount], 0) AS [InvoiceTotalIncTax],
    i.[AmountOutstanding]                   AS [StoredAmountOutstanding],
    i.[InvoiceTotalExTax] + ISNULL(i.[InvoiceTaxAmount], 0) - ISNULL(alloc.[AllocatedAmount], 0)
                                            AS [LedgerAmountOutstanding],
    CASE WHEN ABS(ISNULL(i.[AmountOutstanding], 0)
                  - (i.[InvoiceTotalExTax] + ISNULL(i.[InvoiceTaxAmount], 0) - ISNULL(alloc.[AllocatedAmount], 0))) > 0.01
         THEN 1 ELSE 0 END                  AS [IsSettlementMismatch],
    i.[SettlementStatus],
    i.[DisputeFlag],
    disp.[OpenDisputeCount],
    disp.[OpenDisputedAmount],
    i.[LoyaltyMemberID],
    i.[LoyaltyPointsAccrued],
    i.[LastEditedWhen]                      AS [ChangedWhen]
FROM [Sales].[Invoices] AS i
    INNER JOIN [Sales].[Customers] AS c
        ON c.[CustomerID] = i.[CustomerID]
    LEFT JOIN [Sales].[SalesTerritories] AS ter
        ON ter.[SalesTerritoryID] = i.[SalesTerritoryID]
    OUTER APPLY
    (
        SELECT SUM(pa.[AllocatedAmount] + pa.[SettlementDiscount]) AS [AllocatedAmount]
        FROM [Sales].[PaymentAllocations] AS pa
        WHERE pa.[InvoiceID] = i.[InvoiceID]
    ) AS alloc
    OUTER APPLY
    (
        SELECT
            COUNT(*)                        AS [OpenDisputeCount],
            SUM(d.[DisputedAmount])         AS [OpenDisputedAmount]
        FROM [Sales].[CustomerDisputes] AS d
        WHERE d.[InvoiceID] = i.[InvoiceID]
            AND d.[DisputeStatus] IN (N'OPEN', N'INVESTIGATING', N'AWAITINGCUST', N'ESCALATED')
    ) AS disp;
GO
