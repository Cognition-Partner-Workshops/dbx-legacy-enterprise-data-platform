/*
    Returns.vw_CreditNoteExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5110 - after 5100
    Depends on    : Returns.CreditNotes, Returns.CreditNoteLines,
                    Sales.PaymentAllocations
    Called by     : SSIS credit note extract, finance interface

    Credit notes with their line roll-up and the amount actually applied.
    Numbering is per legal series, so CreditNoteNumber is only unique within
    NumberSeriesCode; consumers that keyed on the number alone have been
    corrected twice and are still doing it in one case.
*/
CREATE VIEW [Returns].[vw_CreditNoteExtract]
AS
SELECT
    cn.[CreditNoteID],
    cn.[CreditNoteNumber],
    cn.[NumberSeriesCode],
    cn.[NumberWithinSeries],
    cn.[CustomerID],
    cn.[ReturnAuthorizationID],
    cn.[OriginalInvoiceID],
    cn.[RegionCode],
    cn.[CreditReasonCode],
    cn.[IssuedDate],
    cn.[TaxPointDate],
    cn.[TaxRegimeCode],
    cn.[CustomerTaxNumber],
    cn.[CurrencyCode],
    cn.[ExchangeRateToUsd],
    cn.[NetAmount],
    cn.[TaxAmount],
    cn.[RestockingFeeAmount],
    cn.[TotalAmount],
    cn.[AppliedAmount]                                              AS [StoredAppliedAmount],
    ISNULL(alloc.[LedgerAppliedAmount], 0)                          AS [LedgerAppliedAmount],
    cn.[TotalAmount] - ISNULL(alloc.[LedgerAppliedAmount], 0)       AS [UnappliedAmount],
    cn.[CreditNoteStatus],
    cn.[IsRefundToCard],
    cn.[PostedToLedgerWhen],
    ISNULL(lines.[LineCount], 0)                                    AS [LineCount],
    lines.[GoodsLineNetAmount],
    lines.[FreightLineNetAmount],
    cn.[LastEditedWhen]                                             AS [ChangedWhen]
FROM [Returns].[CreditNotes] AS cn
    OUTER APPLY
    (
        SELECT
            COUNT(*)                                                                                AS [LineCount],
            SUM(CASE WHEN l.[CreditLineType] = N'GOODS' THEN l.[LineNetAmount] ELSE 0 END)          AS [GoodsLineNetAmount],
            SUM(CASE WHEN l.[CreditLineType] = N'FREIGHT' THEN l.[LineNetAmount] ELSE 0 END)        AS [FreightLineNetAmount]
        FROM [Returns].[CreditNoteLines] AS l
        WHERE l.[CreditNoteID] = cn.[CreditNoteID]
    ) AS lines
    OUTER APPLY
    (
        SELECT SUM(pa.[AllocatedAmount]) AS [LedgerAppliedAmount]
        FROM [Sales].[PaymentAllocations] AS pa
        WHERE pa.[CreditNoteID] = cn.[CreditNoteID]
    ) AS alloc;
GO
