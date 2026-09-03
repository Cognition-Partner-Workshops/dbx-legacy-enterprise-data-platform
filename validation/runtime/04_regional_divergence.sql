/*
    Runtime validation 04 - regional divergence.

    Run against  : WideWorldImportersDW
    Reads        : Fact.Sale, Dimension.Customer, Dimension.Currency,
                   Dimension.Fiscal Calendar, Aggregate.*
    Writes       : nothing

    Never run. Written from the column definitions in
    sqlserver/warehouse/facts/Fact.Sale.Extensions.sql and the regional
    reference data under oracle/reference.

    The three regions are supposed to behave differently - NA books sales and
    use tax, EU books VAT with a reverse-charge path, APAC books GST inclusive
    of the line price. The purpose of these queries is to show that the
    difference is real in the data and not just in the column list, and to
    catch the classic failure where a region's rows quietly fall through the
    default branch of a CASE and get NA treatment.
*/
SET NOCOUNT ON;
GO

/* 1. Tax regime by region. Every NA row should carry a sales-tax regime, every
      EU row a VAT regime, every APAC row a GST regime. Cross-tabulated rows
      that break that pattern are the finding. */
SELECT
    f.[Region Code],
    f.[Tax Regime Code],
    COUNT_BIG(*)                                                        AS SaleRows,
    SUM(CASE WHEN f.[VAT Rate] IS NOT NULL THEN 1 ELSE 0 END)           AS WithVatRate,
    SUM(CASE WHEN f.[GST Rate] IS NOT NULL THEN 1 ELSE 0 END)           AS WithGstRate,
    SUM(CASE WHEN f.[VAT Reverse Charge Flag] = 1 THEN 1 ELSE 0 END)    AS ReverseCharge,
    SUM(CASE WHEN f.[GST Free Flag] = 1 THEN 1 ELSE 0 END)              AS GstFree
FROM Fact.Sale AS f
WHERE f.[Invoice Date Key] >= DATEADD(MONTH, -3, CAST(SYSUTCDATETIME() AS DATE))
GROUP BY f.[Region Code], f.[Tax Regime Code]
ORDER BY f.[Region Code], SaleRows DESC;
GO

/* 2. Rows with no region at all. These take whatever the default branch does,
      which for most of the estate means NA. */
SELECT
    f.[Tax Regime Code],
    COUNT_BIG(*) AS RowsWithoutRegion
FROM Fact.Sale AS f
WHERE f.[Region Code] IS NULL
GROUP BY f.[Tax Regime Code];
GO

/* 3. EU reverse charge without a customer tax registration. A reverse-charge
      line is only valid when the customer's VAT registration was captured;
      without it the invoice is wrong and the VAT return is wrong. */
SELECT TOP (1000)
    f.[Sale Key],
    f.[Customer Key],
    f.[Invoice Date Key],
    f.[Tax Regime Code],
    f.[Customer Tax Registration]
FROM Fact.Sale AS f
WHERE f.[Region Code] = N'EU'
  AND f.[VAT Reverse Charge Flag] = 1
  AND (f.[Customer Tax Registration] IS NULL OR LTRIM(RTRIM(f.[Customer Tax Registration])) = N'')
ORDER BY f.[Invoice Date Key] DESC;
GO

/* 4. APAC GST is inclusive: the line price already contains the tax, and the
      load backs it out. This checks that the arithmetic closes to the cent.
      A systematic one-cent drift here is the truncation the APAC loads do
      instead of rounding, and it is expected; anything larger is not. */
SELECT TOP (1000)
    f.[Sale Key],
    f.[Invoice Date Key],
    f.[Gross Amount],
    f.[Total Excluding Tax],
    f.[Tax Amount],
    f.[GST Rate],
    f.[Gross Amount] - f.[Total Excluding Tax] - f.[Tax Amount] AS ResidualAmount
FROM Fact.Sale AS f
WHERE f.[Region Code] = N'APAC'
  AND f.[Gross Amount] IS NOT NULL
  AND ABS(f.[Gross Amount] - f.[Total Excluding Tax] - f.[Tax Amount]) > 0.01
ORDER BY ABS(f.[Gross Amount] - f.[Total Excluding Tax] - f.[Tax Amount]) DESC;
GO

/* 5. FX conversion coverage. Every non-reporting-currency row needs a rate,
      an effective date and a source; a missing rate means the reporting
      amounts are wrong rather than absent, because the loads default to 1. */
SELECT
    f.[Region Code],
    f.[Transaction Currency Code],
    f.[FX Rate Source Code],
    COUNT_BIG(*)                                                    AS SaleRows,
    SUM(CASE WHEN f.[FX Rate To Reporting] IS NULL THEN 1 ELSE 0 END) AS MissingRate,
    SUM(CASE WHEN f.[FX Rate To Reporting] = 1 THEN 1 ELSE 0 END)     AS UnitRate,
    MIN(f.[FX Rate Effective Date])                                 AS EarliestRateDate,
    MAX(f.[FX Rate Effective Date])                                 AS LatestRateDate
FROM Fact.Sale AS f
WHERE f.[Invoice Date Key] >= DATEADD(MONTH, -3, CAST(SYSUTCDATETIME() AS DATE))
GROUP BY f.[Region Code], f.[Transaction Currency Code], f.[FX Rate Source Code]
ORDER BY MissingRate DESC, SaleRows DESC;
GO

/* 6. Fiscal period alignment. The three regions do not share a fiscal year, so
      the same invoice date lands in different periods depending on region.
      This is correct behaviour and the query exists to demonstrate it, not to
      flag it. */
SELECT
    f.[Region Code],
    f.[Fiscal Year],
    f.[Fiscal Period],
    MIN(f.[Invoice Date Key])   AS FirstInvoiceDate,
    MAX(f.[Invoice Date Key])   AS LastInvoiceDate,
    COUNT_BIG(*)                AS SaleRows
FROM Fact.Sale AS f
WHERE f.[Fiscal Year] IS NOT NULL
GROUP BY f.[Region Code], f.[Fiscal Year], f.[Fiscal Period]
ORDER BY f.[Region Code], f.[Fiscal Year] DESC, f.[Fiscal Period] DESC;
GO

/* 7. Aggregate agreement. The regional aggregate is rebuilt nightly from the
      fact; when it disagrees with a live re-aggregation the rebuild is stale
      or its filter has drifted. */
SELECT
    a.[Region Code],
    a.[Sales Date],
    a.[Total Excluding Tax]     AS AggregateAmount,
    live.LiveAmount,
    a.[Total Excluding Tax] - live.LiveAmount AS DifferenceAmount
FROM Aggregate.[Daily Sales Summary] AS a
OUTER APPLY
    (
        SELECT SUM(f.[Total Excluding Tax]) AS LiveAmount
        FROM Fact.Sale AS f
        WHERE f.[Region Code] = a.[Region Code]
          AND f.[Invoice Date Key] = a.[Sales Date]
    ) AS live
WHERE a.[Sales Date] >= DATEADD(DAY, -7, CAST(SYSUTCDATETIME() AS DATE))
  AND ABS(ISNULL(a.[Total Excluding Tax], 0) - ISNULL(live.LiveAmount, 0)) > 0.01
ORDER BY ABS(ISNULL(a.[Total Excluding Tax], 0) - ISNULL(live.LiveAmount, 0)) DESC;
GO
