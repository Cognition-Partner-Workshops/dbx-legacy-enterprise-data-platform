/*
    Runtime validation 03 - dimension and fact integrity.

    Run against  : WideWorldImportersDW
    Reads        : Dimension.*, Fact.*, Integration.*
    Writes       : nothing

    Never run. Written from the table definitions under
    sqlserver/warehouse/dimensions and sqlserver/warehouse/facts.

    The warehouse has no foreign keys between facts and dimensions - it never
    has - so nothing in the database prevents an orphan key. These queries are
    the substitute, and they are the reason the unknown-member rows exist.
*/
SET NOCOUNT ON;
GO

/* 1. Type 2 chain integrity for the SCD2 dimensions. Exactly one current row
      per durable business key; no overlapping validity windows. Dimension.Customer
      carries both the older [Valid From]/[Valid To] pair and the later
      [Is Current Row]/[Version Number] columns, and the two have to agree. */
SELECT
    c.[WWI Customer ID],
    COUNT(*)                                                        AS TotalVersions,
    SUM(CASE WHEN c.[Is Current Row] = 1 THEN 1 ELSE 0 END)         AS CurrentRowFlagCount,
    SUM(CASE WHEN c.[Valid To] = CAST(N'9999-12-31' AS DATETIME2(7)) THEN 1 ELSE 0 END) AS OpenValidToCount
FROM Dimension.Customer AS c
GROUP BY c.[WWI Customer ID]
HAVING SUM(CASE WHEN c.[Is Current Row] = 1 THEN 1 ELSE 0 END) <> 1
    OR SUM(CASE WHEN c.[Valid To] = CAST(N'9999-12-31' AS DATETIME2(7)) THEN 1 ELSE 0 END) <> 1
ORDER BY TotalVersions DESC;
GO

/* 2. Overlapping Type 2 windows on the same business key. */
SELECT
    a.[WWI Customer ID],
    a.[Customer Key]    AS EarlierKey,
    a.[Valid From]      AS EarlierFrom,
    a.[Valid To]        AS EarlierTo,
    b.[Customer Key]    AS LaterKey,
    b.[Valid From]      AS LaterFrom,
    b.[Valid To]        AS LaterTo
FROM Dimension.Customer AS a
INNER JOIN Dimension.Customer AS b
    ON b.[WWI Customer ID] = a.[WWI Customer ID]
   AND b.[Customer Key] <> a.[Customer Key]
   AND b.[Valid From] < a.[Valid To]
   AND b.[Valid To] > a.[Valid From]
ORDER BY a.[WWI Customer ID], a.[Valid From];
GO

/* 3. Facts pointing at the unknown member. A handful is normal - late-arriving
      dimensions are queued and rekeyed later by DIM_Rekey_LateArriving - but a
      growing count means the rekey is not running or not finding its rows. */
SELECT
    N'Fact.Sale'    AS FactTable,
    N'Customer Key' AS DimensionKey,
    COUNT_BIG(*)    AS UnknownMemberRows
FROM Fact.Sale AS f
WHERE f.[Customer Key] = -1
UNION ALL
SELECT N'Fact.Sale', N'Stock Item Key', COUNT_BIG(*)
FROM Fact.Sale AS f
WHERE f.[Stock Item Key] = -1
UNION ALL
SELECT N'Fact.Purchase', N'Supplier Key', COUNT_BIG(*)
FROM Fact.Purchase AS f
WHERE f.[Supplier Key] = -1
UNION ALL
SELECT N'Fact.GL Posting', N'Cost Center Key', COUNT_BIG(*)
FROM Fact.[GL Posting] AS f
WHERE f.[Cost Center Key] = -1;
GO

/* 4. Orphan surrogate keys: a fact row whose dimension key matches no row in
      the dimension at all. Distinct from the unknown member, and always a
      defect. */
SELECT TOP (1000)
    f.[Sale Key],
    f.[Customer Key],
    f.[Invoice Date Key]
FROM Fact.Sale AS f
WHERE NOT EXISTS
      (
          SELECT 1
          FROM Dimension.Customer AS c
          WHERE c.[Customer Key] = f.[Customer Key]
      )
ORDER BY f.[Sale Key] DESC;
GO

/* 5. Rows parked in the fact load hold table. These are facts the load could
      not place; they are invisible to every report until someone clears them. */
SELECT
    h.[Target Fact Name],
    h.[Missing Dimension Name],
    h.[Hold Reason Code],
    h.[Hold Status Code],
    COUNT(*)                            AS HeldRows,
    MIN(h.[First Held Datetime])        AS OldestHeldOn,
    MAX(h.[Retry Count])                AS MaxRetries
FROM Fact.[Fact Load Hold] AS h
WHERE h.[Released Datetime] IS NULL
GROUP BY h.[Target Fact Name], h.[Missing Dimension Name], h.[Hold Reason Code], h.[Hold Status Code]
ORDER BY HeldRows DESC;
GO

/* 6. Bridge allocation factors that do not sum to one. The load is supposed to
      reject the whole customer when this happens; a row here means it did not. */
SELECT
    b.[WWI Customer ID],
    b.[Membership From],
    SUM(b.[Allocation Factor]) AS TotalAllocationFactor
FROM Dimension.[Customer Buying Group Bridge] AS b
WHERE b.[Is Current Membership] = 1
GROUP BY b.[WWI Customer ID], b.[Membership From]
HAVING ABS(SUM(b.[Allocation Factor]) - 1.0) > 0.0001
ORDER BY TotalAllocationFactor;
GO

/* 7. Date dimension coverage. Every date key referenced by a fact must exist,
      and the calendar must extend far enough forward for the forward-dated
      rows the procurement facts carry. */
SELECT
    MIN(d.[Date])   AS FirstDate,
    MAX(d.[Date])   AS LastDate,
    COUNT(*)        AS DateRows
FROM Dimension.Date AS d;
GO
