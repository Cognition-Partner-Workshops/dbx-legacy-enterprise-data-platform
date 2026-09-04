/*
    Report.vw_ApAgingCurrent

    Object        : Report.vw_ApAgingCurrent
    Deploy target : WideWorldImportersDW
    Reads         : Fact.Monthly AP Aging, Dimension.Supplier,
                    Dimension.Payment Terms.

    Latest frozen AP aging snapshot. Only frozen snapshots are exposed: an
    unfrozen month is still being adjusted by the close and treasury were
    repeatedly caught quoting numbers that moved under them.

    The bucket definitions themselves live in the fact ([Aging Bucket Code]);
    this view only orders them and adds the discount opportunity split that
    the working-capital pack needs.
*/
IF OBJECT_ID(N'Report.vw_ApAgingCurrent', N'V') IS NOT NULL
    DROP VIEW Report.vw_ApAgingCurrent;
GO

CREATE VIEW Report.vw_ApAgingCurrent
AS
SELECT
    ap.[Month End Date Key]                             AS [Month End],
    ap.[Fiscal Year]                                    AS [Fiscal Year],
    ap.[Fiscal Period]                                  AS [Fiscal Period],
    ap.[Region Code]                                    AS [Region],
    ap.[Supplier Key]                                   AS [Supplier Key],
    s.[Supplier]                                        AS [Supplier],
    s.[Category]                                        AS [Supplier Category],
    ap.[Aging Bucket Code]                              AS [Aging Bucket],
    ap.[Aging Bucket Sort Order]                        AS [Bucket Order],
    ap.[Open Invoice Count]                             AS [Open Invoices],
    ap.[Balance Transaction Currency]                   AS [Balance (Transaction Currency)],
    ap.[Balance Reporting]                              AS [Balance],
    ap.[Not Yet Due Reporting]                          AS [Not Yet Due],
    ap.[Balance Reporting] - ap.[Not Yet Due Reporting] AS [Overdue Balance],
    ap.[Blocked For Payment Reporting]                  AS [Blocked For Payment],
    ap.[Discount Still Capturable]                      AS [Discount Still Capturable],
    ap.[Discount Lost To Date]                          AS [Discount Lost],
    ap.[Grni Accrual Reporting]                         AS [GRNI Accrual],
    ap.[Recoverable Tax Reporting]                      AS [Recoverable Tax],
    ap.[Days Payable Outstanding]                       AS [Days Payable Outstanding],
    ap.[Average Days Beyond Terms]                      AS [Average Days Beyond Terms],
    ap.[Match Exception Count]                          AS [Match Exceptions],
    ap.[Supplier Risk Rating Code]                      AS [Supplier Risk Rating],
    SUM(ap.[Balance Reporting]) OVER
    (
        PARTITION BY ap.[Region Code], ap.[Supplier Key]
    )                                                   AS [Supplier Total Balance],
    ROUND(100.0 * ap.[Balance Reporting]
          / NULLIF(SUM(ap.[Balance Reporting]) OVER
                   (PARTITION BY ap.[Region Code]), 0), 2)
                                                        AS [% Of Regional AP],
    CASE WHEN ap.[Average Days Beyond Terms] > 30 THEN N'Chronic Late Payer'
         WHEN ap.[Average Days Beyond Terms] > 5 THEN N'Slipping'
         WHEN ap.[Average Days Beyond Terms] < -5 THEN N'Paying Early'
         ELSE N'On Terms' END                           AS [Payment Behaviour]
FROM Fact.[Monthly AP Aging] AS ap
INNER JOIN
(
    SELECT [Region Code], MAX([Month End Date Key]) AS LatestMonthEnd
    FROM Fact.[Monthly AP Aging]
    WHERE [Snapshot Frozen Flag] = 1
    GROUP BY [Region Code]
) AS latest
    ON latest.[Region Code] = ap.[Region Code]
   AND latest.LatestMonthEnd = ap.[Month End Date Key]
LEFT JOIN Dimension.[Supplier] AS s
    ON s.[Supplier Key] = ap.[Supplier Key]
WHERE ap.[Snapshot Frozen Flag] = 1;
GO
