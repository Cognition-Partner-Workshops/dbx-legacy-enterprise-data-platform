/*
    Report.vw_FinanceCloseStatus

    Object        : Report.vw_FinanceCloseStatus
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Finance Close Summary.

    Period close control board. Each legal entity closes on its own fiscal
    calendar (NA calendar year, EU April-March, APAC July-June) so the fiscal
    period number is not comparable across regions; the derived
    [Close Sequence] gives the group reporting team a single ordering.

    The sub-ledger to GL difference is the number the controllers actually
    chase, so it is exposed both signed and as an absolute breach against the
    per-entity tolerance.
*/
IF OBJECT_ID(N'Report.vw_FinanceCloseStatus', N'V') IS NOT NULL
    DROP VIEW Report.vw_FinanceCloseStatus;
GO

CREATE VIEW Report.vw_FinanceCloseStatus
AS
SELECT
    fc.[Fiscal Year]                                    AS [Fiscal Year],
    fc.[Fiscal Period]                                  AS [Fiscal Period],
    fc.[Legal Entity Code]                              AS [Legal Entity],
    fc.[Region Code]                                    AS [Region],
    fc.[Account Group Code]                             AS [Account Group],
    fc.[Ledger Currency Code]                           AS [Ledger Currency],
    fc.[Period End Date]                                AS [Period End],
    (fc.[Fiscal Year] * 100) + fc.[Fiscal Period]       AS [Close Sequence],
    CASE fc.[Region Code]
        WHEN N'NA' THEN N'January-December'
        WHEN N'EU' THEN N'April-March'
        ELSE N'July-June'
    END                                                 AS [Fiscal Calendar],
    fc.[Opening Balance Local]                          AS [Opening Balance (Local)],
    fc.[Period Debits Local]                            AS [Period Debits (Local)],
    fc.[Period Credits Local]                           AS [Period Credits (Local)],
    fc.[Closing Balance Local]                          AS [Closing Balance (Local)],
    fc.[Consolidation Rate]                             AS [Consolidation Rate],
    fc.[Closing Balance Reporting]                      AS [Closing Balance],
    fc.[Sub Ledger Balance Reporting]                   AS [Sub Ledger Balance],
    fc.[Sub Ledger To GL Difference]                    AS [Sub Ledger To GL Difference],
    ABS(ISNULL(fc.[Sub Ledger To GL Difference], 0))    AS [Absolute Difference],
    fc.[Tolerance Amount]                               AS [Tolerance],
    fc.[Within Tolerance Flag]                          AS [Within Tolerance Flag],
    ABS(ISNULL(fc.[Sub Ledger To GL Difference], 0))
        - ISNULL(fc.[Tolerance Amount], 0)              AS [Amount Over Tolerance],
    fc.[Manual Journal Count]                           AS [Manual Journals],
    fc.[Manual Journal Value]                           AS [Manual Journal Value],
    fc.[Late Posting Count]                             AS [Late Postings],
    fc.[Unposted Journal Count]                         AS [Unposted Journals],
    fc.[Ar Balance Reporting]                           AS [AR Balance],
    fc.[Ap Balance Reporting]                           AS [AP Balance],
    fc.[Grni Accrual Reporting]                         AS [GRNI Accrual],
    fc.[Bad Debt Provision Reporting]                   AS [Bad Debt Provision],
    fc.[Close Status Code]                              AS [Close Status],
    fc.[Close Completed Datetime]                       AS [Close Completed],
    fc.[Days To Close]                                  AS [Days To Close],
    AVG(CONVERT(DECIMAL (9, 2), fc.[Days To Close])) OVER
    (
        PARTITION BY fc.[Legal Entity Code], fc.[Account Group Code]
        ORDER BY (fc.[Fiscal Year] * 100) + fc.[Fiscal Period]
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    )                                                   AS [Rolling 12 Period Days To Close],
    LAG(fc.[Closing Balance Reporting], 1) OVER
    (
        PARTITION BY fc.[Legal Entity Code], fc.[Account Group Code]
        ORDER BY (fc.[Fiscal Year] * 100) + fc.[Fiscal Period]
    )                                                   AS [Prior Period Closing Balance],
    fc.[Closing Balance Reporting]
        - LAG(fc.[Closing Balance Reporting], 1) OVER
          (
              PARTITION BY fc.[Legal Entity Code], fc.[Account Group Code]
              ORDER BY (fc.[Fiscal Year] * 100) + fc.[Fiscal Period]
          )                                             AS [Movement On Prior Period],
    CASE WHEN fc.[Close Status Code] = N'CLOSED'
              AND fc.[Within Tolerance Flag] = 1 THEN N'Clean Close'
         WHEN fc.[Close Status Code] = N'CLOSED' THEN N'Closed With Variance'
         WHEN fc.[Unposted Journal Count] > 0 THEN N'Blocked - Unposted Journals'
         WHEN fc.[Within Tolerance Flag] = 0 THEN N'Blocked - Reconciliation'
         ELSE N'In Progress' END                        AS [Close Assessment],
    fc.[Refreshed Datetime]                             AS [Data As Of]
FROM Aggregate.[Finance Close Summary] AS fc;
GO
