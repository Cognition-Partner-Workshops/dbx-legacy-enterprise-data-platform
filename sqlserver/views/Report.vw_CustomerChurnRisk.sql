/*
    Report.vw_CustomerChurnRisk

    Object        : Report.vw_CustomerChurnRisk
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Customer 360, Aggregate.Customer Rolling 12 Month.

    Churn worklist. The aggregate already scores churn risk; this view applies
    the regional contactability rules on top, because a customer can be at
    risk and still not be contactable:

      NA   - contactable unless the customer opted out of marketing.
      EU   - contactable only with a live consent flag and an unexpired
             retention date; anonymised rows are never contactable.
      APAC - contactable only with explicit consent, and the local rules
             treat an expiring retention date within 30 days as expired.

    The twelve rolling months are pivoted onto the row so the retention team
    can eyeball the decay without opening a second report.
*/
IF OBJECT_ID(N'Report.vw_CustomerChurnRisk', N'V') IS NOT NULL
    DROP VIEW Report.vw_CustomerChurnRisk;
GO

CREATE VIEW Report.vw_CustomerChurnRisk
AS
SELECT
    c.[Customer Key]                                    AS [Customer Key],
    CASE WHEN c.[Anonymised Flag] = 1 THEN N'(anonymised)'
         ELSE c.[Customer Name] END                     AS [Customer],
    c.[Region Code]                                     AS [Region],
    c.[Churn Risk Score]                                AS [Churn Risk Score],
    c.[Churn Risk Band]                                 AS [Churn Risk Band],
    c.[Rfm Score]                                       AS [RFM Score],
    c.[Days Since Last Order]                           AS [Days Since Last Order],
    c.[Tenure Months]                                   AS [Tenure Months],
    c.[Lifetime Net Revenue]                            AS [Lifetime Revenue],
    c.[Average Order Value]                             AS [Average Order Value],
    c.[Loyalty Point Balance]                           AS [Loyalty Points At Risk],
    c.[Overdue Balance Reporting]                       AS [Overdue Balance],
    m0.[Net Revenue Reporting]                          AS [Revenue Current Month],
    m1.[Net Revenue Reporting]                          AS [Revenue Month -1],
    m2.[Net Revenue Reporting]                          AS [Revenue Month -2],
    m3.[Net Revenue Reporting]                          AS [Revenue Month -3],
    m6.[Net Revenue Reporting]                          AS [Revenue Month -6],
    m12.[Net Revenue Reporting]                         AS [Revenue Month -12],
    m0.[Rolling 12 Month Revenue]                       AS [Rolling 12 Month Revenue],
    m0.[Rolling 3 Month Revenue]                        AS [Rolling 3 Month Revenue],
    m0.[Revenue Trend Percent]                          AS [Revenue Trend %],
    m0.[Consecutive Inactive Months]                    AS [Consecutive Inactive Months],
    CASE c.[Region Code]
        WHEN N'NA' THEN CASE WHEN c.[Marketing Consent Flag] = 0 THEN 0 ELSE 1 END
        WHEN N'EU' THEN CASE WHEN c.[Anonymised Flag] = 1 THEN 0
                             WHEN c.[Marketing Consent Flag] <> 1 THEN 0
                             WHEN c.[Retention Expiry Date] < CONVERT(DATE, SYSDATETIME()) THEN 0
                             ELSE 1 END
        ELSE CASE WHEN c.[Marketing Consent Flag] <> 1 THEN 0
                  WHEN c.[Retention Expiry Date]
                       < DATEADD(DAY, 30, CONVERT(DATE, SYSDATETIME())) THEN 0
                  ELSE 1 END
    END                                                 AS [Contactable Flag],
    CASE WHEN c.[Churn Risk Score] >= 80
              AND c.[Lifetime Net Revenue] >= 100000 THEN N'P1 - High Value At Risk'
         WHEN c.[Churn Risk Score] >= 80 THEN N'P2 - At Risk'
         WHEN c.[Churn Risk Score] >= 55 THEN N'P3 - Watch'
         ELSE N'P4 - Monitor Only' END                  AS [Worklist Priority],
    c.[Refreshed Datetime]                              AS [Data As Of]
FROM Aggregate.[Customer 360] AS c
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m0
    ON m0.[Customer Key] = c.[Customer Key] AND m0.[Month Offset] = 0
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m1
    ON m1.[Customer Key] = c.[Customer Key] AND m1.[Month Offset] = 1
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m2
    ON m2.[Customer Key] = c.[Customer Key] AND m2.[Month Offset] = 2
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m3
    ON m3.[Customer Key] = c.[Customer Key] AND m3.[Month Offset] = 3
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m6
    ON m6.[Customer Key] = c.[Customer Key] AND m6.[Month Offset] = 6
LEFT JOIN Aggregate.[Customer Rolling 12 Month] AS m12
    ON m12.[Customer Key] = c.[Customer Key] AND m12.[Month Offset] = 12
WHERE c.[Churn Risk Score] >= 40;
GO
