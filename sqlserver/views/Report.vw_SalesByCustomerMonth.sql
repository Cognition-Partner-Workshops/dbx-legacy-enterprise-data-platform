/*
    Report.vw_SalesByCustomerMonth

    Object        : Report.vw_SalesByCustomerMonth
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Monthly Sales Summary, Dimension.Customer.

    Customer x fiscal period revenue with the comparisons the account managers
    ask for. Fiscal period labels are built per region because the three
    calendars do not share a period numbering: NA reports P1-P12 on the
    calendar year, EU P1-P12 from April, APAC P1-P13 from July.

    Closed periods are served straight from the aggregate; the open period is
    flagged so the pack can say "period to date" rather than pretending it is
    a full month.
*/
IF OBJECT_ID(N'Report.vw_SalesByCustomerMonth', N'V') IS NOT NULL
    DROP VIEW Report.vw_SalesByCustomerMonth;
GO

CREATE VIEW Report.vw_SalesByCustomerMonth
AS
SELECT
    m.[Customer Key]                                             AS [Customer Key],
    c.[Customer]                                                AS [Customer Name],
    c.[Customer Category]                                       AS [Customer Category],
    m.[Region Code]                                              AS [Region],
    m.[Fiscal Calendar Code]                                      AS [Fiscal Calendar],
    m.[Fiscal Year]                                              AS [Fiscal Year],
    m.[Fiscal Period]                                            AS [Fiscal Period],
    CASE m.[Region Code]
        WHEN N'NA'   THEN N'FY' + CONVERT(NVARCHAR(4), m.[Fiscal Year])
                          + N' P' + RIGHT(N'0' + CONVERT(NVARCHAR(2), m.[Fiscal Period]), 2)
        WHEN N'EU'   THEN N'FY' + CONVERT(NVARCHAR(4), m.[Fiscal Year])
                          + N'/' + RIGHT(CONVERT(NVARCHAR(4), m.[Fiscal Year] + 1), 2)
                          + N' P' + RIGHT(N'0' + CONVERT(NVARCHAR(2), m.[Fiscal Period]), 2)
        ELSE N'FY' + CONVERT(NVARCHAR(4), m.[Fiscal Year])
             + N' P' + RIGHT(N'0' + CONVERT(NVARCHAR(2), m.[Fiscal Period]), 2) + N' (13P)'
    END                                                          AS [Fiscal Period Label],
    m.[Calendar Month]                                            AS [Calendar Month],
    m.[Order Count]                                               AS [Orders],
    m.[Invoice Count]                                             AS [Invoices],
    m.[Quantity Sold Base UOM]                                      AS [Units],
    m.[Gross Revenue]                                             AS [Gross Revenue],
    m.[Discount Given]                                            AS [Discount Given],
    m.[Net Revenue]                                               AS [Net Revenue (Local)],
    m.[Net Revenue Reporting]                                      AS [Net Revenue],
    m.[Credit Notes Reporting]                                     AS [Credit Notes],
    m.[Returns Reporting]                                         AS [Returns],
    m.[Net Revenue After Credits]                                   AS [Net Revenue After Credits],
    m.[Gross Margin Reporting]                                     AS [Gross Margin],
    CASE WHEN ISNULL(m.[Net Revenue Reporting], 0) = 0 THEN NULL
         ELSE ROUND(100.0 * m.[Gross Margin Reporting]
                    / m.[Net Revenue Reporting], 2) END             AS [Margin %],
    m.[Average Order Value]                                        AS [Average Order Value],
    m.[Prior Period Net Revenue]                                    AS [Prior Period Net Revenue],
    m.[Period Over Period Percent]                                  AS [Period On Period %],
    m.[Prior Year Net Revenue]                                      AS [Prior Year Net Revenue],
    m.[Year Over Year Percent]                                      AS [Year On Year %],
    m.[Rolling 3 Period Net Revenue]                                 AS [Rolling 3 Period Revenue],
    SUM(m.[Net Revenue Reporting]) OVER
    (
        PARTITION BY m.[Customer Key], m.[Fiscal Year]
        ORDER BY m.[Fiscal Period]
        ROWS UNBOUNDED PRECEDING
    )                                                            AS [Year To Date Revenue],
    CASE WHEN m.[Period Closed Flag] = 1 THEN N'Closed' ELSE N'Period To Date' END
                                                                 AS [Period Status],
    m.[Refreshed Datetime]                                        AS [Data As Of]
FROM Aggregate.[Monthly Sales Summary] AS m
LEFT JOIN Dimension.[Customer] AS c
    ON c.[Customer Key] = m.[Customer Key];
GO
