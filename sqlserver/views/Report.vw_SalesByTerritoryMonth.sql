/*
    Report.vw_SalesByTerritoryMonth

    Object        : Report.vw_SalesByTerritoryMonth
    Deploy target : WideWorldImportersDW
    Reads         : Aggregate.Regional Sales Performance, Dimension.Sales
                    Territory, Dimension.Sales Channel.

    Territory x channel x month against budget, with the three tax regimes
    surfaced side by side. [Indirect Tax Collected] coalesces sales tax, VAT
    output and GST into one column for the group pack, while the individual
    columns stay for the local statutory packs - both are needed and neither
    reconciles to the other because reverse charge sits in EU only.
*/
IF OBJECT_ID(N'Report.vw_SalesByTerritoryMonth', N'V') IS NOT NULL
    DROP VIEW Report.vw_SalesByTerritoryMonth;
GO

CREATE VIEW Report.vw_SalesByTerritoryMonth
AS
SELECT
    r.[Calendar Month]                                   AS [Calendar Month],
    r.[Fiscal Year]                                      AS [Fiscal Year],
    r.[Fiscal Period]                                    AS [Fiscal Period],
    r.[Fiscal Calendar Code]                              AS [Fiscal Calendar],
    r.[Region Code]                                      AS [Region],
    t.[Sales Territory]                                 AS [Territory],
    ch.[Sales Channel]                                  AS [Channel],
    r.[Local Currency Code]                               AS [Local Currency],
    r.[Order Count]                                      AS [Orders],
    r.[Invoice Count]                                    AS [Invoices],
    r.[Active Customer Count]                             AS [Active Customers],
    r.[Active Salesperson Count]                          AS [Active Salespeople],
    r.[Net Sales Local]                                   AS [Net Sales (Local)],
    r.[Net Sales Daily Rate]                               AS [Net Sales],
    r.[Net Sales Monthly Average Rate]                      AS [Net Sales At Average Rate],
    r.[Translation Difference]                           AS [Translation Difference],
    r.[Gross Margin Reporting]                            AS [Gross Margin],
    r.[Margin Percent]                                   AS [Margin %],
    r.[Sales Tax Collected]                               AS [Sales Tax Collected],
    r.[Vat Output Amount]                                 AS [VAT Output],
    r.[Vat Reverse Charge Amount]                          AS [VAT Reverse Charge],
    r.[Gst Collected]                                    AS [GST Collected],
    r.[Gst Free Sales]                                    AS [GST Free Sales],
    r.[Sales Tax Collected] + r.[Vat Output Amount]
        + r.[Gst Collected]                              AS [Indirect Tax Collected],
    r.[Budget Net Sales Reporting]                         AS [Budget],
    r.[Budget Variance Reporting]                         AS [Budget Variance],
    r.[Budget Attainment Percent]                         AS [Budget Attainment %],
    CASE WHEN r.[Budget Attainment Percent] IS NULL THEN N'No Budget'
         WHEN r.[Budget Attainment Percent] >= 100 THEN N'On Or Above'
         WHEN r.[Budget Attainment Percent] >= 90 THEN N'Within 10%'
         ELSE N'Below' END                              AS [Budget Status],
    r.[Prior Year Net Sales]                               AS [Prior Year Net Sales],
    r.[Year Over Year Percent]                             AS [Year On Year %],
    r.[Year To Date Net Sales]                              AS [Year To Date Net Sales],
    r.[Rank In Region By Sales]                             AS [Rank In Region],
    r.[Refreshed Datetime]                               AS [Data As Of]
FROM Aggregate.[Regional Sales Performance] AS r
LEFT JOIN Dimension.[Sales Territory] AS t
    ON t.[Sales Territory Key] = r.[Sales Territory Key]
LEFT JOIN Dimension.[Sales Channel] AS ch
    ON ch.[Sales Channel Key] = r.[Sales Channel Key];
GO
