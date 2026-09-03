/*
    Report.vw_SupplierOnTimeDelivery

    Object        : Report.vw_SupplierOnTimeDelivery
    Deploy target : WideWorldImportersDW
    Reads         : Fact.Purchase Receipt, Dimension.Supplier, Dimension.Date.

    Deliberately not built on the monthly scorecard. Vendor management wanted
    receipt-level detail with a rolling three-month OTIF that does not reset
    at a month boundary, so this view goes back to Fact.Purchase Receipt and
    recomputes it. It is the heaviest view in the reporting layer and should
    have been materialised years ago; it is left as a view because the
    scorecard already covers the monthly numbers and nobody wants two OTIF
    figures that could disagree.
*/
IF OBJECT_ID(N'Report.vw_SupplierOnTimeDelivery', N'V') IS NOT NULL
    DROP VIEW Report.vw_SupplierOnTimeDelivery;
GO

CREATE VIEW Report.vw_SupplierOnTimeDelivery
AS
SELECT
    r.[Receipt Date Key]                                AS [Receipt Date],
    r.[Region Code]                                     AS [Region],
    r.[Supplier Key]                                    AS [Supplier Key],
    s.[Supplier]                                        AS [Supplier],
    r.[Purchase Order Number]                                       AS [PO Number],
    r.[Receipt Number]                                  AS [Receipt Number],
    r.[Quantity Ordered Base UOM]                                AS [Ordered Quantity],
    r.[Quantity Received Base UOM]                               AS [Received Quantity],
    r.[Quantity Rejected Base UOM]                               AS [Rejected Quantity],
    r.[Receipt Value Reporting]                        AS [Received Value],
    r.[Lead Time Days]                                  AS [Lead Time Days],
    r.[Days Late Versus Promise]                                       AS [Days Late],
    r.[On Time Flag]                                    AS [On Time Flag],
    r.[In Full Flag]                                    AS [In Full Flag],
    CASE WHEN r.[On Time Flag] = 1 AND r.[In Full Flag] = 1 THEN 1 ELSE 0 END
                                                        AS [OTIF Flag],
    CASE WHEN r.[Days Late Versus Promise] <= 0 THEN N'On Or Early'
         WHEN r.[Days Late Versus Promise] <= 3 THEN N'1-3 Days Late'
         WHEN r.[Days Late Versus Promise] <= 7 THEN N'4-7 Days Late'
         WHEN r.[Days Late Versus Promise] <= 30 THEN N'8-30 Days Late'
         ELSE N'Over 30 Days Late' END                  AS [Lateness Band],
    roll.ReceiptCount3M                                 AS [Receipts Last 3 Months],
    roll.OtifCount3M                                    AS [OTIF Last 3 Months],
    CASE WHEN ISNULL(roll.ReceiptCount3M, 0) = 0 THEN NULL
         ELSE ROUND(100.0 * roll.OtifCount3M / roll.ReceiptCount3M, 2) END
                                                        AS [Rolling 3 Month OTIF %],
    roll.AverageLeadTime3M                              AS [Rolling 3 Month Lead Time],
    CASE r.[Region Code]
        WHEN N'NA' THEN 95.0
        WHEN N'EU' THEN 97.0
        ELSE 90.0
    END                                                 AS [Regional OTIF Target %],
    CASE WHEN ISNULL(roll.ReceiptCount3M, 0) = 0 THEN NULL
         WHEN 100.0 * roll.OtifCount3M / roll.ReceiptCount3M
              < CASE r.[Region Code] WHEN N'NA' THEN 95.0
                                     WHEN N'EU' THEN 97.0
                                     ELSE 90.0 END
         THEN 1 ELSE 0 END                              AS [Below Target Flag],
    r.[Batch Id]                                        AS [Batch Id]
FROM Fact.[Purchase Receipt] AS r
LEFT JOIN Dimension.[Supplier] AS s
    ON s.[Supplier Key] = r.[Supplier Key]
OUTER APPLY
(
    SELECT COUNT_BIG(*) AS ReceiptCount3M,
           SUM(CASE WHEN h.[On Time Flag] = 1 AND h.[In Full Flag] = 1
                    THEN 1 ELSE 0 END) AS OtifCount3M,
           AVG(CONVERT(DECIMAL(18, 2), h.[Lead Time Days])) AS AverageLeadTime3M
    FROM Fact.[Purchase Receipt] AS h
    WHERE h.[Supplier Key] = r.[Supplier Key]
      AND h.[Receipt Date Key] <= r.[Receipt Date Key]
      AND h.[Receipt Date Key] > DATEADD(MONTH, -3, r.[Receipt Date Key])
) AS roll;
GO
