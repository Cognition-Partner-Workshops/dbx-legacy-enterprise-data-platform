/*
    Sales.tr_OrderLines_MaintainOrderTotals

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7020
    Depends on    : Sales.OrderLines, Sales.Orders
    Fires on      : AFTER INSERT, UPDATE, DELETE on Sales.OrderLines

    Maintains the denormalised order header totals. The header columns exist
    because the order list screen would not scale in 2007 and every report
    since has read them instead of summing the lines.

    The recalculation runs for the whole order, not the changed lines, and
    updates Sales.Orders inside the same transaction. The header LastEditedWhen
    is deliberately not touched here: doing so would make every extract that
    watermarks on the header pick up orders that only changed on a line, which
    is exactly what Sales.vw_OrderLineExtract works around instead.
*/
CREATE TRIGGER [Sales].[tr_OrderLines_MaintainOrderTotals]
    ON [Sales].[OrderLines]
    AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TouchedOrders TABLE ([OrderID] INT NOT NULL PRIMARY KEY);

    INSERT INTO @TouchedOrders ([OrderID])
    SELECT [OrderID] FROM inserted
    UNION
    SELECT [OrderID] FROM deleted;

    UPDATE o
    SET o.[OrderValueExTax] = ISNULL(agg.[NetValue], 0),
        o.[TotalDiscountAmount] = ISNULL(agg.[DiscountValue], 0),
        o.[FulfilmentFlags] =
            CASE WHEN ISNULL(agg.[BackorderedLines], 0) > 0 THEN N'BO,' ELSE N'' END
            + CASE WHEN ISNULL(agg.[AllocatedLines], 0) > 0 THEN N'ALLOC,' ELSE N'' END
            + CASE WHEN ISNULL(agg.[ShippedLines], 0) > 0 THEN N'SHIP,' ELSE N'' END
            + CASE WHEN ISNULL(agg.[PromotionLines], 0) > 0 THEN N'PROMO' ELSE N'' END
    FROM [Sales].[Orders] AS o
        INNER JOIN @TouchedOrders AS t
            ON t.[OrderID] = o.[OrderID]
        OUTER APPLY
        (
            SELECT
                SUM(ol.[LineNetAmount])                                             AS [NetValue],
                SUM(ISNULL(ol.[DiscountAmount], 0))                                 AS [DiscountValue],
                SUM(CASE WHEN ISNULL(ol.[QuantityBackordered], 0) > 0 THEN 1 ELSE 0 END) AS [BackorderedLines],
                SUM(CASE WHEN ISNULL(ol.[QuantityAllocated], 0) > 0 THEN 1 ELSE 0 END)   AS [AllocatedLines],
                SUM(CASE WHEN ISNULL(ol.[QuantityShipped], 0) > 0 THEN 1 ELSE 0 END)     AS [ShippedLines],
                SUM(CASE WHEN ol.[PromotionID] IS NOT NULL THEN 1 ELSE 0 END)            AS [PromotionLines]
            FROM [Sales].[OrderLines] AS ol
            WHERE ol.[OrderID] = o.[OrderID]
                AND ISNULL(ol.[LineStatusCode], N'OPEN') <> N'CANCELLED'
        ) AS agg;
END
GO
