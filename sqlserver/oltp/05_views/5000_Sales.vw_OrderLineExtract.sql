/*
    Sales.vw_OrderLineExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5000 - after 04_functions
    Depends on    : Sales.Orders, Sales.OrderLines, Sales.SalesChannels,
                    Sales.SalesTerritories, Sales.OrderHolds
    Called by     : SSIS extract package for order lines

    The extract contract for order lines. The two status columns are collapsed
    into one DerivedLineStatus here so the warehouse does not have to know
    that OrderStatusCode and the sample's picking columns disagree; the rule
    below is the one the finance team agreed in 2017 and it is duplicated in
    the staging load, where it has since drifted.

    ChangedWhen is the greater of the header and line edit stamps because the
    incremental extract chases a single column and a header-only change still
    has to republish its lines.
*/
CREATE VIEW [Sales].[vw_OrderLineExtract]
AS
SELECT
    ol.[OrderLineID],
    o.[OrderID],
    o.[CustomerID],
    o.[SalespersonPersonID],
    o.[OrderDate],
    o.[ExpectedDeliveryDate],
    o.[SalesChannelID],
    ch.[ChannelCode],
    o.[SalesTerritoryID],
    ter.[TerritoryCode],
    ter.[RegionCode],
    o.[CurrencyCode],
    o.[ExchangeRateToUsd],
    o.[TaxRegimeCode],
    ol.[StockItemID],
    ol.[Description]                        AS [LineDescription],
    ol.[Quantity],
    ol.[UnitPrice],
    ol.[ListUnitPrice],
    ol.[DiscountPercent],
    ol.[DiscountAmount],
    ol.[PromotionID],
    ol.[LineNetAmount],
    ol.[TaxRate],
    ol.[QuantityAllocated],
    ol.[QuantityShipped],
    ol.[QuantityBackordered],
    ol.[LineStatusCode],
    o.[OrderStatusCode],
    CASE
        WHEN o.[OrderStatusCode] = N'CANCELLED' THEN N'CANCELLED'
        WHEN ol.[LineStatusCode] = N'CANCELLED' THEN N'CANCELLED'
        WHEN ol.[QuantityShipped] >= ol.[Quantity] THEN N'SHIPPED'
        WHEN ISNULL(ol.[QuantityBackordered], 0) > 0 THEN N'BACKORDER'
        WHEN ol.[PickingCompletedWhen] IS NOT NULL THEN N'PICKED'
        WHEN ISNULL(ol.[QuantityAllocated], 0) > 0 THEN N'ALLOCATED'
        ELSE N'OPEN'
    END                                     AS [DerivedLineStatus],
    CASE WHEN hold.[OpenHoldCount] > 0 THEN 1 ELSE 0 END AS [HasOpenHold],
    hold.[OpenHoldTypeList],
    o.[FulfilmentFlags],
    CASE WHEN o.[LastEditedWhen] > ol.[LastEditedWhen]
         THEN o.[LastEditedWhen] ELSE ol.[LastEditedWhen] END AS [ChangedWhen]
FROM [Sales].[OrderLines] AS ol
    INNER JOIN [Sales].[Orders] AS o
        ON o.[OrderID] = ol.[OrderID]
    LEFT JOIN [Sales].[SalesChannels] AS ch
        ON ch.[SalesChannelID] = o.[SalesChannelID]
    LEFT JOIN [Sales].[SalesTerritories] AS ter
        ON ter.[SalesTerritoryID] = o.[SalesTerritoryID]
    OUTER APPLY
    (
        SELECT
            COUNT(*)                                        AS [OpenHoldCount],
            STUFF((SELECT N'|' + h2.[HoldTypeCode]
                   FROM [Sales].[OrderHolds] AS h2
                   WHERE h2.[OrderID] = o.[OrderID]
                       AND h2.[ReleasedWhen] IS NULL
                   FOR XML PATH(N''), TYPE).value(N'.', N'NVARCHAR(200)'), 1, 1, N'') AS [OpenHoldTypeList]
        FROM [Sales].[OrderHolds] AS h
        WHERE h.[OrderID] = o.[OrderID]
            AND h.[ReleasedWhen] IS NULL
    ) AS hold;
GO
