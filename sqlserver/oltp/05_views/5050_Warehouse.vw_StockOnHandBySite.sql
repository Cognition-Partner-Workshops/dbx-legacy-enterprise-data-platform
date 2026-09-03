/*
    Warehouse.vw_StockOnHandBySite

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5050 - after 5040
    Depends on    : Warehouse.BinContents, Warehouse.Bins, Warehouse.WarehouseSites,
                    Warehouse.StockItemHoldings, Sales.OrderAllocations
    Called by     : stock reporting, Warehouse.usp_GenerateReplenishmentOrders,
                    stock extract package

    Per-site stock position built from bin contents, with the legacy
    single-row holding figure alongside it. The two are reconciled nightly by
    hand for the primary site only; other sites will always show a variance
    against QuantityOnHandAllSites because that column counts everything.
*/
CREATE VIEW [Warehouse].[vw_StockOnHandBySite]
AS
SELECT
    site.[WarehouseSiteID],
    site.[SiteCode],
    site.[RegionCode],
    site.[UnitOfMeasureSystem],
    bc.[StockItemID],
    SUM(bc.[QuantityOnHand])                                        AS [QuantityOnHand],
    SUM(bc.[QuantityReserved])                                      AS [QuantityReserved],
    SUM(bc.[QuantityAvailable])                                     AS [QuantityAvailable],
    SUM(CASE WHEN b.[IsQuarantine] = 1 THEN bc.[QuantityOnHand] ELSE 0 END)     AS [QuantityQuarantined],
    SUM(CASE WHEN b.[IsChiller] = 1 THEN bc.[QuantityOnHand] ELSE 0 END)        AS [QuantityChilled],
    SUM(CASE WHEN bc.[ExpiryDate] IS NOT NULL AND bc.[ExpiryDate] <= CONVERT(DATE, SYSDATETIME())
             THEN bc.[QuantityOnHand] ELSE 0 END)                   AS [QuantityExpired],
    COUNT(DISTINCT bc.[BinID])                                      AS [BinCount],
    COUNT(DISTINCT bc.[LotNumber])                                  AS [LotCount],
    MAX(bc.[LastMovementWhen])                                      AS [LastMovementWhen],
    MIN(bc.[LastCountedWhen])                                       AS [OldestCountWhen],
    MAX(hold.[QuantityOnHandAllSites])                              AS [LegacyQuantityAllSites]
FROM [Warehouse].[BinContents] AS bc
    INNER JOIN [Warehouse].[Bins] AS b
        ON b.[BinID] = bc.[BinID]
    INNER JOIN [Warehouse].[WarehouseSites] AS site
        ON site.[WarehouseSiteID] = b.[WarehouseSiteID]
    LEFT JOIN [Warehouse].[StockItemHoldings] AS hold
        ON hold.[StockItemID] = bc.[StockItemID]
GROUP BY
    site.[WarehouseSiteID],
    site.[SiteCode],
    site.[RegionCode],
    site.[UnitOfMeasureSystem],
    bc.[StockItemID];
GO
