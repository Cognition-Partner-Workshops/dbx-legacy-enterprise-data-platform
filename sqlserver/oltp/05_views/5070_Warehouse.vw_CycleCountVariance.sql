/*
    Warehouse.vw_CycleCountVariance

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5070 - after 5060
    Depends on    : Warehouse.CycleCounts, Warehouse.CycleCountLines,
                    Warehouse.WarehouseSites, Warehouse.Bins
    Called by     : inventory accuracy reporting, Warehouse.usp_ReconcileCycleCount

    Variance per counted line, with the second count where one was taken. The
    tolerance is held on the count header as a value amount, so a high-value
    item fails tolerance on a single unit while a pallet of low-value stock
    can be out by dozens and pass.
*/
CREATE VIEW [Warehouse].[vw_CycleCountVariance]
AS
SELECT
    cc.[CycleCountID],
    cc.[CountReference],
    cc.[WarehouseSiteID],
    site.[SiteCode],
    site.[RegionCode],
    cc.[CountType],
    cc.[ZoneCode],
    cc.[AbcClass],
    cc.[ScheduledDate],
    cc.[CompletedWhen],
    cc.[CountStatus],
    cc.[ToleranceValueAmount],
    ccl.[CycleCountLineID],
    ccl.[BinID],
    b.[BinCode],
    ccl.[StockItemID],
    ccl.[LotNumber],
    ccl.[SystemQuantity],
    ccl.[CountedQuantity],
    ccl.[SecondCountQuantity],
    ccl.[VarianceQuantity],
    ccl.[UnitCostAtCount],
    ccl.[VarianceValue],
    ccl.[VarianceReasonCode],
    ccl.[LineStatus],
    CASE WHEN ccl.[SecondCountQuantity] IS NOT NULL
              AND ccl.[SecondCountQuantity] <> ccl.[CountedQuantity]
         THEN 1 ELSE 0 END                                          AS [IsRecountDisagreement],
    CASE WHEN ABS(ISNULL(ccl.[VarianceValue], 0)) > ISNULL(cc.[ToleranceValueAmount], 0)
         THEN 1 ELSE 0 END                                          AS [IsOutsideTolerance],
    CASE WHEN ISNULL(ccl.[SystemQuantity], 0) = 0 THEN NULL
         ELSE CONVERT(DECIMAL (9, 4), ccl.[VarianceQuantity] / ccl.[SystemQuantity] * 100)
    END                                                             AS [VariancePercent]
FROM [Warehouse].[CycleCountLines] AS ccl
    INNER JOIN [Warehouse].[CycleCounts] AS cc
        ON cc.[CycleCountID] = ccl.[CycleCountID]
    INNER JOIN [Warehouse].[WarehouseSites] AS site
        ON site.[WarehouseSiteID] = cc.[WarehouseSiteID]
    LEFT JOIN [Warehouse].[Bins] AS b
        ON b.[BinID] = ccl.[BinID];
GO
