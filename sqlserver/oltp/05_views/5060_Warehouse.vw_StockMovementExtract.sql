/*
    Warehouse.vw_StockMovementExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5060 - after 5050
    Depends on    : Warehouse.StockMovementDetails, Warehouse.Bins,
                    Warehouse.WarehouseSites
    Called by     : SSIS stock movement extract

    Append-only movement ledger flattened for extraction. Reversals are kept
    as their own rows and are not netted off here; the warehouse layer decides
    what to do with them, and the two consumers that read this view have made
    different decisions.
*/
CREATE VIEW [Warehouse].[vw_StockMovementExtract]
AS
SELECT
    m.[StockMovementID],
    m.[WarehouseSiteID],
    site.[SiteCode],
    site.[RegionCode],
    m.[StockItemID],
    m.[LotNumber],
    m.[MovementTypeCode],
    m.[ReasonCode],
    m.[Quantity],
    CASE WHEN m.[Quantity] < 0 THEN N'OUT' ELSE N'IN' END           AS [MovementDirection],
    m.[UnitCost],
    m.[ExtendedCost],
    m.[FromBinID],
    fromBin.[BinCode]                                               AS [FromBinCode],
    m.[ToBinID],
    toBin.[BinCode]                                                 AS [ToBinCode],
    m.[ReferenceType],
    m.[ReferenceID],
    m.[MovementWhen],
    m.[PostedByPersonID],
    m.[IsReversal],
    m.[ReversalOfMovementID],
    m.[SourceApplication],
    m.[LastEditedWhen]                                              AS [ChangedWhen]
FROM [Warehouse].[StockMovementDetails] AS m
    INNER JOIN [Warehouse].[WarehouseSites] AS site
        ON site.[WarehouseSiteID] = m.[WarehouseSiteID]
    LEFT JOIN [Warehouse].[Bins] AS fromBin
        ON fromBin.[BinID] = m.[FromBinID]
    LEFT JOIN [Warehouse].[Bins] AS toBin
        ON toBin.[BinID] = m.[ToBinID];
GO
