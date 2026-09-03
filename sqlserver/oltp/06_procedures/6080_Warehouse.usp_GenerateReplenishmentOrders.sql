/*
    Warehouse.usp_GenerateReplenishmentOrders

    Catalog entry : sqlserver_oltp.procedures - Warehouse.GenerateReplenishmentOrders
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6080 - after 6070
    Depends on    : Warehouse.ReplenishmentRules, Warehouse.ReplenishmentOrders,
                    Warehouse.vw_StockOnHandBySite, Warehouse.WarehouseSites
    Called by     : nightly replenishment run

    Proposes replenishment for every rule that breaches its reorder point.
    Three policies are supported and they were added in three different
    decades:
      MINMAX  - order up to maximum;
      ROP     - order the fixed economic quantity implied by days of supply;
      DOS     - order to cover the target days of supply from average demand.

    Proposals are never de-duplicated against yesterday's open proposals for
    APAC sites, because the APAC buyer works from a spreadsheet and asked for
    a fresh list each morning.
*/
CREATE PROCEDURE [Warehouse].[usp_GenerateReplenishmentOrders]
    @WarehouseSiteID    INT = NULL,
    @RunReference       NVARCHAR (40),
    @RunByPersonID      INT,
    @BatchID            BIGINT = NULL,
    @ProposalsCreated   INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @ProposalsCreated = 0;

    BEGIN TRANSACTION;

    INSERT INTO [Warehouse].[ReplenishmentOrders]
    (
        [ReplenishmentRuleID], [WarehouseSiteID], [StockItemID], [GeneratedByRun],
        [ProposedQuantity], [QuantityOnHandAtRun], [QuantityOnOrderAtRun],
        [RequiredByDate], [FulfilmentMethod], [OrderStatus], [LastEditedBy]
    )
    SELECT
        r.[ReplenishmentRuleID],
        r.[WarehouseSiteID],
        r.[StockItemID],
        @RunReference,
        CASE r.[PolicyCode]
            WHEN N'MINMAX' THEN ISNULL(r.[MaximumQuantity], 0) - ISNULL(soh.[QuantityAvailable], 0)
            WHEN N'ROP'    THEN ISNULL(r.[ReorderPoint], 0) + ISNULL(r.[SafetyStockQuantity], 0) - ISNULL(soh.[QuantityAvailable], 0)
            WHEN N'DOS'    THEN CONVERT(DECIMAL (18, 3),
                                        ISNULL(r.[AverageDailyDemand], 0) * ISNULL(r.[DaysOfSupplyTarget], 0)
                                        - ISNULL(soh.[QuantityAvailable], 0))
            ELSE ISNULL(r.[MinimumQuantity], 0)
        END,
        ISNULL(soh.[QuantityOnHand], 0),
        0,
        DATEADD(DAY, r.[LeadTimeDays], CONVERT(DATE, SYSDATETIME())),
        CASE WHEN r.[PreferredSourceSiteID] IS NOT NULL THEN N'TRANSFER' ELSE N'PURCHASE' END,
        N'PROPOSED',
        @RunByPersonID
    FROM [Warehouse].[ReplenishmentRules] AS r
        INNER JOIN [Warehouse].[WarehouseSites] AS site
            ON site.[WarehouseSiteID] = r.[WarehouseSiteID]
        LEFT JOIN [Warehouse].[vw_StockOnHandBySite] AS soh
            ON soh.[WarehouseSiteID] = r.[WarehouseSiteID]
                AND soh.[StockItemID] = r.[StockItemID]
    WHERE r.[IsSuspended] = 0
        AND (@WarehouseSiteID IS NULL OR r.[WarehouseSiteID] = @WarehouseSiteID)
        AND ISNULL(soh.[QuantityAvailable], 0) <= ISNULL(r.[ReorderPoint], 0)
        AND
        (
            site.[RegionCode] = N'APAC'
            OR NOT EXISTS (SELECT 1
                           FROM [Warehouse].[ReplenishmentOrders] AS existing
                           WHERE existing.[WarehouseSiteID] = r.[WarehouseSiteID]
                               AND existing.[StockItemID] = r.[StockItemID]
                               AND existing.[OrderStatus] IN (N'PROPOSED', N'APPROVED'))
        );

    SET @ProposalsCreated = @@ROWCOUNT;

    -- A proposal that computes to nothing or less is rejected in place rather
    -- than deleted, so the run always leaves a trail of what it considered.
    UPDATE [Warehouse].[ReplenishmentOrders]
    SET [OrderStatus] = N'REJECTED',
        [RejectionReason] = N'Computed quantity was not positive at run time',
        [LastEditedWhen] = SYSDATETIME()
    WHERE [GeneratedByRun] = @RunReference
        AND [ProposedQuantity] <= 0;

    UPDATE [Warehouse].[ReplenishmentRules]
    SET [LastReviewedWhen] = SYSDATETIME()
    WHERE [IsSuspended] = 0
        AND (@WarehouseSiteID IS NULL OR [WarehouseSiteID] = @WarehouseSiteID);

    COMMIT TRANSACTION;
END
GO
