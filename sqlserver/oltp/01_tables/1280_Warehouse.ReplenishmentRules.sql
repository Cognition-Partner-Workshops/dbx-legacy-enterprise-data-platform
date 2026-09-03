/*
    Warehouse.ReplenishmentRules

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1280 - after Warehouse.WarehouseSites
    Depends on    : Warehouse.WarehouseSites, Warehouse.StockItems, Application.People
    Called by     : Warehouse.usp_GenerateReplenishmentOrders

    Min/max and reorder-point rules per item per site. Three policies coexist
    because three different consultants implemented one each: MINMAX (the
    original), ROP (reorder point with safety stock) and DOS (days of supply,
    added for the APAC sites). The generation procedure branches on
    PolicyCode with a large CASE, which is exactly how it was written in 2007.
*/
CREATE TABLE [Warehouse].[ReplenishmentRules] (
    [ReplenishmentRuleID]   INT             IDENTITY (1, 1) NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [StockItemID]           INT             NOT NULL,
    [PolicyCode]            NVARCHAR (10)   NOT NULL,
    [MinimumQuantity]       DECIMAL (18, 3) NULL,
    [MaximumQuantity]       DECIMAL (18, 3) NULL,
    [ReorderPoint]          DECIMAL (18, 3) NULL,
    [SafetyStockQuantity]   DECIMAL (18, 3) NULL,
    [DaysOfSupplyTarget]    SMALLINT        NULL,
    [AverageDailyDemand]    DECIMAL (18, 4) NULL,
    [LeadTimeDays]          SMALLINT        NOT NULL,
    [ReviewFrequencyCode]   NVARCHAR (10)   CONSTRAINT [DF_Warehouse_ReplenishmentRules_ReviewFrequencyCode] DEFAULT (N'DAILY') NOT NULL,
    [PreferredSourceSiteID] INT             NULL,
    [IsSuspended]           BIT             CONSTRAINT [DF_Warehouse_ReplenishmentRules_IsSuspended] DEFAULT (0) NOT NULL,
    [LastReviewedWhen]      DATETIME2 (7)   NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_ReplenishmentRules_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_ReplenishmentRules] PRIMARY KEY CLUSTERED ([ReplenishmentRuleID] ASC),
    CONSTRAINT [UQ_Warehouse_ReplenishmentRules_Site_Item] UNIQUE ([WarehouseSiteID], [StockItemID]),
    CONSTRAINT [CK_Warehouse_ReplenishmentRules_Policy] CHECK ([PolicyCode] IN (N'MINMAX', N'ROP', N'DOS')),
    CONSTRAINT [CK_Warehouse_ReplenishmentRules_MinMax] CHECK ([PolicyCode] <> N'MINMAX' OR ([MinimumQuantity] IS NOT NULL AND [MaximumQuantity] > [MinimumQuantity])),
    CONSTRAINT [CK_Warehouse_ReplenishmentRules_Rop] CHECK ([PolicyCode] <> N'ROP' OR [ReorderPoint] IS NOT NULL),
    CONSTRAINT [CK_Warehouse_ReplenishmentRules_Dos] CHECK ([PolicyCode] <> N'DOS' OR [DaysOfSupplyTarget] IS NOT NULL),
    CONSTRAINT [CK_Warehouse_ReplenishmentRules_Frequency] CHECK ([ReviewFrequencyCode] IN (N'DAILY', N'WEEKLY', N'FORTNIGHT', N'MONTHLY')),
    CONSTRAINT [FK_Warehouse_ReplenishmentRules_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentRules_SourceSite] FOREIGN KEY ([PreferredSourceSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentRules_StockItems] FOREIGN KEY ([StockItemID]) REFERENCES [Warehouse].[StockItems] ([StockItemID]),
    CONSTRAINT [FK_Warehouse_ReplenishmentRules_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
