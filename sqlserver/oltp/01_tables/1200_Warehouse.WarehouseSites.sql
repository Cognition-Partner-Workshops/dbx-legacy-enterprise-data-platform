/*
    Warehouse.WarehouseSites

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1200
    Depends on    : Application.Cities, Application.People
    Called by     : Warehouse.Bins, Warehouse.StockTransfers, Shipping.ShipmentHeaders,
                    Warehouse.vw_StockOnHandBySite

    Physical sites. The original sample assumed a single warehouse, so every
    pre-existing stock row belongs to site 1 ('WWI-MAIN') by convention; the
    extension tables carry the site key explicitly.

    Regional divergence: the site's UnitOfMeasureSystem drives whether weights
    on despatch are captured in pounds (NA) or kilograms (EU / APAC), and
    TemperatureScale whether chiller readings are Fahrenheit or Celsius. Both
    are stored as captured, never converted on write.
*/
CREATE TABLE [Warehouse].[WarehouseSites] (
    [WarehouseSiteID]       INT             IDENTITY (1, 1) NOT NULL,
    [SiteCode]              NVARCHAR (10)   NOT NULL,
    [SiteName]              NVARCHAR (80)   NOT NULL,
    [RegionCode]            NCHAR (4)       NOT NULL,
    [CityID]                INT             NULL,
    [SiteType]              NVARCHAR (16)   NOT NULL,
    [UnitOfMeasureSystem]   NVARCHAR (8)    NOT NULL,
    [TemperatureScale]      NCHAR (1)       NOT NULL,
    [TimeZoneIdentifier]    NVARCHAR (60)   NOT NULL,
    [OperatingHoursText]    NVARCHAR (60)   NULL,
    [IsBonded]              BIT             CONSTRAINT [DF_Warehouse_WarehouseSites_IsBonded] DEFAULT (0) NOT NULL,
    [DefaultCarrierCode]    NVARCHAR (12)   NULL,
    [CycleCountPolicyCode]  NVARCHAR (12)   NOT NULL,
    [IsActive]              BIT             CONSTRAINT [DF_Warehouse_WarehouseSites_IsActive] DEFAULT (1) NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Warehouse_WarehouseSites_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Warehouse_WarehouseSites] PRIMARY KEY CLUSTERED ([WarehouseSiteID] ASC),
    CONSTRAINT [UQ_Warehouse_WarehouseSites_SiteCode] UNIQUE ([SiteCode]),
    CONSTRAINT [CK_Warehouse_WarehouseSites_Region] CHECK ([RegionCode] IN (N'NA', N'EU', N'APAC')),
    CONSTRAINT [CK_Warehouse_WarehouseSites_Type] CHECK ([SiteType] IN (N'DC', N'CROSSDOCK', N'FORWARD', N'THIRDPARTY', N'RETURNS')),
    CONSTRAINT [CK_Warehouse_WarehouseSites_UOM] CHECK ([UnitOfMeasureSystem] IN (N'IMPERIAL', N'METRIC')),
    CONSTRAINT [CK_Warehouse_WarehouseSites_TemperatureScale] CHECK ([TemperatureScale] IN (N'C', N'F')),
    CONSTRAINT [CK_Warehouse_WarehouseSites_CycleCountPolicy] CHECK ([CycleCountPolicyCode] IN (N'ABC', N'RANDOM', N'ZONE', N'NONE')),
    CONSTRAINT [FK_Warehouse_WarehouseSites_Cities] FOREIGN KEY ([CityID]) REFERENCES [Application].[Cities] ([CityID]),
    CONSTRAINT [FK_Warehouse_WarehouseSites_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
