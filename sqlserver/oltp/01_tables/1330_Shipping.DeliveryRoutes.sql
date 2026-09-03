/*
    Shipping.DeliveryRoutes

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1330 - after Warehouse.WarehouseSites, Shipping.Carriers
    Depends on    : Warehouse.WarehouseSites, Shipping.Carriers, Application.People
    Called by     : Shipping.DeliveryStops, Shipping.vw_DeliveryPerformance

    Own-fleet routes. The sample's Sales.Invoices already carries a DeliveryRun
    and RunPosition as free text; this table is the master those two strings
    were always meant to reference, added fifteen years later. The link is by
    string equality on RouteCode and is not enforced.
*/
CREATE TABLE [Shipping].[DeliveryRoutes] (
    [DeliveryRouteID]       INT             IDENTITY (1, 1) NOT NULL,
    [RouteCode]             NVARCHAR (10)   NOT NULL,
    [RouteName]             NVARCHAR (60)   NOT NULL,
    [WarehouseSiteID]       INT             NOT NULL,
    [CarrierID]             INT             NULL,
    [VehicleRegistration]   NVARCHAR (20)   NULL,
    [DriverPersonID]        INT             NULL,
    [RunDayPattern]         NVARCHAR (20)   NOT NULL,
    [DepartureTimeLocal]    TIME (0)        NULL,
    [PlannedStopCount]      SMALLINT        NULL,
    [PlannedDistanceKm]     DECIMAL (9, 2)  NULL,
    [IsChillerRoute]        BIT             CONSTRAINT [DF_Shipping_DeliveryRoutes_IsChillerRoute] DEFAULT (0) NOT NULL,
    [RouteStatus]           NVARCHAR (12)   CONSTRAINT [DF_Shipping_DeliveryRoutes_RouteStatus] DEFAULT (N'ACTIVE') NOT NULL,
    [LastEditedBy]          INT             NOT NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_DeliveryRoutes_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_DeliveryRoutes] PRIMARY KEY CLUSTERED ([DeliveryRouteID] ASC),
    CONSTRAINT [UQ_Shipping_DeliveryRoutes_Code] UNIQUE ([RouteCode], [WarehouseSiteID]),
    CONSTRAINT [CK_Shipping_DeliveryRoutes_Status] CHECK ([RouteStatus] IN (N'ACTIVE', N'SEASONAL', N'RETIRED')),
    CONSTRAINT [FK_Shipping_DeliveryRoutes_Sites] FOREIGN KEY ([WarehouseSiteID]) REFERENCES [Warehouse].[WarehouseSites] ([WarehouseSiteID]),
    CONSTRAINT [FK_Shipping_DeliveryRoutes_Carriers] FOREIGN KEY ([CarrierID]) REFERENCES [Shipping].[Carriers] ([CarrierID]),
    CONSTRAINT [FK_Shipping_DeliveryRoutes_Driver] FOREIGN KEY ([DriverPersonID]) REFERENCES [Application].[People] ([PersonID]),
    CONSTRAINT [FK_Shipping_DeliveryRoutes_Application_People] FOREIGN KEY ([LastEditedBy]) REFERENCES [Application].[People] ([PersonID])
);
GO
