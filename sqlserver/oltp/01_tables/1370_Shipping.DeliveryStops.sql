/*
    Shipping.DeliveryStops

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1370 - after Shipping.DeliveryRoutes, Shipping.ShipmentHeaders
    Depends on    : Shipping.DeliveryRoutes, Shipping.ShipmentHeaders, Sales.Customers,
                    Application.People
    Called by     : Shipping.vw_DeliveryPerformance

    Own-fleet stop list for a route on a date. The planned window is captured
    as two times without a date, so an overnight run's window ends "before" it
    starts and the performance view has to add a day when ArrivedWhen is
    earlier than DepartedWhen.
*/
CREATE TABLE [Shipping].[DeliveryStops] (
    [DeliveryStopID]        BIGINT          IDENTITY (1, 1) NOT NULL,
    [DeliveryRouteID]       INT             NOT NULL,
    [RunDate]               DATE            NOT NULL,
    [StopSequence]          SMALLINT        NOT NULL,
    [ShipmentID]            INT             NULL,
    [CustomerID]            INT             NOT NULL,
    [PlannedWindowFrom]     TIME (0)        NULL,
    [PlannedWindowTo]       TIME (0)        NULL,
    [ArrivedWhen]           DATETIME2 (7)   NULL,
    [DepartedWhen]          DATETIME2 (7)   NULL,
    [DwellMinutes]          AS (CASE WHEN [ArrivedWhen] IS NULL OR [DepartedWhen] IS NULL THEN NULL
                                     ELSE DATEDIFF(MINUTE, [ArrivedWhen], [DepartedWhen]) END),
    [StopStatus]            NVARCHAR (12)   CONSTRAINT [DF_Shipping_DeliveryStops_StopStatus] DEFAULT (N'PLANNED') NOT NULL,
    [FailureReasonCode]     NVARCHAR (10)   NULL,
    [ReceivedByName]        NVARCHAR (80)   NULL,
    [ProofOfDeliveryRef]    NVARCHAR (60)   NULL,
    [DriverPersonID]        INT             NULL,
    [LastEditedWhen]        DATETIME2 (7)   CONSTRAINT [DF_Shipping_DeliveryStops_LastEditedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    CONSTRAINT [PK_Shipping_DeliveryStops] PRIMARY KEY CLUSTERED ([DeliveryStopID] ASC),
    CONSTRAINT [UQ_Shipping_DeliveryStops_Sequence] UNIQUE ([DeliveryRouteID], [RunDate], [StopSequence]),
    CONSTRAINT [CK_Shipping_DeliveryStops_Status] CHECK ([StopStatus] IN (N'PLANNED', N'ARRIVED', N'COMPLETED', N'FAILED', N'SKIPPED')),
    CONSTRAINT [CK_Shipping_DeliveryStops_Failure] CHECK ([StopStatus] <> N'FAILED' OR [FailureReasonCode] IS NOT NULL),
    CONSTRAINT [FK_Shipping_DeliveryStops_Routes] FOREIGN KEY ([DeliveryRouteID]) REFERENCES [Shipping].[DeliveryRoutes] ([DeliveryRouteID]),
    CONSTRAINT [FK_Shipping_DeliveryStops_Shipments] FOREIGN KEY ([ShipmentID]) REFERENCES [Shipping].[ShipmentHeaders] ([ShipmentID]),
    CONSTRAINT [FK_Shipping_DeliveryStops_Customers] FOREIGN KEY ([CustomerID]) REFERENCES [Sales].[Customers] ([CustomerID]),
    CONSTRAINT [FK_Shipping_DeliveryStops_Driver] FOREIGN KEY ([DriverPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_DeliveryStops_RunDate]
    ON [Shipping].[DeliveryStops] ([RunDate] ASC, [DeliveryRouteID] ASC)
    INCLUDE ([StopStatus], [CustomerID], [ShipmentID]);
GO
