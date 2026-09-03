/*
    Shipping.ShipmentEvents

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 01_tables / 1360 - after Shipping.ShipmentHeaders
    Depends on    : Shipping.ShipmentHeaders, Application.People
    Called by     : Shipping.usp_RecordShipmentEvent, Shipping.vw_DeliveryPerformance

    Carrier tracking events, appended by the inbound carrier feed and by the
    despatch desk. The carrier's own code is kept verbatim in
    CarrierEventCode and mapped to the internal EventTypeCode by
    Shipping.usp_RecordShipmentEvent; unmapped codes are stored with an
    EventTypeCode of 'UNKNOWN' rather than rejected.

    Duplicate events are expected: the same carrier event arrives from both the
    file feed and the web service, and de-duplication happens on read.
*/
CREATE TABLE [Shipping].[ShipmentEvents] (
    [ShipmentEventID]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [ShipmentID]            INT             NOT NULL,
    [EventSequence]         INT             NOT NULL,
    [EventTypeCode]         NVARCHAR (16)   NOT NULL,
    [CarrierEventCode]      NVARCHAR (20)   NULL,
    [EventWhenLocal]        DATETIME2 (7)   NOT NULL,
    [EventWhenUtc]          DATETIME2 (7)   NULL,
    [EventTimeZoneOffsetMinutes] SMALLINT   NULL,
    [LocationText]          NVARCHAR (120)  NULL,
    [ExceptionCode]         NVARCHAR (10)   NULL,
    [ExceptionNote]         NVARCHAR (400)  NULL,
    [SignedForBy]           NVARCHAR (80)   NULL,
    [SourceFeed]            NVARCHAR (20)   NOT NULL,
    [RecordedWhen]          DATETIME2 (7)   CONSTRAINT [DF_Shipping_ShipmentEvents_RecordedWhen] DEFAULT (SYSDATETIME()) NOT NULL,
    [RecordedByPersonID]    INT             NULL,
    CONSTRAINT [PK_Shipping_ShipmentEvents] PRIMARY KEY CLUSTERED ([ShipmentEventID] ASC),
    CONSTRAINT [CK_Shipping_ShipmentEvents_Type] CHECK ([EventTypeCode] IN (N'COLLECTED', N'INTRANSIT', N'ARRIVEDHUB', N'OUTFORDELIVERY', N'DELIVERED', N'ATTEMPTED', N'REFUSED', N'DAMAGED', N'LOST', N'RETURNED', N'UNKNOWN')),
    CONSTRAINT [CK_Shipping_ShipmentEvents_SourceFeed] CHECK ([SourceFeed] IN (N'CARRIERFILE', N'CARRIERAPI', N'MANUAL', N'HANDHELD')),
    CONSTRAINT [FK_Shipping_ShipmentEvents_Headers] FOREIGN KEY ([ShipmentID]) REFERENCES [Shipping].[ShipmentHeaders] ([ShipmentID]),
    CONSTRAINT [FK_Shipping_ShipmentEvents_Application_People] FOREIGN KEY ([RecordedByPersonID]) REFERENCES [Application].[People] ([PersonID])
);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentEvents_Shipment_Sequence]
    ON [Shipping].[ShipmentEvents] ([ShipmentID] ASC, [EventSequence] ASC)
    INCLUDE ([EventTypeCode], [EventWhenLocal], [ExceptionCode]);
GO

CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentEvents_Exceptions]
    ON [Shipping].[ShipmentEvents] ([RecordedWhen] ASC)
    INCLUDE ([ShipmentID], [ExceptionCode])
    WHERE [ExceptionCode] IS NOT NULL;
GO
