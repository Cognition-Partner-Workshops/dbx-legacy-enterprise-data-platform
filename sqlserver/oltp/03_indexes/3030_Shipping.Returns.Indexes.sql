/*
    Additive indexes on the shipping and returns tables

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 03_indexes / 3030 - after 01_tables
    Depends on    : Shipping.ShipmentHeaders, Shipping.ShipmentEvents,
                    Returns.ReturnAuthorizations, Returns.CreditNotes
    Called by     : Shipping.vw_ShipmentExtract, Shipping.vw_DeliveryPerformance,
                    Returns.vw_ReturnExtract, Returns.vw_CreditNoteExtract

    Tracking-number lookups are filtered because roughly a third of shipments
    are collections with no tracking number at all. The late-delivery index
    exists purely for the carrier scorecard report that runs every Monday.
*/
IF INDEXPROPERTY(OBJECT_ID(N'Shipping.ShipmentHeaders'), N'IX_Shipping_ShipmentHeaders_Extract', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_Extract]
        ON [Shipping].[ShipmentHeaders] ([LastEditedWhen] ASC)
        INCLUDE ([OrderID], [CustomerID], [CarrierID], [ShipmentStatus], [DespatchedWhen], [DeliveredWhen]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Shipping.ShipmentHeaders'), N'IX_Shipping_ShipmentHeaders_Tracking', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_Tracking]
        ON [Shipping].[ShipmentHeaders] ([TrackingNumber] ASC)
        INCLUDE ([CarrierID], [ShipmentStatus])
        WHERE [TrackingNumber] IS NOT NULL;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Shipping.ShipmentHeaders'), N'IX_Shipping_ShipmentHeaders_InFlight', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentHeaders_InFlight]
        ON [Shipping].[ShipmentHeaders] ([PromisedDeliveryWhen] ASC, [CarrierID] ASC)
        INCLUDE ([CustomerID], [ShipmentReference], [ExceptionCode])
        WHERE [ShipmentStatus] IN (N'PLANNED', N'PICKING', N'PACKED', N'DESPATCHED', N'INTRANSIT');
GO

IF INDEXPROPERTY(OBJECT_ID(N'Shipping.ShipmentEvents'), N'IX_Shipping_ShipmentEvents_ShipmentSequence', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentEvents_ShipmentSequence]
        ON [Shipping].[ShipmentEvents] ([ShipmentID] ASC, [EventSequence] DESC)
        INCLUDE ([EventTypeCode], [EventWhenLocal], [ExceptionCode]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Shipping.ShipmentEvents'), N'IX_Shipping_ShipmentEvents_Exceptions', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Shipping_ShipmentEvents_Exceptions]
        ON [Shipping].[ShipmentEvents] ([ExceptionCode] ASC, [EventWhenLocal] ASC)
        INCLUDE ([ShipmentID], [LocationText])
        WHERE [ExceptionCode] IS NOT NULL;
GO

IF INDEXPROPERTY(OBJECT_ID(N'Returns.ReturnAuthorizations'), N'IX_Returns_ReturnAuthorizations_Extract', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Returns_ReturnAuthorizations_Extract]
        ON [Returns].[ReturnAuthorizations] ([LastEditedWhen] ASC)
        INCLUDE ([CustomerID], [AuthorizationStatus], [RegionCode], [TotalExpectedCredit]);
GO

IF INDEXPROPERTY(OBJECT_ID(N'Returns.ReturnAuthorizations'), N'IX_Returns_ReturnAuthorizations_Open', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Returns_ReturnAuthorizations_Open]
        ON [Returns].[ReturnAuthorizations] ([RegionCode] ASC, [RequestedWhen] ASC)
        INCLUDE ([CustomerID], [RmaNumber], [TotalExpectedCredit])
        WHERE [AuthorizationStatus] IN (N'REQUESTED', N'APPROVED', N'AWAITINGGOODS', N'RECEIVED');
GO

IF INDEXPROPERTY(OBJECT_ID(N'Returns.CreditNotes'), N'IX_Returns_CreditNotes_Series', N'IndexID') IS NULL
    CREATE NONCLUSTERED INDEX [IX_Returns_CreditNotes_Series]
        ON [Returns].[CreditNotes] ([NumberSeriesCode] ASC, [NumberWithinSeries] DESC)
        INCLUDE ([CustomerID], [IssuedDate], [NetAmount], [TaxAmount]);
GO
