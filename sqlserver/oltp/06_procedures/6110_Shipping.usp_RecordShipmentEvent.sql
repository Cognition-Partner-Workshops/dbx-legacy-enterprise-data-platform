/*
    Shipping.usp_RecordShipmentEvent

    Catalog entry : sqlserver_oltp.procedures - Shipping.RecordShipmentEvent
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6110 - after 6100
    Depends on    : Shipping.ShipmentEvents, Shipping.ShipmentHeaders,
                    Shipping.DeliveryStops
    Called by     : carrier tracking feed loader, driver handheld interface

    Appends one tracking event and lets it drive the header. Carrier event
    codes are mapped to internal codes by a CASE that has grown one branch per
    carrier onboarding; unmapped codes are stored with an internal code of
    UNKNOWN and are reported on weekly.

    The event timestamp arrives in carrier local time. Where the offset is
    supplied the UTC column is derived; where it is not, the UTC column is
    filled with the local value, which is wrong and known to be wrong.
*/
CREATE PROCEDURE [Shipping].[usp_RecordShipmentEvent]
    @ShipmentID             INT,
    @CarrierEventCode       NVARCHAR (20),
    @EventWhenLocal         DATETIME2 (7),
    @TimeZoneOffsetMinutes  SMALLINT = NULL,
    @LocationText           NVARCHAR (120) = NULL,
    @SignedForBy            NVARCHAR (60) = NULL,
    @ExceptionNote          NVARCHAR (200) = NULL,
    @SourceFeed             NVARCHAR (20) = N'CARRIERAPI',
    @RecordedByPersonID     INT = NULL,
    @BatchID                BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NOT EXISTS (SELECT 1 FROM [Shipping].[ShipmentHeaders] WHERE [ShipmentID] = @ShipmentID)
    BEGIN
        RAISERROR (N'Shipment %d does not exist.', 16, 1, @ShipmentID);
        RETURN;
    END

    DECLARE @EventTypeCode  NVARCHAR (16);
    DECLARE @ExceptionCode  NVARCHAR (10);
    DECLARE @EventSequence  SMALLINT;

    SET @EventTypeCode =
        CASE UPPER(@CarrierEventCode)
            WHEN N'PU'          THEN N'COLLECTED'
            WHEN N'PICKUP'      THEN N'COLLECTED'
            WHEN N'IT'          THEN N'INTRANSIT'
            WHEN N'DEPARTED'    THEN N'INTRANSIT'
            WHEN N'ARRIVED'     THEN N'ARRIVEDHUB'
            WHEN N'OFD'         THEN N'OUTFORDELIVERY'
            WHEN N'OUTFORDEL'   THEN N'OUTFORDELIVERY'
            WHEN N'DL'          THEN N'DELIVERED'
            WHEN N'DELIVERED'   THEN N'DELIVERED'
            WHEN N'POD'         THEN N'DELIVERED'
            WHEN N'DEX'         THEN N'ATTEMPTED'
            WHEN N'RTS'         THEN N'RETURNED'
            WHEN N'CUSTHOLD'    THEN N'UNKNOWN'
            ELSE N'UNKNOWN'
        END;

    SET @ExceptionCode =
        CASE UPPER(@CarrierEventCode)
            WHEN N'DEX'         THEN N'DELFAIL'
            WHEN N'RTS'         THEN N'REFUSED'
            WHEN N'CUSTHOLD'    THEN N'CUSTOMS'
            WHEN N'DAMAGE'      THEN N'DAMAGED'
            ELSE NULL
        END;

    SELECT @EventSequence = ISNULL(MAX(e.[EventSequence]), 0) + 1
    FROM [Shipping].[ShipmentEvents] AS e
    WHERE e.[ShipmentID] = @ShipmentID;

    BEGIN TRANSACTION;

    INSERT INTO [Shipping].[ShipmentEvents]
    (
        [ShipmentID], [EventSequence], [EventTypeCode], [CarrierEventCode],
        [EventWhenLocal], [EventWhenUtc], [EventTimeZoneOffsetMinutes],
        [LocationText], [ExceptionCode], [ExceptionNote], [SignedForBy],
        [SourceFeed], [RecordedByPersonID]
    )
    VALUES
    (
        @ShipmentID, @EventSequence, @EventTypeCode, @CarrierEventCode,
        @EventWhenLocal,
        CASE WHEN @TimeZoneOffsetMinutes IS NULL THEN @EventWhenLocal
             ELSE DATEADD(MINUTE, -@TimeZoneOffsetMinutes, @EventWhenLocal) END,
        @TimeZoneOffsetMinutes,
        @LocationText, @ExceptionCode, @ExceptionNote, @SignedForBy,
        @SourceFeed, @RecordedByPersonID
    );

    IF @EventTypeCode = N'DELIVERED'
    BEGIN
        UPDATE [Shipping].[ShipmentHeaders]
        SET [DeliveredWhen] = @EventWhenLocal,
            [ShipmentStatus] = N'DELIVERED',
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ShipmentID] = @ShipmentID;

        UPDATE [Shipping].[DeliveryStops]
        SET [StopStatus] = N'COMPLETED',
            [ArrivedWhen] = ISNULL([ArrivedWhen], @EventWhenLocal),
            [ReceivedByName] = ISNULL(@SignedForBy, [ReceivedByName]),
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ShipmentID] = @ShipmentID
            AND [StopStatus] NOT IN (N'COMPLETED', N'FAILED');
    END
    ELSE IF @ExceptionCode IS NOT NULL
    BEGIN
        UPDATE [Shipping].[ShipmentHeaders]
        SET [ExceptionCode] = @ExceptionCode,
            [ShipmentStatus] = N'EXCEPTION',
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ShipmentID] = @ShipmentID;
    END
    ELSE IF @EventTypeCode IN (N'COLLECTED', N'INTRANSIT', N'ARRIVEDHUB', N'OUTFORDELIVERY')
    BEGIN
        UPDATE [Shipping].[ShipmentHeaders]
        SET [ShipmentStatus] = N'INTRANSIT',
            [DespatchedWhen] = ISNULL([DespatchedWhen], @EventWhenLocal),
            [LastEditedWhen] = SYSDATETIME()
        WHERE [ShipmentID] = @ShipmentID
            AND [ShipmentStatus] NOT IN (N'DELIVERED', N'CANCELLED');
    END

    COMMIT TRANSACTION;
END
GO
