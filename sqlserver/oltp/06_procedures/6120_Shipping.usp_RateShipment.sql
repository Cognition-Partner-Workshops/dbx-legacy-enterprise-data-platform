/*
    Shipping.usp_RateShipment

    Catalog entry : sqlserver_oltp.procedures - Shipping.RateShipment
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6120 - after 6110
    Depends on    : Shipping.ShipmentHeaders, Shipping.ShipmentLines,
                    Shipping.PackagingTypes, Shipping.ufn_FreightCost,
                    Shipping.Carriers

    Rates a shipment: totals the packages, derives chargeable weight from the
    greater of gross and volumetric weight, then applies the carrier rate card
    through Shipping.ufn_FreightCost and adds the fuel surcharge.

    The rated amount is written to the header and is not recalculated when
    lines change afterwards - freight is agreed at despatch and the invoice
    takes what is on the header.
*/
CREATE PROCEDURE [Shipping].[usp_RateShipment]
    @ShipmentID         INT,
    @OriginZoneCode     NVARCHAR (10),
    @DestinationZoneCode NVARCHAR (10),
    @IsResidential      BIT = 0,
    @RatedByPersonID    INT,
    @BatchID            BIGINT = NULL,
    @FreightCharge      DECIMAL (18, 2) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CarrierID          INT;
    DECLARE @ServiceLevelCode   NVARCHAR (10);
    DECLARE @GrossWeightKg      DECIMAL (18, 3);
    DECLARE @VolumeM3           DECIMAL (18, 4);
    DECLARE @VolumetricKg       DECIMAL (18, 3);
    DECLARE @ChargeableKg       DECIMAL (18, 3);
    DECLARE @PackageCount       SMALLINT;
    DECLARE @FuelPercent        DECIMAL (5, 2);
    DECLARE @BaseFreight        DECIMAL (18, 2);
    DECLARE @CurrencyCode       NCHAR (3);

    SELECT
        @CarrierID = sh.[CarrierID],
        @ServiceLevelCode = sh.[ServiceLevelCode]
    FROM [Shipping].[ShipmentHeaders] AS sh
    WHERE sh.[ShipmentID] = @ShipmentID;

    IF @CarrierID IS NULL
    BEGIN
        RAISERROR (N'Shipment %d does not exist or has no carrier.', 16, 1, @ShipmentID);
        RETURN;
    END

    SELECT
        @PackageCount = COUNT(DISTINCT sl.[PackageNumber]),
        @GrossWeightKg = SUM(ISNULL(sl.[PackageGrossWeightKg], 0)),
        @VolumeM3 = SUM(ISNULL(pt.[VolumeM3], 0))
    FROM [Shipping].[ShipmentLines] AS sl
        LEFT JOIN [Shipping].[PackagingTypes] AS pt
            ON pt.[PackagingTypeID] = sl.[PackagingTypeID]
    WHERE sl.[ShipmentID] = @ShipmentID
        AND sl.[LineStatus] <> N'CANCELLED';

    -- Volumetric weight uses the divisor on the rate card; 5000 is the value
    -- the first carrier used in 2006 and remains the fallback.
    SELECT TOP (1) @VolumetricKg = CONVERT(DECIMAL (18, 3), ISNULL(@VolumeM3, 0) * 1000000.0 / ISNULL(NULLIF(fr.[VolumetricDivisor], 0), 5000))
    FROM [Shipping].[FreightRates] AS fr
    WHERE fr.[CarrierID] = @CarrierID
        AND fr.[ServiceLevelCode] = @ServiceLevelCode
    ORDER BY fr.[EffectiveFromDate] DESC;

    SET @ChargeableKg = CASE WHEN ISNULL(@VolumetricKg, 0) > ISNULL(@GrossWeightKg, 0)
                             THEN @VolumetricKg ELSE @GrossWeightKg END;

    -- The chargeable weight is already the greater of gross and volumetric, so
    -- the rate card is asked for it directly and the function's own volumetric
    -- step is left out by passing no package dimensions.
    SET @BaseFreight = [Shipping].[ufn_FreightCost](@CarrierID, @ServiceLevelCode,
                                                    @OriginZoneCode, @DestinationZoneCode,
                                                    @ChargeableKg,
                                                    NULL, NULL, NULL,
                                                    @IsResidential,
                                                    CONVERT(DATE, SYSDATETIME()));

    SELECT
        @FuelPercent = ISNULL(car.[FuelSurchargePercent], 0)
    FROM [Shipping].[Carriers] AS car
    WHERE car.[CarrierID] = @CarrierID;

    SELECT TOP (1) @CurrencyCode = fr.[CurrencyCode]
    FROM [Shipping].[FreightRates] AS fr
    WHERE fr.[CarrierID] = @CarrierID
        AND fr.[ServiceLevelCode] = @ServiceLevelCode
    ORDER BY fr.[EffectiveFromDate] DESC;

    SET @FreightCharge = ROUND(ISNULL(@BaseFreight, 0) * (1 + @FuelPercent / 100.0), 2);

    UPDATE [Shipping].[ShipmentHeaders]
    SET [TotalPackages] = ISNULL(@PackageCount, 1),
        [TotalGrossWeightKg] = @GrossWeightKg,
        [TotalVolumeM3] = @VolumeM3,
        [ChargeableWeightKg] = @ChargeableKg,
        [FreightChargeAmount] = @FreightCharge,
        [FreightCurrencyCode] = ISNULL(@CurrencyCode, N'USD'),
        [FreightRatedWhen] = SYSDATETIME(),
        [LastEditedBy] = @RatedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [ShipmentID] = @ShipmentID;
END
GO
