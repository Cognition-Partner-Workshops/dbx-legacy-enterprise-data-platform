/*
    Shipping.ufn_FreightCost

    Catalog entry : sqlserver_oltp.functions - Shipping.ufn_FreightCost
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4040 - after 4030
    Depends on    : Shipping.FreightRates
    Called by     : Shipping.usp_RateShipment, Shipping.usp_CreateShipmentFromOrder

    Rates a parcel against the carrier's rate card. Chargeable weight is the
    greater of actual and volumetric weight, and the volumetric divisor is
    per rate row because each carrier uses a different one and two of them
    changed theirs mid-contract without telling us.

    Where no rate row matches the function returns NULL rather than raising;
    the caller treats NULL as "rate manually", which is how roughly one
    despatch in forty ends up on the exceptions list.
*/
CREATE FUNCTION [Shipping].[ufn_FreightCost]
(
    @CarrierID          INT,
    @ServiceLevelCode   NVARCHAR (12),
    @OriginZoneCode     NVARCHAR (10),
    @DestinationZoneCode NVARCHAR (10),
    @ActualWeightKg     DECIMAL (9, 3),
    @LengthCm           DECIMAL (9, 2),
    @WidthCm            DECIMAL (9, 2),
    @HeightCm           DECIMAL (9, 2),
    @IsResidential      BIT,
    @AsAtDate           DATE
)
RETURNS DECIMAL (18, 2)
AS
BEGIN
    DECLARE @BaseCharge         DECIMAL (18, 2);
    DECLARE @PerKgCharge        DECIMAL (18, 4);
    DECLARE @MinimumCharge      DECIMAL (18, 2);
    DECLARE @VolumetricDivisor  INT;
    DECLARE @Surcharge          DECIMAL (18, 2);
    DECLARE @VolumetricWeightKg DECIMAL (9, 3);
    DECLARE @ChargeableWeightKg DECIMAL (9, 3);
    DECLARE @Cost               DECIMAL (18, 2);

    SELECT TOP (1)
        @BaseCharge = fr.[BaseCharge],
        @PerKgCharge = fr.[PerKgCharge],
        @MinimumCharge = fr.[MinimumCharge],
        @VolumetricDivisor = fr.[VolumetricDivisor],
        @Surcharge = CASE WHEN @IsResidential = 1 THEN fr.[ResidentialSurcharge] ELSE 0 END
    FROM [Shipping].[FreightRates] AS fr
    WHERE fr.[CarrierID] = @CarrierID
        AND fr.[ServiceLevelCode] = @ServiceLevelCode
        AND fr.[OriginZoneCode] = @OriginZoneCode
        AND fr.[DestinationZoneCode] = @DestinationZoneCode
        AND @ActualWeightKg >= fr.[WeightFromKg]
        AND @ActualWeightKg < fr.[WeightToKg]
        AND fr.[EffectiveFromDate] <= @AsAtDate
        AND (fr.[EffectiveToDate] IS NULL OR fr.[EffectiveToDate] > @AsAtDate)
    ORDER BY fr.[EffectiveFromDate] DESC;

    IF @BaseCharge IS NULL
        RETURN NULL;

    IF @VolumetricDivisor IS NOT NULL
        AND @LengthCm IS NOT NULL AND @WidthCm IS NOT NULL AND @HeightCm IS NOT NULL
        SET @VolumetricWeightKg = (@LengthCm * @WidthCm * @HeightCm) / @VolumetricDivisor;

    SET @ChargeableWeightKg = @ActualWeightKg;

    IF @VolumetricWeightKg IS NOT NULL AND @VolumetricWeightKg > @ChargeableWeightKg
        SET @ChargeableWeightKg = @VolumetricWeightKg;

    SET @Cost = @BaseCharge + (@ChargeableWeightKg * @PerKgCharge) + ISNULL(@Surcharge, 0);

    IF @MinimumCharge IS NOT NULL AND @Cost < @MinimumCharge
        SET @Cost = @MinimumCharge;

    RETURN ROUND(@Cost, 2);
END
GO
