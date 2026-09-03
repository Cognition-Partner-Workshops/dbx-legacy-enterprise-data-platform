/*
    Loyalty.ufn_PointsForAmount

    Catalog entry : sqlserver_oltp.functions - Loyalty.ufn_PointsForAmount
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4060 - after 4050
    Depends on    : Loyalty.LoyaltyPrograms
    Called by     : Loyalty.usp_AccruePointsForInvoice

    Points earned on a qualifying amount under a programme, before the tier
    multiplier. Programmes earn on the gross or the net amount depending on
    EarnBasis, and the rounding differs: the NA programme rounds down to a
    whole point, the EU programme rounds to the nearest point, and the APAC
    programme keeps a half point that the ledger then truncates anyway.
*/
CREATE FUNCTION [Loyalty].[ufn_PointsForAmount]
(
    @LoyaltyProgramID   INT,
    @QualifyingAmount   DECIMAL (18, 2),
    @EarnMultiplier     DECIMAL (5, 2)
)
RETURNS INT
AS
BEGIN
    DECLARE @PointsPerUnit  DECIMAL (9, 4);
    DECLARE @RegionCode     NCHAR (4);
    DECLARE @Raw            DECIMAL (18, 4);
    DECLARE @Points         INT;

    SELECT
        @PointsPerUnit = lp.[PointsPerCurrencyUnit],
        @RegionCode = lp.[RegionCode]
    FROM [Loyalty].[LoyaltyPrograms] AS lp
    WHERE lp.[LoyaltyProgramID] = @LoyaltyProgramID
        AND lp.[ProgramStatus] IN (N'LIVE', N'PILOT');

    IF @PointsPerUnit IS NULL OR ISNULL(@QualifyingAmount, 0) <= 0
        RETURN 0;

    SET @Raw = @QualifyingAmount * @PointsPerUnit * ISNULL(NULLIF(@EarnMultiplier, 0), 1);

    IF @RegionCode = N'NA'
        SET @Points = CONVERT(INT, FLOOR(@Raw));
    ELSE IF @RegionCode = N'EU'
        SET @Points = CONVERT(INT, ROUND(@Raw, 0));
    ELSE
        SET @Points = CONVERT(INT, ROUND(@Raw * 2, 0) / 2);

    RETURN ISNULL(@Points, 0);
END
GO
