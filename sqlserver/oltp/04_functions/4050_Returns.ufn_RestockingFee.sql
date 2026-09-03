/*
    Returns.ufn_RestockingFee

    Catalog entry : sqlserver_oltp.functions - Returns.ufn_RestockingFee
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4050 - after 4040
    Depends on    : Returns.ReturnReasons
    Called by     : Returns.usp_PostReturnInspection, Returns.usp_IssueCreditNote

    Restocking fee for a returned line. The regional rules diverge and are
    hard-coded here rather than held as data:
      * EU distance selling - no fee inside the statutory withdrawal window,
        whatever the reason row says.
      * NA - the reason's percentage applies, floored at five dollars.
      * APAC - fee only where the customer is at fault, and never on faulty
        goods.
*/
CREATE FUNCTION [Returns].[ufn_RestockingFee]
(
    @ReturnReasonID     INT,
    @RegionCode         NCHAR (4),
    @LineValue          DECIMAL (18, 2),
    @DaysSinceDelivery  INT
)
RETURNS DECIMAL (18, 2)
AS
BEGIN
    DECLARE @Percent            DECIMAL (5, 2);
    DECLARE @Applies            BIT;
    DECLARE @IsCustomerFault    BIT;
    DECLARE @Category           NVARCHAR (16);
    DECLARE @Fee                DECIMAL (18, 2);

    SELECT
        @Percent = ISNULL(rr.[RestockingPercent], 0),
        @Applies = rr.[DefaultRestockingApplies],
        @IsCustomerFault = rr.[IsCustomerFault],
        @Category = rr.[ReasonCategory]
    FROM [Returns].[ReturnReasons] AS rr
    WHERE rr.[ReturnReasonID] = @ReturnReasonID;

    IF @Applies IS NULL OR @Applies = 0
        RETURN 0;

    SET @Fee = ROUND(ISNULL(@LineValue, 0) * @Percent / 100.0, 2);

    IF @RegionCode = N'EU'
    BEGIN
        IF ISNULL(@DaysSinceDelivery, 999) <= 14
            SET @Fee = 0;
    END
    ELSE IF @RegionCode = N'NA'
    BEGIN
        IF @Fee > 0 AND @Fee < 5.00
            SET @Fee = 5.00;
    END
    ELSE IF @RegionCode = N'APAC'
    BEGIN
        IF @IsCustomerFault = 0 OR @Category = N'FAULTY'
            SET @Fee = 0;
    END

    IF @Fee > ISNULL(@LineValue, 0)
        SET @Fee = ISNULL(@LineValue, 0);

    RETURN @Fee;
END
GO
