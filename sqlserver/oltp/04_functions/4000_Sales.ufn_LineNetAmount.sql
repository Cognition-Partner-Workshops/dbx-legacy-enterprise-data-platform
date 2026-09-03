/*
    Sales.ufn_LineNetAmount

    Catalog entry : sqlserver_oltp.functions - Sales.ufn_LineNetAmount
                    (deployed as fn_ per the estate naming convention for
                    SQL Server functions; the catalog name is the logical one)
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 04_functions / 4000 - after 03_indexes
    Depends on    : none
    Called by     : Sales.usp_CalculateOrderDiscounts, Sales.usp_ConvertQuoteToOrder,
                    Sales.vw_OrderLineExtract

    Scalar line arithmetic, used everywhere a line value is needed. Rounding
    differs by tax regime: VAT lines round to two decimals at line level, sales
    tax rounds at invoice level so the line is left unrounded, and GST rounds
    to the nearest cent with a half-up rule. Reproducing all three here is why
    the function is scalar and why it is called row by row.
*/
CREATE FUNCTION [Sales].[ufn_LineNetAmount]
(
    @Quantity           DECIMAL (18, 3),
    @UnitPrice          DECIMAL (18, 2),
    @DiscountPercent    DECIMAL (5, 2),
    @DiscountAmount     DECIMAL (18, 2),
    @TaxRegimeCode      NVARCHAR (12)
)
RETURNS DECIMAL (18, 4)
AS
BEGIN
    DECLARE @Gross      DECIMAL (18, 4);
    DECLARE @Net        DECIMAL (18, 4);

    SET @Gross = ISNULL(@Quantity, 0) * ISNULL(@UnitPrice, 0);
    SET @Net = @Gross - (@Gross * ISNULL(@DiscountPercent, 0) / 100.0) - ISNULL(@DiscountAmount, 0);

    IF @Net < 0
        SET @Net = 0;

    -- Regional rounding, duplicated from the invoice printer because the two
    -- were written by different teams and neither will change first.
    IF @TaxRegimeCode = N'VAT'
        SET @Net = ROUND(@Net, 2);
    ELSE IF @TaxRegimeCode IN (N'GST', N'CONSUMPTION')
        SET @Net = ROUND(@Net, 2, 0);
    ELSE IF @TaxRegimeCode = N'SALESTAX'
        SET @Net = @Net;

    RETURN @Net;
END
GO
