/*
    Sales.usp_CalculateOrderDiscounts

    Catalog entry : sqlserver_oltp.procedures - Sales.CalculateOrderDiscounts
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6010 - after 6000
    Depends on    : Sales.Orders, Sales.OrderLines, Sales.OrderDiscounts,
                    Sales.ufn_DiscountPercentForCustomer, Sales.ufn_LineNetAmount
    Called by     : order entry screen, nightly re-price job

    Re-prices every line on an order: standing customer discount, then any
    promotion discounts already applied, then the order header roll-up.

    The header totals are maintained here rather than derived, and the
    procedure is not idempotent with respect to OrderDiscounts - re-running it
    replaces only the CUSTOMER-sourced rows and leaves promotion rows alone,
    which is why running it twice after a promotion reversal understates the
    discount.
*/
CREATE PROCEDURE [Sales].[usp_CalculateOrderDiscounts]
    @OrderID        INT,
    @RepricedBy     INT,
    @BatchID        BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CustomerID     INT;
    DECLARE @OrderDate      DATE;
    DECLARE @TaxRegimeCode  NVARCHAR (12);
    DECLARE @CurrencyCode   NCHAR (3);
    DECLARE @StandingPct    DECIMAL (5, 2);

    SELECT
        @CustomerID = o.[CustomerID],
        @OrderDate = o.[OrderDate],
        @TaxRegimeCode = ISNULL(o.[TaxRegimeCode], N'SALESTAX'),
        @CurrencyCode = ISNULL(o.[CurrencyCode], N'USD')
    FROM [Sales].[Orders] AS o
    WHERE o.[OrderID] = @OrderID;

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR (N'Order %d does not exist.', 16, 1, @OrderID);
        RETURN;
    END

    SET @StandingPct = [Sales].[ufn_DiscountPercentForCustomer](@CustomerID, @OrderDate);

    BEGIN TRANSACTION;

    DELETE FROM [Sales].[OrderDiscounts]
    WHERE [OrderID] = @OrderID
        AND [DiscountSource] = N'CUSTOMER';

    IF @StandingPct > 0
        INSERT INTO [Sales].[OrderDiscounts]
        (
            [OrderID], [OrderLineID], [DiscountSource], [DiscountPercent],
            [DiscountAmount], [CurrencyCode], [ReasonCode], [ApprovalStatus],
            [AppliedSequence], [LastEditedBy]
        )
        SELECT
            @OrderID,
            ol.[OrderLineID],
            N'CONTRACT',
            @StandingPct,
            ROUND(ol.[Quantity] * ol.[UnitPrice] * @StandingPct / 100.0, 2),
            @CurrencyCode,
            N'STANDING',
            N'NOTREQUIRED',
            0,
            @RepricedBy
        FROM [Sales].[OrderLines] AS ol
        WHERE ol.[OrderID] = @OrderID;

    UPDATE ol
    SET
        ol.[ListUnitPrice] = ISNULL(ol.[ListUnitPrice], ol.[UnitPrice]),
        ol.[DiscountPercent] = @StandingPct,
        ol.[DiscountAmount] = ISNULL(d.[DiscountAmount], 0),
        ol.[LineNetAmount] = [Sales].[ufn_LineNetAmount](ol.[Quantity], ol.[UnitPrice],
                                                          0, ISNULL(d.[DiscountAmount], 0),
                                                          @TaxRegimeCode),
        ol.[LastEditedBy] = @RepricedBy,
        ol.[LastEditedWhen] = SYSDATETIME()
    FROM [Sales].[OrderLines] AS ol
        OUTER APPLY
        (
            SELECT SUM(od.[DiscountAmount]) AS [DiscountAmount]
            FROM [Sales].[OrderDiscounts] AS od
            WHERE od.[OrderLineID] = ol.[OrderLineID]
                AND od.[ApprovalStatus] IN (N'NOTREQUIRED', N'APPROVED')
        ) AS d
    WHERE ol.[OrderID] = @OrderID;

    UPDATE o
    SET
        o.[OrderValueExTax] = ISNULL(t.[NetValue], 0),
        o.[TotalDiscountAmount] = ISNULL(t.[DiscountValue], 0),
        o.[LastEditedBy] = @RepricedBy,
        o.[LastEditedWhen] = SYSDATETIME()
    FROM [Sales].[Orders] AS o
        OUTER APPLY
        (
            SELECT
                SUM(ol.[LineNetAmount])     AS [NetValue],
                SUM(ol.[DiscountAmount])    AS [DiscountValue]
            FROM [Sales].[OrderLines] AS ol
            WHERE ol.[OrderID] = o.[OrderID]
        ) AS t
    WHERE o.[OrderID] = @OrderID;

    COMMIT TRANSACTION;
END
GO
