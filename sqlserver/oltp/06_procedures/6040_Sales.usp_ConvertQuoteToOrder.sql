/*
    Sales.usp_ConvertQuoteToOrder

    Catalog entry : sqlserver_oltp.procedures - Sales.ConvertQuoteToOrder
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6040 - after 6030
    Depends on    : Sales.QuoteHeaders, Sales.QuoteLines, Sales.Orders,
                    Sales.OrderLines, Warehouse.ufn_AvailableToPromise
    Called by     : quote screen

    Turns an accepted quote into an order. Prices are carried across as quoted
    rather than re-derived, which is the entire point of a quote and the
    reason Sales.usp_CalculateOrderDiscounts must not be run afterwards - a
    rule enforced by convention and a comment, nothing else.

    Lines that cannot be promised in full are still created; the shortfall is
    left for the allocation run to raise as a backorder.
*/
CREATE PROCEDURE [Sales].[usp_ConvertQuoteToOrder]
    @QuoteID            INT,
    @WarehouseSiteID    INT,
    @ConvertedByPersonID INT,
    @BatchID            BIGINT = NULL,
    @NewOrderID         INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @CustomerID         INT;
    DECLARE @QuoteStatus        NVARCHAR (12);
    DECLARE @ValidUntilDate     DATE;
    DECLARE @SalesChannelID     INT;
    DECLARE @SalesTerritoryID   INT;
    DECLARE @PriceListID        INT;
    DECLARE @CurrencyCode       NCHAR (3);
    DECLARE @TaxRegimeCode      NVARCHAR (12);
    DECLARE @ContactPersonID    INT;

    SELECT
        @CustomerID = q.[CustomerID],
        @QuoteStatus = q.[QuoteStatus],
        @ValidUntilDate = q.[ValidUntilDate],
        @SalesChannelID = q.[SalesChannelID],
        @PriceListID = q.[PriceListID],
        @CurrencyCode = q.[CurrencyCode]
    FROM [Sales].[QuoteHeaders] AS q
    WHERE q.[QuoteID] = @QuoteID;

    -- Quotes carry no territory of their own; the customer's territory at
    -- conversion time is stamped onto the order and never revisited.
    SELECT @SalesTerritoryID = c.[SalesTerritoryID]
    FROM [Sales].[Customers] AS c
    WHERE c.[CustomerID] = @CustomerID;

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR (N'Quote %d does not exist.', 16, 1, @QuoteID);
        RETURN;
    END

    IF @QuoteStatus <> N'ACCEPTED'
    BEGIN
        RAISERROR (N'Quote %d is not accepted and cannot be converted.', 16, 1, @QuoteID);
        RETURN;
    END

    IF @ValidUntilDate IS NOT NULL AND @ValidUntilDate < CONVERT(DATE, SYSDATETIME())
    BEGIN
        RAISERROR (N'Quote %d expired on the quoted validity date.', 16, 1, @QuoteID);
        RETURN;
    END

    SELECT @TaxRegimeCode = ter.[TaxRegimeCode]
    FROM [Sales].[SalesTerritories] AS ter
    WHERE ter.[SalesTerritoryID] = @SalesTerritoryID;

    SELECT TOP (1) @ContactPersonID = c.[PrimaryContactPersonID]
    FROM [Sales].[Customers] AS c
    WHERE c.[CustomerID] = @CustomerID;

    BEGIN TRANSACTION;

    INSERT INTO [Sales].[Orders]
    (
        [CustomerID], [SalespersonPersonID], [PickedByPersonID], [ContactPersonID],
        [OrderDate], [ExpectedDeliveryDate], [CustomerPurchaseOrderNumber],
        [IsUndersupplyBackordered], [Comments], [DeliveryInstructions], [InternalComments],
        [LastEditedBy], [LastEditedWhen],
        [SalesChannelID], [SalesTerritoryID], [PriceListID], [SourceQuoteID],
        [OrderStatusCode], [CurrencyCode], [TaxRegimeCode], [AmendmentCount]
    )
    SELECT
        q.[CustomerID],
        q.[SalespersonPersonID],
        NULL,
        ISNULL(@ContactPersonID, q.[SalespersonPersonID]),
        CONVERT(DATE, SYSDATETIME()),
        DATEADD(DAY, 5, CONVERT(DATE, SYSDATETIME())),
        CONVERT(NVARCHAR (20), q.[QuoteReference]),
        1,
        N'Converted from quote ' + q.[QuoteReference],
        NULL,
        CONVERT(NVARCHAR (MAX), q.[Comments]),
        @ConvertedByPersonID,
        SYSDATETIME(),
        q.[SalesChannelID],
        @SalesTerritoryID,
        q.[PriceListID],
        q.[QuoteID],
        N'ENTERED',
        q.[CurrencyCode],
        @TaxRegimeCode,
        0
    FROM [Sales].[QuoteHeaders] AS q
    WHERE q.[QuoteID] = @QuoteID;

    SET @NewOrderID = SCOPE_IDENTITY();

    INSERT INTO [Sales].[OrderLines]
    (
        [OrderID], [StockItemID], [Description], [PackageTypeID], [Quantity],
        [UnitPrice], [TaxRate], [PickedQuantity], [LastEditedBy], [LastEditedWhen],
        [ListUnitPrice], [DiscountPercent], [DiscountAmount], [LineNetAmount],
        [LineStatusCode], [RequestedDeliveryDate], [SourceLineReference]
    )
    SELECT
        @NewOrderID,
        ql.[StockItemID],
        ql.[DescriptionSnapshot],
        NULL,
        ql.[Quantity],
        ql.[UnitPrice],
        ql.[TaxRatePercent],
        0,
        @ConvertedByPersonID,
        SYSDATETIME(),
        ql.[UnitPrice],
        ql.[DiscountPercent],
        ROUND(ql.[Quantity] * ql.[UnitPrice] * ql.[DiscountPercent] / 100.0, 2),
        ql.[LineNetAmount],
        N'OPEN',
        DATEADD(DAY, ISNULL(ql.[PromisedLeadTimeDays], 5), CONVERT(DATE, SYSDATETIME())),
        CONVERT(NVARCHAR (40), ql.[QuoteLineID])
    FROM [Sales].[QuoteLines] AS ql
    WHERE ql.[QuoteID] = @QuoteID
        AND ql.[LineStatus] NOT IN (N'DECLINED');

    UPDATE [Sales].[QuoteHeaders]
    SET [QuoteStatus] = N'CONVERTED',
        [ConvertedOrderID] = @NewOrderID,
        [ConvertedWhen] = SYSDATETIME(),
        [LastEditedBy] = @ConvertedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [QuoteID] = @QuoteID;

    UPDATE o
    SET o.[OrderValueExTax] = t.[NetValue]
    FROM [Sales].[Orders] AS o
        CROSS APPLY
        (
            SELECT SUM(ol.[LineNetAmount]) AS [NetValue]
            FROM [Sales].[OrderLines] AS ol
            WHERE ol.[OrderID] = @NewOrderID
        ) AS t
    WHERE o.[OrderID] = @NewOrderID;

    COMMIT TRANSACTION;
END
GO
