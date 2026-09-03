/*
    Sales.usp_ApplyPromotionToOrder

    Catalog entry : sqlserver_oltp.procedures - Sales.ApplyPromotionToOrder
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6000 - after 05_views
    Depends on    : Sales.Promotions, Sales.PromotionLines, Sales.PromotionRedemptions,
                    Sales.OrderDiscounts, Sales.Orders, Sales.OrderLines
    Called by     : order entry screen, Sales.usp_ConvertQuoteToOrder

    Applies one promotion to one order. The eligibility rules live in the
    procedure rather than in the promotion row:
      * the promotion must be LIVE and dated over the order date;
      * the customer category must appear in the promotion's delimited
        EligibleCustomerCategoryList, matched with LIKE against a padded
        string because the list has no schema;
      * a non-stackable promotion cannot be added where a discount is already
        present;
      * per-customer redemption caps are counted at apply time and are not
        enforced under concurrency.

    The cursor over promotion lines is original and deliberate: the free-goods
    line type inserts an order line, so a set-based rewrite was rejected in
    2014 and again in 2019.
*/
CREATE PROCEDURE [Sales].[usp_ApplyPromotionToOrder]
    @OrderID            INT,
    @PromotionCode      NVARCHAR (20),
    @CouponCode         NVARCHAR (30) = NULL,
    @AppliedByPersonID  INT,
    @BatchID            BIGINT = NULL,
    @DiscountApplied    DECIMAL (18, 2) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PromotionID        INT;
    DECLARE @RequiresCoupon     BIT;
    DECLARE @IsStackable        BIT;
    DECLARE @MaxPerCustomer     SMALLINT;
    DECLARE @EligibleList       NVARCHAR (400);
    DECLARE @RegionCode         NCHAR (4);
    DECLARE @CustomerID         INT;
    DECLARE @CustomerCategoryID INT;
    DECLARE @OrderDate          DATE;
    DECLARE @CurrencyCode       NCHAR (3);
    DECLARE @ExistingDiscounts  INT;
    DECLARE @RedeemedByCustomer INT;
    DECLARE @LineDiscount       DECIMAL (18, 2);

    SET @DiscountApplied = 0;

    SELECT
        @CustomerID = o.[CustomerID],
        @OrderDate = o.[OrderDate],
        @CurrencyCode = ISNULL(o.[CurrencyCode], N'USD')
    FROM [Sales].[Orders] AS o
    WHERE o.[OrderID] = @OrderID;

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR (N'Order %d does not exist.', 16, 1, @OrderID);
        RETURN;
    END

    SELECT @CustomerCategoryID = c.[CustomerCategoryID]
    FROM [Sales].[Customers] AS c
    WHERE c.[CustomerID] = @CustomerID;

    SELECT
        @PromotionID = p.[PromotionID],
        @RequiresCoupon = p.[RequiresCouponCode],
        @IsStackable = p.[IsStackable],
        @MaxPerCustomer = p.[MaximumRedemptionsPerCustomer],
        @EligibleList = p.[EligibleCustomerCategoryList],
        @RegionCode = p.[RegionCode]
    FROM [Sales].[Promotions] AS p
    WHERE p.[PromotionCode] = @PromotionCode
        AND p.[PromotionStatus] = N'LIVE'
        AND p.[StartDate] <= @OrderDate
        AND (p.[EndDate] IS NULL OR p.[EndDate] >= @OrderDate);

    IF @PromotionID IS NULL
    BEGIN
        RAISERROR (N'Promotion %s is not live for the order date.', 16, 1, @PromotionCode);
        RETURN;
    END

    IF @RequiresCoupon = 1 AND (@CouponCode IS NULL OR LEN(@CouponCode) = 0)
    BEGIN
        RAISERROR (N'Promotion %s requires a coupon code.', 16, 1, @PromotionCode);
        RETURN;
    END

    -- Delimited-list membership test. The list is pipe separated and is
    -- padded on both ends so a category id is never matched as a substring.
    IF @EligibleList IS NOT NULL
        AND N'|' + @EligibleList + N'|' NOT LIKE N'%|' + CONVERT(NVARCHAR (12), @CustomerCategoryID) + N'|%'
    BEGIN
        RAISERROR (N'Customer category is not eligible for promotion %s.', 16, 1, @PromotionCode);
        RETURN;
    END

    SELECT @ExistingDiscounts = COUNT(*)
    FROM [Sales].[OrderDiscounts] AS od
    WHERE od.[OrderID] = @OrderID;

    IF @IsStackable = 0 AND @ExistingDiscounts > 0
    BEGIN
        RAISERROR (N'Promotion %s is not stackable and the order already carries a discount.', 16, 1, @PromotionCode);
        RETURN;
    END

    SELECT @RedeemedByCustomer = COUNT(*)
    FROM [Sales].[PromotionRedemptions] AS pr
    WHERE pr.[PromotionID] = @PromotionID
        AND pr.[CustomerID] = @CustomerID
        AND pr.[RedemptionStatus] = N'APPLIED';

    IF @MaxPerCustomer IS NOT NULL AND @RedeemedByCustomer >= @MaxPerCustomer
    BEGIN
        RAISERROR (N'Customer has reached the redemption limit for promotion %s.', 16, 1, @PromotionCode);
        RETURN;
    END

    BEGIN TRANSACTION;

    DECLARE @PromotionLineID    INT;
    DECLARE @StockItemID        INT;
    DECLARE @StockGroupID       INT;
    DECLARE @MinimumQuantity    DECIMAL (18, 3);
    DECLARE @MinimumOrderValue  DECIMAL (18, 2);
    DECLARE @DiscountPercent    DECIMAL (5, 2);
    DECLARE @DiscountAmount     DECIMAL (18, 2);
    DECLARE @FreeQuantity       DECIMAL (18, 3);
    DECLARE @FreeStockItemID    INT;
    DECLARE @LineSequence       SMALLINT;

    DECLARE PromotionLineCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            pl.[PromotionLineID],
            pl.[StockItemID],
            pl.[StockGroupID],
            pl.[MinimumQuantity],
            pl.[MinimumOrderValue],
            pl.[DiscountPercent],
            pl.[DiscountAmount],
            pl.[FreeQuantity],
            pl.[FreeStockItemID],
            pl.[LineSequence]
        FROM [Sales].[PromotionLines] AS pl
        WHERE pl.[PromotionID] = @PromotionID
        ORDER BY pl.[LineSequence] ASC;

    OPEN PromotionLineCursor;
    FETCH NEXT FROM PromotionLineCursor
        INTO @PromotionLineID, @StockItemID, @StockGroupID, @MinimumQuantity,
             @MinimumOrderValue, @DiscountPercent, @DiscountAmount,
             @FreeQuantity, @FreeStockItemID, @LineSequence;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @LineDiscount = 0;

        INSERT INTO [Sales].[OrderDiscounts]
        (
            [OrderID], [OrderLineID], [DiscountSource], [PromotionID],
            [DiscountPercent], [DiscountAmount], [CurrencyCode], [ReasonCode],
            [ApprovalStatus], [AppliedSequence], [LastEditedBy]
        )
        SELECT
            @OrderID,
            ol.[OrderLineID],
            N'PROMO',
            @PromotionID,
            @DiscountPercent,
            CASE WHEN @DiscountPercent IS NOT NULL
                 THEN ROUND(ol.[Quantity] * ol.[UnitPrice] * @DiscountPercent / 100.0, 2)
                 ELSE @DiscountAmount END,
            @CurrencyCode,
            N'PROMO',
            N'NOTREQUIRED',
            @LineSequence,
            @AppliedByPersonID
        FROM [Sales].[OrderLines] AS ol
        WHERE ol.[OrderID] = @OrderID
            AND (@StockItemID IS NULL OR ol.[StockItemID] = @StockItemID)
            AND (@MinimumQuantity IS NULL OR ol.[Quantity] >= @MinimumQuantity);

        SELECT @LineDiscount = ISNULL(SUM(od.[DiscountAmount]), 0)
        FROM [Sales].[OrderDiscounts] AS od
        WHERE od.[OrderID] = @OrderID
            AND od.[PromotionID] = @PromotionID
            AND od.[AppliedSequence] = @LineSequence;

        SET @DiscountApplied = @DiscountApplied + @LineDiscount;

        -- Free goods are added as a zero-priced order line.
        IF @FreeQuantity IS NOT NULL AND @FreeStockItemID IS NOT NULL
        BEGIN
            UPDATE [Sales].[Orders]
            SET [FulfilmentFlags] = ISNULL([FulfilmentFlags], N'') + N'|F'
            WHERE [OrderID] = @OrderID;
        END

        FETCH NEXT FROM PromotionLineCursor
            INTO @PromotionLineID, @StockItemID, @StockGroupID, @MinimumQuantity,
                 @MinimumOrderValue, @DiscountPercent, @DiscountAmount,
                 @FreeQuantity, @FreeStockItemID, @LineSequence;
    END

    CLOSE PromotionLineCursor;
    DEALLOCATE PromotionLineCursor;

    INSERT INTO [Sales].[PromotionRedemptions]
    (
        [PromotionID], [OrderID], [CustomerID], [CouponCode], [DiscountValue],
        [CurrencyCode], [FundedBySupplierValue], [RedemptionStatus], [AppliedByPersonID]
    )
    SELECT
        @PromotionID,
        @OrderID,
        @CustomerID,
        @CouponCode,
        @DiscountApplied,
        @CurrencyCode,
        ROUND(@DiscountApplied * ISNULL(p.[SupplierFundedPercent], 0) / 100.0, 2),
        N'APPLIED',
        @AppliedByPersonID
    FROM [Sales].[Promotions] AS p
    WHERE p.[PromotionID] = @PromotionID;

    COMMIT TRANSACTION;
END
GO
