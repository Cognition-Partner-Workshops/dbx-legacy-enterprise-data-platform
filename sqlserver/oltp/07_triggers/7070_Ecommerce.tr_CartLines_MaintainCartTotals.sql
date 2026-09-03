/*
    Ecommerce.tr_CartLines_MaintainCartTotals

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7070
    Depends on    : Ecommerce.CartLines, Ecommerce.CartHeaders
    Fires on      : AFTER INSERT, UPDATE, DELETE on Ecommerce.CartLines

    Basket totals are denormalised onto the header so the mini-basket renders
    from one row. Removed lines are soft-deleted (RemovedWhen) by the web tier
    but hard-deleted by the overnight tidy job, so the trigger has to cope with
    both.

    Estimated tax is calculated three ways because the storefronts were built
    at different times: NA adds destination sales tax on top, EU prices are
    already VAT inclusive so tax is extracted from the subtotal, and APAC adds
    GST on top but at the cart's own displayed rate.
*/
CREATE TRIGGER [Ecommerce].[tr_CartLines_MaintainCartTotals]
    ON [Ecommerce].[CartLines]
    AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Carts TABLE ([CartID] BIGINT NOT NULL PRIMARY KEY);

    INSERT INTO @Carts ([CartID])
    SELECT DISTINCT [CartID] FROM [inserted]
    UNION
    SELECT DISTINCT [CartID] FROM [deleted];

    IF NOT EXISTS (SELECT 1 FROM @Carts)
        RETURN;

    UPDATE h
    SET h.[LineCount] = CONVERT(SMALLINT, ISNULL(c.[LineCount], 0)),
        h.[MerchandiseSubtotal] = ISNULL(c.[Subtotal], 0),
        h.[EstimatedTaxAmount] =
            CASE
                WHEN h.[RegionCode] = N'EU  ' OR h.[IsTaxInclusivePricing] = 1
                    THEN CONVERT(DECIMAL (18, 2), ISNULL(c.[TaxInclusivePortion], 0))
                WHEN h.[RegionCode] = N'APAC'
                    THEN CONVERT(DECIMAL (18, 2), ISNULL(c.[TaxOnTopPortion], 0))
                ELSE CONVERT(DECIMAL (18, 2), ISNULL(c.[TaxOnTopPortion], 0))
            END,
        h.[LastActivityWhen] = SYSDATETIME()
    FROM [Ecommerce].[CartHeaders] AS h
        INNER JOIN @Carts AS t
            ON t.[CartID] = h.[CartID]
        OUTER APPLY
        (
            SELECT COUNT_BIG(1) AS [LineCount],
                   SUM(cl.[LineSubtotal]) AS [Subtotal],
                   SUM(cl.[LineSubtotal] * cl.[DisplayedTaxRate]
                       / NULLIF(100.0 + cl.[DisplayedTaxRate], 0)) AS [TaxInclusivePortion],
                   SUM(cl.[LineSubtotal] * cl.[DisplayedTaxRate] / 100.0) AS [TaxOnTopPortion]
            FROM [Ecommerce].[CartLines] AS cl
            WHERE cl.[CartID] = t.[CartID]
                AND cl.[RemovedWhen] IS NULL
        ) AS c;

    -- Emptying a basket does not abandon it; the abandonment sweep owns that
    -- status and will pick the row up on its own schedule.
    UPDATE h
    SET h.[CheckoutStepReached] = N'BASKET'
    FROM [Ecommerce].[CartHeaders] AS h
        INNER JOIN @Carts AS t
            ON t.[CartID] = h.[CartID]
    WHERE h.[LineCount] = 0
        AND h.[CartStatus] = N'ACTIVE'
        AND ISNULL(h.[CheckoutStepReached], N'') <> N'PLACED';
END
GO
