/*
    Sales.tr_Orders_Amendment

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7000
    Depends on    : Sales.Orders, Sales.OrderAmendments
    Fires on      : AFTER UPDATE on Sales.Orders

    Hand-rolled amendment history. One row per changed column per order, with
    the sequence number taken from the count already held rather than from a
    sequence object, so a concurrent amendment on the same order will
    duplicate a sequence number and fail the unique constraint. Operations
    retry from the screen.

    Only the columns the sales desk argued about are tracked. Everything else
    changes silently, which is why the amendment history does not reconcile to
    the row version.
*/
CREATE TRIGGER [Sales].[tr_Orders_Amendment]
    ON [Sales].[Orders]
    AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT UPDATE([OrderStatusCode])
        AND NOT UPDATE([ExpectedDeliveryDate])
        AND NOT UPDATE([CustomerPurchaseOrderNumber])
        AND NOT UPDATE([OrderValueExTax])
        AND NOT UPDATE([TotalDiscountAmount])
        RETURN;

    DECLARE @AmendedBy INT;

    SELECT TOP (1) @AmendedBy = i.[LastEditedBy] FROM inserted AS i;

    ;WITH [Changes] AS
    (
        SELECT
            i.[OrderID],
            N'OTHER'                                        AS [AmendmentTypeCode],
            N'OrderStatusCode'                              AS [ChangedColumnName],
            CONVERT(NVARCHAR (400), d.[OrderStatusCode])    AS [OldValueText],
            CONVERT(NVARCHAR (400), i.[OrderStatusCode])    AS [NewValueText]
        FROM inserted AS i
            INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
        WHERE ISNULL(i.[OrderStatusCode], N'') <> ISNULL(d.[OrderStatusCode], N'')

        UNION ALL

        SELECT
            i.[OrderID], N'DATE', N'ExpectedDeliveryDate',
            CONVERT(NVARCHAR (400), d.[ExpectedDeliveryDate], 126),
            CONVERT(NVARCHAR (400), i.[ExpectedDeliveryDate], 126)
        FROM inserted AS i
            INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
        WHERE ISNULL(i.[ExpectedDeliveryDate], N'1900-01-01') <> ISNULL(d.[ExpectedDeliveryDate], N'1900-01-01')

        UNION ALL

        SELECT
            i.[OrderID], N'OTHER', N'CustomerPurchaseOrderNumber',
            CONVERT(NVARCHAR (400), d.[CustomerPurchaseOrderNumber]),
            CONVERT(NVARCHAR (400), i.[CustomerPurchaseOrderNumber])
        FROM inserted AS i
            INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
        WHERE ISNULL(i.[CustomerPurchaseOrderNumber], N'') <> ISNULL(d.[CustomerPurchaseOrderNumber], N'')

        UNION ALL

        SELECT
            i.[OrderID], N'PRICE', N'OrderValueExTax',
            CONVERT(NVARCHAR (400), d.[OrderValueExTax]),
            CONVERT(NVARCHAR (400), i.[OrderValueExTax])
        FROM inserted AS i
            INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
        WHERE ISNULL(i.[OrderValueExTax], -1) <> ISNULL(d.[OrderValueExTax], -1)

        UNION ALL

        SELECT
            i.[OrderID], N'PRICE', N'TotalDiscountAmount',
            CONVERT(NVARCHAR (400), d.[TotalDiscountAmount]),
            CONVERT(NVARCHAR (400), i.[TotalDiscountAmount])
        FROM inserted AS i
            INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
        WHERE ISNULL(i.[TotalDiscountAmount], -1) <> ISNULL(d.[TotalDiscountAmount], -1)
    )
    INSERT INTO [Sales].[OrderAmendments]
    (
        [OrderID], [AmendmentSequence], [AmendedWhen], [AmendedByPersonID],
        [AmendmentTypeCode], [TargetTableName], [TargetKeyValue], [ChangedColumnName],
        [OldValueText], [NewValueText], [ReasonCode], [RequiresCustomerApproval],
        [SourceApplication]
    )
    SELECT
        c.[OrderID],
        ISNULL(existing.[MaxSequence], 0)
            + ROW_NUMBER() OVER (PARTITION BY c.[OrderID] ORDER BY c.[ChangedColumnName] ASC),
        SYSDATETIME(),
        ISNULL(@AmendedBy, 1),
        c.[AmendmentTypeCode],
        N'Sales.Orders',
        CONVERT(NVARCHAR (40), c.[OrderID]),
        c.[ChangedColumnName],
        c.[OldValueText],
        c.[NewValueText],
        CASE WHEN c.[ChangedColumnName] = N'ExpectedDeliveryDate' THEN N'DATECHG' ELSE NULL END,
        CASE WHEN c.[AmendmentTypeCode] = N'PRICE' THEN 1 ELSE 0 END,
        APP_NAME()
    FROM [Changes] AS c
        OUTER APPLY
        (
            SELECT MAX(a.[AmendmentSequence]) AS [MaxSequence]
            FROM [Sales].[OrderAmendments] AS a
            WHERE a.[OrderID] = c.[OrderID]
        ) AS existing;

    UPDATE o
    SET o.[AmendmentCount] = ISNULL(o.[AmendmentCount], 0) + agg.[ChangeCount]
    FROM [Sales].[Orders] AS o
        INNER JOIN
        (
            SELECT i.[OrderID], COUNT(*) AS [ChangeCount]
            FROM inserted AS i
                INNER JOIN deleted AS d ON d.[OrderID] = i.[OrderID]
            WHERE ISNULL(i.[OrderStatusCode], N'') <> ISNULL(d.[OrderStatusCode], N'')
                OR ISNULL(i.[OrderValueExTax], -1) <> ISNULL(d.[OrderValueExTax], -1)
            GROUP BY i.[OrderID]
        ) AS agg ON agg.[OrderID] = o.[OrderID];
END
GO
