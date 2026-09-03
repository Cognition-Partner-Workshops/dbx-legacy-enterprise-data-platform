/*
    Sales.tr_Orders_DeletionLog

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7010
    Depends on    : Sales.Orders, Sales.OrderDeletionLog, Integration.DeletedRowLog
    Fires on      : AFTER DELETE on Sales.Orders

    Deletes are rare and are supposed to go through the cancellation path, but
    the 2008 data-fix scripts delete directly and the downstream extracts have
    no other way to see it. The row is written to both the order-specific log
    and the generic one because the warehouse extract reads the first and the
    interface dispatcher reads the second, and nobody merged them.
*/
CREATE TRIGGER [Sales].[tr_Orders_DeletionLog]
    ON [Sales].[Orders]
    AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO [Sales].[OrderDeletionLog]
    (
        [OrderID], [CustomerID], [SalesTerritoryID], [OrderDate],
        [OrderStatusAtDelete], [OrderValueAtDelete], [LineCountAtDelete],
        [IsCascadeFromCustomer]
    )
    SELECT
        d.[OrderID],
        d.[CustomerID],
        d.[SalesTerritoryID],
        d.[OrderDate],
        d.[OrderStatusCode],
        d.[OrderValueExTax],
        (SELECT COUNT(*) FROM [Sales].[OrderLines] AS ol WHERE ol.[OrderID] = d.[OrderID]),
        CASE WHEN NOT EXISTS (SELECT 1 FROM [Sales].[Customers] AS c WHERE c.[CustomerID] = d.[CustomerID])
             THEN 1 ELSE 0 END
    FROM deleted AS d;

    INSERT INTO [Integration].[DeletedRowLog]
    (
        [SourceSchemaName], [SourceTableName], [SourceKeyValue], [SecondaryKeyValue],
        [DeleteReasonCode], [RowSnapshotText], [IsPurgeNotDelete]
    )
    SELECT
        N'Sales',
        N'Orders',
        CONVERT(NVARCHAR (120), d.[OrderID]),
        CONVERT(NVARCHAR (120), d.[CustomerID]),
        N'HARDDEL',
        N'OrderID=' + CONVERT(NVARCHAR (20), d.[OrderID])
            + N'|CustomerID=' + CONVERT(NVARCHAR (20), d.[CustomerID])
            + N'|OrderDate=' + CONVERT(NVARCHAR (20), d.[OrderDate], 126)
            + N'|Status=' + ISNULL(d.[OrderStatusCode], N''),
        0
    FROM deleted AS d;
END
GO
