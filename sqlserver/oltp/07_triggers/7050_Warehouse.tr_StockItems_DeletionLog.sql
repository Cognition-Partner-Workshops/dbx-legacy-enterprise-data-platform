/*
    Warehouse.tr_StockItems_DeletionLog

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7050
    Depends on    : Warehouse.StockItems, Warehouse.StockItemDeletionLog,
                    Warehouse.StockItemHoldings, Integration.DeletedRowLog
    Fires on      : AFTER DELETE on Warehouse.StockItems

    Deletes are rare but they happen (bad catalogue loads, duplicate SKUs), and
    the nightly extract has no way of seeing them. Two logs are written: the
    product team's own deletion table, which they report from, and the generic
    Integration.DeletedRowLog the extracts read. Nobody agreed to consolidate
    them.

    The reason code is not available to the trigger, so it is left NULL and the
    product team key it in afterwards from the screen.
*/
CREATE TRIGGER [Warehouse].[tr_StockItems_DeletionLog]
    ON [Warehouse].[StockItems]
    AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM [deleted])
        RETURN;

    INSERT INTO [Warehouse].[StockItemDeletionLog]
    (
        [StockItemID], [StockItemName], [SupplierID], [WasDiscontinued],
        [QuantityOnHandAtDelete], [DeletedWhen], [DeleteReasonCode]
    )
    SELECT
        d.[StockItemID],
        d.[StockItemName],
        d.[SupplierID],
        CASE WHEN d.[ValidTo] IS NOT NULL AND d.[ValidTo] < SYSUTCDATETIME() THEN 1 ELSE 0 END,
        h.[QuantityOnHandAllSites],
        SYSDATETIME(),
        NULL
    FROM [deleted] AS d
        LEFT JOIN [Warehouse].[StockItemHoldings] AS h
            ON h.[StockItemID] = d.[StockItemID];

    INSERT INTO [Integration].[DeletedRowLog]
    (
        [SourceSchemaName], [SourceTableName], [SourceKeyValue], [SecondaryKeyValue],
        [DeletedWhen], [DeleteReasonCode], [RowSnapshotText], [IsPurgeNotDelete]
    )
    SELECT
        N'Warehouse',
        N'StockItems',
        CONVERT(NVARCHAR (120), d.[StockItemID]),
        CONVERT(NVARCHAR (120), d.[SupplierID]),
        SYSDATETIME(),
        NULL,
        N'NAME=' + ISNULL(d.[StockItemName], N'') +
        N'|BRAND=' + ISNULL(d.[Brand], N'') +
        N'|SIZE=' + ISNULL(d.[Size], N'') +
        N'|UNITPRICE=' + CONVERT(NVARCHAR (30), d.[UnitPrice]),
        0
    FROM [deleted] AS d;
END
GO
