/*
    Integration.vw_DeletedKeysForExtract

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5150 - after 5140
    Depends on    : Integration.DeletedRowLog, Sales.OrderDeletionLog,
                    Warehouse.StockItemDeletionLog
    Called by     : SSIS incremental extract packages

    The delete side of change tracking. Three logs exist for historical
    reasons - the generic one written by the newer triggers and two
    table-specific ones that predate it - so this view unions them and
    tolerates the same delete appearing twice, once from each source.
    Consumers are expected to apply deletes idempotently.
*/
CREATE VIEW [Integration].[vw_DeletedKeysForExtract]
AS
SELECT
    d.[SourceSchemaName],
    d.[SourceTableName],
    d.[SourceKeyValue],
    d.[DeletedWhen],
    d.[DeletedByLogin],
    d.[IsPurgeNotDelete],
    N'GENERIC'                                  AS [LogSource]
FROM [Integration].[DeletedRowLog] AS d
UNION ALL
SELECT
    N'Sales',
    N'Orders',
    CONVERT(NVARCHAR (120), o.[OrderID]),
    o.[DeletedWhen],
    o.[DeletedByLogin],
    0,
    N'ORDERLOG'
FROM [Sales].[OrderDeletionLog] AS o
UNION ALL
SELECT
    N'Warehouse',
    N'StockItems',
    CONVERT(NVARCHAR (120), s.[StockItemID]),
    s.[DeletedWhen],
    s.[DeletedByLogin],
    0,
    N'ITEMLOG'
FROM [Warehouse].[StockItemDeletionLog] AS s;
GO
