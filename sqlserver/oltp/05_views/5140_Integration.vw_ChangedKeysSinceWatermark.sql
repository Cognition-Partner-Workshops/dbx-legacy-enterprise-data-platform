/*
    Integration.vw_ChangedKeysSinceWatermark

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 05_views / 5140 - after 5130
    Depends on    : Integration.ChangeTrackingWatermark, Sales.Orders,
                    Sales.Invoices, Sales.Customers, Shipping.ShipmentHeaders,
                    Returns.ReturnAuthorizations
    Called by     : SSIS incremental extract packages

    Change-tracking helper for the incremental extracts. The watermark row for
    the consumer 'DWH' is read inline for each of the five chased tables and
    the overlap window is subtracted, which is why rows are routinely
    re-extracted: the overlap is per table and defaults to fifteen minutes.

    The union is deliberately narrow - key, table and change stamp only. The
    extract goes back to the base table for the payload.
*/
CREATE VIEW [Integration].[vw_ChangedKeysSinceWatermark]
AS
SELECT N'Sales' AS [SourceSchemaName], N'Orders' AS [SourceTableName],
       CONVERT(NVARCHAR (120), o.[OrderID]) AS [SourceKeyValue],
       o.[LastEditedWhen] AS [ChangedWhen], N'U' AS [OperationCode]
FROM [Sales].[Orders] AS o
    INNER JOIN [Integration].[ChangeTrackingWatermark] AS w
        ON w.[ConsumerCode] = N'DWH'
            AND w.[SourceSchemaName] = N'Sales'
            AND w.[SourceTableName] = N'Orders'
WHERE w.[FullReloadRequested] = 1
    OR w.[LastExtractedWhen] IS NULL
    OR o.[LastEditedWhen] > DATEADD(MINUTE, -w.[OverlapMinutes], w.[LastExtractedWhen])
UNION ALL
SELECT N'Sales', N'Invoices',
       CONVERT(NVARCHAR (120), i.[InvoiceID]),
       i.[LastEditedWhen], N'U'
FROM [Sales].[Invoices] AS i
    INNER JOIN [Integration].[ChangeTrackingWatermark] AS w
        ON w.[ConsumerCode] = N'DWH'
            AND w.[SourceSchemaName] = N'Sales'
            AND w.[SourceTableName] = N'Invoices'
WHERE w.[FullReloadRequested] = 1
    OR w.[LastExtractedWhen] IS NULL
    OR i.[LastEditedWhen] > DATEADD(MINUTE, -w.[OverlapMinutes], w.[LastExtractedWhen])
UNION ALL
SELECT N'Sales', N'Customers',
       CONVERT(NVARCHAR (120), c.[CustomerID]),
       c.[ValidFrom], N'U'
FROM [Sales].[Customers] AS c
    INNER JOIN [Integration].[ChangeTrackingWatermark] AS w
        ON w.[ConsumerCode] = N'DWH'
            AND w.[SourceSchemaName] = N'Sales'
            AND w.[SourceTableName] = N'Customers'
WHERE w.[FullReloadRequested] = 1
    OR w.[LastExtractedWhen] IS NULL
    OR c.[ValidFrom] > DATEADD(MINUTE, -w.[OverlapMinutes], w.[LastExtractedWhen])
UNION ALL
SELECT N'Shipping', N'ShipmentHeaders',
       CONVERT(NVARCHAR (120), sh.[ShipmentID]),
       sh.[LastEditedWhen], N'U'
FROM [Shipping].[ShipmentHeaders] AS sh
    INNER JOIN [Integration].[ChangeTrackingWatermark] AS w
        ON w.[ConsumerCode] = N'DWH'
            AND w.[SourceSchemaName] = N'Shipping'
            AND w.[SourceTableName] = N'ShipmentHeaders'
WHERE w.[FullReloadRequested] = 1
    OR w.[LastExtractedWhen] IS NULL
    OR sh.[LastEditedWhen] > DATEADD(MINUTE, -w.[OverlapMinutes], w.[LastExtractedWhen])
UNION ALL
SELECT N'Returns', N'ReturnAuthorizations',
       CONVERT(NVARCHAR (120), ra.[ReturnAuthorizationID]),
       ra.[LastEditedWhen], N'U'
FROM [Returns].[ReturnAuthorizations] AS ra
    INNER JOIN [Integration].[ChangeTrackingWatermark] AS w
        ON w.[ConsumerCode] = N'DWH'
            AND w.[SourceSchemaName] = N'Returns'
            AND w.[SourceTableName] = N'ReturnAuthorizations'
WHERE w.[FullReloadRequested] = 1
    OR w.[LastExtractedWhen] IS NULL
    OR ra.[LastEditedWhen] > DATEADD(MINUTE, -w.[OverlapMinutes], w.[LastExtractedWhen]);
GO
