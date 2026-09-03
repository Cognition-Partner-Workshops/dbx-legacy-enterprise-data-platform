/*
    08_seed / 8030 - Integration watermark registrations

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : after 8020_seed_returns_loyalty_reference.sql
    Depends on    : Integration.ChangeTrackingWatermark
    Called by     : Integration.usp_GetChangeWatermark,
                    Integration.usp_SetChangeWatermark,
                    Integration.vw_ChangedKeysSinceWatermark, the EXT_SQL_*
                    extract packages

    One row per consumer and source table. LastExtractedWhen is deliberately
    left NULL so the first run of each extract behaves as a full load; the
    extract procedures then maintain it.

    The overlap minutes differ by table rather than by consumer, which is the
    accretion of a decade of "just widen the window for that one feed" tickets.
*/
INSERT INTO [Integration].[ChangeTrackingWatermark]
(
    [ConsumerCode], [SourceSchemaName], [SourceTableName], [WatermarkColumnName],
    [OverlapMinutes], [FullReloadRequested], [UpdatedByProcess]
)
SELECT * FROM (VALUES
    (N'EXT_SQL_SALES',     N'Sales',       N'Orders',                 N'LastEditedWhen',  15, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SALES',     N'Sales',       N'OrderLines',             N'LastEditedWhen',  15, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SALES',     N'Sales',       N'Invoices',               N'LastEditedWhen',  30, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SALES',     N'Sales',       N'CustomerPayments',       N'LastEditedWhen',  60, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SALES',     N'Sales',       N'OrderAmendments',        N'AmendedWhen',     60, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SEGMENT',   N'Sales',       N'CustomerSegmentAssignments', N'LastEditedWhen', 120, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_INVENTORY', N'Warehouse',   N'StockMovementDetails',   N'LastEditedWhen',   5, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_INVENTORY', N'Warehouse',   N'BinContents',            N'LastEditedWhen',  15, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_INVENTORY', N'Warehouse',   N'CycleCountLines',        N'LastEditedWhen',  60, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_INVENTORY', N'Warehouse',   N'StockTransfers',         N'LastEditedWhen',  30, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SHIPPING',  N'Shipping',    N'ShipmentHeaders',        N'LastEditedWhen',  15, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_SHIPPING',  N'Shipping',    N'ShipmentEvents',         N'RecordedWhen',     5, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_RETURNS',   N'Returns',     N'ReturnAuthorizations',   N'LastEditedWhen',  60, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_RETURNS',   N'Returns',     N'CreditNotes',            N'LastEditedWhen',  60, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_LOYALTY',   N'Loyalty',     N'LoyaltyPointsLedger',    N'EntryWhen',       30, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_LOYALTY',   N'Loyalty',     N'LoyaltyMembers',         N'LastEditedWhen',  30, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_WEB',       N'Ecommerce',   N'WebSessions',            N'StartedWhen',     10, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_WEB',       N'Ecommerce',   N'CartHeaders',            N'LastActivityWhen', 10, 0, N'8030_seed_integration_watermarks.sql'),
    (N'EXT_SQL_DELETES',   N'Integration', N'DeletedRowLog',          N'DeletedWhen',      0, 0, N'8030_seed_integration_watermarks.sql')
) AS s ([ConsumerCode], [SourceSchemaName], [SourceTableName], [WatermarkColumnName],
        [OverlapMinutes], [FullReloadRequested], [UpdatedByProcess])
WHERE NOT EXISTS
(
    SELECT 1
    FROM [Integration].[ChangeTrackingWatermark] AS w
    WHERE w.[ConsumerCode] = s.[ConsumerCode]
        AND w.[SourceSchemaName] = s.[SourceSchemaName]
        AND w.[SourceTableName] = s.[SourceTableName]
);
GO
