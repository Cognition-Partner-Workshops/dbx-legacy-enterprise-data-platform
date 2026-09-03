/*
    Warehouse.tr_StockMovementDetails_MaintainHoldings

    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 07_triggers / 7040
    Depends on    : Warehouse.StockMovementDetails, Warehouse.StockItemHoldings,
                    Warehouse.BinContents
    Fires on      : AFTER INSERT on Warehouse.StockMovementDetails

    Denormalisation maintenance. The all-sites roll-up on StockItemHoldings is
    recalculated from BinContents for every stock item touched by the batch,
    which is correct but expensive; the older per-row arithmetic it replaced
    drifted whenever a posting was reversed.

    QuantityOnHand on the sample table is an INT and is still read by the web
    site, so it is rounded down from the decimal position rather than being
    retired.
*/
CREATE TRIGGER [Warehouse].[tr_StockMovementDetails_MaintainHoldings]
    ON [Warehouse].[StockMovementDetails]
    AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM [inserted])
        RETURN;

    DECLARE @Touched TABLE
    (
        [StockItemID]       INT             NOT NULL PRIMARY KEY,
        [LastMovementWhen]  DATETIME2 (7)   NOT NULL
    );

    INSERT INTO @Touched ([StockItemID], [LastMovementWhen])
    SELECT i.[StockItemID], MAX(i.[MovementWhen])
    FROM [inserted] AS i
    GROUP BY i.[StockItemID];

    UPDATE h
    SET h.[QuantityOnHandAllSites] = ISNULL(b.[QuantityOnHand], 0),
        h.[QuantityReservedAllSites] = ISNULL(b.[QuantityReserved], 0),
        h.[QuantityOnHand] = CONVERT(INT, FLOOR(ISNULL(b.[QuantityOnHand], 0))),
        h.[LastMovementWhen] = t.[LastMovementWhen],
        h.[LastEditedWhen] = SYSDATETIME()
    FROM [Warehouse].[StockItemHoldings] AS h
        INNER JOIN @Touched AS t
            ON t.[StockItemID] = h.[StockItemID]
        OUTER APPLY
        (
            SELECT SUM(bc.[QuantityOnHand]) AS [QuantityOnHand],
                   SUM(bc.[QuantityReserved]) AS [QuantityReserved]
            FROM [Warehouse].[BinContents] AS bc
            WHERE bc.[StockItemID] = t.[StockItemID]
        ) AS b;

    -- Counts are the only movement type that also stamps the count date; the
    -- cycle-count procedure used to do this itself and both writes remain.
    UPDATE h
    SET h.[LastCountedWhen] = c.[MovementWhen]
    FROM [Warehouse].[StockItemHoldings] AS h
        INNER JOIN
        (
            SELECT i.[StockItemID], MAX(i.[MovementWhen]) AS [MovementWhen]
            FROM [inserted] AS i
            WHERE i.[MovementTypeCode] = N'COUNT'
            GROUP BY i.[StockItemID]
        ) AS c
            ON c.[StockItemID] = h.[StockItemID];
END
GO
