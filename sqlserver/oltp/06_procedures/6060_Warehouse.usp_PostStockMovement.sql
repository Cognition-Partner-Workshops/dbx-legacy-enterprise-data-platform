/*
    Warehouse.usp_PostStockMovement

    Catalog entry : sqlserver_oltp.procedures - Warehouse.PostStockMovement
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6060 - after 6050
    Depends on    : Warehouse.StockMovementDetails, Warehouse.BinContents,
                    Warehouse.Bins, Warehouse.StockItemHoldings
    Called by     : every other warehouse procedure in this package

    The single write path into the movement ledger. Posts one movement, keeps
    the bin content rows in step and updates the denormalised all-sites
    holding figures.

    Negative quantities are outbound. The procedure will drive a bin negative
    rather than refuse the movement - operations insisted, because the pickers
    are right more often than the system is - and records the fact in the
    holding row's LastMovementWhen only, so the negative has to be found by
    report.
*/
CREATE PROCEDURE [Warehouse].[usp_PostStockMovement]
    @WarehouseSiteID    INT,
    @StockItemID        INT,
    @FromBinID          INT = NULL,
    @ToBinID            INT = NULL,
    @LotNumber          NVARCHAR (30) = NULL,
    @MovementTypeCode   NVARCHAR (10),
    @ReasonCode         NVARCHAR (10) = NULL,
    @Quantity           DECIMAL (18, 3),
    @UnitCost           DECIMAL (18, 4) = NULL,
    @ReferenceType      NVARCHAR (20) = NULL,
    @ReferenceID        BIGINT = NULL,
    @PostedByPersonID   INT,
    @SourceApplication  NVARCHAR (30) = N'WMS',
    @BatchID            BIGINT = NULL,
    @StockMovementID    BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @Quantity = 0
    BEGIN
        RAISERROR (N'A zero quantity movement cannot be posted.', 16, 1);
        RETURN;
    END

    IF @FromBinID IS NULL AND @ToBinID IS NULL
    BEGIN
        RAISERROR (N'A movement must name a from bin, a to bin, or both.', 16, 1);
        RETURN;
    END

    SELECT @StockMovementID = NEXT VALUE FOR [Sequences].[StockMovementID];

    BEGIN TRANSACTION;

    INSERT INTO [Warehouse].[StockMovementDetails]
    (
        [StockMovementID], [WarehouseSiteID], [StockItemID], [FromBinID], [ToBinID], [LotNumber],
        [MovementTypeCode], [ReasonCode], [Quantity], [UnitCost],
        [ReferenceType], [ReferenceID], [MovementWhen], [PostedByPersonID],
        [IsReversal], [SourceApplication]
    )
    VALUES
    (
        @StockMovementID, @WarehouseSiteID, @StockItemID, @FromBinID, @ToBinID, @LotNumber,
        @MovementTypeCode, @ReasonCode, @Quantity, @UnitCost,
        @ReferenceType, @ReferenceID, SYSDATETIME(), @PostedByPersonID,
        0, @SourceApplication
    );

    IF @FromBinID IS NOT NULL
        UPDATE [Warehouse].[BinContents]
        SET [QuantityOnHand] = [QuantityOnHand] - ABS(@Quantity),
            [LastMovementWhen] = SYSDATETIME(),
            [LastEditedBy] = @PostedByPersonID,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [BinID] = @FromBinID
            AND [StockItemID] = @StockItemID
            AND (([LotNumber] IS NULL AND @LotNumber IS NULL) OR [LotNumber] = @LotNumber);

    IF @ToBinID IS NOT NULL
    BEGIN
        IF EXISTS (SELECT 1 FROM [Warehouse].[BinContents]
                   WHERE [BinID] = @ToBinID
                       AND [StockItemID] = @StockItemID
                       AND (([LotNumber] IS NULL AND @LotNumber IS NULL) OR [LotNumber] = @LotNumber))
            UPDATE [Warehouse].[BinContents]
            SET [QuantityOnHand] = [QuantityOnHand] + ABS(@Quantity),
                [LastMovementWhen] = SYSDATETIME(),
                [LastEditedBy] = @PostedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [BinID] = @ToBinID
                AND [StockItemID] = @StockItemID
                AND (([LotNumber] IS NULL AND @LotNumber IS NULL) OR [LotNumber] = @LotNumber);
        ELSE
            INSERT INTO [Warehouse].[BinContents]
            (
                [BinID], [StockItemID], [LotNumber], [QuantityOnHand],
                [QuantityReserved], [UnitCostAtReceipt], [ReceivedWhen],
                [LastMovementWhen], [ContentStatus], [LastEditedBy]
            )
            VALUES
            (
                @ToBinID, @StockItemID, @LotNumber, ABS(@Quantity),
                0, @UnitCost, SYSDATETIME(),
                SYSDATETIME(), N'GOOD', @PostedByPersonID
            );
    END

    UPDATE [Warehouse].[StockItemHoldings]
    SET [QuantityOnHandAllSites] = ISNULL([QuantityOnHandAllSites], 0)
                                 + CASE WHEN @ToBinID IS NOT NULL AND @FromBinID IS NOT NULL THEN 0
                                        WHEN @ToBinID IS NOT NULL THEN ABS(@Quantity)
                                        ELSE -ABS(@Quantity) END,
        [LastMovementWhen] = SYSDATETIME()
    WHERE [StockItemID] = @StockItemID;

    COMMIT TRANSACTION;
END
GO
