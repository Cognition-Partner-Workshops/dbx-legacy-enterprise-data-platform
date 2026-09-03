/*
    Warehouse.usp_TransferStockBetweenSites

    Catalog entry : sqlserver_oltp.procedures - Warehouse.TransferStockBetweenSites
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6090 - after 6080
    Depends on    : Warehouse.StockTransfers, Warehouse.StockTransferLines,
                    Warehouse.usp_PostStockMovement, Warehouse.WarehouseSites
    Called by     : transfer screen, replenishment approval

    Drives one transfer through despatch and receipt. The mode is chosen by
    @Action - a single procedure with a verb parameter, which is how the
    original was written and how every caller now expects it.

    Cross-border transfers between regions must carry a customs reference
    before despatch; intra-region transfers do not. In-transit stock is
    tracked on the denormalised holding row and is the only place it exists.
*/
CREATE PROCEDURE [Warehouse].[usp_TransferStockBetweenSites]
    @StockTransferID    INT,
    @Action             NVARCHAR (10),
    @ActionedByPersonID INT,
    @BatchID            BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FromSiteID     INT;
    DECLARE @ToSiteID       INT;
    DECLARE @TransferStatus NVARCHAR (12);
    DECLARE @IsCrossBorder  BIT;
    DECLARE @CustomsRef     NVARCHAR (40);
    DECLARE @FromRegion     NCHAR (4);
    DECLARE @ToRegion       NCHAR (4);

    SELECT
        @FromSiteID = t.[FromWarehouseSiteID],
        @ToSiteID = t.[ToWarehouseSiteID],
        @TransferStatus = t.[TransferStatus],
        @IsCrossBorder = t.[IsCrossBorder],
        @CustomsRef = t.[CustomsDeclarationRef]
    FROM [Warehouse].[StockTransfers] AS t
    WHERE t.[StockTransferID] = @StockTransferID;

    IF @FromSiteID IS NULL
    BEGIN
        RAISERROR (N'Stock transfer %d does not exist.', 16, 1, @StockTransferID);
        RETURN;
    END

    SELECT @FromRegion = [RegionCode] FROM [Warehouse].[WarehouseSites] WHERE [WarehouseSiteID] = @FromSiteID;
    SELECT @ToRegion = [RegionCode] FROM [Warehouse].[WarehouseSites] WHERE [WarehouseSiteID] = @ToSiteID;

    DECLARE @StockTransferLineID    BIGINT;
    DECLARE @StockItemID            INT;
    DECLARE @LotNumber              NVARCHAR (30);
    DECLARE @FromBinID              INT;
    DECLARE @ToBinID                INT;
    DECLARE @Quantity               DECIMAL (18, 3);
    DECLARE @UnitCost               DECIMAL (18, 4);
    DECLARE @MovementID             BIGINT;

    IF @Action = N'DESPATCH'
    BEGIN
        IF @TransferStatus <> N'PICKING'
        BEGIN
            RAISERROR (N'Transfer %d must be approved before despatch.', 16, 1, @StockTransferID);
            RETURN;
        END

        IF (@FromRegion <> @ToRegion OR @IsCrossBorder = 1) AND (@CustomsRef IS NULL OR LEN(@CustomsRef) = 0)
        BEGIN
            RAISERROR (N'Transfer %d crosses a customs border and has no declaration reference.', 16, 1, @StockTransferID);
            RETURN;
        END

        BEGIN TRANSACTION;

        DECLARE DespatchCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                l.[StockTransferLineID], l.[StockItemID], l.[LotNumber],
                l.[FromBinID], ISNULL(l.[QuantityDespatched], l.[QuantityRequested]),
                l.[UnitCostAtDespatch]
            FROM [Warehouse].[StockTransferLines] AS l
            WHERE l.[StockTransferID] = @StockTransferID
                AND l.[LineStatus] = N'OPEN';

        OPEN DespatchCursor;
        FETCH NEXT FROM DespatchCursor
            INTO @StockTransferLineID, @StockItemID, @LotNumber, @FromBinID, @Quantity, @UnitCost;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [Warehouse].[usp_PostStockMovement]
                @WarehouseSiteID = @FromSiteID,
                @StockItemID = @StockItemID,
                @FromBinID = @FromBinID,
                @ToBinID = NULL,
                @LotNumber = @LotNumber,
                @MovementTypeCode = N'TRANSFER',
                @ReasonCode = N'TRANSFER',
                @Quantity = @Quantity,
                @UnitCost = @UnitCost,
                @ReferenceType = N'STOCKTRANSFER',
                @ReferenceID = @StockTransferID,
                @PostedByPersonID = @ActionedByPersonID,
                @SourceApplication = N'WMS',
                @BatchID = @BatchID,
                @StockMovementID = @MovementID OUTPUT;

            UPDATE [Warehouse].[StockTransferLines]
            SET [QuantityDespatched] = @Quantity,
                [LineStatus] = N'DESPATCHED',
                [LastEditedBy] = @ActionedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [StockTransferLineID] = @StockTransferLineID;

            UPDATE [Warehouse].[StockItemHoldings]
            SET [QuantityInTransit] = ISNULL([QuantityInTransit], 0) + @Quantity
            WHERE [StockItemID] = @StockItemID;

            FETCH NEXT FROM DespatchCursor
                INTO @StockTransferLineID, @StockItemID, @LotNumber, @FromBinID, @Quantity, @UnitCost;
        END

        CLOSE DespatchCursor;
        DEALLOCATE DespatchCursor;

        UPDATE [Warehouse].[StockTransfers]
        SET [TransferStatus] = N'INTRANSIT',
            [DespatchedWhen] = SYSDATETIME(),
            [LastEditedBy] = @ActionedByPersonID,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [StockTransferID] = @StockTransferID;

        COMMIT TRANSACTION;
    END
    ELSE IF @Action = N'RECEIVE'
    BEGIN
        IF @TransferStatus <> N'INTRANSIT'
        BEGIN
            RAISERROR (N'Transfer %d is not in transit.', 16, 1, @StockTransferID);
            RETURN;
        END

        BEGIN TRANSACTION;

        DECLARE ReceiptCursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                l.[StockTransferLineID], l.[StockItemID], l.[LotNumber],
                l.[ToBinID], ISNULL(l.[QuantityReceived], l.[QuantityDespatched]),
                l.[UnitCostAtDespatch]
            FROM [Warehouse].[StockTransferLines] AS l
            WHERE l.[StockTransferID] = @StockTransferID
                AND l.[LineStatus] = N'DESPATCHED';

        OPEN ReceiptCursor;
        FETCH NEXT FROM ReceiptCursor
            INTO @StockTransferLineID, @StockItemID, @LotNumber, @ToBinID, @Quantity, @UnitCost;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC [Warehouse].[usp_PostStockMovement]
                @WarehouseSiteID = @ToSiteID,
                @StockItemID = @StockItemID,
                @FromBinID = NULL,
                @ToBinID = @ToBinID,
                @LotNumber = @LotNumber,
                @MovementTypeCode = N'TRANSFER',
                @ReasonCode = N'TRANSFER',
                @Quantity = @Quantity,
                @UnitCost = @UnitCost,
                @ReferenceType = N'STOCKTRANSFER',
                @ReferenceID = @StockTransferID,
                @PostedByPersonID = @ActionedByPersonID,
                @SourceApplication = N'WMS',
                @BatchID = @BatchID,
                @StockMovementID = @MovementID OUTPUT;

            UPDATE [Warehouse].[StockTransferLines]
            SET [QuantityReceived] = @Quantity,
                [LineStatus] = CASE WHEN [QuantityVariance] <> 0 THEN N'SHORT' ELSE N'RECEIVED' END,
                [LastEditedBy] = @ActionedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [StockTransferLineID] = @StockTransferLineID;

            UPDATE [Warehouse].[StockItemHoldings]
            SET [QuantityInTransit] = ISNULL([QuantityInTransit], 0) - @Quantity
            WHERE [StockItemID] = @StockItemID;

            FETCH NEXT FROM ReceiptCursor
                INTO @StockTransferLineID, @StockItemID, @LotNumber, @ToBinID, @Quantity, @UnitCost;
        END

        CLOSE ReceiptCursor;
        DEALLOCATE ReceiptCursor;

        UPDATE [Warehouse].[StockTransfers]
        SET [TransferStatus] = CASE WHEN EXISTS (SELECT 1 FROM [Warehouse].[StockTransferLines]
                                                 WHERE [StockTransferID] = @StockTransferID
                                                     AND [LineStatus] = N'SHORT')
                                    THEN N'VARIANCE' ELSE N'RECEIVED' END,
            [ReceivedWhen] = SYSDATETIME(),
            [LastEditedBy] = @ActionedByPersonID,
            [LastEditedWhen] = SYSDATETIME()
        WHERE [StockTransferID] = @StockTransferID;

        COMMIT TRANSACTION;
    END
    ELSE
    BEGIN
        RAISERROR (N'Unsupported action %s; expected DESPATCH or RECEIVE.', 16, 1, @Action);
        RETURN;
    END
END
GO
