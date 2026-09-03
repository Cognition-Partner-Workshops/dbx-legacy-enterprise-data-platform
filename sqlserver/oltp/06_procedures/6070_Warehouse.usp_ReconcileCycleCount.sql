/*
    Warehouse.usp_ReconcileCycleCount

    Catalog entry : sqlserver_oltp.procedures - Warehouse.ReconcileCycleCount
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6070 - after 6060
    Depends on    : Warehouse.CycleCounts, Warehouse.CycleCountLines,
                    Warehouse.BinContents, Warehouse.usp_PostStockMovement
    Called by     : cycle count screen, warehouse supervisor

    Closes a count and posts adjustments for every line outside tolerance.
    Lines inside tolerance are accepted silently and the system quantity is
    left alone, so small errors persist until the item is counted again.

    A line whose second count disagrees with the first is not posted at all -
    it is set to VARIANCE and a recount header is created that points back at
    the original through RecountOfCycleCountID.
*/
CREATE PROCEDURE [Warehouse].[usp_ReconcileCycleCount]
    @CycleCountID       INT,
    @ApprovedByPersonID INT,
    @BatchID            BIGINT = NULL,
    @AdjustmentCount    INT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @WarehouseSiteID    INT;
    DECLARE @CountStatus        NVARCHAR (12);
    DECLARE @Tolerance          DECIMAL (18, 2);
    DECLARE @RecountID          INT;

    SELECT
        @WarehouseSiteID = cc.[WarehouseSiteID],
        @CountStatus = cc.[CountStatus],
        @Tolerance = ISNULL(cc.[ToleranceValueAmount], 0)
    FROM [Warehouse].[CycleCounts] AS cc
    WHERE cc.[CycleCountID] = @CycleCountID;

    IF @WarehouseSiteID IS NULL
    BEGIN
        RAISERROR (N'Cycle count %d does not exist.', 16, 1, @CycleCountID);
        RETURN;
    END

    IF @CountStatus NOT IN (N'COUNTED', N'RECOUNT')
    BEGIN
        RAISERROR (N'Cycle count %d is not in a countable state.', 16, 1, @CycleCountID);
        RETURN;
    END

    SET @AdjustmentCount = 0;

    DECLARE @CycleCountLineID   BIGINT;
    DECLARE @BinID              INT;
    DECLARE @StockItemID        INT;
    DECLARE @LotNumber          NVARCHAR (30);
    DECLARE @VarianceQuantity   DECIMAL (18, 3);
    DECLARE @VarianceValue      DECIMAL (18, 2);
    DECLARE @UnitCost           DECIMAL (18, 4);
    DECLARE @MovementID         BIGINT;

    DECLARE CountLineCursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            ccl.[CycleCountLineID],
            ccl.[BinID],
            ccl.[StockItemID],
            ccl.[LotNumber],
            ISNULL(ccl.[CountedQuantity], 0) - ISNULL(ccl.[SystemQuantity], 0),
            ccl.[VarianceValue],
            ccl.[UnitCostAtCount]
        FROM [Warehouse].[CycleCountLines] AS ccl
        WHERE ccl.[CycleCountID] = @CycleCountID
            AND ccl.[LineStatus] = N'COUNTED';

    OPEN CountLineCursor;
    FETCH NEXT FROM CountLineCursor
        INTO @CycleCountLineID, @BinID, @StockItemID, @LotNumber,
             @VarianceQuantity, @VarianceValue, @UnitCost;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (SELECT 1 FROM [Warehouse].[CycleCountLines] AS c2
                   WHERE c2.[CycleCountLineID] = @CycleCountLineID
                       AND c2.[SecondCountQuantity] IS NOT NULL
                       AND c2.[SecondCountQuantity] <> c2.[CountedQuantity])
        BEGIN
            UPDATE [Warehouse].[CycleCountLines]
            SET [LineStatus] = N'VARIANCE',
                [LastEditedBy] = @ApprovedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [CycleCountLineID] = @CycleCountLineID;
        END
        ELSE IF @VarianceQuantity <> 0 AND ABS(ISNULL(@VarianceValue, 0)) > @Tolerance
        BEGIN
            EXEC [Warehouse].[usp_PostStockMovement]
                @WarehouseSiteID = @WarehouseSiteID,
                @StockItemID = @StockItemID,
                @FromBinID = NULL,
                @ToBinID = @BinID,
                @LotNumber = @LotNumber,
                @MovementTypeCode = N'ADJUST',
                @ReasonCode = N'CYCLECNT',
                @Quantity = @VarianceQuantity,
                @UnitCost = @UnitCost,
                @ReferenceType = N'CYCLECOUNT',
                @ReferenceID = @CycleCountID,
                @PostedByPersonID = @ApprovedByPersonID,
                @SourceApplication = N'WMS',
                @BatchID = @BatchID,
                @StockMovementID = @MovementID OUTPUT;

            UPDATE [Warehouse].[CycleCountLines]
            SET [AdjustmentMovementID] = @MovementID,
                [LineStatus] = N'ADJUSTED',
                [LastEditedBy] = @ApprovedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [CycleCountLineID] = @CycleCountLineID;

            SET @AdjustmentCount = @AdjustmentCount + 1;
        END
        ELSE
        BEGIN
            UPDATE [Warehouse].[CycleCountLines]
            SET [LineStatus] = N'COUNTED',
                [LastEditedBy] = @ApprovedByPersonID,
                [LastEditedWhen] = SYSDATETIME()
            WHERE [CycleCountLineID] = @CycleCountLineID;
        END

        FETCH NEXT FROM CountLineCursor
            INTO @CycleCountLineID, @BinID, @StockItemID, @LotNumber,
                 @VarianceQuantity, @VarianceValue, @UnitCost;
    END

    CLOSE CountLineCursor;
    DEALLOCATE CountLineCursor;

    UPDATE [Warehouse].[BinContents]
    SET [LastCountedWhen] = SYSDATETIME()
    WHERE [BinID] IN (SELECT [BinID] FROM [Warehouse].[CycleCountLines]
                      WHERE [CycleCountID] = @CycleCountID);

    IF EXISTS (SELECT 1 FROM [Warehouse].[CycleCountLines]
               WHERE [CycleCountID] = @CycleCountID AND [LineStatus] = N'VARIANCE')
    BEGIN
        INSERT INTO [Warehouse].[CycleCounts]
        (
            [CountReference], [WarehouseSiteID], [CountType], [ZoneCode], [AbcClass],
            [ScheduledDate], [ToleranceValueAmount], [CountStatus],
            [RecountOfCycleCountID], [LastEditedBy]
        )
        SELECT
            LEFT(cc.[CountReference], 16) + N'-R',
            cc.[WarehouseSiteID],
            N'SPOT',
            cc.[ZoneCode],
            cc.[AbcClass],
            DATEADD(DAY, 1, CONVERT(DATE, SYSDATETIME())),
            cc.[ToleranceValueAmount],
            N'SCHEDULED',
            cc.[CycleCountID],
            @ApprovedByPersonID
        FROM [Warehouse].[CycleCounts] AS cc
        WHERE cc.[CycleCountID] = @CycleCountID;

        SET @RecountID = SCOPE_IDENTITY();

        INSERT INTO [Warehouse].[CycleCountLines]
        (
            [CycleCountID], [BinID], [StockItemID], [LotNumber], [SystemQuantity],
            [UnitCostAtCount], [LineStatus], [LastEditedBy]
        )
        SELECT
            @RecountID,
            ccl.[BinID],
            ccl.[StockItemID],
            ccl.[LotNumber],
            ccl.[SystemQuantity],
            ccl.[UnitCostAtCount],
            N'PENDING',
            @ApprovedByPersonID
        FROM [Warehouse].[CycleCountLines] AS ccl
        WHERE ccl.[CycleCountID] = @CycleCountID
            AND ccl.[LineStatus] = N'VARIANCE';
    END

    UPDATE [Warehouse].[CycleCounts]
    SET [CountStatus] = N'RECONCILED',
        [CompletedWhen] = SYSDATETIME(),
        [ApprovedByPersonID] = @ApprovedByPersonID,
        [TotalVarianceValue] = (SELECT SUM(ccl.[VarianceValue])
                                FROM [Warehouse].[CycleCountLines] AS ccl
                                WHERE ccl.[CycleCountID] = @CycleCountID),
        [LastEditedBy] = @ApprovedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [CycleCountID] = @CycleCountID;
END
GO
