/*
    Returns.usp_PostReturnInspection

    Catalog entry : sqlserver_oltp.procedures - Returns.PostReturnInspection
    Deploy target : WideWorldImporters (OLTP)
    Deploy order  : 06_procedures / 6140 - after 6130
    Depends on    : Returns.ReturnInspections, Returns.ReturnLines,
                    Returns.ReturnReasons, Warehouse.usp_PostStockMovement

    Records an inspection against a received return line and moves the stock
    according to the disposition:
      RESALE   - back into the receiving site's default return bin;
      REPAIR   - into the quarantine bin, no availability;
      SCRAP    - written off with a scrap movement;
      SUPPLIER - held for supplier claim, no movement at all.

    The disposition on the line is overwritten by every inspection, so a line
    inspected twice keeps only the last decision while the ledger keeps both
    movements.
*/
CREATE PROCEDURE [Returns].[usp_PostReturnInspection]
    @ReturnLineID           BIGINT,
    @WarehouseSiteID        INT,
    @InspectedByPersonID    INT,
    @ConditionGrade         NCHAR (1),
    @QuantityInspected      DECIMAL (18, 3),
    @QuantityPassed         DECIMAL (18, 3),
    @FaultCode              NVARCHAR (10) = NULL,
    @DispositionCode        NVARCHAR (10),
    @TargetBinID            INT = NULL,
    @MeasurementText        NVARCHAR (200) = NULL,
    @BatchID                BIGINT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @StockItemID    INT;
    DECLARE @LotNumber      NVARCHAR (30);
    DECLARE @UnitPrice      DECIMAL (18, 2);
    DECLARE @Sequence       SMALLINT;
    DECLARE @AllowsResale   BIT;
    DECLARE @MovementID     BIGINT;

    SELECT
        @StockItemID = rl.[StockItemID],
        @LotNumber = rl.[LotNumber],
        @UnitPrice = rl.[UnitPriceAtSale],
        @AllowsResale = rr.[AllowsResale]
    FROM [Returns].[ReturnLines] AS rl
        INNER JOIN [Returns].[ReturnReasons] AS rr
            ON rr.[ReturnReasonID] = rl.[ReturnReasonID]
    WHERE rl.[ReturnLineID] = @ReturnLineID;

    IF @StockItemID IS NULL
    BEGIN
        RAISERROR (N'Return line %d does not exist.', 16, 1, @ReturnLineID);
        RETURN;
    END

    IF @DispositionCode = N'RESALE' AND @AllowsResale = 0
    BEGIN
        RAISERROR (N'The return reason on line %d does not permit resale.', 16, 1, @ReturnLineID);
        RETURN;
    END

    SELECT @Sequence = ISNULL(MAX(i.[InspectionSequence]), 0) + 1
    FROM [Returns].[ReturnInspections] AS i
    WHERE i.[ReturnLineID] = @ReturnLineID;

    BEGIN TRANSACTION;

    INSERT INTO [Returns].[ReturnInspections]
    (
        [ReturnLineID], [InspectionSequence], [InspectedWhen], [InspectedByPersonID],
        [WarehouseSiteID], [ConditionGrade], [QuantityInspected], [QuantityPassed],
        [FaultCode], [DispositionCode], [MeasurementText], [IsChallenged]
    )
    VALUES
    (
        @ReturnLineID, @Sequence, SYSDATETIME(), @InspectedByPersonID,
        @WarehouseSiteID, @ConditionGrade, @QuantityInspected, @QuantityPassed,
        @FaultCode, @DispositionCode, @MeasurementText, 0
    );

    IF @DispositionCode = N'RESALE' AND @QuantityPassed > 0
        EXEC [Warehouse].[usp_PostStockMovement]
            @WarehouseSiteID = @WarehouseSiteID,
            @StockItemID = @StockItemID,
            @FromBinID = NULL,
            @ToBinID = @TargetBinID,
            @LotNumber = @LotNumber,
            @MovementTypeCode = N'RETURN',
            @ReasonCode = N'RESALE',
            @Quantity = @QuantityPassed,
            @UnitCost = @UnitPrice,
            @ReferenceType = N'RETURNLINE',
            @ReferenceID = @ReturnLineID,
            @PostedByPersonID = @InspectedByPersonID,
            @SourceApplication = N'RMA',
            @BatchID = @BatchID,
            @StockMovementID = @MovementID OUTPUT;

    IF @DispositionCode = N'SCRAP' AND @QuantityInspected > 0
        EXEC [Warehouse].[usp_PostStockMovement]
            @WarehouseSiteID = @WarehouseSiteID,
            @StockItemID = @StockItemID,
            @FromBinID = @TargetBinID,
            @ToBinID = NULL,
            @LotNumber = @LotNumber,
            @MovementTypeCode = N'SCRAP',
            @ReasonCode = N'RTNSCRAP',
            @Quantity = @QuantityInspected,
            @UnitCost = @UnitPrice,
            @ReferenceType = N'RETURNLINE',
            @ReferenceID = @ReturnLineID,
            @PostedByPersonID = @InspectedByPersonID,
            @SourceApplication = N'RMA',
            @BatchID = @BatchID,
            @StockMovementID = @MovementID OUTPUT;

    UPDATE [Returns].[ReturnLines]
    SET [QuantityAccepted] = ISNULL([QuantityAccepted], 0) + @QuantityPassed,
        [QuantityScrapped] = ISNULL([QuantityScrapped], 0)
                           + CASE WHEN @DispositionCode = N'SCRAP' THEN @QuantityInspected - @QuantityPassed ELSE 0 END,
        [DispositionCode] = @DispositionCode,
        [LineStatus] = N'INSPECTED',
        [LastEditedBy] = @InspectedByPersonID,
        [LastEditedWhen] = SYSDATETIME()
    WHERE [ReturnLineID] = @ReturnLineID;

    COMMIT TRANSACTION;
END
GO
