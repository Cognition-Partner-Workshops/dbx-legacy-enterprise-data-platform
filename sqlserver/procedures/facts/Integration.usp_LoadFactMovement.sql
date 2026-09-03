/*
    Integration.usp_LoadFactMovement

    Object        : Integration.usp_LoadFactMovement
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Movement.Extensions.
    Called by     : FACT_Load_Movement, INV_Load_Stock_Movement.
    Reads         : stg.StockMovement, stg.StandardCost.
    Depends on    : the etl control procedures.

    The costing pass is row-by-row on purpose. Weighted-average cost depends on
    the running balance after every preceding movement for the same item and
    site, so the movements have to be walked in sequence. This is the slowest
    step in the nightly batch and has been on the "to be rewritten" list since
    2016.

    Regional costing methods diverge and are not interchangeable:
      NA   - moving weighted average, recalculated on every receipt.
      EU   - FIFO layers; the layer cost is taken from stg.StockMovement where
             the WMS has already consumed the layers.
      APAC - standard cost with the difference posted as purchase price
             variance, because the APAC ERP was acquired with the business and
             never converted.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactMovement', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactMovement;
GO

CREATE PROCEDURE Integration.usp_LoadFactMovement
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @LoadStartDate      DATE = NULL,
    @LoadEndDate        DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution   BIT = 0;
    DECLARE @SourceRowCount  BIGINT = 0;
    DECLARE @InsertRowCount  BIGINT = 0;
    DECLARE @DeleteRowCount  BIGINT = 0;
    DECLARE @RejectRowCount  BIGINT = 0;
    DECLARE @WatermarkFrom   NVARCHAR(50);
    DECLARE @WatermarkTo     NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Movement',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactMovement',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'WWI_WMS',
            @ObjectName       = N'Fact.Movement',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        SET @LoadStartDate = ISNULL(@LoadStartDate, TRY_CONVERT(DATE, @WatermarkFrom));
        SET @LoadEndDate   = ISNULL(@LoadEndDate, TRY_CONVERT(DATE, @WatermarkTo));
        IF @LoadStartDate IS NULL SET @LoadStartDate = CONVERT(DATE, '2013-01-01');
        IF @LoadEndDate   IS NULL SET @LoadEndDate   = CONVERT(DATE, SYSDATETIME());

        SELECT
            mv.MovementId,
            mv.MovementDate,
            mv.MovementTypeCode,
            mv.MovementReasonCode,
            mv.StockItemCode,
            mv.WarehouseSiteCode,
            mv.BinCode,
            mv.RegionCode,
            mv.Quantity,
            mv.UomCode,
            ISNULL(mv.UomConversionFactor, 1.0) AS UomConversionFactor,
            mv.UnitCost,
            mv.FifoLayerCost,
            mv.InvoiceNumber,
            mv.PoNumber,
            mv.StocktakeReference,
            CONVERT(DECIMAL(18, 4), NULL) AS WeightedAverageCost,
            CONVERT(DECIMAL(18, 4), NULL) AS StandardCost,
            CONVERT(DECIMAL(18, 2), NULL) AS PurchasePriceVariance,
            CONVERT(DECIMAL(18, 2), NULL) AS MovementValue,
            CONVERT(NVARCHAR(6), NULL)    AS CostingMethodCode
        INTO #MovementWork
        FROM stg.StockMovement AS mv
        WHERE mv.MovementDate >= @LoadStartDate
          AND mv.MovementDate <= @LoadEndDate;

        SET @SourceRowCount = @@ROWCOUNT;

        UPDATE w
        SET StandardCost = sc.StandardUnitCost
        FROM #MovementWork AS w
        OUTER APPLY
        (
            SELECT TOP (1) s.StandardUnitCost
            FROM stg.StandardCost AS s
            WHERE s.StockItemCode = w.StockItemCode
              AND s.RegionCode = w.RegionCode
              AND s.EffectiveFromDate <= w.MovementDate
            ORDER BY s.EffectiveFromDate DESC
        ) AS sc;

        /* Weighted-average walk for NA. One item/site at a time, in movement
           order, carrying the running balance and running value forward. */
        DECLARE @ItemCode   NVARCHAR(50);
        DECLARE @SiteCode   NVARCHAR(20);
        DECLARE @MovementId BIGINT;
        DECLARE @Quantity   DECIMAL(18, 4);
        DECLARE @UnitCost   DECIMAL(18, 4);
        DECLARE @PrevItem   NVARCHAR(50) = N'';
        DECLARE @PrevSite   NVARCHAR(20) = N'';
        DECLARE @RunQty     DECIMAL(18, 4) = 0;
        DECLARE @RunValue   DECIMAL(18, 2) = 0;
        DECLARE @AvgCost    DECIMAL(18, 4) = 0;

        DECLARE cost_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT StockItemCode, WarehouseSiteCode, MovementId,
                   Quantity * UomConversionFactor, UnitCost
            FROM #MovementWork
            WHERE RegionCode = N'NA'
            ORDER BY StockItemCode, WarehouseSiteCode, MovementDate, MovementId;

        OPEN cost_cursor;
        FETCH NEXT FROM cost_cursor INTO @ItemCode, @SiteCode, @MovementId, @Quantity, @UnitCost;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @ItemCode <> @PrevItem OR @SiteCode <> @PrevSite
            BEGIN
                SELECT @RunQty = ISNULL(SUM(f.[Quantity Base UOM]), 0),
                       @RunValue = ISNULL(SUM(f.[Movement Value Reporting]), 0)
                FROM Fact.[Movement] AS f
                INNER JOIN Dimension.[Stock Item] AS di
                    ON di.[Stock Item Key] = f.[Stock Item Key]
                INNER JOIN Dimension.[Warehouse Site] AS ds
                    ON ds.[Warehouse Site Key] = f.[Warehouse Site Key]
                WHERE di.[Stock Item Code] = @ItemCode
                  AND ds.[Site Code] = @SiteCode
                  AND f.[Date Key] < @LoadStartDate;

                SET @PrevItem = @ItemCode;
                SET @PrevSite = @SiteCode;
            END;

            IF @Quantity > 0
            BEGIN
                SET @RunValue = @RunValue + ROUND(@Quantity * ISNULL(@UnitCost, 0), 2);
                SET @RunQty   = @RunQty + @Quantity;
            END
            ELSE
            BEGIN
                SET @RunValue = @RunValue + ROUND(@Quantity * @AvgCost, 2);
                SET @RunQty   = @RunQty + @Quantity;
            END;

            SET @AvgCost = CASE WHEN @RunQty = 0 THEN @AvgCost
                                ELSE ROUND(@RunValue / @RunQty, 4) END;

            UPDATE #MovementWork
            SET WeightedAverageCost = @AvgCost,
                MovementValue       = ROUND(@Quantity * @AvgCost, 2),
                CostingMethodCode   = N'WAVG'
            WHERE MovementId = @MovementId;

            FETCH NEXT FROM cost_cursor INTO @ItemCode, @SiteCode, @MovementId, @Quantity, @UnitCost;
        END;

        CLOSE cost_cursor;
        DEALLOCATE cost_cursor;

        /* EU: FIFO layer cost comes from the WMS. */
        UPDATE #MovementWork
        SET MovementValue     = ROUND(Quantity * UomConversionFactor * ISNULL(FifoLayerCost, UnitCost), 2),
            CostingMethodCode = N'FIFO'
        WHERE RegionCode = N'EU';

        /* APAC: standard cost, variance to actual booked as PPV. */
        UPDATE #MovementWork
        SET MovementValue         = ROUND(Quantity * UomConversionFactor * ISNULL(StandardCost, UnitCost), 2),
            PurchasePriceVariance = CASE WHEN Quantity > 0 AND StandardCost IS NOT NULL
                                         THEN ROUND(Quantity * UomConversionFactor
                                                    * (ISNULL(UnitCost, StandardCost) - StandardCost), 2)
                                         ELSE 0 END,
            CostingMethodCode     = N'STD'
        WHERE RegionCode = N'APAC';

        SELECT @RejectRowCount = COUNT_BIG(*) FROM #MovementWork WHERE MovementValue IS NULL;

        IF @RejectRowCount > 0
        BEGIN
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_WMS',
                @ObjectName         = N'Fact.Movement',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'NO_COST',
                @RejectReason       = N'Movement could not be costed under any regional method',
                @RejectStage        = N'Fact';

            DELETE FROM #MovementWork WHERE MovementValue IS NULL;
        END;

        DELETE FROM Fact.[Movement]
        WHERE [Movement Date Key] >= @LoadStartDate
          AND [Movement Date Key] <= @LoadEndDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Movement]
        (
            [Date Key], [Movement Date Key], [Stock Item Key], [Warehouse Site Key],
            [Customer Key], [Supplier Key], [Transaction Type Key], [WWI Stock Item Transaction ID],
            [Quantity], [Lineage Key], [Region Code], [Bin Code], [Movement Reason Code],
            [Movement Direction Code], [Quantity Base UOM], [Quantity Source UOM],
            [Source UOM Code], [Weighted Average Cost], [Fifo Layer Cost], [Standard Cost],
            [Purchase Price Variance], [Movement Value], [Costing Method Code],
            [Invoice Number], [Po Number], [Stocktake Reference], [Natural Key Hash],
            [Batch Id], [Load Datetime]
        )
        SELECT
            w.MovementDate, w.MovementDate,
            ISNULL(item.[Stock Item Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            -1, -1,
            ISNULL(tt.[Transaction Type Key], 0),
            w.MovementId,
            w.Quantity * w.UomConversionFactor,
            0,
            w.RegionCode, w.BinCode, w.MovementReasonCode,
            CASE WHEN w.Quantity >= 0 THEN N'IN' ELSE N'OUT' END,
            w.Quantity * w.UomConversionFactor, w.Quantity, w.UomCode,
            w.WeightedAverageCost, w.FifoLayerCost, w.StandardCost,
            w.PurchasePriceVariance, w.MovementValue, w.CostingMethodCode,
            w.InvoiceNumber, w.PoNumber, w.StocktakeReference,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONVERT(NVARCHAR(30), w.MovementId))),
            @BatchId, SYSDATETIME()
        FROM #MovementWork AS w
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = w.StockItemCode
           AND w.MovementDate >= item.[Valid From] AND w.MovementDate < item.[Valid To]
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Site Code] = w.WarehouseSiteCode
        LEFT JOIN Dimension.[Transaction Type] AS tt
            ON tt.[Transaction Type Code] = w.MovementTypeCode;

        SET @InsertRowCount = @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Movement',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_WMS',
            @ObjectName         = N'Fact.Movement',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount,
                @RowsRejected       = @RejectRowCount;

        DROP TABLE #MovementWork;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'cost_cursor') >= 0
        BEGIN
            CLOSE cost_cursor;
            DEALLOCATE cost_cursor;
        END;

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Movement',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactMovement',
            @ErrorDescription   = @ErrorMessage;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Failed';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
