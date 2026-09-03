/*
    stg.usp_AppendIncremental_OrderLine

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_ORDERLINE (SSIS)
    Reads         : raw.SqlOrderLine, raw.SqlOrder, stg.[Order], stg.StockItem,
                    ref.Region, ref.FxRateDaily, ref.TaxJurisdiction
    Writes        : stg.OrderLine, err.RejectedOrderLine, work.LateArrivingDimensionQueue
    Control       : etl.usp_GetWatermark, etl.usp_SetWatermark, etl.usp_LogRowCount,
                    etl.usp_LogRejectedRecord, etl.usp_LogError

    Append-only incremental keyed on LastEditedWhen. The OLTP database keeps
    LastEditedWhen accurate (unlike Oracle), so the watermark is trustworthy, but
    the extract still overlaps by the configured grace window because the OLTP
    clock and the ETL server clock have never been in sync.

    Tax on an order line is calculated, not taken from the source: the OLTP holds
    a single TaxRate column that means three different things.
        NA   TaxRate is the combined state+county+city rate, so the line tax is
             calculated from the ship-to jurisdiction rate, not the OLTP rate.
        EU   TaxRate is the VAT rate and tax is calculated on the discounted net.
        APAC TaxRate is the GST rate and tax is calculated per line and rounded
             per line, which is why APAC invoice totals differ by cents from EU.
*/

IF OBJECT_ID(N'stg.usp_AppendIncremental_OrderLine', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_AppendIncremental_OrderLine;
GO

CREATE PROCEDURE stg.usp_AppendIncremental_OrderLine
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP',
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName    NVARCHAR(200) = N'stg.OrderLine';
    DECLARE @WatermarkFrom NVARCHAR(50);
    DECLARE @WatermarkTo   NVARCHAR(50);
    DECLARE @FromUtc       DATETIME2(3);
    DECLARE @SourceRows    BIGINT = 0;
    DECLARE @InsertedRows  BIGINT = 0;
    DECLARE @RejectedRows  BIGINT = 0;
    DECLARE @MaxEditedWhen DATETIME2(3);

    BEGIN TRY
        EXEC etl.usp_GetWatermark
            @SourceSystemCode  = @SourceSystemCode,
            @ObjectName        = @ObjectName,
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo   OUTPUT;

        SET @FromUtc = TRY_CONVERT(DATETIME2(3), @WatermarkFrom, 126);
        SET @FromUtc = ISNULL(@FromUtc, CONVERT(DATETIME2(3), '1900-01-01'));

        SELECT
            SourceOrderLineId   = LTRIM(RTRIM(l.OrderLineID)),
            SourceOrderId       = LTRIM(RTRIM(l.OrderID)),
            OrderLineBusinessKey = CONCAT(stg.ufn_SourceSystemKey(l.SourceSystemCode, l.OrderID, 1), N'|', LTRIM(RTRIM(l.OrderLineID))),
            OrderBusinessKey    = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.OrderID, 1),
            StockItemBusinessKey = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.StockItemID, 1),
            LineNumber          = CONVERT(INT, stg.ufn_SafeDecimal(l.OrderLineID, N'.')),
            LineDescription     = LEFT(stg.ufn_CleanString(l.Description, 0), 200),
            PackageTypeCode     = NULLIF(UPPER(LTRIM(RTRIM(l.PackageTypeID))), N''),
            OrderedQuantity     = CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(l.Quantity, N'.')),
            PickedQuantity      = CONVERT(DECIMAL(18,4), stg.ufn_SafeDecimal(l.PickedQuantity, N'.')),
            UnitPriceAmount     = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.UnitPrice, N'.')),
            SourceTaxRate       = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(l.TaxRate, N'.')),
            LineDiscountAmount  = CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(l.LineDiscountAmount, N'.')),
            LineDiscountPercent = CONVERT(DECIMAL(9,4), stg.ufn_SafeDecimal(l.LineDiscountPercent, N'.')),
            PromotionBusinessKey = stg.ufn_SourceSystemKey(l.SourceSystemCode, l.PromotionLineID, 1),
            PickingCompletedWhenUtc = CONVERT(DATETIME2(3), stg.ufn_SafeDate(l.PickingCompletedWhen, N'NA')),
            LineStatusCode      = COALESCE(cx.ConformedCodeValue, NULLIF(UPPER(LTRIM(RTRIM(l.LineStatusCode))), N''), N'UNKNOWN'),
            LastEditedWhenUtc   = CONVERT(DATETIME2(3), stg.ufn_SafeDate(l.LastEditedWhen, N'NA')),
            QuantityText        = l.Quantity,
            UnitPriceText       = l.UnitPrice,
            StockItemReference  = l.StockItemID
        INTO #IncomingOrderLine
        FROM raw.SqlOrderLine AS l
        LEFT JOIN ref.CodeCrosswalk AS cx
            ON  cx.CodeDomainCode   = N'ORDER_LINE_STATUS'
            AND cx.SourceSystemCode = l.SourceSystemCode
            AND cx.SourceCodeValue  = l.LineStatusCode
            AND cx.EffectiveToDate IS NULL
        WHERE l.BatchId = @BatchId
          AND (
                  @ReloadFullHistory = 1
               OR CONVERT(DATETIME2(3), stg.ufn_SafeDate(l.LastEditedWhen, N'NA')) > @FromUtc
               OR stg.ufn_SafeDate(l.LastEditedWhen, N'NA') IS NULL
              );

        SELECT @SourceRows = COUNT_BIG(*) FROM #IncomingOrderLine;

        BEGIN TRANSACTION;

        INSERT INTO err.RejectedOrderLine
        (
            BatchId, PackageExecutionId, SourceSystemCode, OrderBusinessKey, OrderLineBusinessKey,
            LineNumber, StockItemReference, OrderedQuantityText, UnitPriceText,
            RejectReasonCode, RejectReason, RejectStage, RecordPayload
        )
        SELECT
            @BatchId, @PackageExecutionId, @SourceSystemCode, i.OrderBusinessKey, i.OrderLineBusinessKey,
            CONVERT(NVARCHAR(20), i.LineNumber), i.StockItemReference, i.QuantityText, i.UnitPriceText,
            CASE
                WHEN i.OrderedQuantity IS NULL OR i.UnitPriceAmount IS NULL THEN N'BAD_NUMERIC'
                WHEN i.OrderedQuantity < 0                                  THEN N'NEG_QTY'
                ELSE N'ORPHAN_HEADER'
            END,
            CASE
                WHEN i.OrderedQuantity IS NULL OR i.UnitPriceAmount IS NULL
                    THEN N'Quantity or UnitPrice will not convert to a decimal'
                WHEN i.OrderedQuantity < 0
                    THEN N'Quantity is negative; returns belong on stg.Return, not on the order'
                ELSE N'no matching order header in stg.[Order] for this batch'
            END,
            N'Stage',
            CONCAT(N'{"OrderLineID":"', i.SourceOrderLineId, N'","OrderID":"', i.SourceOrderId,
                   N'","Quantity":"', i.QuantityText, N'","UnitPrice":"', i.UnitPriceText, N'"}')
        FROM #IncomingOrderLine AS i
        WHERE i.OrderedQuantity IS NULL
           OR i.UnitPriceAmount IS NULL
           OR i.OrderedQuantity < 0
           OR NOT EXISTS
              (
                  SELECT 1
                  FROM stg.[Order] AS o
                  WHERE o.OrderBusinessKey = i.OrderBusinessKey
                    AND o.BatchId          = @BatchId
              );

        SET @RejectedRows = @@ROWCOUNT;

        --  Stock items that have not reached staging yet are queued rather than
        --  rejected: the line is still a real order line and the dimension will
        --  catch up on a later batch.
        INSERT INTO work.LateArrivingDimensionQueue
        (
            BatchId, PackageExecutionId, DimensionName, MissingBusinessKey, SourceSystemCode,
            FirstSeenObjectName, OccurrenceCount, InferredAttributesJson
        )
        SELECT
            @BatchId, @PackageExecutionId, N'StockItem', i.StockItemBusinessKey, @SourceSystemCode,
            @ObjectName, COUNT_BIG(*),
            CONCAT(N'{"Description":"', MIN(i.LineDescription), N'"}')
        FROM #IncomingOrderLine AS i
        WHERE i.StockItemBusinessKey IS NOT NULL
          AND NOT EXISTS
              (
                  SELECT 1
                  FROM stg.StockItem AS s
                  WHERE s.StockItemBusinessKey = i.StockItemBusinessKey
                    AND s.BatchId              = @BatchId
              )
        GROUP BY i.StockItemBusinessKey;

        INSERT INTO stg.OrderLine
        (
            OrderLineBusinessKey, OrderBusinessKey, SourceSystemCode, LineNumber, StockItemBusinessKey,
            ProductBusinessKey, LineDescription, PackageTypeCode, OrderedQuantity, PickedQuantity,
            UnitPriceAmount, LineDiscountAmount, LineDiscountPercent, NetLineAmount, TaxRegimeCode,
            TaxRatePercent, TaxAmount, GrossLineAmount, TransactionCurrencyCode, NetLineAmountUsd,
            PromotionBusinessKey, PickingCompletedWhenUtc, LineStatusCode, OrderDate, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            i.OrderLineBusinessKey,
            i.OrderBusinessKey,
            @SourceSystemCode,
            i.LineNumber,
            i.StockItemBusinessKey,
            si.ProductBusinessKey,
            i.LineDescription,
            i.PackageTypeCode,
            i.OrderedQuantity,
            i.PickedQuantity,
            i.UnitPriceAmount,
            ISNULL(i.LineDiscountAmount, 0),
            i.LineDiscountPercent,
            n.NetLineAmount,
            rg.TaxRegimeCode,
            n.EffectiveTaxRate,
            n.TaxAmount,
            n.NetLineAmount + n.TaxAmount,
            o.TransactionCurrencyCode,
            CONVERT(DECIMAL(19,4), n.NetLineAmount * ISNULL(fx.ConversionRate, 1)),
            i.PromotionBusinessKey,
            i.PickingCompletedWhenUtc,
            i.LineStatusCode,
            o.OrderDate,
            CASE
                WHEN i.StockItemBusinessKey IS NULL THEN N'WARN'
                WHEN fx.ConversionRate IS NULL AND o.TransactionCurrencyCode <> N'USD' THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(i.OrderLineBusinessKey, N'|', i.OrderedQuantity, N'|', i.UnitPriceAmount, N'|',
                       i.LineDiscountAmount, N'|', i.LineStatusCode, N'|', n.TaxAmount)),
            @BatchId,
            @PackageExecutionId
        FROM #IncomingOrderLine AS i
        INNER JOIN stg.[Order] AS o
            ON  o.OrderBusinessKey = i.OrderBusinessKey
            AND o.BatchId          = @BatchId
        LEFT JOIN ref.Region AS rg
            ON rg.RegionCode = o.RegionCode
        LEFT JOIN stg.StockItem AS si
            ON  si.StockItemBusinessKey = i.StockItemBusinessKey
            AND si.BatchId              = @BatchId
        OUTER APPLY
        (
            --  NA only: the combined rate for the customer's ship-to jurisdiction
            --  beats the single OLTP TaxRate column.
            SELECT TOP (1) j.CombinedRatePercent
            FROM stg.CustomerAddress AS ca
            INNER JOIN ref.TaxJurisdiction AS j
                ON  j.TaxJurisdictionCode = ca.TaxJurisdictionCode
                AND j.EffectiveToDate IS NULL
            WHERE rg.TaxRegimeCode        = N'SALESTAX'
              AND ca.CustomerBusinessKey  = o.CustomerBusinessKey
              AND ca.BatchId              = @BatchId
            ORDER BY CASE WHEN ca.AddressUsageCode = N'SHIPTO' THEN 0 ELSE 1 END,
                     ca.IsPrimaryAddress DESC
        ) AS tj
        CROSS APPLY
        (
            SELECT
                NetLineAmount    = CONVERT(DECIMAL(19,4),
                                       (i.OrderedQuantity * i.UnitPriceAmount) - ISNULL(i.LineDiscountAmount, 0)),
                EffectiveTaxRate = COALESCE(tj.CombinedRatePercent, i.SourceTaxRate, 0)
        ) AS r0
        CROSS APPLY
        (
            SELECT
                r0.NetLineAmount,
                r0.EffectiveTaxRate,
                TaxAmount =
                    CASE rg.TaxRegimeCode
                         --  APAC rounds GST per line to the minor unit.
                         WHEN N'GST' THEN ROUND(r0.NetLineAmount * r0.EffectiveTaxRate / 100.0, 2)
                         --  EU keeps four decimals on the line and rounds VAT at invoice level.
                         WHEN N'VAT' THEN CONVERT(DECIMAL(19,4), r0.NetLineAmount * r0.EffectiveTaxRate / 100.0)
                         --  NA uses the jurisdiction rate and rounds half up per line.
                         ELSE ROUND(r0.NetLineAmount * r0.EffectiveTaxRate / 100.0, 2)
                    END
        ) AS n
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = o.TransactionCurrencyCode
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = N'SPOT'
              AND f.RateDate        <= o.OrderDate
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE i.OrderedQuantity IS NOT NULL
          AND i.UnitPriceAmount IS NOT NULL
          AND i.OrderedQuantity >= 0;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        SELECT @MaxEditedWhen = MAX(i.LastEditedWhenUtc) FROM #IncomingOrderLine AS i;

        IF @MaxEditedWhen IS NOT NULL
            SET @WatermarkTo = CONVERT(NVARCHAR(50), @MaxEditedWhen, 126);

        EXEC etl.usp_SetWatermark
            @SourceSystemCode   = @SourceSystemCode,
            @ObjectName         = @ObjectName,
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedOrderLine',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'OrderLineBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_ORDERLINE',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_AppendIncremental_OrderLine';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
