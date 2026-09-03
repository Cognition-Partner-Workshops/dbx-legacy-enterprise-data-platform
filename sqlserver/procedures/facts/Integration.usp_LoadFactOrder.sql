/*
    Integration.usp_LoadFactOrder

    Object        : Integration.usp_LoadFactOrder
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Order.Extensions and the dimension loads.
    Called by     : FACT_Load_Order (nightly) and SLS_Refresh_Backlog (hourly,
                    open orders only).
    Reads         : stg.SalesOrderLine, stg.SalesOrderHeader,
                    stg.BackorderReason.
    Depends on    : the etl control procedures.

    Load pattern  : MERGE, not delete-by-window. An order line changes state
                    for weeks (allocated, part-despatched, closed) and the
                    despatch and backlog reports both need the current state on
                    the same surrogate key, so the row is updated in place.
                    Fact.Order therefore has no correction rows at all - the
                    opposite choice to Fact.Sale, and the reason the two facts
                    disagree on a restated order until the invoice is raised.

    Open quantity is stored rather than derived because the backlog view is
    hit hundreds of times a day and computing it at query time was the largest
    single consumer of CPU on the 2018 server.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactOrder', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactOrder;
GO

CREATE PROCEDURE Integration.usp_LoadFactOrder
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @OpenOrdersOnly     BIT = 0,
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @UpdateRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Order',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactOrder',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode  = N'WWI_OLTP',
            @ObjectName        = N'Fact.Order',
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo OUTPUT;

        SELECT
            hdr.OrderNumber,
            lin.OrderLineNumber,
            hdr.OrderDate,
            hdr.RequestedDeliveryDate,
            hdr.PromisedDeliveryDate,
            hdr.RegionCode,
            hdr.CurrencyCode,
            hdr.CustomerBusinessKey,
            hdr.SalespersonCode,
            hdr.SalesChannelCode,
            hdr.CustomerSegmentCode,
            lin.StockItemCode,
            lin.PromotionCode,
            lin.QuantityOrdered,
            ISNULL(lin.QuantityAllocated, 0)  AS QuantityAllocated,
            ISNULL(lin.QuantityDespatched, 0) AS QuantityDespatched,
            lin.UnitPrice,
            ISNULL(lin.LineDiscountAmount, 0) AS LineDiscountAmount,
            lin.OrderLineStatusCode,
            lin.BackorderReasonCode,
            lin.SourceRowVersion,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(hdr.OrderNumber, N'|', lin.OrderLineNumber))) AS NaturalKeyHash
        INTO #OrderWork
        FROM stg.SalesOrderLine AS lin
        INNER JOIN stg.SalesOrderHeader AS hdr
            ON hdr.OrderNumber = lin.OrderNumber
        WHERE (@OpenOrdersOnly = 0 OR lin.OrderLineStatusCode IN (N'OPEN', N'PART', N'BACKORD'))
          AND (hdr.LastModifiedDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
               OR lin.LastModifiedDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
               OR @ReloadFullHistory = 1);

        SET @SourceRowCount = @@ROWCOUNT;

        /* Orders with a quantity of zero are a source-system artefact of the
           quotation module and are rejected as a group rather than row by row;
           there are thousands of them and the reject queue used to drown. */
        IF EXISTS (SELECT 1 FROM #OrderWork WHERE QuantityOrdered <= 0)
        BEGIN
            SELECT @RejectRowCount = COUNT_BIG(*) FROM #OrderWork WHERE QuantityOrdered <= 0;

            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_OLTP',
                @ObjectName         = N'Fact.Order',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'ZERO_QTY',
                @RejectReason       = N'Quotation-module order lines with zero quantity',
                @RejectStage        = N'Fact';

            DELETE FROM #OrderWork WHERE QuantityOrdered <= 0;
        END;

        MERGE Fact.[Order] AS tgt
        USING
        (
            SELECT
                w.*,
                ISNULL(cust.[Customer Key], 0)      AS CustomerKey,
                ISNULL(item.[Stock Item Key], 0)    AS StockItemKey,
                ISNULL(sp.[Salesperson Key], 0)     AS SalespersonKey,
                ISNULL(chan.[Sales Channel Key], 0) AS SalesChannelKey,
                ISNULL(seg.[Customer Segment Key], 0) AS CustomerSegmentKey,
                CASE WHEN w.PromotionCode IS NULL THEN -1
                     ELSE ISNULL(promo.[Promotion Key], 0) END AS PromotionKey,
                w.QuantityOrdered - w.QuantityDespatched AS QuantityOpen,
                ROUND(w.QuantityOrdered * w.UnitPrice, 2) AS GrossOrderValue,
                ROUND(w.QuantityOrdered * w.UnitPrice - w.LineDiscountAmount, 2) AS NetOrderValue,
                ROUND((w.QuantityOrdered - w.QuantityDespatched) * w.UnitPrice, 2) AS OpenOrderValue
            FROM #OrderWork AS w
            LEFT JOIN Dimension.[Customer] AS cust
                ON cust.[WWI Customer ID] = TRY_CONVERT(INT, w.CustomerBusinessKey)
               AND cust.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
            LEFT JOIN Dimension.[Stock Item] AS item
                ON item.[Stock Item Code] = w.StockItemCode
               AND item.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
            LEFT JOIN Dimension.[Salesperson] AS sp
                ON sp.[Salesperson Code] = w.SalespersonCode
               AND sp.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
            LEFT JOIN Dimension.[Sales Channel] AS chan
                ON chan.[Channel Code] = w.SalesChannelCode
            LEFT JOIN Dimension.[Customer Segment] AS seg
                ON seg.[Segment Code] = w.CustomerSegmentCode
               AND seg.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
            LEFT JOIN Dimension.[Promotion] AS promo
                ON promo.[Promotion Code] = w.PromotionCode
               AND promo.[Valid To] = CONVERT(DATETIME2(7), '9999-12-31')
        ) AS src
            ON tgt.[Natural Key Hash] = src.NaturalKeyHash
        WHEN MATCHED AND
        (
            tgt.[Quantity Despatched] <> src.QuantityDespatched
            OR tgt.[Quantity Allocated] <> src.QuantityAllocated
            OR ISNULL(tgt.[Order Line Status Code], N'') <> ISNULL(src.OrderLineStatusCode, N'')
            OR ISNULL(tgt.[Net Order Value], 0) <> src.NetOrderValue
        )
        THEN UPDATE SET
            tgt.[Quantity Allocated]     = src.QuantityAllocated,
            tgt.[Quantity Despatched]    = src.QuantityDespatched,
            tgt.[Quantity Open]          = src.QuantityOpen,
            tgt.[Order Line Status Code] = src.OrderLineStatusCode,
            tgt.[Backorder Reason Code]  = src.BackorderReasonCode,
            tgt.[Gross Order Value]      = src.GrossOrderValue,
            tgt.[Line Discount Amount]   = src.LineDiscountAmount,
            tgt.[Net Order Value]        = src.NetOrderValue,
            tgt.[Open Order Value]       = src.OpenOrderValue,
            tgt.[Promised Delivery Date] = src.PromisedDeliveryDate,
            tgt.[Batch Id]               = @BatchId,
            tgt.[Load Datetime]          = SYSDATETIME()
        WHEN NOT MATCHED BY TARGET
        THEN INSERT
        (
            [Order Date Key], [Requested Delivery Date Key], [Promised Delivery Date],
            [Customer Key], [Stock Item Key], [Salesperson Key], [Sales Channel Key],
            [Customer Segment Key], [Promotion Key], [Order Number], [Order Line Number],
            [Region Code], [Transaction Currency Code], [Quantity Ordered], [Quantity Allocated],
            [Quantity Despatched], [Quantity Open], [Unit Price], [Gross Order Value],
            [Line Discount Amount], [Net Order Value], [Open Order Value],
            [Order Line Status Code], [Backorder Reason Code], [Natural Key Hash],
            [Batch Id], [Load Datetime]
        )
        VALUES
        (
            src.OrderDate, src.RequestedDeliveryDate, src.PromisedDeliveryDate,
            src.CustomerKey, src.StockItemKey, src.SalespersonKey, src.SalesChannelKey,
            src.CustomerSegmentKey, src.PromotionKey, src.OrderNumber, src.OrderLineNumber,
            src.RegionCode, src.CurrencyCode, src.QuantityOrdered, src.QuantityAllocated,
            src.QuantityDespatched, src.QuantityOpen, src.UnitPrice, src.GrossOrderValue,
            src.LineDiscountAmount, src.NetOrderValue, src.OpenOrderValue,
            src.OrderLineStatusCode, src.BackorderReasonCode, src.NaturalKeyHash,
            @BatchId, SYSDATETIME()
        );

        SELECT @InsertRowCount = COUNT_BIG(*)
        FROM Fact.[Order]
        WHERE [Batch Id] = @BatchId AND [Load Datetime] >= DATEADD(HOUR, -12, SYSDATETIME());

        SET @UpdateRowCount = @SourceRowCount - @InsertRowCount;
        IF @UpdateRowCount < 0 SET @UpdateRowCount = 0;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Order',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @UpdateRowCount     = @UpdateRowCount,
            @RejectRowCount     = @RejectRowCount;

        IF @OpenOrdersOnly = 0
            EXECUTE etl.usp_SetWatermark
                @SourceSystemCode   = N'WWI_OLTP',
                @ObjectName         = N'Fact.Order',
                @WatermarkTo        = @WatermarkTo,
                @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsUpdated        = @UpdateRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;

        DROP TABLE #OrderWork;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Order',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactOrder',
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
