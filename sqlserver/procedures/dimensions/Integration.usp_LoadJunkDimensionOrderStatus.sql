/*
    Object        : [Integration].[usp_LoadJunkDimensionOrderStatus]
    Deploy target : WideWorldImportersDW
    Depends on    : stg.Order, Dimension.Order Status Junk,
                    the etl control framework
    Called by     : DIM_Refresh_OrderStatusJunk, run before the order and sale
                    fact loads

    The junk dimension for the eleven order-level flags plus the three status
    codes. The full Cartesian product is over a hundred thousand combinations and
    fewer than nine hundred have ever occurred, so this is built on demand: the
    order staging table is scanned for combinations not already present and only
    those are added. That is the 2013 rewrite; the original built the whole
    product at deployment and the fact-load lookup was measurably slower for it.

    [Combination Hash] is what the fact load joins on - fourteen equality
    predicates was the previous version and it did not use the index.

    Regional divergence lands here through the flags themselves rather than
    through regional columns: [Is Tax Exempt] is a resale certificate in NA and a
    reverse-charge VAT flag in EU, and [Is Marketplace Order] is almost always
    zero outside APAC. The dimension carries no region because an order status
    combination is regionless by construction, which routinely confuses new
    analysts.
*/
SET NOCOUNT ON;
GO

CREATE PROCEDURE [Integration].[usp_LoadJunkDimensionOrderStatus]
    @BatchId            BIGINT,
    @PackageName        NVARCHAR(200) = N'DIM_Refresh_OrderStatusJunk',
    @SourceSystemCode   NVARCHAR(20)  = N'SQL_OLTP',
    @LineageKey         INT           = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageExecutionId BIGINT;
    DECLARE @Now                DATETIME2(7) = SYSDATETIME();
    DECLARE @SourceRowCount     BIGINT = 0;
    DECLARE @InsertedCount      BIGINT = 0;
    DECLARE @UpdatedCount       BIGINT = 0;
    DECLARE @ErrorMessage       NVARCHAR(MAX);

    EXEC [etl].[usp_LogPackageStart]
         @BatchId            = @BatchId,
         @PackageName        = @PackageName,
         @ProjectName        = N'WWI_DW_Dimensions',
         @StepName           = N'JunkDimension',
         @PackageExecutionId = @PackageExecutionId OUTPUT;

    BEGIN TRY
        IF OBJECT_ID(N'tempdb..#Combination') IS NOT NULL DROP TABLE #Combination;

        SELECT DISTINCT
              UPPER(ISNULL(o.[OrderStatusCode], N'NEW'))            AS [Order Status Code]
            , UPPER(ISNULL(o.[FulfilmentStatusCode], N'NONE'))      AS [Fulfilment Status Code]
            , UPPER(ISNULL(o.[PaymentStatusCode], N'UNPAID'))       AS [Payment Status Code]
            , CONVERT(BIT, ISNULL(o.[IsBackorder], 0))              AS [Is Backorder]
            , CONVERT(BIT, ISNULL(o.[IsUndersupply], 0))            AS [Is Undersupply]
            , CONVERT(BIT, ISNULL(o.[IsRushOrder], 0))              AS [Is Rush Order]
            , CONVERT(BIT, ISNULL(o.[IsGiftOrder], 0))              AS [Is Gift Order]
            , CONVERT(BIT, ISNULL(o.[IsCreditHeld], 0))             AS [Is Credit Held]
            , CONVERT(BIT, ISNULL(o.[IsManualPriceOverride], 0))    AS [Is Manual Price Override]
            , CONVERT(BIT, ISNULL(o.[IsPromotionApplied], 0))       AS [Is Promotion Applied]
            , CONVERT(BIT, ISNULL(o.[IsTaxExempt], 0))              AS [Is Tax Exempt]
            , CONVERT(BIT, ISNULL(o.[IsCrossBorder], 0))            AS [Is Cross Border]
            , CONVERT(BIT, ISNULL(o.[IsMarketplaceOrder], 0))       AS [Is Marketplace Order]
            , CONVERT(BIT, ISNULL(o.[IsSubscriptionOrder], 0))      AS [Is Subscription Order]
        INTO #Combination
        FROM [stg].[Order] AS o;

        SET @SourceRowCount = @@ROWCOUNT;

        ALTER TABLE #Combination ADD [Combination Hash] VARBINARY(32) NULL;

        UPDATE #Combination
        SET [Combination Hash] = HASHBYTES(N'SHA2_256',
                CONCAT_WS(N'|', [Order Status Code], [Fulfilment Status Code], [Payment Status Code],
                          CONVERT(NVARCHAR(1), [Is Backorder]), CONVERT(NVARCHAR(1), [Is Undersupply]),
                          CONVERT(NVARCHAR(1), [Is Rush Order]), CONVERT(NVARCHAR(1), [Is Gift Order]),
                          CONVERT(NVARCHAR(1), [Is Credit Held]),
                          CONVERT(NVARCHAR(1), [Is Manual Price Override]),
                          CONVERT(NVARCHAR(1), [Is Promotion Applied]),
                          CONVERT(NVARCHAR(1), [Is Tax Exempt]),
                          CONVERT(NVARCHAR(1), [Is Cross Border]),
                          CONVERT(NVARCHAR(1), [Is Marketplace Order]),
                          CONVERT(NVARCHAR(1), [Is Subscription Order])));

        INSERT INTO [Dimension].[Order Status Junk]
            ([Order Status Code], [Order Status Description], [Fulfilment Status Code],
             [Payment Status Code], [Is Backorder], [Is Undersupply], [Is Rush Order],
             [Is Gift Order], [Is Credit Held], [Is Manual Price Override],
             [Is Promotion Applied], [Is Tax Exempt], [Is Cross Border],
             [Is Marketplace Order], [Is Subscription Order], [Status Group Code],
             [Exception Flag Count], [Requires Attention], [Combination Hash],
             [First Seen On], [First Seen Batch Id], [Occurrence Count], [Last Load Batch Id])
        SELECT
              c.[Order Status Code]
            , CASE c.[Order Status Code]
                  WHEN N'NEW'  THEN N'Entered, not yet confirmed'
                  WHEN N'CONF' THEN N'Confirmed'
                  WHEN N'PICK' THEN N'Picking'
                  WHEN N'PACK' THEN N'Packed'
                  WHEN N'SHIP' THEN N'Shipped'
                  WHEN N'INV'  THEN N'Invoiced'
                  WHEN N'CANC' THEN N'Cancelled'
                  WHEN N'HOLD' THEN N'On hold'
                  ELSE CONCAT(N'Unmapped status ', c.[Order Status Code])
              END
            , c.[Fulfilment Status Code]
            , c.[Payment Status Code]
            , c.[Is Backorder], c.[Is Undersupply], c.[Is Rush Order], c.[Is Gift Order]
            , c.[Is Credit Held], c.[Is Manual Price Override], c.[Is Promotion Applied]
            , c.[Is Tax Exempt], c.[Is Cross Border], c.[Is Marketplace Order]
            , c.[Is Subscription Order]
            , CASE
                  WHEN c.[Order Status Code] IN (N'CANC')              THEN N'CLOSED'
                  WHEN c.[Order Status Code] = N'INV'                  THEN N'CLOSED'
                  WHEN c.[Order Status Code] = N'HOLD'                 THEN N'EXCEPTION'
                  WHEN c.[Is Credit Held] = 1 OR c.[Is Undersupply] = 1 THEN N'EXCEPTION'
                  WHEN c.[Order Status Code] = N'NEW'                  THEN N'OPEN'
                  ELSE N'INPROGRESS'
              END
            , CONVERT(SMALLINT, c.[Is Backorder] + c.[Is Undersupply] + c.[Is Credit Held]
                              + c.[Is Manual Price Override])
            , CASE WHEN c.[Is Credit Held] = 1
                        OR c.[Is Undersupply] = 1
                        OR (c.[Is Backorder] = 1 AND c.[Is Rush Order] = 1)
                   THEN 1 ELSE 0 END
            , c.[Combination Hash]
            , @Now
            , @BatchId
            , 0
            , @BatchId
        FROM #Combination AS c
        WHERE NOT EXISTS (SELECT 1
                          FROM [Dimension].[Order Status Junk] AS d
                          WHERE d.[Combination Hash] = c.[Combination Hash]);

        SET @InsertedCount = @@ROWCOUNT;

        /* The attention rule was re-cut in 2018 and existing combinations were
           never restated, so it is reapplied over the whole dimension each run. */
        UPDATE d
        SET d.[Requires Attention]  = CASE WHEN d.[Is Credit Held] = 1
                                                OR d.[Is Undersupply] = 1
                                                OR (d.[Is Backorder] = 1 AND d.[Is Rush Order] = 1)
                                           THEN 1 ELSE 0 END,
            d.[Exception Flag Count]= CONVERT(SMALLINT,
                                          CONVERT(INT, d.[Is Backorder]) + CONVERT(INT, d.[Is Undersupply])
                                        + CONVERT(INT, d.[Is Credit Held])
                                        + CONVERT(INT, d.[Is Manual Price Override])),
            d.[Last Load Batch Id]  = @BatchId
        FROM [Dimension].[Order Status Junk] AS d
        WHERE d.[Order Status Junk Key] > 0;

        SET @UpdatedCount = @@ROWCOUNT;

        INSERT INTO [Integration].[DimensionLoadAudit]
            ([Dimension Name], [Region Code], [Batch Id], [Package Execution Id],
             [Source Row Count], [Type 1 Update Count], [New Member Count],
             [Load Pattern], [Started On], [Completed On])
        VALUES
            (N'Order Status Junk', N'GLOBAL', @BatchId, @PackageExecutionId,
             @SourceRowCount, @UpdatedCount, @InsertedCount, N'JunkOnDemand', @Now, SYSDATETIME());

        EXEC [etl].[usp_LogRowCount]
             @PackageExecutionId = @PackageExecutionId,
             @ObjectName         = N'Dimension.Order Status Junk',
             @SourceRowCount     = @SourceRowCount,
             @InsertRowCount     = @InsertedCount,
             @UpdateRowCount     = @UpdatedCount;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Succeeded',
             @RowsRead           = @SourceRowCount,
             @RowsInserted       = @InsertedCount,
             @RowsUpdated        = @UpdatedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();

        EXEC [etl].[usp_LogError]
             @PackageExecutionId = @PackageExecutionId,
             @BatchId            = @BatchId,
             @ErrorSeverity      = N'Error',
             @SourceName         = @PackageName,
             @SourceComponent    = N'Dimension.Order Status Junk',
             @ProcedureName      = N'Integration.usp_LoadJunkDimensionOrderStatus',
             @ErrorDescription   = @ErrorMessage;

        EXEC [etl].[usp_LogPackageEnd]
             @PackageExecutionId = @PackageExecutionId,
             @Status             = N'Failed',
             @RowsRead           = @SourceRowCount;

        THROW;
    END CATCH;
END;
GO
