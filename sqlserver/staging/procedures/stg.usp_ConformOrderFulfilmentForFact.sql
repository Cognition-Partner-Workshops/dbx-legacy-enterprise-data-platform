/*
    stg.usp_ConformOrderFulfilmentForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_OrderFulfilment (SSIS)
    Reads         : stg.[Order], stg.OrderLine, stg.Sale, stg.CustomerTransaction,
                    ref.Region
    Writes        : work.OrderLineEnriched, stg.OrderFulfilment
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    The accumulating-snapshot fact wants one row per order with the milestone
    timestamps side by side. Nothing in the estate holds them that way, so the
    load first enriches the order lines into work.OrderLineEnriched - the lookup
    resolution that the order and sale loads each do separately - and then
    collapses that to the order grain.

    Allocation and picking are inferred, not recorded: an order is allocated once
    every line has a picked quantity greater than zero, and picked at the last
    line picking completion. Cash is the point at which the AR balance for the
    invoice reaches zero, which is why this load runs after the customer
    transaction conform and not before it.
*/

IF OBJECT_ID(N'stg.usp_ConformOrderFulfilmentForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformOrderFulfilmentForFact;
GO

CREATE PROCEDURE stg.usp_ConformOrderFulfilmentForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'WWI_OLTP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.OrderFulfilment';
    DECLARE @WorkObject   NVARCHAR(200) = N'work.OrderLineEnriched';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @EnrichedRows BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.[Order] AS o
        WHERE o.BatchId = @BatchId;

        DELETE FROM work.OrderLineEnriched
        WHERE BatchId = @BatchId;

        DELETE FROM stg.OrderFulfilment
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO work.OrderLineEnriched
        (
            BatchId, PackageExecutionId, OrderLineBusinessKey, OrderBusinessKey,
            CustomerBusinessKey, StockItemBusinessKey, SalespersonBusinessKey,
            PromotionBusinessKey, GeographyBusinessKey, RegionCode, OrderDate,
            OrderedQuantity, NetLineAmount, NetLineAmountUsd, TaxRegimeCode, TaxAmount,
            LookupFailureList, LookupFailureCount, IsReadyForFact
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            ol.OrderLineBusinessKey,
            ol.OrderBusinessKey,
            o.CustomerBusinessKey,
            ol.StockItemBusinessKey,
            o.SalespersonBusinessKey,
            COALESCE(ol.PromotionBusinessKey, o.PromotionBusinessKey),
            ca.GeographyBusinessKey,
            o.RegionCode,
            ol.OrderDate,
            ol.OrderedQuantity,
            ol.NetLineAmount,
            ol.NetLineAmountUsd,
            ol.TaxRegimeCode,
            ol.TaxAmount,
            CONCAT_WS(N',',
                CASE WHEN o.CustomerBusinessKey IS NULL      THEN N'CUSTOMER' END,
                CASE WHEN ol.StockItemBusinessKey IS NULL    THEN N'STOCKITEM' END,
                CASE WHEN o.SalespersonBusinessKey IS NULL   THEN N'SALESPERSON' END,
                CASE WHEN ca.GeographyBusinessKey IS NULL    THEN N'GEOGRAPHY' END),
            CASE WHEN o.CustomerBusinessKey IS NULL    THEN 1 ELSE 0 END
          + CASE WHEN ol.StockItemBusinessKey IS NULL  THEN 1 ELSE 0 END
          + CASE WHEN o.SalespersonBusinessKey IS NULL THEN 1 ELSE 0 END
          + CASE WHEN ca.GeographyBusinessKey IS NULL  THEN 1 ELSE 0 END,
            CASE
                WHEN o.CustomerBusinessKey IS NOT NULL
                     AND ol.StockItemBusinessKey IS NOT NULL THEN 1
                ELSE 0
            END
        FROM stg.OrderLine AS ol
        INNER JOIN stg.[Order] AS o
            ON  o.OrderBusinessKey = ol.OrderBusinessKey
            AND o.BatchId          = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) a.GeographyBusinessKey
            FROM stg.CustomerAddress AS a
            WHERE a.CustomerBusinessKey = o.CustomerBusinessKey
              AND a.BatchId             = @BatchId
              AND a.AddressUsageCode    = N'SHIPTO'
            ORDER BY a.StagingCustomerAddressId DESC
        ) AS ca
        WHERE ol.BatchId = @BatchId;

        SET @EnrichedRows = @@ROWCOUNT;

        WITH OrderMilestone AS
        (
            SELECT
                ole.OrderBusinessKey,
                ole.CustomerBusinessKey,
                ole.RegionCode,
                OrderedAt      = MIN(ole.OrderDate),
                OrderNetAmount = SUM(ISNULL(ole.NetLineAmount, 0)),
                OrderNetUsd    = SUM(ISNULL(ole.NetLineAmountUsd, 0)),
                LineCount      = COUNT_BIG(*),
                ReadyLineCount = SUM(CONVERT(INT, ole.IsReadyForFact))
            FROM work.OrderLineEnriched AS ole
            WHERE ole.BatchId = @BatchId
            GROUP BY ole.OrderBusinessKey, ole.CustomerBusinessKey, ole.RegionCode
        ),
        PickingMilestone AS
        (
            SELECT
                ol.OrderBusinessKey,
                PickedAt        = MAX(ol.PickingCompletedWhenUtc),
                UnpickedLines   = SUM(CASE WHEN ISNULL(ol.PickedQuantity, 0) = 0 THEN 1 ELSE 0 END)
            FROM stg.OrderLine AS ol
            WHERE ol.BatchId = @BatchId
            GROUP BY ol.OrderBusinessKey
        ),
        InvoiceMilestone AS
        (
            SELECT
                s.OrderBusinessKey,
                InvoicedAt      = MIN(s.InvoiceDateTimeUtc),
                InvoicedAmount  = SUM(ISNULL(s.SaleGrossAmount, 0)),
                CurrencyCode    = MIN(s.TransactionCurrencyCode),
                CashReceivedAt  = MAX(ct.SettledAt),
                CashReceived    = SUM(CASE WHEN ct.SettledAt IS NOT NULL
                                           THEN ISNULL(s.SaleGrossAmount, 0) ELSE 0 END),
                ModifiedAt      = MAX(ISNULL(s.SourceModifiedDate, s.LoadedAtUtc))
            FROM stg.Sale AS s
            OUTER APPLY
            (
                SELECT TOP (1)
                    SettledAt = CONVERT(DATETIME2(3), t.LastModifiedAt)
                FROM stg.CustomerTransaction AS t
                WHERE t.BatchId            = @BatchId
                  AND t.InvoiceNumber      = s.SourceInvoiceId
                  AND ISNULL(t.OutstandingBalance, 0) <= 0
                ORDER BY t.LastModifiedAt DESC
            ) AS ct
            WHERE s.BatchId = @BatchId
              AND s.OrderBusinessKey IS NOT NULL
            GROUP BY s.OrderBusinessKey
        )
        INSERT INTO stg.OrderFulfilment
        (
            OrderNumber, SourceSystemCode, OrderBusinessKey, CustomerBusinessKey, OrderedAt,
            AllocatedAt, PickedAt, InvoicedAt, CashReceivedAt, OrderNetAmount, InvoicedAmount,
            CashReceivedAmount, TransactionCurrency, OrderNetAmountUsd, DaysOrderToInvoice,
            DaysInvoiceToCash, FulfilmentStatusCode, RegionCode, LastModifiedAt, DqStatusCode,
            RowHash, BatchId, PackageExecutionId
        )
        SELECT
            om.OrderBusinessKey,
            @SourceSystemCode,
            om.OrderBusinessKey,
            om.CustomerBusinessKey,
            CONVERT(DATETIME2(3), om.OrderedAt),
            CASE WHEN ISNULL(pm.UnpickedLines, 1) = 0 THEN pm.PickedAt END,
            pm.PickedAt,
            im.InvoicedAt,
            im.CashReceivedAt,
            om.OrderNetAmount,
            im.InvoicedAmount,
            im.CashReceived,
            LEFT(COALESCE(im.CurrencyCode, N'USD'), 3),
            om.OrderNetUsd,
            DATEDIFF(DAY, om.OrderedAt, im.InvoicedAt),
            DATEDIFF(DAY, im.InvoicedAt, im.CashReceivedAt),
            CASE
                WHEN im.CashReceivedAt IS NOT NULL THEN N'SETTLED'
                WHEN im.InvoicedAt IS NOT NULL     THEN N'INVOICED'
                WHEN pm.PickedAt IS NOT NULL       THEN N'PICKED'
                ELSE N'OPEN'
            END,
            om.RegionCode,
            COALESCE(im.ModifiedAt, CONVERT(DATETIME2(3), om.OrderedAt)),
            CASE
                WHEN om.CustomerBusinessKey IS NULL             THEN N'FAIL'
                WHEN om.ReadyLineCount < om.LineCount           THEN N'WARN'
                WHEN im.InvoicedAt < CONVERT(DATETIME2(3), om.OrderedAt) THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(om.OrderBusinessKey, N'|', om.OrderNetAmount, N'|', im.InvoicedAmount,
                       N'|', im.InvoicedAt, N'|', im.CashReceivedAt)),
            @BatchId,
            @PackageExecutionId
        FROM OrderMilestone AS om
        LEFT JOIN PickingMilestone AS pm
            ON pm.OrderBusinessKey = om.OrderBusinessKey
        LEFT JOIN InvoiceMilestone AS im
            ON im.OrderBusinessKey = om.OrderBusinessKey;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.OrderFulfilment AS f
        WHERE f.BatchId      = @BatchId
          AND f.DqStatusCode = N'FAIL';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @WorkObject,
            @SourceRowCount     = @EnrichedRows,
            @TargetRowCount     = @EnrichedRows,
            @InsertRowCount     = @EnrichedRows,
            @RejectRowCount     = 0;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @RejectRowCount     = @FailRows;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'FACT_Load_OrderFulfilment',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformOrderFulfilmentForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
