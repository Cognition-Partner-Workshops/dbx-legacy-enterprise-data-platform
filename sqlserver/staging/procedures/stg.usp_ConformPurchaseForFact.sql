/*
    stg.usp_ConformPurchaseForFact

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : FACT_Load_Purchase (SSIS)
    Reads         : stg.PurchaseOrder, stg.PurchaseOrderLine, stg.Receipt,
                    stg.ApInvoiceLine, stg.Supplier, stg.StockItem
    Writes        : work.PurchaseLineEnriched, stg.Purchase
    Control       : etl.usp_LogRowCount, etl.usp_LogError

    FACT_Purchase is at purchase order line grain but selects the supplier, the
    stock item and the buyer as if they were on the line, and it wants freight
    apportioned to the line rather than held on the header. The apportionment and
    the three-way match state are worked out once in work.PurchaseLineEnriched,
    which the purchase receipt load reads as well.

    Tax handling differs by region and the fact carries the recoverable part
    only: NA lines are quoted net of sales tax so nothing is recoverable, EU
    lines carry recoverable input VAT unless the supplier line is reverse
    charged, and APAC lines carry the GST input credit where the supplier is GST
    registered.
*/

IF OBJECT_ID(N'stg.usp_ConformPurchaseForFact', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_ConformPurchaseForFact;
GO

CREATE PROCEDURE stg.usp_ConformPurchaseForFact
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Purchase';
    DECLARE @WorkObject   NVARCHAR(200) = N'work.PurchaseLineEnriched';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @EnrichedRows BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @FailRows     BIGINT = 0;

    BEGIN TRY
        SELECT @SourceRows = COUNT_BIG(*)
        FROM stg.PurchaseOrderLine AS pol
        WHERE pol.BatchId = @BatchId;

        DELETE FROM work.PurchaseLineEnriched
        WHERE BatchId = @BatchId;

        DELETE FROM stg.Purchase
        WHERE BatchId = @BatchId;

        BEGIN TRANSACTION;

        INSERT INTO work.PurchaseLineEnriched
        (
            BatchId, PackageExecutionId, PurchaseOrderLineBusinessKey, PurchaseOrderBusinessKey,
            SupplierBusinessKey, ProductBusinessKey, CostCenterCode, ContractBusinessKey,
            LedgerCode, RegionCode, OrderDate, OrderQuantityBaseUom, ExtendedAmountUsd,
            RecoverableTaxAmount, ReceiptedQuantity, InvoicedQuantity, ThreeWayMatchStatusCode,
            PriceVariancePercent, ContractComplianceFlag, LookupFailureList, IsReadyForFact
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            pol.PurchaseOrderLineBusinessKey,
            pol.PurchaseOrderBusinessKey,
            po.SupplierBusinessKey,
            pol.ProductBusinessKey,
            COALESCE(pol.CostCenterCode, po.CostCenterCode),
            po.ContractBusinessKey,
            po.LedgerCode,
            po.RegionCode,
            po.OrderDate,
            pol.OrderQuantityBaseUom,
            pol.ExtendedAmountUsd,
            -- Recoverability is a regional rule, not a supplier attribute.
            CASE po.RegionCode
                WHEN N'EU'   THEN CASE WHEN s.VatRecoveryEligibleFlag = 1
                                       THEN ISNULL(pol.RecoverableTaxAmount, pol.TaxAmount)
                                       ELSE 0 END
                WHEN N'APAC' THEN CASE WHEN s.GstRegisteredFlag = 1
                                       THEN ISNULL(pol.TaxAmount, 0)
                                       ELSE 0 END
                ELSE CONVERT(DECIMAL(19,4), 0)
            END,
            ISNULL(rc.ReceivedQuantity, 0),
            ISNULL(il.InvoicedQuantity, 0),
            CASE
                WHEN ISNULL(il.InvoicedQuantity, 0) = 0                     THEN N'UNMATCHED'
                WHEN ISNULL(rc.ReceivedQuantity, 0) = 0                     THEN N'TWO_WAY'
                WHEN ABS(ISNULL(rc.ReceivedQuantity, 0)
                       - ISNULL(il.InvoicedQuantity, 0)) <= 0.001           THEN N'MATCHED'
                ELSE N'QTY_VARIANCE'
            END,
            CASE
                WHEN ISNULL(pol.UnitPriceAmount, 0) = 0 THEN NULL
                ELSE ROUND(100.0 * (ISNULL(il.InvoicedUnitPrice, pol.UnitPriceAmount)
                                    - pol.UnitPriceAmount) / pol.UnitPriceAmount, 4)
            END,
            CASE WHEN po.ContractBusinessKey IS NOT NULL THEN 1 ELSE 0 END,
            CONCAT_WS(N',',
                CASE WHEN po.SupplierBusinessKey IS NULL  THEN N'SUPPLIER' END,
                CASE WHEN pol.ProductBusinessKey IS NULL  THEN N'PRODUCT' END,
                CASE WHEN si.StockItemBusinessKey IS NULL THEN N'STOCKITEM' END),
            CASE
                WHEN po.SupplierBusinessKey IS NOT NULL
                     AND pol.ProductBusinessKey IS NOT NULL THEN 1
                ELSE 0
            END
        FROM stg.PurchaseOrderLine AS pol
        INNER JOIN stg.PurchaseOrder AS po
            ON  po.PurchaseOrderBusinessKey = pol.PurchaseOrderBusinessKey
            AND po.BatchId                  = @BatchId
        LEFT JOIN stg.Supplier AS s
            ON  s.SupplierBusinessKey = po.SupplierBusinessKey
            AND s.BatchId             = @BatchId
            AND s.IsSurvivorRow       = 1
        OUTER APPLY
        (
            SELECT TOP (1) si2.StockItemBusinessKey
            FROM stg.StockItem AS si2
            WHERE si2.ProductBusinessKey = pol.ProductBusinessKey
              AND si2.BatchId            = @BatchId
            ORDER BY si2.StagingStockItemId
        ) AS si
        OUTER APPLY
        (
            SELECT ReceivedQuantity = SUM(ISNULL(r.ReceivedQuantityBaseUom, r.ReceivedQuantity))
            FROM stg.Receipt AS r
            WHERE r.PurchaseOrderLineBusinessKey = pol.PurchaseOrderLineBusinessKey
              AND r.BatchId                      = @BatchId
        ) AS rc
        OUTER APPLY
        (
            SELECT
                InvoicedQuantity = SUM(ISNULL(ail.Quantity, 0)),
                InvoicedUnitPrice = MAX(ail.UnitPriceAmount)
            FROM stg.ApInvoiceLine AS ail
            WHERE ail.PurchaseOrderLineBusinessKey = pol.PurchaseOrderLineBusinessKey
              AND ail.BatchId                      = @BatchId
        ) AS il
        WHERE pol.BatchId = @BatchId;

        SET @EnrichedRows = @@ROWCOUNT;

        INSERT INTO stg.Purchase
        (
            PurchaseOrderLineBusinessKey, SourceSystemCode, PurchaseOrderNumber,
            PurchaseOrderLineNumber, SupplierBusinessKey, StockItemBusinessKey,
            OrderPlacedDate, ExpectedReceiptDate, QuantityOrdered, UnitCostAmount,
            FreightAmount, TransactionCurrency, ExtendedAmountUsd, RecoverableTaxAmount,
            SupplierRegionCode, BuyerCode, ThreeWayMatchStatusCode, LastModifiedAt,
            DqStatusCode, RowHash, BatchId, PackageExecutionId
        )
        SELECT
            pol.PurchaseOrderLineBusinessKey,
            @SourceSystemCode,
            po.PurchaseOrderNumber,
            pol.LineNumber,
            ple.SupplierBusinessKey,
            si.StockItemBusinessKey,
            po.OrderDate,
            COALESCE(pol.NeedByDate, po.PromisedDate, po.NeedByDate),
            pol.OrderQuantity,
            pol.UnitPriceAmount,
            -- Header freight is spread across the lines by extended value; the ERP
            -- has never held it on the line and finance has never wanted it there.
            CASE
                WHEN ISNULL(po.OrderTotalAmount, 0) = 0 THEN 0
                ELSE ROUND(ISNULL(po.FreightAmount, 0)
                           * ISNULL(pol.ExtendedAmount, 0) / po.OrderTotalAmount, 2)
            END,
            LEFT(ISNULL(po.TransactionCurrencyCode, N'USD'), 3),
            pol.ExtendedAmountUsd,
            ple.RecoverableTaxAmount,
            ISNULL(s.RegionCode, po.RegionCode),
            CONVERT(NVARCHAR(30), po.BuyerEmployeeKey),
            ple.ThreeWayMatchStatusCode,
            CONVERT(DATETIME2(3), COALESCE(po.SourceModifiedDate, pol.LoadedAtUtc)),
            CASE
                WHEN ple.IsReadyForFact = 0                      THEN N'FAIL'
                WHEN pol.OrderQuantity IS NULL                   THEN N'FAIL'
                WHEN ABS(ISNULL(ple.PriceVariancePercent, 0)) > 10 THEN N'WARN'
                ELSE pol.DqStatusCode
            END,
            HASHBYTES('SHA2_256',
                CONCAT(pol.PurchaseOrderLineBusinessKey, N'|', pol.OrderQuantity, N'|',
                       pol.UnitPriceAmount, N'|', po.OrderDate, N'|',
                       ple.ThreeWayMatchStatusCode)),
            @BatchId,
            @PackageExecutionId
        FROM work.PurchaseLineEnriched AS ple
        INNER JOIN stg.PurchaseOrderLine AS pol
            ON  pol.PurchaseOrderLineBusinessKey = ple.PurchaseOrderLineBusinessKey
            AND pol.BatchId                      = @BatchId
        INNER JOIN stg.PurchaseOrder AS po
            ON  po.PurchaseOrderBusinessKey = ple.PurchaseOrderBusinessKey
            AND po.BatchId                  = @BatchId
        LEFT JOIN stg.Supplier AS s
            ON  s.SupplierBusinessKey = ple.SupplierBusinessKey
            AND s.BatchId             = @BatchId
            AND s.IsSurvivorRow       = 1
        OUTER APPLY
        (
            SELECT TOP (1) si2.StockItemBusinessKey
            FROM stg.StockItem AS si2
            WHERE si2.ProductBusinessKey = ple.ProductBusinessKey
              AND si2.BatchId            = @BatchId
            ORDER BY si2.StagingStockItemId
        ) AS si
        WHERE ple.BatchId = @BatchId;

        SET @InsertedRows = @@ROWCOUNT;

        SELECT @FailRows = COUNT_BIG(*)
        FROM stg.Purchase AS p
        WHERE p.BatchId      = @BatchId
          AND p.DqStatusCode = N'FAIL';

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @WorkObject,
            @SourceRowCount     = @SourceRows,
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
            @SourceName         = N'FACT_Load_Purchase',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_ConformPurchaseForFact';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
