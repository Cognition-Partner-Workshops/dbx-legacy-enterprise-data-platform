/*
    Integration.usp_LoadFactPurchaseReceipt

    Object        : Integration.usp_LoadFactPurchaseReceipt
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Purchase Receipt, Fact.Purchase.
    Called by     : FACT_Load_Purchase_Receipt, PRC_Load_Goods_Receipt.
    Reads         : stg.GoodsReceiptLine, stg.GoodsReceiptHeader,
                    stg.QualityInspectionResult.
    Depends on    : the etl control procedures.

    Insert-only. A goods receipt is never amended in the source - a mistake is
    corrected with a reversal receipt, which arrives as its own row with a
    negative quantity, so the fact simply loads what it is given.

    On-time and in-full are evaluated against the PO need-by date with a
    regional grace window, because the three regions signed different service
    terms with their carriers:
      NA   - same-day, no grace.
      EU   - one working day grace (the cross-docking hub).
      APAC - three calendar days grace on sea freight, none on air.

    Quality holds also diverge: APAC receipts are held pending inspection by
    default and released by the inspection feed, EU receipts are held only for
    controlled categories, NA receipts are never held on receipt.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactPurchaseReceipt', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactPurchaseReceipt;
GO

CREATE PROCEDURE Integration.usp_LoadFactPurchaseReceipt
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @ReloadFullHistory  BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Purchase_Receipt',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactPurchaseReceipt',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode  = N'WWI_WMS',
            @ObjectName        = N'Fact.Purchase Receipt',
            @ReloadFullHistory = @ReloadFullHistory,
            @WatermarkFrom     = @WatermarkFrom OUTPUT,
            @WatermarkTo       = @WatermarkTo OUTPUT;

        INSERT INTO Fact.[Purchase Receipt]
        (
            [Receipt Date Key], [Purchase Order Date Key], [Supplier Key], [Stock Item Key],
            [Warehouse Site Key], [Vendor Contract Key], [Currency Key], [Employee Key],
            [Region Code], [Receipt Number], [Receipt Line Number], [Po Number],
            [Po Line Number], [Delivery Note Number], [Container Reference],
            [Quantity Ordered], [Quantity Received], [Quantity Rejected], [Quantity On Hold],
            [Source UOM Code], [Over Receipt Quantity], [Transaction Currency Code],
            [Unit Cost], [Receipt Value], [Receipt Value Reporting], [Fx Rate],
            [Freight In Amount], [Customs Duty Amount], [Days Late], [On Time Flag],
            [In Full Flag], [Quality Status Code], [Natural Key Hash],
            [Inferred Member Flag], [Lineage Key], [Batch Id], [Load Datetime]
        )
        SELECT
            hdr.ReceiptDate,
            po.PoDate,
            ISNULL(sup.[Supplier Key], 0),
            ISNULL(item.[Stock Item Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            CASE WHEN hdr.ContractReference IS NULL THEN -1
                 ELSE ISNULL(vc.[Vendor Contract Key], 0) END,
            ISNULL(cur.[Currency Key], 0),
            CASE WHEN hdr.ReceivedByCode IS NULL THEN -1 ELSE ISNULL(emp.[Employee Key], 0) END,
            hdr.RegionCode,
            hdr.ReceiptNumber,
            lin.ReceiptLineNumber,
            lin.PoNumber,
            lin.PoLineNumber,
            hdr.DeliveryNoteNumber,
            hdr.ContainerReference,
            po.QuantityOrdered,
            lin.QuantityReceived,
            ISNULL(lin.QuantityRejected, 0),
            CASE
                WHEN hdr.RegionCode = N'APAC' THEN lin.QuantityReceived
                WHEN hdr.RegionCode = N'EU' AND item.[Controlled Category Flag] = 1 THEN lin.QuantityReceived
                ELSE 0
            END,
            lin.UomCode,
            CASE WHEN lin.QuantityReceived > po.QuantityOrdered
                 THEN lin.QuantityReceived - po.QuantityOrdered ELSE 0 END,
            hdr.CurrencyCode,
            lin.UnitCost,
            ROUND(lin.QuantityReceived * lin.UnitCost, 2),
            ROUND(lin.QuantityReceived * lin.UnitCost * ISNULL(hdr.FxRate, 1.0), 2),
            ISNULL(hdr.FxRate, 1.0),
            ISNULL(lin.FreightInAmount, 0),
            ISNULL(lin.CustomsDutyAmount, 0),
            DATEDIFF(DAY, po.NeedByDate, hdr.ReceiptDate),
            CASE
                WHEN hdr.RegionCode = N'NA'
                     AND DATEDIFF(DAY, po.NeedByDate, hdr.ReceiptDate) <= 0 THEN 1
                WHEN hdr.RegionCode = N'EU'
                     AND DATEDIFF(DAY, po.NeedByDate, hdr.ReceiptDate) <= 1 THEN 1
                WHEN hdr.RegionCode = N'APAC' AND hdr.TransportModeCode = N'SEA'
                     AND DATEDIFF(DAY, po.NeedByDate, hdr.ReceiptDate) <= 3 THEN 1
                WHEN hdr.RegionCode = N'APAC' AND hdr.TransportModeCode <> N'SEA'
                     AND DATEDIFF(DAY, po.NeedByDate, hdr.ReceiptDate) <= 0 THEN 1
                ELSE 0
            END,
            CASE WHEN lin.QuantityReceived >= po.QuantityOrdered THEN 1 ELSE 0 END,
            CASE
                WHEN qi.InspectionResultCode IS NOT NULL THEN qi.InspectionResultCode
                WHEN hdr.RegionCode = N'APAC' THEN N'PEND'
                WHEN hdr.RegionCode = N'EU' AND item.[Controlled Category Flag] = 1 THEN N'PEND'
                ELSE N'PASS'
            END,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(hdr.ReceiptNumber, N'|', lin.ReceiptLineNumber))),
            CASE WHEN sup.[Supplier Key] IS NULL OR item.[Stock Item Key] IS NULL THEN 1 ELSE 0 END,
            0,
            @BatchId,
            SYSDATETIME()
        FROM stg.GoodsReceiptLine AS lin
        INNER JOIN stg.GoodsReceiptHeader AS hdr
            ON hdr.ReceiptNumber = lin.ReceiptNumber
        LEFT JOIN stg.OraclePurchaseOrderLineFlat AS po
            ON po.PoNumber = lin.PoNumber
           AND po.PoLineNumber = lin.PoLineNumber
        LEFT JOIN Dimension.[Supplier] AS sup
            ON sup.[Supplier Reference] = hdr.SupplierBusinessKey
           AND hdr.ReceiptDate >= sup.[Valid From] AND hdr.ReceiptDate < sup.[Valid To]
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = lin.StockItemCode
           AND hdr.ReceiptDate >= item.[Valid From] AND hdr.ReceiptDate < item.[Valid To]
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Warehouse Site Code] = hdr.WarehouseSiteCode
        LEFT JOIN Dimension.[Vendor Contract] AS vc
            ON vc.[Contract Reference] = hdr.ContractReference
           AND hdr.ReceiptDate >= vc.[Valid From] AND hdr.ReceiptDate < vc.[Valid To]
        LEFT JOIN Dimension.[Currency] AS cur
            ON cur.[Currency Code] = hdr.CurrencyCode
        LEFT JOIN Dimension.[Employee] AS emp
            ON emp.[Employee Code] = hdr.ReceivedByCode
           AND hdr.ReceiptDate >= emp.[Valid From] AND hdr.ReceiptDate < emp.[Valid To]
        LEFT JOIN stg.QualityInspectionResult AS qi
            ON qi.ReceiptNumber = lin.ReceiptNumber
           AND qi.ReceiptLineNumber = lin.ReceiptLineNumber
        WHERE hdr.ReceiptDatetime > TRY_CONVERT(DATETIME2(3), @WatermarkFrom)
          AND NOT EXISTS
          (
              SELECT 1 FROM Fact.[Purchase Receipt] AS f
              WHERE f.[Receipt Number] = hdr.ReceiptNumber
                AND f.[Receipt Line Number] = lin.ReceiptLineNumber
          );

        SET @InsertRowCount = @@ROWCOUNT;
        SET @SourceRowCount = @InsertRowCount;

        /* Receipts against a PO the warehouse has never seen are counted but
           still loaded, because the stock physically arrived. */
        SELECT @RejectRowCount = COUNT_BIG(*)
        FROM Fact.[Purchase Receipt]
        WHERE [Batch Id] = @BatchId AND [Purchase Order Date Key] IS NULL;

        IF @RejectRowCount > 0
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'WWI_WMS',
                @ObjectName         = N'Fact.Purchase Receipt',
                @BusinessKey        = N'(grouped)',
                @RejectReasonCode   = N'RECEIPT_NO_PO',
                @RejectReason       = N'Receipt loaded with no matching purchase order line',
                @RejectStage        = N'Fact';

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Purchase Receipt',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'WWI_WMS',
            @ObjectName         = N'Fact.Purchase Receipt',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = @ErrorNumber,
            @SourceName         = N'Fact.Purchase Receipt',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactPurchaseReceipt',
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
