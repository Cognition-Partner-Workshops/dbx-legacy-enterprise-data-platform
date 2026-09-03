/*
    Integration.usp_LoadFactPurchase

    Object        : Integration.usp_LoadFactPurchase
    Deploy target : WideWorldImportersDW
    Deploy order  : after Fact.Purchase.Extensions and the dimension loads.
    Called by     : FACT_Load_Purchase, PRC_Load_Purchase_Oracle.
    Reads         : stg.OraclePurchaseOrderLine, stg.OraclePurchaseOrderHeader,
                    stg.VendorContractPrice, stg.ExchangeRateDaily.
    Depends on    : the etl control procedures.

    The purchase side comes out of the Oracle procurement schema, so the
    business keys are the Oracle PO_HDR_ID / PO_LINE_ID pair rather than a
    document number, and the document number itself (PO number) is carried as
    a degenerate dimension.

    Incoterms drive the cost build-up and diverge by region:
      NA   - DDP, duty and freight already inside the supplier unit price;
             landed cost equals extended cost.
      EU   - DAP inside the customs union with reverse-charge VAT, freight
             billed separately by the carrier and allocated by weight.
      APAC - FOB, so freight, insurance and customs duty are all added on top
             and the landed cost can be 20% above the PO value.

    Contract price variance is calculated here rather than in the aggregate
    because the contract that was live on the PO date is needed, and the
    contract dimension is SCD2.
*/
IF OBJECT_ID(N'Integration.usp_LoadFactPurchase', N'P') IS NOT NULL
    DROP PROCEDURE Integration.usp_LoadFactPurchase;
GO

CREATE PROCEDURE Integration.usp_LoadFactPurchase
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

    DECLARE @OwnsExecution  BIT = 0;
    DECLARE @SourceRowCount BIGINT = 0;
    DECLARE @InsertRowCount BIGINT = 0;
    DECLARE @DeleteRowCount BIGINT = 0;
    DECLARE @RejectRowCount BIGINT = 0;
    DECLARE @WatermarkFrom  NVARCHAR(50);
    DECLARE @WatermarkTo    NVARCHAR(50);

    IF @PackageExecutionId IS NULL
    BEGIN
        EXECUTE etl.usp_LogPackageStart
            @BatchId            = @BatchId,
            @PackageName        = N'FACT_Load_Purchase',
            @ProjectName        = N'WWI_Facts',
            @StepName           = N'LoadFactPurchase',
            @PackageExecutionId = @PackageExecutionId OUTPUT;
        SET @OwnsExecution = 1;
    END;

    BEGIN TRY
        EXECUTE etl.usp_GetWatermark
            @SourceSystemCode = N'ORA_PROC',
            @ObjectName       = N'Fact.Purchase',
            @WatermarkFrom    = @WatermarkFrom OUTPUT,
            @WatermarkTo      = @WatermarkTo OUTPUT;

        SET @LoadStartDate = ISNULL(@LoadStartDate, TRY_CONVERT(DATE, @WatermarkFrom));
        SET @LoadEndDate   = ISNULL(@LoadEndDate, TRY_CONVERT(DATE, @WatermarkTo));
        IF @LoadStartDate IS NULL SET @LoadStartDate = CONVERT(DATE, '2013-01-01');
        IF @LoadEndDate   IS NULL SET @LoadEndDate   = CONVERT(DATE, SYSDATETIME());

        SELECT
            hdr.PO_NUMBER          AS PoNumber,
            lin.PO_LINE_NUM        AS PoLineNumber,
            hdr.PO_DATE            AS PoDate,
            lin.NEED_BY_DATE       AS ExpectedReceiptDate,
            hdr.SUPPLIER_NUM       AS SupplierBusinessKey,
            lin.ITEM_CODE          AS StockItemCode,
            hdr.COST_CENTER_CODE   AS CostCenterCode,
            hdr.SITE_CODE          AS WarehouseSiteCode,
            hdr.BUYER_CODE         AS BuyerCode,
            hdr.CONTRACT_REF       AS ContractReference,
            hdr.REGION_CD          AS RegionCode,
            hdr.CURRENCY_CD        AS CurrencyCode,
            hdr.INCOTERM_CD        AS IncotermCode,
            hdr.PAYMENT_TERMS_CD   AS PaymentTermsCode,
            lin.QTY_ORDERED        AS QuantityOrdered,
            lin.UOM_CD             AS SourceUomCode,
            ISNULL(lin.UOM_CONV_FACTOR, 1.0) AS UomConversionFactor,
            lin.UNIT_PRICE         AS UnitPrice,
            ISNULL(lin.FREIGHT_AMT, 0)  AS FreightAmount,
            ISNULL(lin.DUTY_AMT, 0)     AS DutyAmount,
            ISNULL(lin.INSURANCE_AMT, 0) AS InsuranceAmount,
            lin.LINE_STATUS_CD     AS LineStatusCode,
            CONVERT(VARBINARY(32), HASHBYTES('SHA2_256',
                CONCAT(hdr.PO_NUMBER, N'|', lin.PO_LINE_NUM))) AS NaturalKeyHash
        INTO #PurchaseWork
        FROM stg.OraclePurchaseOrderLine AS lin
        INNER JOIN stg.OraclePurchaseOrderHeader AS hdr
            ON hdr.PO_HDR_ID = lin.PO_HDR_ID
        WHERE hdr.PO_DATE >= @LoadStartDate
          AND hdr.PO_DATE <= @LoadEndDate
          AND hdr.PO_STATUS_CD NOT IN (N'CANC', N'DRAFT');

        SET @SourceRowCount = @@ROWCOUNT;

        /* Oracle cancels a line by leaving it in place with a null price. Those
           rows are routed to the reject queue with the PO number as the key so
           procurement can chase the buyer. */
        DECLARE @RejectKey NVARCHAR(200);
        DECLARE reject_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT CONCAT(PoNumber, N'/', PoLineNumber)
            FROM #PurchaseWork
            WHERE UnitPrice IS NULL OR QuantityOrdered IS NULL OR QuantityOrdered <= 0;

        OPEN reject_cursor;
        FETCH NEXT FROM reject_cursor INTO @RejectKey;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXECUTE etl.usp_LogRejectedRecord
                @PackageExecutionId = @PackageExecutionId,
                @BatchId            = @BatchId,
                @SourceSystemCode   = N'ORA_PROC',
                @ObjectName         = N'Fact.Purchase',
                @BusinessKey        = @RejectKey,
                @RejectReasonCode   = N'PO_LINE_INVALID',
                @RejectReason       = N'PO line has no price or no quantity',
                @RejectStage        = N'Fact';

            SET @RejectRowCount = @RejectRowCount + 1;
            FETCH NEXT FROM reject_cursor INTO @RejectKey;
        END;

        CLOSE reject_cursor;
        DEALLOCATE reject_cursor;

        DELETE FROM #PurchaseWork
        WHERE UnitPrice IS NULL OR QuantityOrdered IS NULL OR QuantityOrdered <= 0;

        DELETE FROM Fact.[Purchase]
        WHERE [Purchase Date Key] >= @LoadStartDate
          AND [Purchase Date Key] <= @LoadEndDate;

        SET @DeleteRowCount = @@ROWCOUNT;

        INSERT INTO Fact.[Purchase]
        (
            [Purchase Date Key], [Expected Receipt Date], [Supplier Key], [Stock Item Key],
            [Cost Center Key], [Warehouse Site Key], [Buyer Employee Key], [Vendor Contract Key],
            [Payment Terms Key], [Po Number], [Po Line Number], [Region Code], [Incoterm Code],
            [Transaction Currency Code], [Fx Rate], [Quantity Ordered], [Quantity Base UOM],
            [Source UOM Code], [Unit Price], [Extended Cost Amount], [Freight In Amount],
            [Customs Duty Amount], [Insurance Amount], [Landed Cost Amount],
            [Landed Cost Reporting], [Contract Unit Price], [Contract Price Variance],
            [Line Status Code], [Natural Key Hash], [Inferred Member Flag],
            [Batch Id], [Load Datetime]
        )
        SELECT
            w.PoDate,
            w.ExpectedReceiptDate,
            ISNULL(sup.[Supplier Key], 0),
            ISNULL(item.[Stock Item Key], 0),
            ISNULL(cc.[Cost Center Key], 0),
            ISNULL(site.[Warehouse Site Key], 0),
            CASE WHEN w.BuyerCode IS NULL THEN -1 ELSE ISNULL(emp.[Employee Key], 0) END,
            CASE WHEN w.ContractReference IS NULL THEN -1 ELSE ISNULL(vc.[Vendor Contract Key], 0) END,
            ISNULL(terms.[Payment Terms Key], 0),
            w.PoNumber,
            w.PoLineNumber,
            w.RegionCode,
            w.IncotermCode,
            w.CurrencyCode,
            ISNULL(fx.RateToReporting, 1.0),
            w.QuantityOrdered,
            w.QuantityOrdered * w.UomConversionFactor,
            w.SourceUomCode,
            w.UnitPrice,
            ROUND(w.QuantityOrdered * w.UnitPrice, 2),
            /* Freight/duty/insurance only build into landed cost where the
               incoterm leaves them with us. */
            CASE WHEN w.RegionCode = N'NA' THEN 0 ELSE w.FreightAmount END,
            CASE WHEN w.RegionCode = N'NA' THEN 0 ELSE w.DutyAmount END,
            CASE WHEN w.RegionCode = N'APAC' THEN w.InsuranceAmount ELSE 0 END,
            ROUND(w.QuantityOrdered * w.UnitPrice
                  + CASE WHEN w.RegionCode = N'NA' THEN 0 ELSE w.FreightAmount + w.DutyAmount END
                  + CASE WHEN w.RegionCode = N'APAC' THEN w.InsuranceAmount ELSE 0 END, 2),
            ROUND((w.QuantityOrdered * w.UnitPrice
                  + CASE WHEN w.RegionCode = N'NA' THEN 0 ELSE w.FreightAmount + w.DutyAmount END
                  + CASE WHEN w.RegionCode = N'APAC' THEN w.InsuranceAmount ELSE 0 END)
                  * ISNULL(fx.RateToReporting, 1.0), 2),
            cp.ContractUnitPrice,
            CASE WHEN cp.ContractUnitPrice IS NULL THEN NULL
                 ELSE ROUND((w.UnitPrice - cp.ContractUnitPrice) * w.QuantityOrdered, 2) END,
            w.LineStatusCode,
            w.NaturalKeyHash,
            CASE WHEN sup.[Supplier Key] IS NULL THEN 1 ELSE 0 END,
            @BatchId,
            SYSDATETIME()
        FROM #PurchaseWork AS w
        LEFT JOIN Dimension.[Supplier] AS sup
            ON sup.[Supplier Reference] = w.SupplierBusinessKey
           AND w.PoDate >= sup.[Valid From] AND w.PoDate < sup.[Valid To]
        LEFT JOIN Dimension.[Stock Item] AS item
            ON item.[Stock Item Code] = w.StockItemCode
           AND w.PoDate >= item.[Valid From] AND w.PoDate < item.[Valid To]
        LEFT JOIN Dimension.[Cost Center] AS cc
            ON cc.[Cost Center Code] = w.CostCenterCode
           AND w.PoDate >= cc.[Valid From] AND w.PoDate < cc.[Valid To]
        LEFT JOIN Dimension.[Warehouse Site] AS site
            ON site.[Site Code] = w.WarehouseSiteCode
        LEFT JOIN Dimension.[Employee] AS emp
            ON emp.[Employee Code] = w.BuyerCode
           AND w.PoDate >= emp.[Valid From] AND w.PoDate < emp.[Valid To]
        LEFT JOIN Dimension.[Vendor Contract] AS vc
            ON vc.[Contract Reference] = w.ContractReference
           AND w.PoDate >= vc.[Valid From] AND w.PoDate < vc.[Valid To]
        LEFT JOIN Dimension.[Payment Terms] AS terms
            ON terms.[Terms Code] = w.PaymentTermsCode
        OUTER APPLY
        (
            SELECT TOP (1) r.RateToReporting
            FROM stg.ExchangeRateDaily AS r
            WHERE r.CurrencyCode = w.CurrencyCode
              AND r.RateSourceCode = N'GROUP'
              AND r.RateDate <= w.PoDate
            ORDER BY r.RateDate DESC
        ) AS fx
        OUTER APPLY
        (
            SELECT TOP (1) c.ContractUnitPrice
            FROM stg.VendorContractPrice AS c
            WHERE c.ContractReference = w.ContractReference
              AND c.ItemCode = w.StockItemCode
              AND c.EffectiveFromDate <= w.PoDate
              AND (c.EffectiveToDate IS NULL OR c.EffectiveToDate > w.PoDate)
            ORDER BY c.EffectiveFromDate DESC
        ) AS cp;

        SET @InsertRowCount = @@ROWCOUNT;

        EXECUTE etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = N'Fact.Purchase',
            @SourceRowCount     = @SourceRowCount,
            @InsertRowCount     = @InsertRowCount,
            @DeleteRowCount     = @DeleteRowCount,
            @RejectRowCount     = @RejectRowCount;

        EXECUTE etl.usp_SetWatermark
            @SourceSystemCode   = N'ORA_PROC',
            @ObjectName         = N'Fact.Purchase',
            @WatermarkTo        = @WatermarkTo,
            @PackageExecutionId = @PackageExecutionId;

        IF @OwnsExecution = 1
            EXECUTE etl.usp_LogPackageEnd
                @PackageExecutionId = @PackageExecutionId,
                @Status             = N'Succeeded',
                @RowsRead           = @SourceRowCount,
                @RowsInserted       = @InsertRowCount,
                @RowsDeleted        = @DeleteRowCount,
                @RowsRejected       = @RejectRowCount,
                @WatermarkFrom      = @WatermarkFrom,
                @WatermarkTo        = @WatermarkTo;

        DROP TABLE #PurchaseWork;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();

        IF CURSOR_STATUS('local', 'reject_cursor') >= 0
        BEGIN
            CLOSE reject_cursor;
            DEALLOCATE reject_cursor;
        END;

        EXECUTE etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @ErrorCode          = ERROR_NUMBER(),
            @SourceName         = N'Fact.Purchase',
            @SourceComponent    = N'Fact load',
            @ProcedureName      = N'Integration.usp_LoadFactPurchase',
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
