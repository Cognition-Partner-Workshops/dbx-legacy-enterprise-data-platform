/*
    stg.usp_TruncateAndReload_Product

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Called by     : STG_LOAD_PRODUCT (SSIS), after WORK_BUILD_PRODUCT_CROSSWALK
    Reads         : raw.OracleProductMaster, raw.SqlStockItem, work.ProductCrosswalk,
                    ref.UnitOfMeasure, ref.UomConversion, ref.FxRateDaily
    Writes        : stg.Product, err.RejectedProduct
    Control       : etl.usp_LogRowCount, etl.usp_LogError, err.usp_LogRejectedRows

    ERP products are the spine of the product dimension; the OLTP stock item only
    contributes the barcode, the retail price and the chiller flag, and only for
    the ~80% of rows MDM has crosswalked. The unmatched 20% still load - the
    warehouse would rather have a product with no stock item than no product -
    but they carry CrosswalkConfidenceCode = UNMATCHED so the dimension load can
    exclude them from the sales-facing hierarchy.

    Unit handling: base and sell UOM are conformed through ref.UnitOfMeasure and
    the source conversion factor is only trusted when both codes resolve. Weights
    arriving in LB (all NA-sourced rows and the older global rows) are converted
    to KG through ref.UomConversion so that the warehouse never sees two units.

    Standard cost is converted at the SPOT rate for the batch date; finance
    revalues at period end anyway, so a stale rate here is not worth a reject.
*/

IF OBJECT_ID(N'stg.usp_TruncateAndReload_Product', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateAndReload_Product;
GO

CREATE PROCEDURE stg.usp_TruncateAndReload_Product
(
    @BatchId            BIGINT,
    @PackageExecutionId BIGINT = NULL,
    @SourceSystemCode   NVARCHAR(20) = N'ORA_ERP'
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ObjectName   NVARCHAR(200) = N'stg.Product';
    DECLARE @SourceRows   BIGINT = 0;
    DECLARE @InsertedRows BIGINT = 0;
    DECLARE @DeletedRows  BIGINT = 0;
    DECLARE @RejectedRows BIGINT = 0;
    DECLARE @RateDate     DATE = CAST(SYSUTCDATETIME() AS DATE);
    DECLARE @LbToKgFactor DECIMAL(18,8);

    BEGIN TRY
        SELECT @LbToKgFactor = uc.ConversionFactor
        FROM ref.UomConversion AS uc
        WHERE uc.FromUomCode          = N'LB'
          AND uc.ToUomCode            = N'KG'
          AND uc.StockItemBusinessKey = N'*';

        --  The steward table has been missing this row twice in ten years, so the
        --  load carries the constant rather than producing NULL weights.
        SET @LbToKgFactor = ISNULL(@LbToKgFactor, 0.45359237);

        SELECT @SourceRows = COUNT_BIG(*)
        FROM raw.OracleProductMaster AS r
        WHERE r.BatchId = @BatchId;

        BEGIN TRANSACTION;

        DELETE FROM stg.Product
        WHERE BatchId = @BatchId;

        SET @DeletedRows = @@ROWCOUNT;

        INSERT INTO err.RejectedProduct
        (
            BatchId, PackageExecutionId, SourceSystemCode, SourceProductId, ProductBusinessKey,
            ProductName, Barcode, RejectReasonCode, RejectReason, RejectStage,
            FailedColumnName, FailedValue, RecordPayload
        )
        SELECT
            @BatchId,
            @PackageExecutionId,
            r.SourceSystemCode,
            r.PRODUCT_ID,
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRODUCT_ID, 1),
            LEFT(ISNULL(r.PRODUCT_DESC, N''), 400),
            si.Barcode,
            CASE
                WHEN stg.ufn_CleanString(r.PRODUCT_DESC, 0) IS NULL THEN N'MISSING_NAME'
                WHEN u1.UomCode IS NULL OR u2.UomCode IS NULL       THEN N'BAD_UOM'
                ELSE N'NEGATIVE_COST'
            END,
            CASE
                WHEN stg.ufn_CleanString(r.PRODUCT_DESC, 0) IS NULL
                    THEN N'PRODUCT_DESC is empty after cleaning'
                WHEN u1.UomCode IS NULL OR u2.UomCode IS NULL
                    THEN N'BASE_UOM_CD or SELL_UOM_CD is not in ref.UnitOfMeasure'
                ELSE N'STANDARD_COST_AMT is negative'
            END,
            N'Stage',
            CASE
                WHEN stg.ufn_CleanString(r.PRODUCT_DESC, 0) IS NULL THEN N'PRODUCT_DESC'
                WHEN u1.UomCode IS NULL                             THEN N'BASE_UOM_CD'
                WHEN u2.UomCode IS NULL                             THEN N'SELL_UOM_CD'
                ELSE N'STANDARD_COST_AMT'
            END,
            LEFT(CONCAT(r.PRODUCT_DESC, N' / ', r.BASE_UOM_CD, N' / ', r.SELL_UOM_CD,
                        N' / ', r.STANDARD_COST_AMT), 400),
            CONCAT(N'{"PRODUCT_ID":"', r.PRODUCT_ID, N'","PRODUCT_CD":"', r.PRODUCT_CD,
                   N'","BASE_UOM_CD":"', r.BASE_UOM_CD, N'","STANDARD_COST_AMT":"',
                   r.STANDARD_COST_AMT, N'"}')
        FROM raw.OracleProductMaster AS r
        LEFT JOIN ref.UnitOfMeasure AS u1
            ON u1.UomCode = UPPER(LTRIM(RTRIM(r.BASE_UOM_CD)))
        LEFT JOIN ref.UnitOfMeasure AS u2
            ON u2.UomCode = UPPER(LTRIM(RTRIM(r.SELL_UOM_CD)))
        LEFT JOIN work.ProductCrosswalk AS xw
            ON  xw.ErpProductBusinessKey = stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRODUCT_ID, 1)
            AND xw.BatchId               = @BatchId
        LEFT JOIN stg.StockItem AS si
            ON  si.OltpStockItemId = xw.OltpStockItemId
            AND si.BatchId         = @BatchId
        WHERE r.BatchId = @BatchId
          AND (
                  stg.ufn_CleanString(r.PRODUCT_DESC, 0) IS NULL
               OR u1.UomCode IS NULL
               OR u2.UomCode IS NULL
               OR stg.ufn_SafeDecimal(r.STANDARD_COST_AMT, N'.') < 0
              );

        SET @RejectedRows = @@ROWCOUNT;

        INSERT INTO stg.Product
        (
            ProductBusinessKey, SourceSystemCode, SourceProductId, ErpProductCode, OltpStockItemId,
            CrosswalkConfidenceCode, ProductName, ProductDescription, CategoryCode, SubCategoryCode,
            BrandName, BaseUomCode, SellUomCode, UomConversionFactor, StandardCostAmount,
            StandardCostCurrencyCode, StandardCostAmountUsd, ListPriceAmount, RecommendedRetailAmount,
            TaxClassCode, HazmatClassCode, TemperatureClassCode, IsChillerStock, ShelfLifeDays,
            CountryOfOriginCode, HsTariffCode, PrimarySupplierBusinessKey, LifecycleStatusCode,
            DiscontinuedDate, Barcode, TypicalWeightPerUnitKg, SourceModifiedDate, DqStatusCode,
            RowHash, ChangeHash, BatchId, PackageExecutionId
        )
        SELECT
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRODUCT_ID, 1),
            @SourceSystemCode,
            LTRIM(RTRIM(r.PRODUCT_ID)),
            stg.ufn_CleanString(r.PRODUCT_CD, 1),
            xw.OltpStockItemId,
            ISNULL(xw.MatchMethodCode, N'UNMATCHED'),
            LEFT(stg.ufn_CleanString(r.PRODUCT_DESC, 0), 200),
            LEFT(stg.ufn_CleanString(r.PRODUCT_LONG_DESC, 0), 1000),
            NULLIF(UPPER(LTRIM(RTRIM(r.CATEGORY_CD))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.SUB_CATEGORY_CD))), N''),
            COALESCE(si.BrandName, NULLIF(UPPER(LTRIM(RTRIM(r.BRAND_CD))), N'')),
            u1.UomCode,
            u2.UomCode,
            CASE
                WHEN u1.UomCode = u2.UomCode THEN CONVERT(DECIMAL(18,6), 1)
                ELSE COALESCE(
                        CONVERT(DECIMAL(18,6), stg.ufn_SafeDecimal(r.UOM_CONVERSION_FACTOR, N'.')),
                        CONVERT(DECIMAL(18,6), uc.ConversionFactor))
            END,
            CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(r.STANDARD_COST_AMT, N'.')),
            LEFT(UPPER(LTRIM(RTRIM(r.STANDARD_COST_CURR_CD))), 3),
            CONVERT(DECIMAL(19,4),
                stg.ufn_SafeDecimal(r.STANDARD_COST_AMT, N'.')
                * ISNULL(fx.ConversionRate,
                         CASE WHEN UPPER(LTRIM(RTRIM(r.STANDARD_COST_CURR_CD))) = N'USD' THEN 1 ELSE NULL END)),
            CONVERT(DECIMAL(19,4), stg.ufn_SafeDecimal(r.LIST_PRICE_AMT, N'.')),
            si.RecommendedRetailAmount,
            NULLIF(UPPER(LTRIM(RTRIM(r.TAX_CLASS_CD))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.HAZMAT_CLASS_CD))), N''),
            NULLIF(UPPER(LTRIM(RTRIM(r.TEMPERATURE_CLASS_CD))), N''),
            COALESCE(si.IsChillerStock,
                     CASE WHEN UPPER(LTRIM(RTRIM(r.TEMPERATURE_CLASS_CD))) IN (N'CHILLER', N'FROZEN')
                          THEN 1 ELSE 0 END),
            CONVERT(SMALLINT, stg.ufn_SafeDecimal(r.SHELF_LIFE_DAYS, N'.')),
            LEFT(UPPER(LTRIM(RTRIM(r.COUNTRY_OF_ORIGIN_CD))), 2),
            LEFT(REPLACE(LTRIM(RTRIM(r.HS_TARIFF_CD)), N'.', N''), 20),
            stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRIMARY_SUPP_ID, 1),
            NULLIF(UPPER(LTRIM(RTRIM(r.LIFECYCLE_STATUS_CD))), N''),
            stg.ufn_SafeDate(r.DISCONTINUED_DT, N'NA'),
            si.Barcode,
            --  NA source rows carry pounds; everything else is already metric.
            CASE
                WHEN si.TypicalWeightPerUnitKg IS NULL THEN NULL
                WHEN u1.UomCode = N'LB' THEN CONVERT(DECIMAL(18,3), si.TypicalWeightPerUnitKg * @LbToKgFactor)
                ELSE si.TypicalWeightPerUnitKg
            END,
            CONVERT(DATETIME2(3), stg.ufn_SafeDate(r.LAST_UPDATE_DT, N'NA')),
            CASE
                WHEN xw.OltpStockItemId IS NULL THEN N'WARN'
                WHEN stg.ufn_SafeDecimal(r.STANDARD_COST_AMT, N'.') IS NULL THEN N'WARN'
                ELSE N'PASS'
            END,
            HASHBYTES('SHA2_256',
                CONCAT(r.PRODUCT_DESC, N'|', r.CATEGORY_CD, N'|', r.SUB_CATEGORY_CD, N'|',
                       r.BRAND_CD, N'|', r.LIFECYCLE_STATUS_CD, N'|', r.TAX_CLASS_CD)),
            HASHBYTES('SHA2_256',
                CONCAT(r.BASE_UOM_CD, N'|', r.SELL_UOM_CD, N'|', r.STANDARD_COST_AMT, N'|',
                       r.PRIMARY_SUPP_ID, N'|', r.COUNTRY_OF_ORIGIN_CD, N'|', r.HS_TARIFF_CD)),
            @BatchId,
            @PackageExecutionId
        FROM raw.OracleProductMaster AS r
        INNER JOIN ref.UnitOfMeasure AS u1
            ON u1.UomCode = UPPER(LTRIM(RTRIM(r.BASE_UOM_CD)))
        INNER JOIN ref.UnitOfMeasure AS u2
            ON u2.UomCode = UPPER(LTRIM(RTRIM(r.SELL_UOM_CD)))
        LEFT JOIN ref.UomConversion AS uc
            ON  uc.FromUomCode          = u1.UomCode
            AND uc.ToUomCode            = u2.UomCode
            AND uc.StockItemBusinessKey = N'*'
        LEFT JOIN work.ProductCrosswalk AS xw
            ON  xw.ErpProductBusinessKey = stg.ufn_SourceSystemKey(r.SourceSystemCode, r.PRODUCT_ID, 1)
            AND xw.BatchId               = @BatchId
        LEFT JOIN stg.StockItem AS si
            ON  si.OltpStockItemId = xw.OltpStockItemId
            AND si.BatchId         = @BatchId
        OUTER APPLY
        (
            SELECT TOP (1) f.ConversionRate
            FROM ref.FxRateDaily AS f
            WHERE f.FromCurrencyCode = LEFT(UPPER(LTRIM(RTRIM(r.STANDARD_COST_CURR_CD))), 3)
              AND f.ToCurrencyCode   = N'USD'
              AND f.RateTypeCode     = N'SPOT'
              AND f.RateDate        <= @RateDate
            ORDER BY f.RateDate DESC
        ) AS fx
        WHERE r.BatchId = @BatchId
          AND stg.ufn_CleanString(r.PRODUCT_DESC, 0) IS NOT NULL
          AND ISNULL(stg.ufn_SafeDecimal(r.STANDARD_COST_AMT, N'.'), 0) >= 0;

        SET @InsertedRows = @@ROWCOUNT;

        COMMIT TRANSACTION;

        EXEC etl.usp_LogRowCount
            @PackageExecutionId = @PackageExecutionId,
            @ObjectName         = @ObjectName,
            @SourceRowCount     = @SourceRows,
            @TargetRowCount     = @InsertedRows,
            @InsertRowCount     = @InsertedRows,
            @DeleteRowCount     = @DeletedRows,
            @RejectRowCount     = @RejectedRows;

        IF @RejectedRows > 0
            EXEC err.usp_LogRejectedRows
                @BatchId            = @BatchId,
                @PackageExecutionId = @PackageExecutionId,
                @RejectTableName    = N'err.RejectedProduct',
                @ObjectName         = @ObjectName,
                @BusinessKeyColumn  = N'ProductBusinessKey';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        EXEC etl.usp_LogError
            @PackageExecutionId = @PackageExecutionId,
            @BatchId            = @BatchId,
            @ErrorSeverity      = N'Error',
            @SourceName         = N'STG_LOAD_PRODUCT',
            @SourceComponent    = @ObjectName,
            @ProcedureName      = N'stg.usp_TruncateAndReload_Product';

        THROW;
    END CATCH;

    RETURN 0;
END;
GO
