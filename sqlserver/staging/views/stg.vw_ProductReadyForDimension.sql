/*
    stg.vw_ProductReadyForDimension

    Deploy target : WideWorldImporters_Staging  (SQLSERVER_STAGING_DB)
    Read by       : the DIM_Product warehouse load

    The estate has two product masters that were never merged: the ERP item master
    and the OLTP stock item list. This view is the single place where they are
    presented as one row per conformed product, using work.ProductCrosswalk when
    it has resolved a pair and falling back to whichever side exists on its own.
    Unmatched OLTP-only items are still published - they sell, so the warehouse
    needs them - but they carry ProductSourceCode = 'OLTP_ONLY' so the finance
    reports can exclude items that have no standard cost.
*/

IF OBJECT_ID(N'stg.vw_ProductReadyForDimension', N'V') IS NOT NULL
    DROP VIEW stg.vw_ProductReadyForDimension;
GO

CREATE VIEW stg.vw_ProductReadyForDimension
AS
SELECT
    COALESCE(p.ProductBusinessKey, si.StockItemBusinessKey)     AS ProductBusinessKey,
    CASE
        WHEN p.ProductBusinessKey IS NOT NULL AND si.StockItemBusinessKey IS NOT NULL THEN N'MATCHED'
        WHEN p.ProductBusinessKey IS NOT NULL THEN N'ERP_ONLY'
        ELSE N'OLTP_ONLY'
    END                                                         AS ProductSourceCode,
    p.ErpProductCode,
    si.OltpStockItemId,
    p.CrosswalkConfidenceCode,
    COALESCE(p.ProductName, si.StockItemName)                   AS ProductName,
    p.ProductDescription,
    COALESCE(p.BrandName, si.BrandName)                         AS BrandName,
    p.CategoryCode,
    p.SubCategoryCode,
    COALESCE(p.BaseUomCode, N'EA')                              AS BaseUomCode,
    p.SellUomCode,
    p.UomConversionFactor,
    p.StandardCostAmountUsd,
    COALESCE(si.UnitPriceAmount, p.ListPriceAmount)             AS ListPriceAmount,
    COALESCE(si.RecommendedRetailAmount, p.RecommendedRetailAmount) AS RecommendedRetailAmount,
    p.TaxClassCode,
    p.HazmatClassCode,
    p.TemperatureClassCode,
    COALESCE(si.IsChillerStock, p.IsChillerStock)               AS IsChillerStock,
    p.ShelfLifeDays,
    p.CountryOfOriginCode,
    p.HsTariffCode,
    COALESCE(p.PrimarySupplierBusinessKey, si.SupplierBusinessKey) AS PrimarySupplierBusinessKey,
    COALESCE(p.LifecycleStatusCode, N'ACTIVE')                  AS LifecycleStatusCode,
    p.DiscontinuedDate,
    COALESCE(p.Barcode, si.Barcode)                             AS Barcode,
    COALESCE(p.TypicalWeightPerUnitKg, si.TypicalWeightPerUnitKg) AS TypicalWeightPerUnitKg,
    si.UnitPackageCode,
    si.OuterPackageCode,
    si.QuantityPerOuter,
    si.MarketingTagList,
    COALESCE(p.RowHash, si.RowHash)                             AS RowHash,
    COALESCE(p.ChangeHash, si.ChangeHash)                       AS ChangeHash,
    COALESCE(p.BatchId, si.BatchId)                             AS BatchId,
    COALESCE(p.PackageExecutionId, si.PackageExecutionId)       AS PackageExecutionId,
    COALESCE(p.LoadedAtUtc, si.LoadedAtUtc)                     AS LoadedAtUtc
FROM stg.Product AS p
FULL OUTER JOIN stg.StockItem AS si
    ON  si.ProductBusinessKey = p.ProductBusinessKey
    AND si.BatchId            = p.BatchId
WHERE ISNULL(p.DqStatusCode, N'PASS') IN (N'PASS', N'WARN');
GO
