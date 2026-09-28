"""Per-dimension staging -> dimension transformations (the Derived Column / Lookup / Conditional Split logic of
the DIM_Load_* data flows and the attribute conditioning inside Integration.usp_Load*Dimension).

Every function is a pure DataFrame -> DataFrame transform (no Delta IO) so it can be unit tested; the notebooks
wire IO, lookups against other dimensions and the etl.* lifecycle around them.
"""

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from wwi_dimensions.runtime import band

REGION_TAX_TREATMENT = {"NA": "SALES_TAX_EXCLUSIVE", "EU": "VAT_INCLUSIVE", "APAC": "GST_INCLUSIVE"}
CREDIT_LIMIT_BANDS = ((10_000, "BRONZE"), (50_000, "SILVER"), (250_000, "GOLD"))
QUOTA_BANDS = ((250_000, "Q1"), (1_000_000, "Q2"), (5_000_000, "Q3"))
PRICE_BANDS = ((10, "LOW"), (50, "MID"), (250, "HIGH"))
COST_BANDS = ((5, "C1"), (25, "C2"), (100, "C3"))
SEGMENT_TIERS = ((250, "T4"), (500, "T3"), (750, "T2"))


def _paymentDays(termsCol):
    """'N30' / 'NET45' / 'COD' -> 30 / 45 / 0 (Integration.usp_MigrateStagedSupplierData)."""
    digits = F.regexp_extract(F.upper(termsCol), r"(\d+)", 1)
    return F.when(digits == "", F.lit(0)).otherwise(digits.cast("int"))


# ----------------------------------------------------------------------------------------------------------------
# Supplier / Vendor Contract / Stock Item (hybrid family)
# ----------------------------------------------------------------------------------------------------------------

def transformSupplier(stg: DataFrame) -> DataFrame:
    """stg.Supplier -> Dimension.Supplier attributes. Only survivor rows of a duplicate group become members."""
    df = stg.where(F.coalesce(F.col("IsSurvivorRow"), F.lit(True)) == True)  # noqa: E712
    return df.select(
        "SupplierBusinessKey",
        F.col("OltpSupplierId").cast("int").alias("WWISupplierID"),
        "SourceSystemCode",
        F.col("SourceSupplierId").alias("SourceSupplierReference"),
        F.coalesce(F.col("SupplierNameStandardized"), F.col("SupplierName")).alias("Supplier"),
        F.coalesce(F.col("ErpSupplierNumber"), F.col("SupplierShortName")).alias("SupplierReference"),
        F.col("SupplierCategoryCode").alias("CategoryCode"),
        F.lit(None).cast("string").alias("PrimaryContact"),
        F.coalesce(F.upper(F.col("RegionCode")), F.lit("GLOBAL")).alias("RegionCode"),
        F.lit(None).cast("string").alias("CountryCode"),
        F.coalesce(F.col("SupplierStatusCode"), F.lit("ACTIVE")).alias("SupplierStatusCode"),
        F.when(F.col("SupplierStatusCode").isin("APPROVED", "ACTIVE"), F.lit("APPROVED"))
        .when(F.col("SupplierStatusCode").isin("PENDING", "ONBOARDING"), F.lit("PENDING"))
        .otherwise(F.lit("NOT_APPROVED")).alias("ApprovalStatusCode"),
        (F.upper(F.coalesce(F.col("ScorecardRatingCode"), F.lit(""))).isin("A", "PREFERRED", "STRATEGIC")).alias("IsPreferredSupplier"),
        F.col("ScorecardRatingCode").alias("RiskRatingCode"),
        F.when(F.upper(F.col("ScorecardRatingCode")) == "A", 10).when(F.upper(F.col("ScorecardRatingCode")) == "B", 30)
        .when(F.upper(F.col("ScorecardRatingCode")) == "C", 60).when(F.col("ScorecardRatingCode").isNull(), F.lit(None)).otherwise(90).cast("int").alias("SupplierRiskScore"),
        F.col("LeadTimeDays").cast("int").alias("ContractLeadTimeDays"),
        F.lit(None).cast("decimal(5,2)").alias("QualityRating"),
        "PaymentTermsCode",
        _paymentDays(F.col("PaymentTermsCode")).alias("PaymentDays"),
        "PaymentMethodCode",
        F.col("TransactionCurrencyCode").alias("SettlementCurrencyCode"),
        F.lit(None).cast("string").alias("BankCountryCode"),
        (F.coalesce(F.col("WithholdingCode"), F.lit("NONE")) != "NONE").alias("IsCrossBorderPayee"),
        F.when(F.col("OnHoldFlag") == True, F.lit("BLOCKED")).otherwise(F.lit("CLEARED")).alias("SanctionScreeningStatus"),  # noqa: E712
        F.to_date(F.col("SourceModifiedDate")).alias("SanctionScreenedOn"),
        F.col("DiversityClassCode").alias("DiversityClassificationCode"),
        F.coalesce(F.col("OnHoldFlag"), F.lit(False)).alias("OnHoldFlag"),
        "HoldReasonCode",
        (F.coalesce(F.col("IsSurvivorRow"), F.lit(True)) == False).alias("IsSupersededDuplicate"),  # noqa: E712
        "SourceModifiedDate",
    )


def transformVendorContract(stg: DataFrame, businessDate) -> DataFrame:
    """stg.VendorContract -> Dimension.Vendor Contract. Amendments of one contract number are separate business keys."""
    return stg.select(
        "ContractBusinessKey",
        "ContractNumber",
        F.coalesce(F.regexp_extract(F.col("ContractBusinessKey"), r"-A(\d+)$", 1).cast("smallint"), F.lit(0)).alias("AmendmentNumber"),
        "SupplierBusinessKey",
        F.concat_ws(" ", F.col("ContractNumber"), F.col("ContractTypeCode")).alias("ContractTitle"),
        "ContractTypeCode",
        F.when(F.col("EndDate") < F.lit(str(businessDate)).cast("date"), F.lit("EXPIRED"))
        .otherwise(F.coalesce(F.col("ContractStatusCode"), F.lit("ACTIVE"))).alias("ContractStatusCode"),
        F.coalesce(F.upper(F.col("RegionCode")), F.lit("GLOBAL")).alias("RegionCode"),
        F.col("GoverningLawCode").alias("GoverningLawCountryCode"),
        F.when(F.col("GoverningLawCode").isin("US", "USA", "CA", "CAN", "MX", "MEX"), F.lit("NA"))
        .when(F.col("GoverningLawCode").isin("SG", "SGP", "AU", "AUS", "JP", "JPN", "HK", "HKG", "NZ", "NZL", "IN", "IND"), F.lit("APAC"))
        .when(F.col("GoverningLawCode").isNull(), F.lit(None)).otherwise(F.lit("EU")).alias("GoverningLawRegion"),
        F.col("TransactionCurrencyCode").alias("ContractCurrencyCode"),
        F.col("StartDate").alias("ContractStartDate"),
        F.col("EndDate").alias("ContractEndDate"),
        F.coalesce(F.col("AutoRenewFlag"), F.lit(False)).alias("AutoRenewFlag"),
        F.col("NoticePeriodDays").cast("int").alias("RenewalNoticeDays"),
        F.col("CommittedAmount").cast("decimal(18,2)").alias("CommittedSpendAmount"),
        F.col("CommittedAmountUsd").cast("decimal(18,2)").alias("CommittedSpendReporting"),
        F.col("ConsumedAmount").cast("decimal(18,2)").alias("ConsumedAmount"),
        F.lit(None).cast("string").alias("PaymentTermsCode"),
        F.when(F.col("TopRebatePercent") > 0, F.lit("REBATE")).otherwise(F.lit("NONE")).alias("PriceProtectionCode"),
        F.when(F.col("RebateTierCount") >= 1, F.col("TopRebatePercent") / F.greatest(F.col("RebateTierCount").cast("decimal(9,4)"), F.lit(1))).cast("decimal(9,4)").alias("RebateTier1Percentage"),
        F.when(F.col("RebateTierCount") >= 2, F.col("TopRebatePercent")).cast("decimal(9,4)").alias("RebateTier2Percentage"),
        F.when(F.col("RebateTierCount").isNull() | (F.col("RebateTierCount") == 0), F.lit("NONE"))
        .otherwise(F.concat(F.lit("TIER"), F.col("RebateTierCount").cast("string"))).alias("RebateTierCode"),
        F.lit(None).cast("decimal(9,4)").alias("ServiceLevelTargetPct"),
        F.col("SignedDate").alias("SignedOn"),
        "SourceSystemCode",
        F.lit(None).cast("bigint").alias("ClosedByLineageKey"),
        F.col("StartDate").cast("timestamp").alias("ContractStartTimestamp"),
    )


def transformStockItem(stg: DataFrame, product: Optional[DataFrame]) -> DataFrame:
    """stg.StockItem (+ stg.Product crosswalk on ProductBusinessKey) -> Dimension.Stock Item."""
    s = stg.alias("s")
    if product is not None:
        p = product.select(
            F.col("ProductBusinessKey").alias("_pbk"), F.col("CategoryCode").alias("_CategoryCode"),
            F.col("StandardCostAmount").alias("_StandardCostAmount"), F.col("TaxClassCode").alias("_TaxClassCode"),
            F.col("HazmatClassCode").alias("_HazmatClassCode"), F.col("LifecycleStatusCode").alias("_LifecycleStatusCode"),
            F.col("DiscontinuedDate").alias("_DiscontinuedDate"), F.col("UomConversionFactor").alias("_UomConversionFactor"),
            F.col("ProductDescription").alias("_ProductDescription"),
            F.col("PrimarySupplierBusinessKey").alias("_PrimarySupplierBusinessKey"),
        ).dropDuplicates(["_pbk"])
        df = s.join(p, F.col("s.ProductBusinessKey") == F.col("_pbk"), "left")
    else:
        df = s
        for c in ("_CategoryCode", "_TaxClassCode", "_HazmatClassCode", "_LifecycleStatusCode", "_ProductDescription", "_PrimarySupplierBusinessKey"):
            df = df.withColumn(c, F.lit(None).cast("string"))
        df = df.withColumn("_StandardCostAmount", F.lit(None).cast("decimal(19,4)")).withColumn("_DiscontinuedDate", F.lit(None).cast("date")).withColumn("_UomConversionFactor", F.lit(None).cast("decimal(18,6)"))
    unitPrice = F.col("UnitPriceAmount").cast("decimal(18,2)")
    stdCost = F.col("_StandardCostAmount").cast("decimal(18,4)")
    return df.select(
        F.col("s.StockItemBusinessKey").alias("StockItemBusinessKey"),
        F.col("OltpStockItemId").cast("int").alias("WWIStockItemID"),
        F.col("s.SourceSystemCode").alias("SourceSystemCode"),
        F.col("StockItemName").alias("StockItem"),
        F.col("BrandName").alias("Brand"),
        F.col("SizeText").alias("Size"),
        F.col("UnitPackageCode").alias("SellingPackage"),
        F.col("OuterPackageCode").alias("BuyingPackage"),
        F.col("LeadTimeDays").cast("int").alias("LeadTimeDays"),
        F.col("QuantityPerOuter").cast("int").alias("QuantityPerOuter"),
        F.coalesce(F.col("IsChillerStock"), F.lit(False)).alias("IsChillerStock"),
        "Barcode",
        F.col("TaxRatePercent").cast("decimal(18,3)").alias("TaxRate"),
        unitPrice.alias("UnitPrice"),
        F.col("RecommendedRetailAmount").cast("decimal(18,2)").alias("RecommendedRetailPrice"),
        F.col("TypicalWeightPerUnitKg").cast("decimal(18,3)").alias("TypicalWeightPerUnit"),
        band("UnitPriceAmount", PRICE_BANDS, "PREMIUM").alias("ListingPriceBand"),
        stdCost.alias("StandardCostAmount"),
        F.when(F.col("_StandardCostAmount").isNull(), F.lit("UNKNOWN")).otherwise(band("_StandardCostAmount", COST_BANDS, "C4")).alias("StandardCostBand"),
        F.when((unitPrice > 0) & stdCost.isNotNull(), ((unitPrice - stdCost) / unitPrice * 100)).cast("decimal(9,4)").alias("GrossMarginPercent"),
        band("UnitPriceAmount", PRICE_BANDS, "PREMIUM").alias("PriceBandCode"),
        F.when(F.col("_HazmatClassCode").isNotNull(), F.lit("HAZMAT")).when(F.col("IsChillerStock") == True, F.lit("CHILLED")).otherwise(F.lit("STANDARD")).alias("HandlingCode"),  # noqa: E712
        F.coalesce(F.col("s.SupplierBusinessKey"), F.col("_PrimarySupplierBusinessKey")).alias("PrimarySupplierId"),
        F.col("_CategoryCode").alias("CategoryCode"),
        F.col("_TaxClassCode").alias("TaxCategoryCode"),
        F.col("_HazmatClassCode").alias("HazardClassCode"),
        ((F.upper(F.coalesce(F.col("_LifecycleStatusCode"), F.lit(""))) == "DISCONTINUED") | F.col("_DiscontinuedDate").isNotNull()).alias("IsDiscontinued"),
        F.col("_DiscontinuedDate").alias("DiscontinuedOn"),
        F.coalesce(F.col("_UomConversionFactor").cast("decimal(18,3)"), F.col("QuantityPerOuter").cast("decimal(18,3)")).alias("PackSizeQuantity"),
        F.col("_ProductDescription").alias("MarketingDescription"),
        F.col("MarketingTagList").alias("SearchKeywords"),
        F.lit(None).cast("string").alias("ImageURL"),
        F.lit(None).cast("string").alias("MerchandisingNotes"),
        F.col("s.SourceModifiedDate").alias("SourceModifiedDate"),
    )


# ----------------------------------------------------------------------------------------------------------------
# Employee / Salesperson
# ----------------------------------------------------------------------------------------------------------------

def transformEmployee(stg: DataFrame, businessDate) -> DataFrame:
    bd = F.lit(str(businessDate)).cast("date")
    managers = stg.select(F.col("ManagerEmployeeKey").alias("_mgrBk")).where(F.col("_mgrBk").isNotNull()).distinct()
    df = stg.join(managers, stg["EmployeeBusinessKey"] == managers["_mgrBk"], "left")
    terminated = F.col("TerminationDate").isNotNull() & (F.col("TerminationDate") <= bd)
    return df.select(
        "EmployeeBusinessKey",
        F.regexp_extract(F.col("SourceEmployeeId"), r"(\d+)", 1).cast("int").alias("WWIEmployeeID"),
        F.col("SourceEmployeeId").alias("EmployeeNumber"),
        "SourceSystemCode",
        F.when(F.col("RetentionMaskedFlag") == True, F.lit("Retention Masked")).otherwise(F.col("EmployeeFullName")).alias("Employee"),  # noqa: E712
        F.when(F.col("RetentionMaskedFlag") == True, F.lit(None)).otherwise(F.col("PreferredName")).alias("PreferredName"),  # noqa: E712
        F.coalesce(F.col("IsSalesperson"), F.lit(False)).alias("IsSalesperson"),
        F.coalesce(F.upper(F.col("RegionCode")), F.lit("GLOBAL")).alias("RegionCode"),
        F.upper(F.regexp_replace(F.coalesce(F.col("DepartmentName"), F.lit("UNASSIGNED")), r"[^A-Za-z0-9]", "")).alias("DepartmentCode"),
        "DepartmentName",
        "JobTitle",
        F.when(F.upper(F.col("JobTitle")).rlike("CHIEF|PRESIDENT|DIRECTOR"), F.lit("EXEC"))
        .when(F.upper(F.col("JobTitle")).rlike("MANAGER|HEAD|LEAD"), F.lit("MGMT"))
        .when(F.upper(F.col("JobTitle")).rlike("SENIOR|PRINCIPAL"), F.lit("SENIOR")).otherwise(F.lit("STAFF")).alias("JobGradeCode"),
        "CostCenterCode",
        F.lit(None).cast("string").alias("WorkLocationCode"),
        "HireDate",
        "TerminationDate",
        F.when(terminated, F.lit("TERMINATED")).when(F.col("HireDate") > bd, F.lit("PRE_HIRE")).otherwise(F.lit("ACTIVE")).alias("EmploymentStatusCode"),
        F.when(F.col("IsWarehouseStaff") == True, F.lit("HOURLY")).otherwise(F.lit("SALARIED")).alias("EmploymentTypeCode"),  # noqa: E712
        F.col("_mgrBk").isNotNull().alias("IsManager"),
        (~terminated).alias("IsActive"),
        F.lit(None).cast("int").alias("ManagerEmployeeKey"),
        F.col("ManagerEmployeeKey").alias("ManagerEmployeeNumber"),
        F.lit(None).cast("smallint").alias("OrganisationLevel"),
        F.col("_mgrBk").isNull().alias("IsLeafNode"),
        F.floor(F.months_between(F.coalesce(F.col("TerminationDate"), bd), F.col("HireDate")) / 12).cast("int").alias("TenureYears"),
        F.when(F.upper(F.col("RegionCode")) == "NA", F.lit("BIWEEKLY")).otherwise(F.lit("MONTHLY")).alias("PayrollCalendarCode"),
        F.lit(None).cast("string").alias("CollectiveAgreementCode"),
        F.lit(None).cast("string").alias("WorkPermitTypeCode"),
        F.when(F.col("RetentionMaskedFlag") == True, F.lit(None)).otherwise(F.col("WorkEmailAddress")).alias("WorkEmailAddress"),  # noqa: E712
        F.lit(None).cast("bigint").alias("RekeyedByLineageKey"),
    )


def transformSalesperson(stg: DataFrame) -> DataFrame:
    return stg.select(
        "SalespersonBusinessKey",
        "EmployeeBusinessKey",
        F.lit(None).cast("int").alias("EmployeeKey"),
        F.regexp_extract(F.coalesce(F.col("EmployeeBusinessKey"), F.lit("")), r"(\d+)", 1).cast("int").alias("WWIEmployeeID"),
        "SourceSystemCode",
        F.col("SalespersonName").alias("Salesperson"),
        F.col("SalespersonBusinessKey").alias("SalespersonCode"),
        F.lit(None).cast("string").alias("RegionCode"),
        "SalesTerritoryCode",
        F.lit(None).cast("int").alias("SalesTerritoryKey"),
        F.when(F.col("CommissionPlanCode").rlike("(?i)MGR|MANAGER"), F.lit("SALES_MANAGER")).otherwise(F.lit("ACCOUNT_EXEC")).alias("SalesRoleCode"),
        F.lit("ALL").alias("ChannelResponsibilityCode"),
        "CommissionPlanCode",
        F.col("CommissionRatePercent").cast("decimal(9,4)").alias("CommissionRate"),
        F.when(F.col("CommissionRatePercent").isNull(), F.lit("NONE")).when(F.col("CommissionRatePercent") >= 10, F.lit("ACCELERATED")).otherwise(F.lit("STANDARD")).alias("CommissionSchemeCode"),
        F.col("QuotaAmount").cast("decimal(18,2)").alias("AnnualQuotaAmount"),
        "QuotaCurrencyCode",
        F.col("QuotaAmountUsd").cast("decimal(18,2)").alias("AnnualQuotaReporting"),
        F.year(F.coalesce(F.col("EffectiveFromDate"), F.current_date())).cast("string").alias("QuotaFiscalYear"),
        F.coalesce(F.col("QuotaPeriodCode"), F.lit("ANNUAL")).alias("QuotaBasisCode"),
        F.when(F.col("QuotaAmountUsd").isNull(), F.lit("NONE")).otherwise(band("QuotaAmountUsd", QUOTA_BANDS, "Q4")).alias("QuotaBandCode"),
        F.lit(None).cast("string").alias("ManagerEmployeeNumber"),
        F.coalesce(F.col("IsActive"), F.lit(True)).alias("IsActive"),
        F.col("EffectiveFromDate").alias("SalesStartDate"),
        F.col("EffectiveToDate").alias("SalesEndDate"),
        F.col("EffectiveFromDate").cast("timestamp").alias("EffectiveFromTimestamp"),
    )


# ----------------------------------------------------------------------------------------------------------------
# City / Sales Territory / categories
# ----------------------------------------------------------------------------------------------------------------

def transformCity(stg: DataFrame) -> DataFrame:
    postcode = F.upper(F.regexp_replace(F.trim(F.col("PostalCodeRaw")), r"\s+", " "))
    return stg.select(
        "CityBusinessKey",
        F.col("WWICityID").cast("bigint").alias("WWICityID"),
        "SourceSystemCode",
        F.col("CityName").alias("City"),
        "LocalScriptCityName",
        "StateProvince",
        F.upper(F.col("CountryCode")).alias("CountryCode"),
        "Continent",
        "Subregion",
        F.coalesce(F.upper(F.col("RegionCode")), F.lit("GLOBAL")).alias("RegionCode"),
        F.concat_ws("-", F.upper(F.col("RegionCode")), F.upper(F.col("CountryCode"))).alias("CityRegionCode"),
        "SalesTerritoryCode",
        F.lit(None).cast("int").alias("SalesTerritoryKey"),
        F.coalesce(F.col("LatestRecordedPopulation").cast("bigint"), F.lit(0)).alias("LatestRecordedPopulation"),
        F.when(postcode == "", F.lit(None)).otherwise(postcode).alias("PostcodeStandardized"),
        F.coalesce(F.col("PostalRuleSetCode"), F.lit("NONE")).alias("PostalFormatCode"),
        F.when(F.col("PostalCodeRaw").isNull(), F.lit("MISSING")).when(F.col("PostalCodeRaw").rlike(r"^\d{5}(-\d{4})?$"), F.lit("US-ZIP"))
        .when(F.col("PostalCodeRaw").rlike(r"^[A-Za-z]{1,2}\d[A-Za-z\d]? ?\d[A-Za-z]{2}$"), F.lit("UK")).otherwise(F.lit("OTHER")).alias("PostalStandardCode"),
        "CountyName",
        F.col("CountyFipsCode").alias("CountyFIPSCode"),
        "MetropolitanStatisticalArea",
        F.col("NutsLevel3Code").alias("NUTSLevel3Code"),
        "DistrictName",
        "PrefectureOrProvince",
        "LocalityName",
        "TimeZoneName",
        F.col("UtcOffsetMinutes").cast("int").alias("UTCOffsetMinutes"),
        F.coalesce(F.col("ObservesDaylightSaving"), F.lit(False)).alias("ObservesDaylightSaving"),
        "TaxJurisdictionCode",
        "SourceChangedOn",
    )


def applyPopulationThreshold(source: DataFrame, current: DataFrame, thresholdPercent: float) -> DataFrame:
    """usp_LoadCityDimension: population drift below the threshold is a Type 1 correction, not a new version.

    Returns the source with `LatestRecordedPopulation` reset to the current value for sub-threshold changes and
    `_PopulationType1` carrying the real value to write through afterwards.
    """
    cur = current.select(F.col("CityBusinessKey").alias("_cbk"), F.col("LatestRecordedPopulation").alias("_curPop"))
    df = source.join(cur, source["CityBusinessKey"] == cur["_cbk"], "left")
    pct = F.when(F.coalesce(F.col("_curPop"), F.lit(0)) == 0, F.lit(100.0)).otherwise(
        F.abs(F.col("LatestRecordedPopulation") - F.col("_curPop")) * 100.0 / F.col("_curPop"))
    subThreshold = F.col("_curPop").isNotNull() & (F.col("LatestRecordedPopulation") != F.col("_curPop")) & (pct < F.lit(float(thresholdPercent)))
    return (
        df.withColumn("_PopulationType1", F.when(subThreshold, F.col("LatestRecordedPopulation")))
        .withColumn("LatestRecordedPopulation", F.when(subThreshold, F.col("_curPop")).otherwise(F.col("LatestRecordedPopulation")))
        .drop("_cbk", "_curPop")
    )


def transformSalesTerritory(stg: DataFrame) -> DataFrame:
    return stg.select(
        "SalesTerritoryCode",
        F.regexp_extract(F.col("SalesTerritoryBusinessKey"), r"(\d+)", 1).cast("int").alias("WWISalesTerritoryID"),
        F.col("SalesTerritoryName").alias("SalesTerritory"),
        F.upper(F.col("RegionCode")).alias("RegionCode"),
        F.year(F.current_date()).cast("int").alias("AlignmentYear"),
        F.upper(F.col("RegionCode")).alias("ParentTerritoryCode"),
        F.lit("TERRITORY").alias("TerritoryLevelCode"),
        F.concat_ws("/", F.upper(F.col("RegionCode")), F.col("SalesTerritoryCode")).alias("TerritoryPath"),
        F.col("TerritoryManagerKey").alias("TerritoryManagerEmployeeNo"),
        F.col("CountryCodeList").alias("CoverageList"),
        F.when(F.col("CountryCodeList").isNull(), F.lit("UNASSIGNED"))
        .when(F.size(F.split(F.col("CountryCodeList"), r"[,;|]")) > 1, F.lit("MULTI_COUNTRY")).otherwise(F.lit("SINGLE_COUNTRY")).alias("CoverageModelCode"),
        F.lit(None).cast("string").alias("TaxJurisdictionCode"),
        "ReportingCurrencyCode",
        "FiscalCalendarCode",
        F.lit(None).cast("decimal(18,2)").alias("AnnualTargetAmount"),
        F.col("ReportingCurrencyCode").alias("TargetCurrencyCode"),
        "SourceSystemCode",
        F.coalesce(F.col("IsActive"), F.lit(True)).alias("IsActive"),
        F.lit(None).cast("date").alias("RetiredOn"),
    )


def transformCustomerCategory(stg: DataFrame) -> DataFrame:
    grp = F.upper(F.coalesce(F.col("CategoryGroupCode"), F.lit("")))
    region = F.upper(F.col("RegionCode"))
    return stg.select(
        F.col("CustomerCategoryCode").alias("CategoryCode"),
        F.regexp_extract(F.col("CustomerCategoryBusinessKey"), r"(\d+)", 1).cast("int").alias("WWICustomerCategoryID"),
        F.col("CustomerCategoryName").alias("CustomerCategory"),
        F.col("CategoryGroupCode").alias("CategoryGroup"),
        F.lit(None).cast("string").alias("DefaultPaymentTermsCode"),
        F.lit(None).cast("decimal(18,2)").alias("DefaultCreditLimitAmount"),
        F.col("DiscountEligiblePercent").cast("decimal(9,4)").alias("DefaultDiscountPercentage"),
        F.when(region == "NA", F.col("SourceCategoryCode")).alias("NASegmentCode"),
        F.when(region == "EU", F.col("SourceCategoryCode")).alias("EUSectorCode"),
        F.when(region == "APAC", F.col("SourceCategoryCode")).alias("APACTradeCode"),
        grp.contains("RETAIL").alias("IsRetail"),
        grp.contains("WHOLESALE").alias("IsWholesale"),
        grp.contains("INTERNAL").alias("IsInternal"),
        (grp.contains("GOV") | grp.contains("PUBLIC")).alias("IsGovernment"),
        "SourceSystemCode",
        F.coalesce(F.col("IsActive"), F.lit(True)).alias("IsActive"),
    )


def transformProductCategory(stg: DataFrame) -> DataFrame:
    tax = F.upper(F.col("TaxClassCode"))
    return stg.select(
        F.col("ProductCategoryCode").alias("CategoryCode"),
        F.regexp_extract(F.col("ProductCategoryBusinessKey"), r"(\d+)", 1).cast("int").alias("WWIProductCategoryID"),
        F.col("ProductCategoryName").alias("ProductCategory"),
        "ParentCategoryCode",
        F.col("MerchandiseGroupCode").alias("DepartmentCode"),
        F.col("HierarchyLevel").cast("int").alias("HierarchyLevel"),
        F.coalesce(F.col("IsLeafCategory"), F.lit(True)).alias("IsLeafCategory"),
        F.coalesce(F.col("HierarchyPath"), F.concat_ws("/", F.col("ParentCategoryCode"), F.col("ProductCategoryCode"))).alias("CategoryPath"),
        F.coalesce(F.col("MerchandiseGroupCode"), F.split(F.coalesce(F.col("HierarchyPath"), F.col("ProductCategoryCode")), "/").getItem(0)).alias("ReportingRollupCode"),
        "TaxClassCode",
        F.when(tax.isin("EXEMPT", "ZERO"), F.lit("EXEMPT")).when(tax.isNull(), F.lit(None)).otherwise(F.lit("TAXABLE")).alias("NATaxCategoryCode"),
        F.when(tax.isin("EXEMPT", "ZERO"), F.lit("ZERO")).when(tax.isin("REDUCED", "FOOD", "BOOKS"), F.lit("REDUCED")).when(tax.isNull(), F.lit(None)).otherwise(F.lit("STANDARD")).alias("EUVATRateCategory"),
        F.when(tax.isin("EXEMPT", "ZERO"), F.lit("ZERO_RATED")).when(tax.isNull(), F.lit(None)).otherwise(F.lit("STANDARD_RATED")).alias("APACGSTCategoryCode"),
        F.col("ProductCount").cast("int").alias("ProductCount"),
        "SourceSystemCode",
        F.coalesce(F.col("IsActive"), F.lit(True)).alias("IsActive"),
    )


# ----------------------------------------------------------------------------------------------------------------
# Customer Segment / Promotion
# ----------------------------------------------------------------------------------------------------------------

def transformCustomerSegment(stg: DataFrame) -> DataFrame:
    churn = F.upper(F.col("ChurnRiskBand"))
    return stg.select(
        "SegmentBusinessKey",
        F.col("WWICustomerSegmentID").cast("int").alias("WWICustomerSegmentID"),
        "SegmentCode",
        F.coalesce(F.col("SegmentName"), F.col("SegmentCode")).alias("CustomerSegment"),
        F.coalesce(F.col("SegmentFamilyCode"), F.regexp_extract(F.col("SegmentCode"), r"^([A-Za-z]+)", 1)).alias("SegmentFamilyCode"),
        F.upper(F.col("RegionCode")).alias("RegionCode"),
        "ScoringModelCode",
        F.col("ScoringModelVersion").cast("string").alias("ScoringModelVersion"),
        "ScoringFrequencyCode",
        "LastScoredOn",
        "RecencyBand", "FrequencyBand", "MonetaryBand", "LifetimeValueBand", "ChurnRiskBand",
        F.col("MinimumScore").cast("decimal(9,4)").alias("MinimumScore"),
        F.col("MaximumScore").cast("decimal(9,4)").alias("MaximumScore"),
        F.col("RecencyScoreFloor").cast("int").alias("RecencyScoreFloor"),
        F.col("FrequencyScoreFloor").cast("int").alias("FrequencyScoreFloor"),
        F.col("MonetaryValueFloor").cast("decimal(18,2)").alias("MonetaryValueFloor"),
        F.col("MonetaryValueFloor").cast("decimal(18,2)").alias("MonetaryFloorReporting"),
        F.when(F.col("MinimumScore").isNull(), F.lit("UNSCORED")).otherwise(band("MinimumScore", SEGMENT_TIERS, "T1")).alias("SegmentTierCode"),
        churn.isin("HIGH", "CRITICAL", "VERY HIGH").alias("ChurnWatchFlag"),
        F.col("TargetContactFrequency").cast("smallint").alias("TargetContactFrequency"),
        F.coalesce(F.col("RequiresProfilingConsent"), F.lit(False)).alias("RequiresProfilingConsent"),
        (F.coalesce(F.col("MarketplaceOnly"), F.lit(False)) | F.col("ScoringModelCode").isNull()).alias("ExcludedFromModelling"),
        F.coalesce(F.col("MarketplaceOnly"), F.lit(False)).alias("MarketplaceOnly"),
        "SourceSystemCode",
        "SourceChangedOn",
    )


def resolveSegmentAssignments(assignments: DataFrame, segments: DataFrame, regionCode: Optional[str] = None) -> DataFrame:
    """Integration.usp_MigrateStagedCustomerSegmentData - the segment each current customer should point at.

    * segment requires profiling consent and the customer has none -> the family's `_DEF` default segment, else -2
    * segment excluded from modelling -> -2 Not Applicable
    * otherwise the segment's current surrogate key
    Returns (CustomerBusinessKey, RegionCode, CustomerSegmentKey); latest AssignedOn per customer wins.
    """
    a = assignments
    if regionCode:
        a = a.where(F.upper(F.col("RegionCode")) == regionCode.upper())
    a = a.withColumn("_rn", F.row_number().over(Window.partitionBy("CustomerBusinessKey").orderBy(F.col("AssignedOn").desc_nulls_last(), F.col("SegmentCode")))).where("_rn = 1").drop("_rn")
    seg = segments.select(
        F.col("SegmentCode").alias("_segCode"), F.upper(F.col("RegionCode")).alias("_segRegion"), F.col("CustomerSegmentKey").alias("_segKey"),
        F.col("SegmentFamilyCode").alias("_segFamily"), F.col("RequiresProfilingConsent").alias("_segConsent"), F.col("ExcludedFromModelling").alias("_segExcluded"),
    )
    dflt = segments.where(F.col("SegmentCode").rlike("_DEF$")).select(
        F.col("SegmentFamilyCode").alias("_dfFamily"), F.upper(F.col("RegionCode")).alias("_dfRegion"), F.col("CustomerSegmentKey").alias("_dfKey"),
    ).dropDuplicates(["_dfFamily", "_dfRegion"])
    joined = (
        a.join(seg, (a["SegmentCode"] == seg["_segCode"]) & (F.upper(a["RegionCode"]) == seg["_segRegion"]), "inner")
        .join(dflt, (F.col("_segFamily") == F.col("_dfFamily")) & (F.col("_segRegion") == F.col("_dfRegion")), "left")
    )
    key = (
        F.when((F.col("_segConsent") == True) & (F.coalesce(F.col("ProfilingConsentFlag"), F.lit(False)) == False), F.coalesce(F.col("_dfKey"), F.lit(-2)))  # noqa: E712
        .when(F.col("_segExcluded") == True, F.lit(-2))  # noqa: E712
        .otherwise(F.col("_segKey"))
    )
    return joined.select("CustomerBusinessKey", F.upper(F.col("RegionCode")).alias("RegionCode"), key.cast("int").alias("CustomerSegmentKey"))


def transformPromotion(stg: DataFrame) -> DataFrame:
    region = F.upper(F.col("RegionCode"))
    taxDefault = F.when(region == "NA", F.lit(REGION_TAX_TREATMENT["NA"])).when(region == "EU", F.lit(REGION_TAX_TREATMENT["EU"])).when(region == "APAC", F.lit(REGION_TAX_TREATMENT["APAC"])).otherwise(F.lit("UNSPECIFIED"))
    overlapWindow = Window.partitionBy(F.coalesce(F.col("RegionCode"), F.lit("GLOBAL")), F.coalesce(F.col("AppliesToCategoryCode"), F.lit("ALL")))
    df = stg.withColumn("_others", F.collect_list(F.struct("PromotionBusinessKey", "StartDate", "EndDate")).over(overlapWindow))
    overlaps = F.exists(
        F.col("_others"),
        lambda o: (o["PromotionBusinessKey"] != F.col("PromotionBusinessKey")) & (F.coalesce(o["StartDate"], F.lit("1900-01-01").cast("date")) <= F.coalesce(F.col("EndDate"), F.lit("9999-12-31").cast("date")))
        & (F.coalesce(o["EndDate"], F.lit("9999-12-31").cast("date")) >= F.coalesce(F.col("StartDate"), F.lit("1900-01-01").cast("date"))),
    )
    return df.select(
        "PromotionBusinessKey",
        F.regexp_extract(F.col("PromotionBusinessKey"), r"(\d+)", 1).cast("int").alias("WWIPromotionID"),
        "PromotionCode",
        F.coalesce(F.col("PromotionName"), F.col("PromotionCode")).alias("PromotionName"),
        F.coalesce(F.col("PromotionTypeCode"), F.lit("GENERIC")).alias("PromotionTypeCode"),
        F.when(F.col("DiscountPercent").isNotNull(), F.lit("PERCENT_OFF")).when(F.col("DiscountAmount").isNotNull(), F.lit("AMOUNT_OFF")).otherwise(F.lit("OTHER")).alias("MechanicCode"),
        F.col("DiscountPercent").cast("decimal(18,4)").alias("Parameter1"),
        F.col("DiscountAmount").cast("decimal(18,4)").alias("Parameter2"),
        F.col("BudgetAmountUsd").cast("decimal(18,4)").alias("Parameter3"),
        F.col("DiscountCurrencyCode").alias("ParameterCurrencyCode"),
        F.coalesce(region, F.lit("GLOBAL")).alias("RegionCode"),
        F.coalesce(F.col("AppliesToChannelCode"), F.lit("ALL")).alias("ChannelScopeCode"),
        F.when(F.col("AppliesToCategoryCode").isNull(), F.lit("ALL")).otherwise(F.lit("CATEGORY")).alias("ProductScopeCode"),
        F.col("AppliesToCategoryCode").alias("ProductCategoryCode"),
        F.lit("ALL").alias("CustomerScopeCode"),
        "StartDate",
        "EndDate",
        F.lit("INTERNAL").alias("FundingSourceCode"),
        F.col("BudgetAmountUsd").cast("decimal(18,2)").alias("BudgetAmount"),
        F.coalesce(F.col("TaxTreatmentCode"), taxDefault).alias("TaxTreatmentCode"),
        F.when(F.col("DiscountPercent").isNotNull(), F.lit("PERCENT")).otherwise(F.lit("AMOUNT")).alias("DiscountBasisCode"),
        (F.datediff(F.col("EndDate"), F.col("StartDate")) + 1).cast("int").alias("CampaignDurationDays"),
        F.lit(False).alias("IsCoFunded"),
        F.coalesce(overlaps, F.lit(False)).alias("HasOverlappingCampaign"),
        "SourceSystemCode",
        F.coalesce(F.col("IsActive"), F.lit(True)).alias("IsActive"),
        F.col("StartDate").cast("timestamp").alias("StartTimestamp"),
    )
