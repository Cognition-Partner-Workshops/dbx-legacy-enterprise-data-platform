"""Dimension specifications: target columns, business keys and SCD attribute lists.

Column names follow the naming contract for warehouse tables: the legacy
`Dimension.<Name>` table becomes `gold.dim_<name>` and each legacy column keeps
its PascalCase name with the spaces removed (`[Valid From]` -> `ValidFrom`,
`[Is Current Row]` -> `IsCurrentRow`, `[Row Hash Type 2]` -> `RowHashType2`).

The SCD attribute lists are the exact CONCAT_WS lists hashed by the legacy
Integration.usp_MigrateStaged*Data procedures; SCD pattern and the inferred
member flag come from sqlserver/warehouse/dimensions/01_dimension_key_registry.sql.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

RESERVED_KEY_LOW = -9
RESERVED_KEY_HIGH = 0
FIRST_REAL_KEY = 1
UNKNOWN_KEY = -1
NOT_APPLICABLE_KEY = -2
INVALID_KEY = -3
INFERRED_PENDING_KEY = -4
ERROR_KEY = -9
DEFAULT_BLOCK_SIZE = 1000

# Reserved surrogate keys, per 00_dimension_schemas_and_sequences.sql / 90_unknown_members.sql.
RESERVED_MEMBERS: Tuple[Tuple[int, str], ...] = (
    (-1, "Unknown"),
    (-2, "Not Applicable"),
    (-3, "Invalid"),
    (-4, "Inferred Pending"),
    (-9, "Error"),
)

OPEN_ENDED_TIMESTAMP = "9999-12-31 23:59:59"
EPOCH_TIMESTAMP = "1900-01-01 00:00:00"

# Metadata columns shared by every SCD2 / hybrid dimension (mirrors the trailing
# block of each Dimension.*.sql script: both the original Valid From/Valid To
# pair and the later Effective*/Is Current Row/Version Number columns are kept).
SCD2_METADATA_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("EffectiveFrom", "TIMESTAMP"),
    ("EffectiveTo", "TIMESTAMP"),
    ("EffectiveFromDate", "DATE"),
    ("EffectiveSequence", "SMALLINT"),
    ("IsCurrentRow", "BOOLEAN"),
    ("VersionNumber", "INT"),
    ("RowHashType2", "STRING"),
    ("RowHashType1", "STRING"),
    ("IsInferredMember", "BOOLEAN"),
    ("InferredCreatedOn", "TIMESTAMP"),
    ("EnrichedOn", "TIMESTAMP"),
    ("ValidFrom", "TIMESTAMP"),
    ("ValidTo", "TIMESTAMP"),
    ("LineageKey", "BIGINT"),
    ("LastLoadBatchId", "BIGINT"),
    ("LastLoadPackageExecutionId", "BIGINT"),
)

# SCD1 dimensions carry a single row per business key.
SCD1_METADATA_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("RowHashType1", "STRING"),
    ("IsCurrentRow", "BOOLEAN"),
    ("ValidFrom", "TIMESTAMP"),
    ("ValidTo", "TIMESTAMP"),
    ("LineageKey", "BIGINT"),
    ("LastLoadBatchId", "BIGINT"),
    ("LastLoadPackageExecutionId", "BIGINT"),
)


@dataclass(frozen=True)
class DimensionSpec:
    """Everything the SCD helper needs to know about one dimension."""

    name: str                                  # legacy dimension name, e.g. "Stock Item"
    tableName: str                             # gold table, e.g. "dim_stock_item"
    keyColumn: str                             # surrogate key column, e.g. "StockItemKey"
    businessKeyColumn: str                     # natural key column matched against staging
    scdPattern: str                            # SCD1 | SCD2 | Hybrid
    supportsInferred: bool
    attributeColumns: Tuple[Tuple[str, str], ...]   # (column, sparkType) excluding key + metadata
    scd2Columns: Tuple[str, ...] = ()
    scd1Columns: Tuple[str, ...] = ()
    labelColumns: Tuple[str, ...] = ()         # text columns that receive 'Unknown' etc. on reserved rows
    reservedDefaults: Dict[str, object] = field(default_factory=dict)
    changeTimestampColumn: Optional[str] = None  # source column that dates a Type 2 change (else load time)

    @property
    def metadataColumns(self) -> Tuple[Tuple[str, str], ...]:
        return SCD1_METADATA_COLUMNS if self.scdPattern == "SCD1" else SCD2_METADATA_COLUMNS

    @property
    def allColumns(self) -> Tuple[Tuple[str, str], ...]:
        return ((self.keyColumn, "INT"),) + self.attributeColumns + self.metadataColumns

    @property
    def attributeNames(self) -> Tuple[str, ...]:
        return tuple(c for c, _ in self.attributeColumns)

    @property
    def isType2(self) -> bool:
        return self.scdPattern in ("SCD2", "Hybrid")

    def fullTableName(self, catalog: str) -> str:
        return f"{catalog}.gold.{self.tableName}"


def _cols(*pairs):
    return tuple(pairs)


CUSTOMER = DimensionSpec(
    name="Customer",
    tableName="dim_customer",
    keyColumn="CustomerKey",
    businessKeyColumn="CustomerBusinessKey",
    scdPattern="Hybrid",
    supportsInferred=True,
    attributeColumns=_cols(
        ("CustomerBusinessKey", "STRING"),
        ("WWICustomerID", "INT"),
        ("SourceSystemCode", "STRING"),
        ("Customer", "STRING"),
        ("BillToCustomer", "STRING"),
        ("Category", "STRING"),
        ("CustomerCategoryKey", "INT"),
        ("CustomerSegmentKey", "INT"),
        ("BuyingGroup", "STRING"),
        ("PrimaryContact", "STRING"),
        ("PostalCode", "STRING"),
        ("PostalCodeStandardized", "STRING"),
        ("ZIPPlusFour", "STRING"),
        ("PostalFormatCode", "STRING"),
        ("CountryCode", "STRING"),
        ("RegionCode", "STRING"),
        ("CreditLimitAmount", "DECIMAL(18,2)"),
        ("CreditLimitCurrencyCode", "STRING"),
        ("CreditLimitAmountUsd", "DECIMAL(18,2)"),
        ("CreditLimitBand", "STRING"),
        ("IsOnCreditHold", "BOOLEAN"),
        ("IsTaxExempt", "BOOLEAN"),
        ("SalesTaxJurisdictionCode", "STRING"),
        ("VATRegistrationNumber", "STRING"),
        ("VATValidationStatus", "STRING"),
        ("IsReverseChargeEligible", "BOOLEAN"),
        ("GSTRegistrationNumber", "STRING"),
        ("GSTTreatmentCode", "STRING"),
        ("BusinessNumberType", "STRING"),
        ("LocalScriptName", "STRING"),
        ("AccountStatusCode", "STRING"),
        ("AccountOpenedDate", "DATE"),
        ("PaymentTermsCode", "STRING"),
        ("StandardDiscountPercentage", "DECIMAL(9,4)"),
        ("PhoneNumberStandardized", "STRING"),
        ("WebsiteURL", "STRING"),
        ("ConsentBasisCode", "STRING"),
        ("ConsentSourceCode", "STRING"),
        ("MarketingConsentFlag", "BOOLEAN"),
        ("ProfilingConsentFlag", "BOOLEAN"),
        ("ErasureRequestedOn", "DATE"),
        ("IsPseudonymized", "BOOLEAN"),
        ("RetentionYears", "INT"),
        ("RetentionExpiryDate", "DATE"),
        ("FiscalYearStartMonth", "INT"),
    ),
    scd2Columns=(
        "Customer", "BillToCustomer", "Category", "BuyingGroup", "PostalCode", "CountryCode",
        "CreditLimitAmount", "SalesTaxJurisdictionCode", "VATRegistrationNumber",
        "GSTRegistrationNumber", "GSTTreatmentCode", "AccountStatusCode",
    ),
    scd1Columns=(
        "PrimaryContact", "PhoneNumberStandardized", "WebsiteURL", "ConsentBasisCode",
        "MarketingConsentFlag", "ProfilingConsentFlag", "ErasureRequestedOn",
        "StandardDiscountPercentage",
    ),
    labelColumns=("Customer", "BillToCustomer", "Category", "BuyingGroup", "PrimaryContact"),
    reservedDefaults={"PostalCode": "N/A", "RegionCode": "GLOBAL", "WWICustomerID": "KEY"},
    changeTimestampColumn="SourceModifiedDate",
)

SUPPLIER = DimensionSpec(
    name="Supplier",
    tableName="dim_supplier",
    keyColumn="SupplierKey",
    businessKeyColumn="SupplierBusinessKey",
    scdPattern="Hybrid",
    supportsInferred=True,
    attributeColumns=_cols(
        ("SupplierBusinessKey", "STRING"),
        ("WWISupplierID", "INT"),
        ("SourceSystemCode", "STRING"),
        ("SourceSupplierReference", "STRING"),
        ("Supplier", "STRING"),
        ("SupplierReference", "STRING"),
        ("CategoryCode", "STRING"),
        ("PrimaryContact", "STRING"),
        ("RegionCode", "STRING"),
        ("CountryCode", "STRING"),
        ("SupplierStatusCode", "STRING"),
        ("ApprovalStatusCode", "STRING"),
        ("IsPreferredSupplier", "BOOLEAN"),
        ("RiskRatingCode", "STRING"),
        ("SupplierRiskScore", "INT"),
        ("ContractLeadTimeDays", "INT"),
        ("QualityRating", "DECIMAL(5,2)"),
        ("PaymentTermsCode", "STRING"),
        ("PaymentDays", "INT"),
        ("PaymentMethodCode", "STRING"),
        ("SettlementCurrencyCode", "STRING"),
        ("BankCountryCode", "STRING"),
        ("IsCrossBorderPayee", "BOOLEAN"),
        ("SanctionScreeningStatus", "STRING"),
        ("SanctionScreenedOn", "DATE"),
        ("DiversityClassificationCode", "STRING"),
        ("OnHoldFlag", "BOOLEAN"),
        ("HoldReasonCode", "STRING"),
        ("IsSupersededDuplicate", "BOOLEAN"),
    ),
    scd2Columns=(
        "Supplier", "CategoryCode", "PaymentTermsCode", "PaymentDays", "BankCountryCode",
        "ApprovalStatusCode", "RiskRatingCode", "IsPreferredSupplier", "ContractLeadTimeDays",
    ),
    scd1Columns=("PrimaryContact", "SanctionScreeningStatus", "SanctionScreenedOn", "QualityRating"),
    labelColumns=("Supplier", "CategoryCode", "PrimaryContact"),
    reservedDefaults={"RegionCode": "GLOBAL", "WWISupplierID": "KEY"},
    changeTimestampColumn="SourceModifiedDate",
)

STOCK_ITEM = DimensionSpec(
    name="Stock Item",
    tableName="dim_stock_item",
    keyColumn="StockItemKey",
    businessKeyColumn="StockItemBusinessKey",
    scdPattern="Hybrid",
    supportsInferred=True,
    attributeColumns=_cols(
        ("StockItemBusinessKey", "STRING"),
        ("WWIStockItemID", "INT"),
        ("SourceSystemCode", "STRING"),
        ("StockItem", "STRING"),
        ("Brand", "STRING"),
        ("Size", "STRING"),
        ("SellingPackage", "STRING"),
        ("BuyingPackage", "STRING"),
        ("LeadTimeDays", "INT"),
        ("QuantityPerOuter", "INT"),
        ("IsChillerStock", "BOOLEAN"),
        ("Barcode", "STRING"),
        ("TaxRate", "DECIMAL(18,3)"),
        ("UnitPrice", "DECIMAL(18,2)"),
        ("RecommendedRetailPrice", "DECIMAL(18,2)"),
        ("TypicalWeightPerUnit", "DECIMAL(18,3)"),
        ("ListingPriceBand", "STRING"),
        ("StandardCostAmount", "DECIMAL(18,4)"),
        ("StandardCostBand", "STRING"),
        ("GrossMarginPercent", "DECIMAL(9,4)"),
        ("PriceBandCode", "STRING"),
        ("HandlingCode", "STRING"),
        ("PrimarySupplierId", "STRING"),
        ("PrimarySupplierKey", "INT"),
        ("CategoryCode", "STRING"),
        ("ProductCategoryKey", "INT"),
        ("TaxCategoryCode", "STRING"),
        ("HazardClassCode", "STRING"),
        ("IsDiscontinued", "BOOLEAN"),
        ("DiscontinuedOn", "DATE"),
        ("PackSizeQuantity", "DECIMAL(18,3)"),
        ("MarketingDescription", "STRING"),
        ("SearchKeywords", "STRING"),
        ("ImageURL", "STRING"),
        ("MerchandisingNotes", "STRING"),
    ),
    scd2Columns=(
        "StockItem", "ListingPriceBand", "StandardCostBand", "PrimarySupplierId", "TaxCategoryCode",
        "HazardClassCode", "IsDiscontinued", "CategoryCode", "PackSizeQuantity",
    ),
    scd1Columns=("MarketingDescription", "SearchKeywords", "ImageURL", "MerchandisingNotes"),
    labelColumns=("StockItem", "Brand", "Size", "SellingPackage", "BuyingPackage"),
    reservedDefaults={"WWIStockItemID": "KEY", "LeadTimeDays": 0, "QuantityPerOuter": 0,
                      "IsChillerStock": False, "TaxRate": 0, "UnitPrice": 0, "TypicalWeightPerUnit": 0},
    changeTimestampColumn="SourceModifiedDate",
)

CITY = DimensionSpec(
    name="City",
    tableName="dim_city",
    keyColumn="CityKey",
    businessKeyColumn="CityBusinessKey",
    scdPattern="SCD2",
    supportsInferred=True,
    attributeColumns=_cols(
        ("CityBusinessKey", "STRING"),
        ("WWICityID", "BIGINT"),
        ("SourceSystemCode", "STRING"),
        ("City", "STRING"),
        ("LocalScriptCityName", "STRING"),
        ("StateProvince", "STRING"),
        ("CountryCode", "STRING"),
        ("Continent", "STRING"),
        ("Subregion", "STRING"),
        ("RegionCode", "STRING"),
        ("CityRegionCode", "STRING"),
        ("SalesTerritoryCode", "STRING"),
        ("SalesTerritoryKey", "INT"),
        ("LatestRecordedPopulation", "BIGINT"),
        ("PostcodeStandardized", "STRING"),
        ("PostalFormatCode", "STRING"),
        ("PostalStandardCode", "STRING"),
        ("CountyName", "STRING"),
        ("CountyFIPSCode", "STRING"),
        ("MetropolitanStatisticalArea", "STRING"),
        ("NUTSLevel3Code", "STRING"),
        ("DistrictName", "STRING"),
        ("PrefectureOrProvince", "STRING"),
        ("LocalityName", "STRING"),
        ("TimeZoneName", "STRING"),
        ("UTCOffsetMinutes", "INT"),
        ("ObservesDaylightSaving", "BOOLEAN"),
        ("TaxJurisdictionCode", "STRING"),
    ),
    scd2Columns=(
        "City", "StateProvince", "CountryCode", "SalesTerritoryCode", "LatestRecordedPopulation",
        "PostcodeStandardized", "CountyFIPSCode", "NUTSLevel3Code", "PrefectureOrProvince",
        "TaxJurisdictionCode",
    ),
    labelColumns=("City", "StateProvince", "Continent", "Subregion"),
    reservedDefaults={"WWICityID": "KEY", "RegionCode": "GLOBAL", "LatestRecordedPopulation": 0},
    changeTimestampColumn="SourceChangedOn",
)

EMPLOYEE = DimensionSpec(
    name="Employee",
    tableName="dim_employee",
    keyColumn="EmployeeKey",
    businessKeyColumn="EmployeeBusinessKey",
    scdPattern="SCD2",
    supportsInferred=False,
    attributeColumns=_cols(
        ("EmployeeBusinessKey", "STRING"),
        ("WWIEmployeeID", "INT"),
        ("EmployeeNumber", "STRING"),
        ("SourceSystemCode", "STRING"),
        ("Employee", "STRING"),
        ("PreferredName", "STRING"),
        ("IsSalesperson", "BOOLEAN"),
        ("RegionCode", "STRING"),
        ("DepartmentCode", "STRING"),
        ("DepartmentName", "STRING"),
        ("JobTitle", "STRING"),
        ("JobGradeCode", "STRING"),
        ("CostCenterCode", "STRING"),
        ("WorkLocationCode", "STRING"),
        ("HireDate", "DATE"),
        ("TerminationDate", "DATE"),
        ("EmploymentStatusCode", "STRING"),
        ("EmploymentTypeCode", "STRING"),
        ("IsManager", "BOOLEAN"),
        ("IsActive", "BOOLEAN"),
        ("ManagerEmployeeKey", "INT"),
        ("ManagerEmployeeNumber", "STRING"),
        ("OrganisationLevel", "SMALLINT"),
        ("IsLeafNode", "BOOLEAN"),
        ("TenureYears", "INT"),
        ("PayrollCalendarCode", "STRING"),
        ("CollectiveAgreementCode", "STRING"),
        ("WorkPermitTypeCode", "STRING"),
        ("WorkEmailAddress", "STRING"),
        ("RekeyedByLineageKey", "BIGINT"),
    ),
    scd2Columns=(
        "Employee", "PreferredName", "JobTitle", "JobGradeCode", "DepartmentCode", "CostCenterCode",
        "ManagerEmployeeNumber", "EmploymentStatusCode", "EmploymentTypeCode", "WorkLocationCode",
        "TerminationDate", "IsSalesperson", "CollectiveAgreementCode", "WorkPermitTypeCode",
    ),
    labelColumns=("Employee", "PreferredName"),
    reservedDefaults={"WWIEmployeeID": "KEY", "IsSalesperson": False, "RegionCode": "GLOBAL"},
)

SALESPERSON = DimensionSpec(
    name="Salesperson",
    tableName="dim_salesperson",
    keyColumn="SalespersonKey",
    businessKeyColumn="SalespersonBusinessKey",
    scdPattern="SCD2",
    supportsInferred=False,
    attributeColumns=_cols(
        ("SalespersonBusinessKey", "STRING"),
        ("EmployeeBusinessKey", "STRING"),
        ("EmployeeKey", "INT"),
        ("WWIEmployeeID", "INT"),
        ("SourceSystemCode", "STRING"),
        ("Salesperson", "STRING"),
        ("SalespersonCode", "STRING"),
        ("RegionCode", "STRING"),
        ("SalesTerritoryCode", "STRING"),
        ("SalesTerritoryKey", "INT"),
        ("SalesRoleCode", "STRING"),
        ("ChannelResponsibilityCode", "STRING"),
        ("CommissionPlanCode", "STRING"),
        ("CommissionRate", "DECIMAL(9,4)"),
        ("CommissionSchemeCode", "STRING"),
        ("AnnualQuotaAmount", "DECIMAL(18,2)"),
        ("QuotaCurrencyCode", "STRING"),
        ("AnnualQuotaReporting", "DECIMAL(18,2)"),
        ("QuotaFiscalYear", "STRING"),
        ("QuotaBasisCode", "STRING"),
        ("QuotaBandCode", "STRING"),
        ("ManagerEmployeeNumber", "STRING"),
        ("IsActive", "BOOLEAN"),
        ("SalesStartDate", "DATE"),
        ("SalesEndDate", "DATE"),
    ),
    scd2Columns=(
        "SalesRoleCode", "SalesTerritoryCode", "CommissionPlanCode", "CommissionRate",
        "AnnualQuotaAmount", "QuotaCurrencyCode", "ManagerEmployeeNumber",
        "ChannelResponsibilityCode", "SalesEndDate",
    ),
    labelColumns=("Salesperson",),
    reservedDefaults={"WWIEmployeeID": "KEY", "RegionCode": "GLOBAL"},
    changeTimestampColumn="EffectiveFromTimestamp",
)

CUSTOMER_CATEGORY = DimensionSpec(
    name="Customer Category",
    tableName="dim_customer_category",
    keyColumn="CustomerCategoryKey",
    businessKeyColumn="CategoryCode",
    scdPattern="SCD1",
    supportsInferred=False,
    attributeColumns=_cols(
        ("CategoryCode", "STRING"),
        ("WWICustomerCategoryID", "INT"),
        ("CustomerCategory", "STRING"),
        ("CategoryGroup", "STRING"),
        ("DefaultPaymentTermsCode", "STRING"),
        ("DefaultCreditLimitAmount", "DECIMAL(18,2)"),
        ("DefaultDiscountPercentage", "DECIMAL(9,4)"),
        ("NASegmentCode", "STRING"),
        ("EUSectorCode", "STRING"),
        ("APACTradeCode", "STRING"),
        ("IsRetail", "BOOLEAN"),
        ("IsWholesale", "BOOLEAN"),
        ("IsInternal", "BOOLEAN"),
        ("IsGovernment", "BOOLEAN"),
        ("SourceSystemCode", "STRING"),
        ("IsActive", "BOOLEAN"),
    ),
    scd1Columns=(
        "CustomerCategory", "CategoryGroup", "DefaultPaymentTermsCode", "DefaultCreditLimitAmount",
        "DefaultDiscountPercentage", "NASegmentCode", "EUSectorCode", "APACTradeCode",
    ),
    labelColumns=("CustomerCategory", "CategoryGroup"),
    reservedDefaults={"WWICustomerCategoryID": "KEY", "IsActive": True},
)

PRODUCT_CATEGORY = DimensionSpec(
    name="Product Category",
    tableName="dim_product_category",
    keyColumn="ProductCategoryKey",
    businessKeyColumn="CategoryCode",
    scdPattern="SCD1",
    supportsInferred=False,
    attributeColumns=_cols(
        ("CategoryCode", "STRING"),
        ("WWIProductCategoryID", "INT"),
        ("ProductCategory", "STRING"),
        ("ParentCategoryCode", "STRING"),
        ("DepartmentCode", "STRING"),
        ("HierarchyLevel", "INT"),
        ("IsLeafCategory", "BOOLEAN"),
        ("CategoryPath", "STRING"),
        ("ReportingRollupCode", "STRING"),
        ("TaxClassCode", "STRING"),
        ("NATaxCategoryCode", "STRING"),
        ("EUVATRateCategory", "STRING"),
        ("APACGSTCategoryCode", "STRING"),
        ("ProductCount", "INT"),
        ("SourceSystemCode", "STRING"),
        ("IsActive", "BOOLEAN"),
    ),
    scd1Columns=("ProductCategory", "CategoryPath", "NATaxCategoryCode", "EUVATRateCategory",
                 "APACGSTCategoryCode"),
    labelColumns=("ProductCategory", "CategoryPath"),
    reservedDefaults={"WWIProductCategoryID": "KEY", "IsActive": True},
)

SALES_TERRITORY = DimensionSpec(
    name="Sales Territory",
    tableName="dim_sales_territory",
    keyColumn="SalesTerritoryKey",
    businessKeyColumn="SalesTerritoryCode",
    scdPattern="SCD1",
    supportsInferred=False,
    attributeColumns=_cols(
        ("SalesTerritoryCode", "STRING"),
        ("WWISalesTerritoryID", "INT"),
        ("SalesTerritory", "STRING"),
        ("RegionCode", "STRING"),
        ("AlignmentYear", "INT"),
        ("ParentTerritoryCode", "STRING"),
        ("TerritoryLevelCode", "STRING"),
        ("TerritoryPath", "STRING"),
        ("TerritoryManagerEmployeeNo", "STRING"),
        ("CoverageList", "STRING"),
        ("CoverageModelCode", "STRING"),
        ("TaxJurisdictionCode", "STRING"),
        ("ReportingCurrencyCode", "STRING"),
        ("FiscalCalendarCode", "STRING"),
        ("AnnualTargetAmount", "DECIMAL(18,2)"),
        ("TargetCurrencyCode", "STRING"),
        ("SourceSystemCode", "STRING"),
        ("IsActive", "BOOLEAN"),
        ("RetiredOn", "DATE"),
    ),
    scd1Columns=("SalesTerritory", "ParentTerritoryCode", "TerritoryManagerEmployeeNo", "CoverageList",
                 "AnnualTargetAmount", "AlignmentYear"),
    labelColumns=("SalesTerritory",),
    reservedDefaults={"WWISalesTerritoryID": "KEY", "RegionCode": "GLOBAL", "IsActive": True},
)

CUSTOMER_SEGMENT = DimensionSpec(
    name="Customer Segment",
    tableName="dim_customer_segment",
    keyColumn="CustomerSegmentKey",
    businessKeyColumn="SegmentBusinessKey",
    scdPattern="SCD2",
    supportsInferred=False,
    attributeColumns=_cols(
        ("SegmentBusinessKey", "STRING"),
        ("WWICustomerSegmentID", "INT"),
        ("SegmentCode", "STRING"),
        ("CustomerSegment", "STRING"),
        ("SegmentFamilyCode", "STRING"),
        ("RegionCode", "STRING"),
        ("ScoringModelCode", "STRING"),
        ("ScoringModelVersion", "STRING"),
        ("ScoringFrequencyCode", "STRING"),
        ("LastScoredOn", "DATE"),
        ("RecencyBand", "STRING"),
        ("FrequencyBand", "STRING"),
        ("MonetaryBand", "STRING"),
        ("LifetimeValueBand", "STRING"),
        ("ChurnRiskBand", "STRING"),
        ("MinimumScore", "DECIMAL(9,4)"),
        ("MaximumScore", "DECIMAL(9,4)"),
        ("RecencyScoreFloor", "INT"),
        ("FrequencyScoreFloor", "INT"),
        ("MonetaryValueFloor", "DECIMAL(18,2)"),
        ("MonetaryFloorReporting", "DECIMAL(18,2)"),
        ("SegmentTierCode", "STRING"),
        ("ChurnWatchFlag", "BOOLEAN"),
        ("TargetContactFrequency", "SMALLINT"),
        ("RequiresProfilingConsent", "BOOLEAN"),
        ("ExcludedFromModelling", "BOOLEAN"),
        ("MarketplaceOnly", "BOOLEAN"),
        ("SourceSystemCode", "STRING"),
    ),
    scd2Columns=(
        "CustomerSegment", "ScoringModelCode", "ScoringModelVersion", "MinimumScore", "MaximumScore",
        "RecencyBand", "FrequencyBand", "MonetaryBand", "ChurnRiskBand",
    ),
    labelColumns=("CustomerSegment", "SegmentCode"),
    reservedDefaults={"WWICustomerSegmentID": "KEY", "RegionCode": "GLOBAL"},
    changeTimestampColumn="SourceChangedOn",
)

VENDOR_CONTRACT = DimensionSpec(
    name="Vendor Contract",
    tableName="dim_vendor_contract",
    keyColumn="VendorContractKey",
    businessKeyColumn="ContractBusinessKey",
    scdPattern="SCD2",
    supportsInferred=False,
    attributeColumns=_cols(
        ("ContractBusinessKey", "STRING"),
        ("ContractNumber", "STRING"),
        ("AmendmentNumber", "SMALLINT"),
        ("SupplierBusinessKey", "STRING"),
        ("SupplierKey", "INT"),
        ("ContractTitle", "STRING"),
        ("ContractTypeCode", "STRING"),
        ("ContractStatusCode", "STRING"),
        ("RegionCode", "STRING"),
        ("GoverningLawCountryCode", "STRING"),
        ("GoverningLawRegion", "STRING"),
        ("ContractCurrencyCode", "STRING"),
        ("ContractStartDate", "DATE"),
        ("ContractEndDate", "DATE"),
        ("AutoRenewFlag", "BOOLEAN"),
        ("RenewalNoticeDays", "INT"),
        ("CommittedSpendAmount", "DECIMAL(18,2)"),
        ("CommittedSpendReporting", "DECIMAL(18,2)"),
        ("ConsumedAmount", "DECIMAL(18,2)"),
        ("PaymentTermsCode", "STRING"),
        ("PriceProtectionCode", "STRING"),
        ("RebateTier1Percentage", "DECIMAL(9,4)"),
        ("RebateTier2Percentage", "DECIMAL(9,4)"),
        ("RebateTierCode", "STRING"),
        ("ServiceLevelTargetPct", "DECIMAL(9,4)"),
        ("SignedOn", "DATE"),
        ("SourceSystemCode", "STRING"),
        ("ClosedByLineageKey", "BIGINT"),
    ),
    scd2Columns=(
        "ContractTitle", "ContractStatusCode", "ContractEndDate", "CommittedSpendAmount",
        "PaymentTermsCode", "PriceProtectionCode", "RebateTier1Percentage", "RebateTier2Percentage",
        "ServiceLevelTargetPct", "AmendmentNumber",
    ),
    labelColumns=("ContractTitle", "ContractNumber"),
    reservedDefaults={"RegionCode": "GLOBAL"},
    changeTimestampColumn="ContractStartTimestamp",
)

PROMOTION = DimensionSpec(
    name="Promotion",
    tableName="dim_promotion",
    keyColumn="PromotionKey",
    businessKeyColumn="PromotionBusinessKey",
    scdPattern="SCD2",
    supportsInferred=True,
    attributeColumns=_cols(
        ("PromotionBusinessKey", "STRING"),
        ("WWIPromotionID", "INT"),
        ("PromotionCode", "STRING"),
        ("PromotionName", "STRING"),
        ("PromotionTypeCode", "STRING"),
        ("MechanicCode", "STRING"),
        ("Parameter1", "DECIMAL(18,4)"),
        ("Parameter2", "DECIMAL(18,4)"),
        ("Parameter3", "DECIMAL(18,4)"),
        ("ParameterCurrencyCode", "STRING"),
        ("RegionCode", "STRING"),
        ("ChannelScopeCode", "STRING"),
        ("ProductScopeCode", "STRING"),
        ("ProductCategoryCode", "STRING"),
        ("CustomerScopeCode", "STRING"),
        ("StartDate", "DATE"),
        ("EndDate", "DATE"),
        ("FundingSourceCode", "STRING"),
        ("BudgetAmount", "DECIMAL(18,2)"),
        ("TaxTreatmentCode", "STRING"),
        ("DiscountBasisCode", "STRING"),
        ("CampaignDurationDays", "INT"),
        ("IsCoFunded", "BOOLEAN"),
        ("HasOverlappingCampaign", "BOOLEAN"),
        ("SourceSystemCode", "STRING"),
        ("IsActive", "BOOLEAN"),
    ),
    scd2Columns=(
        "PromotionName", "MechanicCode", "Parameter1", "Parameter2", "Parameter3", "StartDate",
        "EndDate", "ProductScopeCode", "CustomerScopeCode", "FundingSourceCode", "BudgetAmount",
    ),
    labelColumns=("PromotionName", "PromotionCode"),
    reservedDefaults={"WWIPromotionID": "KEY", "RegionCode": "GLOBAL"},
    changeTimestampColumn="StartTimestamp",
)

SPECS: Dict[str, DimensionSpec] = {
    s.name: s
    for s in (
        CUSTOMER, CUSTOMER_CATEGORY, CUSTOMER_SEGMENT, SUPPLIER, VENDOR_CONTRACT, STOCK_ITEM,
        PRODUCT_CATEGORY, EMPLOYEE, SALESPERSON, CITY, SALES_TERRITORY, PROMOTION,
    )
}

# Fact columns rewritten by the late-arriving rekey (Integration.usp_RekeyLateArrivingDimensions
# repoints Fact.Sale / Fact.Movement / Fact.Purchase; the DIM_Rekey_LateArriving package also
# verifies Customer / Stock Item / City on Fact.Sale).
FACT_REKEY_TARGETS: Tuple[Tuple[str, str, str], ...] = (
    ("Customer", "fact_sale", "CustomerKey"),
    ("Customer", "fact_sale", "BillToCustomerKey"),
    ("Stock Item", "fact_sale", "StockItemKey"),
    ("City", "fact_sale", "CityKey"),
    ("Salesperson", "fact_sale", "SalespersonKey"),
    ("Stock Item", "fact_movement", "StockItemKey"),
    ("Customer", "fact_movement", "CustomerKey"),
    ("Supplier", "fact_movement", "SupplierKey"),
    ("Supplier", "fact_purchase", "SupplierKey"),
    ("Stock Item", "fact_purchase", "StockItemKey"),
)


def spec(name: str) -> DimensionSpec:
    return SPECS[name]
