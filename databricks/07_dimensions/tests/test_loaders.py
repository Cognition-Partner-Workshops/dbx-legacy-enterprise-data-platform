"""Transform-level tests for the per-dimension conditioning (loaders.py / customer.py / employee.py)."""

from datetime import date, datetime

from pyspark.sql import functions as F

from wwi_dimensions import customer, employee, loaders, scd, specs

BUSINESS_DATE = date(2025, 3, 31)
NOW = datetime(2025, 3, 31, 2, 0, 0)


def test_transform_supplier_payment_days_and_survivor_filter(spark):
    stg = spark.createDataFrame(
        [("SUP-1", "Acme", "ACME LTD", "N30", True, "A", None, False),
         ("SUP-2", "Dup", None, "NET45", False, "B", "NONE", True),
         ("SUP-3", "Cod", None, "COD", True, None, "WHT10", True)],
        "SupplierBusinessKey STRING, SupplierName STRING, SupplierNameStandardized STRING, PaymentTermsCode STRING, "
        "IsSurvivorRow BOOLEAN, ScorecardRatingCode STRING, WithholdingCode STRING, OnHoldFlag BOOLEAN",
    )
    for c in ("OltpSupplierId", "SourceSystemCode", "SourceSupplierId", "ErpSupplierNumber", "SupplierShortName", "SupplierCategoryCode",
              "RegionCode", "SupplierStatusCode", "LeadTimeDays", "PaymentMethodCode", "TransactionCurrencyCode", "DiversityClassCode",
              "HoldReasonCode", "SourceModifiedDate"):
        stg = stg.withColumn(c, F.lit(None).cast("string"))
    out = {r["SupplierBusinessKey"]: r for r in loaders.transformSupplier(stg).collect()}
    assert set(out) == {"SUP-1", "SUP-3"}                      # non-survivor dropped
    assert out["SUP-1"]["Supplier"] == "ACME LTD" and out["SUP-1"]["PaymentDays"] == 30 and out["SUP-1"]["IsPreferredSupplier"] is True
    assert out["SUP-3"]["PaymentDays"] == 0 and out["SUP-3"]["IsCrossBorderPayee"] is True and out["SUP-3"]["SanctionScreeningStatus"] == "BLOCKED"


def test_transform_vendor_contract_expires_lapsed_and_parses_amendment(spark):
    stg = spark.createDataFrame(
        [("VC-100-A2", "VC-100", "SUP-1", "MSA", "ACTIVE", "DE", "EUR", date(2024, 1, 1), date(2024, 12, 31), False, 30, 1000.0, 1100.0, 10.0, 2, 5.0, None, "ERP", "eu"),
         ("VC-200", "VC-200", "SUP-2", "SOW", "ACTIVE", "US", "USD", date(2025, 1, 1), date(2025, 12, 31), True, 60, 500.0, 500.0, 0.0, 0, 0.0, date(2024, 12, 1), "ERP", None)],
        "ContractBusinessKey STRING, ContractNumber STRING, SupplierBusinessKey STRING, ContractTypeCode STRING, ContractStatusCode STRING, "
        "GoverningLawCode STRING, TransactionCurrencyCode STRING, StartDate DATE, EndDate DATE, AutoRenewFlag BOOLEAN, NoticePeriodDays INT, "
        "CommittedAmount DOUBLE, CommittedAmountUsd DOUBLE, ConsumedAmount DOUBLE, RebateTierCount INT, TopRebatePercent DOUBLE, SignedDate DATE, SourceSystemCode STRING, RegionCode STRING",
    )
    out = {r["ContractBusinessKey"]: r for r in loaders.transformVendorContract(stg, BUSINESS_DATE).collect()}
    assert out["VC-100-A2"]["ContractStatusCode"] == "EXPIRED" and out["VC-100-A2"]["AmendmentNumber"] == 2
    assert out["VC-100-A2"]["GoverningLawRegion"] == "EU" and out["VC-100-A2"]["RebateTierCode"] == "TIER2" and out["VC-100-A2"]["RegionCode"] == "EU"
    assert out["VC-200"]["ContractStatusCode"] == "ACTIVE" and out["VC-200"]["GoverningLawRegion"] == "NA" and out["VC-200"]["RebateTierCode"] == "NONE" and out["VC-200"]["RegionCode"] == "GLOBAL"


def test_transform_stock_item_bands_margin_and_product_crosswalk(spark):
    stg = spark.createDataFrame(
        [("SI-1", "PRD-1", "Widget", 12.5, True, None), ("SI-2", "PRD-X", "Gadget", 300.0, False, "SUP-9")],
        "StockItemBusinessKey STRING, ProductBusinessKey STRING, StockItemName STRING, UnitPriceAmount DOUBLE, IsChillerStock BOOLEAN, SupplierBusinessKey STRING",
    )
    for c in ("OltpStockItemId", "SourceSystemCode", "BrandName", "SizeText", "UnitPackageCode", "OuterPackageCode", "LeadTimeDays", "QuantityPerOuter",
              "Barcode", "TaxRatePercent", "RecommendedRetailAmount", "TypicalWeightPerUnitKg", "MarketingTagList", "SourceModifiedDate"):
        stg = stg.withColumn(c, F.lit(None).cast("string"))
    product = spark.createDataFrame(
        [("PRD-1", "CAT-A", 5.0, "STD", "HZ3", "ACTIVE", None, 6.0, "A widget", "SUP-1")],
        "ProductBusinessKey STRING, CategoryCode STRING, StandardCostAmount DOUBLE, TaxClassCode STRING, HazmatClassCode STRING, "
        "LifecycleStatusCode STRING, DiscontinuedDate DATE, UomConversionFactor DOUBLE, ProductDescription STRING, PrimarySupplierBusinessKey STRING",
    )
    out = {r["StockItemBusinessKey"]: r for r in loaders.transformStockItem(stg, product).collect()}
    assert out["SI-1"]["ListingPriceBand"] == "MID" and out["SI-1"]["StandardCostBand"] == "C2" and out["SI-1"]["HandlingCode"] == "HAZMAT"
    assert float(out["SI-1"]["GrossMarginPercent"]) == 60.0 and out["SI-1"]["PrimarySupplierId"] == "SUP-1" and out["SI-1"]["CategoryCode"] == "CAT-A"
    assert out["SI-2"]["ListingPriceBand"] == "PREMIUM" and out["SI-2"]["StandardCostBand"] == "UNKNOWN" and out["SI-2"]["PrimarySupplierId"] == "SUP-9"
    assert out["SI-2"]["GrossMarginPercent"] is None and out["SI-2"]["IsDiscontinued"] is False


def test_transform_employee_status_manager_and_tenure(spark):
    stg = spark.createDataFrame(
        [("EMP-1", "1001", "Ann Boss", None, None, "Sales", "Sales Director", date(2015, 1, 1), None, "NA", False, False),
         ("EMP-2", "1002", "Bob Rep", "EMP-1", None, "Sales", "Account Manager", date(2020, 6, 1), None, "NA", True, False),
         ("EMP-3", "1003", "Cid Gone", "EMP-1", None, "Sales", "Rep", date(2018, 1, 1), date(2024, 1, 1), "EU", True, True)],
        "EmployeeBusinessKey STRING, SourceEmployeeId STRING, EmployeeFullName STRING, ManagerEmployeeKey STRING, PreferredName STRING, "
        "DepartmentName STRING, JobTitle STRING, HireDate DATE, TerminationDate DATE, RegionCode STRING, IsSalesperson BOOLEAN, RetentionMaskedFlag BOOLEAN",
    )
    for c in ("SourceSystemCode", "CostCenterCode", "WorkEmailAddress", "IsWarehouseStaff"):
        stg = stg.withColumn(c, F.lit(None).cast("string" if c != "IsWarehouseStaff" else "boolean"))
    out = {r["EmployeeBusinessKey"]: r for r in loaders.transformEmployee(stg, BUSINESS_DATE).collect()}
    assert out["EMP-1"]["IsManager"] is True and out["EMP-1"]["IsLeafNode"] is False and out["EMP-1"]["JobGradeCode"] == "EXEC" and out["EMP-1"]["TenureYears"] == 10
    assert out["EMP-2"]["EmploymentStatusCode"] == "ACTIVE" and out["EMP-2"]["ManagerEmployeeNumber"] == "EMP-1" and out["EMP-2"]["JobGradeCode"] == "MGMT"
    assert out["EMP-3"]["EmploymentStatusCode"] == "TERMINATED" and out["EMP-3"]["IsActive"] is False and out["EMP-3"]["Employee"] == "Retention Masked"
    assert out["EMP-3"]["TenureYears"] == 6 and out["EMP-3"]["PayrollCalendarCode"] == "MONTHLY"


def test_employee_manager_repair_and_hierarchy(spark, catalog, cleanGold):
    spec = specs.EMPLOYEE
    from wwi_dimensions import runtime
    runtime.prepareDimension(spark, catalog, spec)
    src = spark.createDataFrame(
        [("EMP-1", "Ann", None), ("EMP-2", "Bob", "EMP-1"), ("EMP-3", "Cid", "EMP-2"), ("EMP-4", "Dan", "EMP-404")],
        "EmployeeBusinessKey STRING, Employee STRING, ManagerEmployeeNumber STRING",
    )
    scd.applyScd(spark, catalog, spec, src, 1, 10, loadTimestamp=NOW)
    repaired = employee.repairManagerKeys(spark, catalog, 1, 10, lineageKey=10)
    employee.refreshHierarchy(spark, catalog)
    rows = {r["EmployeeBusinessKey"]: r for r in spark.table(spec.fullTableName(catalog)).where("EmployeeKey > 0").collect()}
    assert repaired == 4
    assert rows["EMP-1"]["ManagerEmployeeKey"] == -2 and rows["EMP-1"]["OrganisationLevel"] == 1 and rows["EMP-1"]["IsLeafNode"] is False
    assert rows["EMP-2"]["ManagerEmployeeKey"] == rows["EMP-1"]["EmployeeKey"] and rows["EMP-2"]["OrganisationLevel"] == 2
    assert rows["EMP-3"]["ManagerEmployeeKey"] == rows["EMP-2"]["EmployeeKey"] and rows["EMP-3"]["OrganisationLevel"] == 3 and rows["EMP-3"]["IsLeafNode"] is True
    assert rows["EMP-4"]["ManagerEmployeeKey"] == -1 and rows["EMP-4"]["RekeyedByLineageKey"] == 10
    # idempotent: second pass changes nothing
    assert employee.repairManagerKeys(spark, catalog, 2, 20, lineageKey=20) == 0


def test_salesperson_territory_bridge_closes_and_opens(spark, catalog, cleanGold):
    from wwi_dimensions import runtime
    spec = specs.SALESPERSON
    runtime.prepareDimension(spark, catalog, spec)
    scd.applyScd(spark, catalog, spec, spark.createDataFrame([("SP-1", "Sam", "T-5", 5)], "SalespersonBusinessKey STRING, Salesperson STRING, SalesTerritoryCode STRING, SalesTerritoryKey INT"), 1, 10, loadTimestamp=NOW)
    first = employee.maintainTerritoryBridge(spark, catalog, NOW, lineageKey=10)
    assert first == {"closed": 0, "opened": 1}
    scd.applyScd(spark, catalog, spec, spark.createDataFrame([("SP-1", "Sam", "T-6", 6)], "SalespersonBusinessKey STRING, Salesperson STRING, SalesTerritoryCode STRING, SalesTerritoryKey INT"), 2, 20, loadTimestamp=datetime(2025, 4, 1))
    second = employee.maintainTerritoryBridge(spark, catalog, datetime(2025, 4, 1), lineageKey=20)
    assert second == {"closed": 1, "opened": 1}
    bridge = spark.table(f"{catalog}.gold.dim_salesperson_territory_bridge").collect()
    assert sorted((r["SalesTerritoryKey"], r["IsCurrentAssignment"]) for r in bridge) == [(5, False), (6, True)]
    assert spark.table(spec.fullTableName(catalog)).where("SalespersonKey > 0").count() == 2   # territory move = new Type 2 version
    assert employee.maintainTerritoryBridge(spark, catalog, datetime(2025, 4, 2), lineageKey=21) == {"closed": 0, "opened": 0}


def test_city_population_threshold_splits_type1_from_type2(spark):
    source = spark.createDataFrame([("C-1", 1020), ("C-2", 2000), ("C-3", 500)], "CityBusinessKey STRING, LatestRecordedPopulation BIGINT")
    current = spark.createDataFrame([("C-1", 1000), ("C-2", 1000)], "CityBusinessKey STRING, LatestRecordedPopulation BIGINT")
    out = {r["CityBusinessKey"]: r for r in loaders.applyPopulationThreshold(source, current, 5.0).collect()}
    assert out["C-1"]["LatestRecordedPopulation"] == 1000 and out["C-1"]["_PopulationType1"] == 1020   # 2% drift -> Type 1
    assert out["C-2"]["LatestRecordedPopulation"] == 2000 and out["C-2"]["_PopulationType1"] is None    # 100% -> Type 2
    assert out["C-3"]["LatestRecordedPopulation"] == 500 and out["C-3"]["_PopulationType1"] is None     # new member


def test_promotion_tax_treatment_default_and_overlap(spark):
    stg = spark.createDataFrame(
        [("PR-1", "SPRING", "NA", "CAT-1", date(2025, 3, 1), date(2025, 3, 31), 10.0, None, None),
         ("PR-2", "EASTER", "NA", "CAT-1", date(2025, 3, 20), date(2025, 4, 10), None, 5.0, "CUSTOM"),
         ("PR-3", "SOLO", "APAC", None, date(2025, 3, 1), date(2025, 3, 2), None, None, None)],
        "PromotionBusinessKey STRING, PromotionCode STRING, RegionCode STRING, AppliesToCategoryCode STRING, StartDate DATE, EndDate DATE, "
        "DiscountPercent DOUBLE, DiscountAmount DOUBLE, TaxTreatmentCode STRING",
    )
    for c in ("PromotionName", "PromotionTypeCode", "DiscountCurrencyCode", "AppliesToChannelCode", "BudgetAmountUsd", "SourceSystemCode", "IsActive"):
        stg = stg.withColumn(c, F.lit(None).cast("boolean" if c == "IsActive" else "string"))
    out = {r["PromotionBusinessKey"]: r for r in loaders.transformPromotion(stg).collect()}
    assert out["PR-1"]["TaxTreatmentCode"] == "SALES_TAX_EXCLUSIVE" and out["PR-1"]["MechanicCode"] == "PERCENT_OFF" and out["PR-1"]["HasOverlappingCampaign"] is True
    assert out["PR-2"]["TaxTreatmentCode"] == "CUSTOM" and out["PR-2"]["MechanicCode"] == "AMOUNT_OFF" and out["PR-2"]["CampaignDurationDays"] == 22
    assert out["PR-3"]["TaxTreatmentCode"] == "GST_INCLUSIVE" and out["PR-3"]["HasOverlappingCampaign"] is False and out["PR-3"]["ProductScopeCode"] == "ALL"


def test_segment_assignment_consent_and_exclusions(spark):
    segments = spark.createDataFrame(
        [("GOLD", "NA", 11, "RFM", True, False), ("RFM_DEF", "NA", 12, "RFM", False, False), ("MKT", "NA", 13, "MKT", False, True), ("GOLD", "EU", 21, "RFM", True, False)],
        "SegmentCode STRING, RegionCode STRING, CustomerSegmentKey INT, SegmentFamilyCode STRING, RequiresProfilingConsent BOOLEAN, ExcludedFromModelling BOOLEAN",
    )
    assignments = spark.createDataFrame(
        [("CUST-1", "NA", "GOLD", True, date(2025, 3, 1)), ("CUST-2", "NA", "GOLD", False, date(2025, 3, 1)), ("CUST-3", "NA", "MKT", True, date(2025, 3, 1)),
         ("CUST-4", "EU", "GOLD", False, date(2025, 3, 1)), ("CUST-1", "NA", "MKT", True, date(2025, 1, 1))],
        "CustomerBusinessKey STRING, RegionCode STRING, SegmentCode STRING, ProfilingConsentFlag BOOLEAN, AssignedOn DATE",
    )
    out = {r["CustomerBusinessKey"]: r["CustomerSegmentKey"] for r in loaders.resolveSegmentAssignments(assignments, segments).collect()}
    assert out == {"CUST-1": 11, "CUST-2": 12, "CUST-3": -2, "CUST-4": -2}


def test_customer_source_preparation_and_conform(spark):
    customers = spark.createDataFrame(
        [("CUST-1", "NA", "Acme", True, 20000.0, "ACTIVE", "US"), ("CUST-2", "EU", "Beta", True, None, None, "DE"), ("CUST-3", "NA", "Dup", False, 1.0, None, "US")],
        "CustomerBusinessKey STRING, RegionCode STRING, CustomerName STRING, IsSurvivorRow BOOLEAN, CreditLimitAmountUsd DOUBLE, CustomerStatusCode STRING, PrimaryCountryCode STRING",
    )
    addresses = spark.createDataFrame(
        [("CUST-1", False, "Springfield", "IL", "Sangamon", "62701", "US"), ("CUST-1", True, "Chicago", "IL", "Cook", "60601-1234", "US")],
        "CustomerBusinessKey STRING, IsPrimaryAddress BOOLEAN, CityName STRING, StateProvinceCode STRING, CountyName STRING, PostalCodeRaw STRING, CountryCode STRING",
    )
    prepared = customer.prepareCustomerSource(customers, "NA", addresses)
    rows = {r["CustomerBusinessKey"]: r for r in prepared.collect()}
    assert set(rows) == {"CUST-1"}                                   # EU row and non-survivor filtered out
    assert rows["CUST-1"]["CityName"] == "Chicago" and rows["CUST-1"]["PostalCodeRaw"] == "60601-1234"
    conformed = customer.conformCustomer(prepared.withColumn("Customer", F.col("CustomerName")).withColumn("RetentionYears", F.lit(7)).withColumn("LastActivityDate", F.lit("2025-01-15").cast("date")))
    r = conformed.collect()[0]
    assert r["CreditLimitBand"] == "SILVER" and r["AccountStatusCode"] == "ACTIVE" and r["CustomerSegmentKey"] == -2
    assert str(r["RetentionExpiryDate"]) == "2032-01-15" and r["CountryCode"] == "US"
