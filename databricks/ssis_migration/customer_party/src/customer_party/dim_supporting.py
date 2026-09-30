"""DIM_Load_CustomerCategory, DIM_Load_CustomerSegment, DIM_Load_Employee,
DIM_Load_Salesperson, DIM_Load_SalesTerritory, DIM_Load_Promotion.

Legacy `stg.CustomerCategory` / `stg.CustomerSegment` are never populated by
any package in the estate (no STG_* package targets them), so those two
dimensions stage their input directly from the OLTP source tables through
federation - a documented deviation.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from customer_party.config import HIGH_DATE, LEGACY_OLTP, PipelineConfig, utcNow
from customer_party.extract import RECORD_KIND_SEGMENT, readRawRecordKind
from customer_party.scd import (
    UNKNOWN_KEY,
    ScdSpec,
    applyScd1,
    applyScd2,
    rowHash,
    withReservedMembers,
)
from customer_party.staging import (
    STG_EMPLOYEE,
    STG_PROMOTION,
    STG_SALES_TERRITORY,
    STG_SALESPERSON,
    payloadCol,
)
from customer_party.tables import overwriteTable, tableExists, withLoadMetadata

STG_CUSTOMER_CATEGORY = "stg_customer_category"
STG_CUSTOMER_SEGMENT = "stg_customer_segment"
DIM_CUSTOMER_CATEGORY = "dim_customer_category"
DIM_CUSTOMER_SEGMENT = "dim_customer_segment"
DIM_EMPLOYEE = "dim_employee"
DIM_SALESPERSON = "dim_salesperson"
DIM_SALESPERSON_TERRITORY_BRIDGE = "dim_salesperson_territory_bridge"
DIM_SALES_TERRITORY = "dim_sales_territory"
DIM_PROMOTION = "dim_promotion"

SCORING_MODEL_VERSION = "RFM-2024.1"


def _existing(spark: SparkSession, fqn: str) -> DataFrame | None:
    return spark.table(fqn).drop("batch_id", "load_ts") if tableExists(spark, fqn) else None


# --------------------------------------------------------------------------- customer category (SCD1)
CATEGORY_SPEC = ScdSpec(
    keyCol="customer_category_key",
    businessKeyCol="wwi_customer_category_id",
    trackedCols=("customer_category", "category_name_upper", "discount_band_code", "discount_eligible_percent"),
)


def discountBandCode(percentCol: Column) -> Column:
    return F.when(percentCol >= 15, F.lit("D3")).when(percentCol >= 5, F.lit("D2")).otherwise(F.lit("D1"))


def stageCustomerCategories(categoryDf: DataFrame, customerDf: DataFrame) -> DataFrame:
    """Categories with the average standard discount of their customers as `discount_eligible_percent`."""
    discounts = customerDf.groupBy(F.col("CustomerCategoryID").alias("c_cat")).agg(
        F.avg("StandardDiscountPercentage").cast("decimal(9,4)").alias("discount_eligible_percent")
    )
    return (
        categoryDf.join(discounts, F.col("CustomerCategoryID") == F.col("c_cat"), "left")
        .select(
            F.col("CustomerCategoryID").cast("bigint").alias("wwi_customer_category_id"),
            F.trim(F.col("CustomerCategoryName")).alias("customer_category"),
            F.coalesce(F.col("discount_eligible_percent"), F.lit(0)).cast("decimal(9,4)").alias("discount_eligible_percent"),
            F.col("ValidFrom").cast("timestamp").alias("source_valid_from"),
            F.col("ValidTo").cast("timestamp").alias("source_valid_to"),
        )
    )


def buildCategoryCandidates(stagedDf: DataFrame) -> DataFrame:
    return (
        stagedDf.withColumn("category_name_upper", F.upper(F.trim(F.col("customer_category"))))
        .withColumn("discount_band_code", discountBandCode(F.col("discount_eligible_percent")))
        .withColumn("valid_from", F.col("source_valid_from"))
        .withColumn("valid_to", F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn("source_row_hash", rowHash(CATEGORY_SPEC.trackedCols))
    )


def loadCustomerCategoryDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_CustomerCategory (SCD1 overwrite on checksum change)."""
    categories = spark.table(f"{LEGACY_OLTP}.Sales.CustomerCategories")
    customers = spark.table(f"{LEGACY_OLTP}.Sales.Customers")
    staged = stageCustomerCategories(categories, customers)
    overwriteTable(withLoadMetadata(staged, cfg), cfg.fqn(STG_CUSTOMER_CATEGORY))
    fqn = cfg.fqn(DIM_CUSTOMER_CATEGORY)
    merged = applyScd1(_existing(spark, fqn), buildCategoryCandidates(spark.table(cfg.fqn(STG_CUSTOMER_CATEGORY)).drop("batch_id", "load_ts")), CATEGORY_SPEC)
    merged = withReservedMembers(spark, merged, CATEGORY_SPEC, ("customer_category", "category_name_upper"))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    return spark.table(fqn)


# --------------------------------------------------------------------------- customer segment (SCD2)
SEGMENT_SPEC = ScdSpec(
    keyCol="customer_segment_key",
    businessKeyCol="wwi_segment_id",
    trackedCols=(
        "segment_code", "segment_name", "segment_family", "region_code", "monetary_value_floor",
        "monetary_floor_reporting", "segment_tier_code", "churn_watch_flag", "scoring_model_version",
        "consent_required", "retention_months", "is_active",
    ),
)


def segmentTierCode(recencyCol: Column, frequencyCol: Column) -> Column:
    return (
        F.when((recencyCol >= 4) & (frequencyCol >= 4), F.lit("PLATINUM"))
        .when(recencyCol >= 3, F.lit("GOLD"))
        .when(recencyCol >= 2, F.lit("SILVER"))
        .otherwise(F.lit("BRONZE"))
    )


def monetaryFloorReporting(regionCol: Column, floorCol: Column) -> Column:
    return (
        F.when(regionCol == "EU", floorCol * F.lit(1.08))
        .when(regionCol == "APAC", floorCol * F.lit(0.74))
        .otherwise(floorCol)
    ).cast("decimal(18,2)")


def stageCustomerSegments(segmentDf: DataFrame, segmentAssignRaw: DataFrame) -> DataFrame:
    """Segment definitions plus the RFM floors implied by their current assignments (score = monetary floor)."""
    assigned = segmentAssignRaw.groupBy(payloadCol("CustomerSegmentID", "bigint").alias("a_seg")).agg(
        F.min(payloadCol("ScoreValue", "decimal(18,2)")).alias("monetary_value_floor"),
        F.count("*").alias("assigned_customer_count"),
    )
    priority = F.col("PriorityOrder").cast("int")
    return (
        segmentDf.join(assigned, F.col("CustomerSegmentID").cast("bigint") == F.col("a_seg"), "left")
        .select(
            F.col("CustomerSegmentID").cast("bigint").alias("wwi_segment_id"),
            F.upper(F.trim(F.col("SegmentCode"))).alias("segment_code"),
            F.trim(F.col("SegmentName")).alias("segment_name"),
            F.upper(F.trim(F.col("SegmentFamily"))).alias("segment_family"),
            F.upper(F.trim(F.col("RegionCode"))).alias("region_code"),
            F.col("SegmentRuleText").alias("segment_rule_text"),
            F.col("ConsentRequired").alias("consent_required"),
            F.col("ConsentBasisCode").alias("consent_basis_code"),
            F.col("RetentionMonths").cast("int").alias("retention_months"),
            priority.alias("priority_order"),
            F.col("IsExclusive").alias("is_exclusive"),
            F.col("IsActive").alias("is_active"),
            F.coalesce(F.col("monetary_value_floor"), F.lit(0)).cast("decimal(18,2)").alias("monetary_value_floor"),
            F.coalesce(F.col("assigned_customer_count"), F.lit(0)).alias("assigned_customer_count"),
            F.when(priority <= 10, F.lit(5)).when(priority <= 20, F.lit(4)).when(priority <= 30, F.lit(3)).when(priority <= 40, F.lit(2)).otherwise(F.lit(1)).alias("recency_score_floor"),
            F.when(F.upper(F.col("SegmentFamily")) == "VALUE", F.lit(4)).when(F.upper(F.col("SegmentFamily")) == "LIFECYCLE", F.lit(2)).otherwise(F.lit(3)).alias("frequency_score_floor"),
            F.when(F.upper(F.col("SegmentFamily")) == "RISK", F.lit("HIGH")).when(F.upper(F.col("SegmentFamily")) == "LIFECYCLE", F.lit("MED")).otherwise(F.lit("LOW")).alias("churn_risk_band"),
            F.col("LastEditedWhen").cast("timestamp").alias("source_modified_date"),
        )
    )


def buildSegmentCandidates(stagedDf: DataFrame, scoringModelVersion: str = SCORING_MODEL_VERSION) -> DataFrame:
    return (
        stagedDf.withColumn("monetary_floor_reporting", monetaryFloorReporting(F.col("region_code"), F.col("monetary_value_floor")))
        .withColumn("segment_tier_code", segmentTierCode(F.col("recency_score_floor"), F.col("frequency_score_floor")))
        .withColumn("churn_watch_flag", F.col("churn_risk_band").isin("HIGH", "CRIT"))
        .withColumn("scoring_model_version", F.lit(scoringModelVersion))
        .withColumn("source_row_hash", rowHash(SEGMENT_SPEC.trackedCols))
    )


def loadCustomerSegmentDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_CustomerSegment (SCD2)."""
    segments = spark.table(f"{LEGACY_OLTP}.Sales.CustomerSegments")
    staged = stageCustomerSegments(segments, readRawRecordKind(spark, cfg, RECORD_KIND_SEGMENT))
    overwriteTable(withLoadMetadata(staged, cfg), cfg.fqn(STG_CUSTOMER_SEGMENT))
    fqn = cfg.fqn(DIM_CUSTOMER_SEGMENT)
    candidates = buildSegmentCandidates(spark.table(cfg.fqn(STG_CUSTOMER_SEGMENT)).drop("batch_id", "load_ts"))
    merged = applyScd2(_existing(spark, fqn), candidates, SEGMENT_SPEC, F.lit(utcNow()).cast("timestamp"))
    merged = withReservedMembers(spark, merged, SEGMENT_SPEC, ("segment_name", "segment_code"))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    return spark.table(fqn)


# --------------------------------------------------------------------------- employee (SCD2)
EMPLOYEE_SPEC = ScdSpec(
    keyCol="employee_key",
    businessKeyCol="wwi_employee_id",
    trackedCols=(
        "employee", "preferred_name", "given_name", "family_name", "is_salesperson", "role_code", "email_address",
        "phone_number_last4", "employment_status_code", "payroll_calendar_code", "manager_employee_id",
    ),
)


def employmentStatusCode(terminationCol: Column, asOfCol: Column) -> Column:
    return F.when(terminationCol.isNull(), F.lit("ACTIVE")).when(terminationCol > asOfCol, F.lit("NOTICE")).otherwise(F.lit("LEAVER"))


def payrollCalendarCode(regionCol: Column) -> Column:
    return F.when(regionCol == "NA", F.lit("SEMI-MONTHLY")).when(regionCol == "EU", F.lit("MONTHLY-EOM")).otherwise(F.lit("MONTHLY-25TH"))


def buildEmployeeCandidates(stagedDf: DataFrame, asOf: datetime) -> DataFrame:
    """Manager = the salesperson with the most recent ValidFrom among the employee's peers is not known in WWI;
    the estate resolves managers through `Manager Employee ID`, which People does not carry, so the manager
    key resolves to -1 (Unknown) and is repaired by `repairManagerKeys` when a manager ID is present."""
    now = F.lit(asOf).cast("timestamp")
    termination = F.when(F.col("is_current_flag") == "N", F.col("valid_to"))
    return (
        stagedDf.select(
            F.col("employee_id").cast("bigint").alias("wwi_employee_id"),
            F.col("full_name").alias("employee"),
            "preferred_name",
            "given_name",
            "family_name",
            F.coalesce(F.col("is_salesperson"), F.lit(False)).alias("is_salesperson"),
            "role_code",
            "email_address",
            "phone_number_last4",
            F.col("region_code").alias("payroll_region_code"),
            F.col("valid_from").alias("hire_date"),
            termination.alias("termination_date"),
            F.lit(None).cast("bigint").alias("manager_employee_id"),
        )
        .withColumn("employment_status_code", employmentStatusCode(F.col("termination_date"), now))
        .withColumn("tenure_years", (F.datediff(F.coalesce(F.col("termination_date"), now), F.col("hire_date")) / F.lit(365)).cast("int"))
        .withColumn("payroll_calendar_code", payrollCalendarCode(F.col("payroll_region_code")))
        .withColumn("manager_key", F.lit(UNKNOWN_KEY).cast("bigint"))
        .withColumn("source_row_hash", rowHash(EMPLOYEE_SPEC.trackedCols))
    )


def repairManagerKeys(dimDf: DataFrame) -> DataFrame:
    """`UPDATE e SET Manager Key = m.Employee Key ... WHERE e.Manager Key = -1` and rebuild reporting depth."""
    managers = dimDf.where(F.col("is_current_row") & (F.col("employee_key") >= 0)).select(
        F.col("wwi_employee_id").alias("m_id"), F.col("employee_key").alias("m_key")
    )
    repaired = (
        dimDf.join(managers, (F.col("manager_employee_id") == F.col("m_id")) & (F.col("manager_key") == UNKNOWN_KEY) & F.col("is_current_row"), "left")
        .withColumn("manager_key", F.coalesce(F.col("m_key"), F.col("manager_key")))
        .drop("m_id", "m_key")
    )
    return repaired.withColumn("reporting_depth", F.when(F.col("manager_key").isin(UNKNOWN_KEY, 0), F.lit(1)).otherwise(F.lit(2)))


def loadEmployeeDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_Employee (SCD2 + manager key repair)."""
    staged = spark.table(cfg.fqn(STG_EMPLOYEE))
    fqn = cfg.fqn(DIM_EMPLOYEE)
    asOf = utcNow()
    merged = applyScd2(_existing(spark, fqn), buildEmployeeCandidates(staged, asOf), EMPLOYEE_SPEC, F.lit(asOf).cast("timestamp"))
    merged = repairManagerKeys(withReservedMembers(spark, merged, EMPLOYEE_SPEC, ("employee", "preferred_name")))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    return spark.table(fqn)


# --------------------------------------------------------------------------- sales territory (SCD1)
TERRITORY_SPEC = ScdSpec(
    keyCol="sales_territory_key",
    businessKeyCol="wwi_territory_id",
    trackedCols=(
        "sales_territory", "region_code", "country_code", "parent_region_code", "territory_path",
        "tax_jurisdiction_normalised", "fiscal_calendar_code", "reporting_currency_code", "is_active",
    ),
)


def territoryPath(areaCol: Column, parentCol: Column, codeCol: Column) -> Column:
    return F.concat_ws("/", F.upper(F.trim(areaCol)), F.upper(F.trim(parentCol)), F.upper(F.trim(codeCol)))


def taxJurisdictionNormalised(regionCol: Column, taxCol: Column) -> Column:
    tax = F.trim(F.coalesce(taxCol, F.lit("")))
    return (
        F.when(regionCol == "NA", F.concat(F.lit("US-"), F.substring(tax, -2, 2)))
        .when(regionCol == "EU", F.concat(F.lit("VAT-"), F.substring(tax, 1, 2)))
        .otherwise(F.concat(F.lit("GST-"), F.upper(tax)))
    )


def buildTerritoryCandidates(stagedDf: DataFrame) -> DataFrame:
    return (
        stagedDf.select(
            F.col("sales_territory_code").alias("wwi_territory_id"),
            F.col("sales_territory_id").cast("bigint").alias("source_territory_id"),
            F.col("sales_territory_name").alias("sales_territory"),
            "region_code",
            "country_code",
            F.col("parent_territory_code").alias("parent_region_code"),
            F.col("region_code").alias("area_code"),
            "territory_level",
            "tax_jurisdiction_code",
            "fiscal_calendar_code",
            "reporting_currency_code",
            "is_active",
        )
        .withColumn("territory_path", territoryPath(F.col("area_code"), F.col("parent_region_code"), F.col("wwi_territory_id")))
        .withColumn("tax_jurisdiction_normalised", taxJurisdictionNormalised(F.col("region_code"), F.col("tax_jurisdiction_code")))
        .withColumn("source_row_hash", rowHash(TERRITORY_SPEC.trackedCols))
    )


def reparentOrphans(dimDf: DataFrame) -> DataFrame:
    """Territories whose parent is neither a territory nor a region root are reparented to UNASSIGNED."""
    parents = dimDf.where(F.col("sales_territory_key") >= 0).select(F.col("wwi_territory_id").alias("p_id")).distinct()
    regionRoots = F.col("parent_region_code").isin("NA", "EU", "APAC")
    joined = dimDf.join(parents, F.col("parent_region_code") == F.col("p_id"), "left")
    orphan = (F.col("sales_territory_key") >= 0) & F.col("p_id").isNull() & ~regionRoots
    return (
        joined.withColumn("parent_region_code", F.when(orphan, F.lit("UNASSIGNED")).otherwise(F.col("parent_region_code")))
        .withColumn("territory_path", F.when(orphan, F.concat(F.lit("UNASSIGNED/"), F.col("wwi_territory_id"))).otherwise(F.col("territory_path")))
        .drop("p_id")
    )


def loadSalesTerritoryDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_SalesTerritory (SCD1 + hierarchy rebuild)."""
    staged = spark.table(cfg.fqn(STG_SALES_TERRITORY))
    fqn = cfg.fqn(DIM_SALES_TERRITORY)
    merged = applyScd1(_existing(spark, fqn), buildTerritoryCandidates(staged), TERRITORY_SPEC)
    merged = reparentOrphans(withReservedMembers(spark, merged, TERRITORY_SPEC, ("sales_territory",)))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    return spark.table(fqn)


# --------------------------------------------------------------------------- salesperson (SCD2)
SALESPERSON_SPEC = ScdSpec(
    keyCol="salesperson_key",
    businessKeyCol="wwi_salesperson_id",
    trackedCols=(
        "salesperson", "sales_territory_key", "sales_territory_code", "annual_quota_amount", "quota_currency_code",
        "annual_quota_reporting", "quota_band_code", "commission_scheme_code",
    ),
)


def quotaBandCode(quotaCol: Column) -> Column:
    return (
        F.when(quotaCol >= 5_000_000, F.lit("Q4"))
        .when(quotaCol >= 2_000_000, F.lit("Q3"))
        .when(quotaCol >= 500_000, F.lit("Q2"))
        .otherwise(F.lit("Q1"))
    )


def commissionSchemeCode(regionCol: Column) -> Column:
    return F.when(regionCol == "NA", F.lit("ACCEL-NA")).when(regionCol == "EU", F.lit("FLAT-EU")).otherwise(F.lit("TIERED-APAC"))


def buildSalespersonCandidates(stagedDf: DataFrame, territoryDimDf: DataFrame) -> DataFrame:
    """Territory surrogate lookup (miss -> -1 Unknown, `Unknown Territory` output) and quota derivations."""
    territories = territoryDimDf.where(F.col("sales_territory_key") >= 0).select(
        F.col("wwi_territory_id").alias("t_code"),
        F.col("source_territory_id").cast("string").alias("t_id"),
        F.col("sales_territory_key").alias("t_key"),
        F.col("region_code").alias("t_region"),
    )
    joined = stagedDf.join(
        territories,
        (F.col("sales_territory_code") == F.col("t_code")) | (F.col("sales_territory_code") == F.col("t_id")),
        "left",
    )
    return (
        joined.select(
            F.col("employee_id").cast("bigint").alias("wwi_salesperson_id"),
            F.col("full_name").alias("salesperson"),
            F.coalesce(F.col("t_key"), F.lit(UNKNOWN_KEY)).cast("bigint").alias("sales_territory_key"),
            "sales_territory_code",
            F.coalesce(F.col("t_region"), F.lit("NA")).alias("region_code"),
            F.col("quota_amount").alias("annual_quota_amount"),
            "quota_currency_code",
            F.col("quota_amount_usd").alias("annual_quota_usd"),
            F.col("effective_from_date"),
            F.col("t_key").isNull().alias("territory_unresolved"),
        )
        .withColumn("annual_quota_reporting", F.when(F.col("quota_currency_code") == "USD", F.col("annual_quota_amount")).cast("decimal(18,2)"))
        .withColumn("quota_band_code", quotaBandCode(F.col("annual_quota_amount")))
        .withColumn("commission_scheme_code", commissionSchemeCode(F.col("region_code")))
        .withColumn("source_row_hash", rowHash(SALESPERSON_SPEC.trackedCols))
    )


def buildTerritoryBridge(dimDf: DataFrame, asOf: datetime) -> DataFrame:
    """Salesperson-territory bridge: one current assignment per current salesperson version."""
    now = F.lit(asOf).cast("timestamp")
    return dimDf.where(F.col("is_current_row") & (F.col("salesperson_key") >= 0)).select(
        "salesperson_key",
        "sales_territory_key",
        F.col("valid_from").alias("assignment_started"),
        F.lit(None).cast("timestamp").alias("assignment_ended"),
        F.lit(True).alias("is_current_assignment"),
        now.alias("bridge_refreshed_at"),
    )


def loadSalespersonDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_Salesperson (SCD2 + territory bridge)."""
    staged = spark.table(cfg.fqn(STG_SALESPERSON))
    territories = spark.table(cfg.fqn(DIM_SALES_TERRITORY))
    fqn = cfg.fqn(DIM_SALESPERSON)
    asOf = utcNow()
    candidates = buildSalespersonCandidates(staged, territories)
    merged = applyScd2(_existing(spark, fqn), candidates, SALESPERSON_SPEC, F.col("effective_from_date"))
    merged = withReservedMembers(spark, merged, SALESPERSON_SPEC, ("salesperson",))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    bridge = buildTerritoryBridge(spark.table(fqn), asOf)
    overwriteTable(withLoadMetadata(bridge, cfg), cfg.fqn(DIM_SALESPERSON_TERRITORY_BRIDGE))
    return spark.table(fqn)


# --------------------------------------------------------------------------- promotion (SCD2)
PROMOTION_SPEC = ScdSpec(
    keyCol="promotion_key",
    businessKeyCol="wwi_promotion_id",
    trackedCols=(
        "promotion", "promotion_code", "region_code", "mechanic_code", "discount_percent", "discount_amount",
        "tax_treatment_code", "discount_basis_code", "campaign_start_date", "campaign_end_date",
        "campaign_duration_days", "is_co_funded", "promotion_status",
    ),
)


def taxTreatmentCode(regionCol: Column) -> Column:
    return F.when(regionCol == "NA", F.lit("TAX-AFTER-DISCOUNT")).when(regionCol == "EU", F.lit("VAT-ON-NET")).otherwise(F.lit("GST-ON-GROSS"))


def discountBasisCode(percentCol: Column, amountCol: Column) -> Column:
    return (
        F.when((percentCol > 0) & (amountCol > 0), F.lit("BOTH"))
        .when(percentCol > 0, F.lit("PERCENT"))
        .when(amountCol > 0, F.lit("AMOUNT"))
        .otherwise(F.lit("NONE"))
    )


def buildPromotionCandidates(stagedDf: DataFrame, asOf: datetime) -> tuple[DataFrame, DataFrame]:
    now = F.lit(asOf).cast("timestamp")
    fundingSource = F.when(F.col("supplier_funded_percent") >= 100, F.lit("SUPP")).when(F.col("supplier_funded_percent") > 0, F.lit("JOINT")).otherwise(F.lit("WWI"))
    derived = (
        stagedDf.select(
            F.col("promotion_id").cast("bigint").alias("wwi_promotion_id"),
            F.col("promotion_name").alias("promotion"),
            "promotion_code",
            "region_code",
            "mechanic_code",
            "campaign_reference",
            "discount_percent",
            "discount_amount",
            F.col("valid_from_date").alias("campaign_start_date"),
            F.col("valid_to_date").alias("campaign_end_date"),
            "budget_amount",
            "budget_currency_code",
            "supplier_funded_percent",
            "promotion_status",
            "redemption_count",
            "redeemed_value",
        )
        .withColumn("funding_source_code", fundingSource)
        .withColumn("tax_treatment_code", taxTreatmentCode(F.col("region_code")))
        .withColumn("discount_basis_code", discountBasisCode(F.col("discount_percent"), F.col("discount_amount")))
        .withColumn("campaign_duration_days", F.datediff(F.col("campaign_end_date"), F.col("campaign_start_date")))
        .withColumn("is_co_funded", F.col("funding_source_code").isin("SUPP", "JOINT"))
        .withColumn("is_campaign_current", F.col("campaign_end_date") >= now)
        .withColumn("has_overlapping_campaign", F.lit(False))
        .withColumn("source_row_hash", rowHash(PROMOTION_SPEC.trackedCols))
    )
    invalidWindow = F.col("campaign_end_date") < F.col("campaign_start_date")
    return derived.where(~invalidWindow), derived.where(invalidWindow)


def flagOverlappingCampaigns(dimDf: DataFrame) -> DataFrame:
    """`UPDATE p SET Has Overlapping Campaign = 1` where another promotion in the region overlaps the window."""
    others = dimDf.where(F.col("promotion_key") >= 0).select(
        F.col("promotion_key").alias("o_key"),
        F.col("region_code").alias("o_region"),
        F.col("campaign_start_date").alias("o_start"),
        F.col("campaign_end_date").alias("o_end"),
    )
    overlap = dimDf.join(
        others,
        (F.col("o_key") != F.col("promotion_key"))
        & (F.col("o_region") == F.col("region_code"))
        & (F.col("o_start") <= F.col("campaign_end_date"))
        & (F.col("o_end") >= F.col("campaign_start_date")),
        "left",
    )
    flagged = overlap.groupBy("promotion_key").agg(F.max(F.col("o_key").isNotNull()).alias("has_overlap"))
    return (
        dimDf.join(flagged, "promotion_key", "left")
        .withColumn("has_overlapping_campaign", F.coalesce(F.col("has_overlap"), F.lit(False)))
        .drop("has_overlap")
    )


def loadPromotionDimension(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Load_Promotion (SCD2 with campaign-window validity)."""
    staged = spark.table(cfg.fqn(STG_PROMOTION))
    fqn = cfg.fqn(DIM_PROMOTION)
    asOf = utcNow()
    candidates, _invalid = buildPromotionCandidates(staged, asOf)
    merged = applyScd2(_existing(spark, fqn), candidates, PROMOTION_SPEC, F.lit(asOf).cast("timestamp"))
    merged = flagOverlappingCampaigns(withReservedMembers(spark, merged, PROMOTION_SPEC, ("promotion", "promotion_code")))
    overwriteTable(withLoadMetadata(merged, cfg), fqn)
    return spark.table(fqn)


def rankCurrent(df: DataFrame, keyCol: str, orderCol: str) -> DataFrame:
    """Helper for tests: latest row per key."""
    w = Window.partitionBy(keyCol).orderBy(F.col(orderCol).desc())
    return df.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")
