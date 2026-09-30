"""Reconciliation evidence for the 22 customer_party packages -> otterorders_migration.evidence.recon_results.

Every package gets a row_count check and an order-independent checksum check
(`sum(xxhash64(business columns))`). None of the legacy targets of this group
are populated by an SSIS run on the baseline host (raw/stg/work/err tables are
empty; `Dimension.Customer` / `Dimension.Employee` hold the vendor sample data,
not the output of these packages), so every comparison is against an expected
result re-derived from the source data with the package's own transformation
(`"baseline": "source_derived"`) and the verdict is capped at PARTIAL.
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_party import dim_customer, dim_supporting, extract, staging
from customer_party.config import (
    ACTOR,
    BRANCH_TAG,
    HARNESS_VERSION,
    LEGACY_DW,
    LEGACY_STAGING,
    LOW_DATE,
    PipelineConfig,
    utcNow,
)
from customer_party.dedup import (
    WORK_CUSTOMER_ADDRESS_STANDARDIZED,
    WORK_CUSTOMER_DEDUP,
    deduplicateCustomers,
)
from customer_party.quality import DQ_RULE_RESULT, evaluateRules
from customer_party.rekey import REKEY_RESULT, WORK_LATE_ARRIVING_QUEUE
from customer_party.tables import tableExists
from customer_party.watermark import WATERMARK_TABLE, getExtractWindow

RECON_RESULTS = "recon_results"

RECON_SCHEMA = (
    "run_id string, run_at timestamp, unit string, unit_type string, verdict string, branch string, "
    "source_object string, target_object string, checks string, summary string, git_sha string, "
    "actor string, harness_version string"
)

ExpectedBuilder = Callable[[SparkSession, PipelineConfig], DataFrame]


@dataclass(frozen=True)
class PackageRecon:
    package: str
    legacyTarget: str
    targetTable: str
    businessCols: tuple[str, ...]
    expected: ExpectedBuilder
    targetFilter: Callable[[DataFrame], DataFrame] | None = None
    note: str = ""
    matchKeyCol: str | None = None


def checksumOf(df: DataFrame, cols: tuple[str, ...]) -> tuple[int, str]:
    present = [c for c in cols if c in df.columns]
    hashed = df.select(F.xxhash64(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in present])).alias("h"))
    row = hashed.agg(F.count("*").alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s")).collect()[0]
    return int(row["n"]), str(row["s"] if row["s"] is not None else 0)


def legacyRowCount(spark: SparkSession, legacyTarget: str) -> int | None:
    catalog, schema, table = legacyTarget.split(".", 2)
    fqn = f"{catalog}.{schema}.`{table}`"
    if not spark.catalog.tableExists(fqn):
        return None
    return spark.table(fqn).count()


def _bronzeBatch(table: str) -> ExpectedBuilder:
    return lambda spark, cfg: spark.table(cfg.fqn(table)).where(F.col("batch_id") == cfg.batchId)


def _watermarkTo(spark: SparkSession, cfg: PipelineConfig, objectName: str) -> datetime:
    wm = (
        spark.table(cfg.fqn(WATERMARK_TABLE))
        .where(F.col("object_name") == objectName)
        .agg(F.max("watermark_value"))
        .collect()[0][0]
    )
    return wm if wm is not None else utcNow()


def _extractWindow(spark: SparkSession, cfg: PipelineConfig, objectName: str) -> tuple[datetime, datetime]:
    """The window this batch's extract logged; falls back to the full history up to the watermark."""
    logged = getExtractWindow(spark, cfg, extract.ORACLE_SOURCE_SYSTEM, objectName)
    return logged if logged is not None else (LOW_DATE, _watermarkTo(spark, cfg, objectName))


def _expectedCustomerMaster(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Re-derive the bronze batch from Oracle over the window the extract actually covered."""
    windowFrom, windowTo = _extractWindow(spark, cfg, "WWI_MDM.CUST_MASTER")
    rows = extract.transformCustomerMaster(
        spark.table("wwi_legacy_oracle.wwi_mdm.cust_master"),
        spark.table("wwi_legacy_oracle.wwi_mdm.cust_classification"),
        spark.table("wwi_legacy_oracle.wwi_mdm.cust_credit_profile"),
        windowFrom,
        windowTo,
        utcNow(),
    )
    deletes = extract.detectDeletes(spark.table("wwi_legacy_oracle.wwi_audit.change_log"), "CUST_MASTER", windowFrom, windowTo)
    return rows.unionByName(
        deletes.select(F.col("pk_value").cast("bigint").alias("cust_id"), F.lit("Y").alias("delete_flag")), allowMissingColumns=True
    )


def _expectedCustomerAddress(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    windowFrom, windowTo = _extractWindow(spark, cfg, "WWI_MDM.CUST_ADDRESS")
    return extract.transformCustomerAddress(spark.table("wwi_legacy_oracle.wwi_mdm.cust_address"), windowFrom, windowTo)


def _expectedSegments(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    rows = extract.transformCustomerSegments(
        spark.table("wwi_legacy_oltp.Sales.CustomerSegmentAssignments"), spark.table("wwi_legacy_oltp.Sales.CustomerSegments"), utcNow()
    )
    return extract.toRawSqlOrderRows(rows, extract.RECORD_KIND_SEGMENT, "CustomerSegmentAssignmentID")


def _expectedPeople(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    rows = extract.transformPeople(spark.table("wwi_legacy_oltp.Application.People"))
    return extract.toRawSqlOrderRows(rows, extract.RECORD_KIND_PERSON, "PersonID")


def _expectedTerritories(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    rows = extract.transformSalesTerritories(
        spark.table("wwi_legacy_oltp.Sales.SalesTerritories"), spark.table("wwi_legacy_oltp.Sales.SalesQuotas"),
        spark.table("wwi_legacy_oltp.Sales.CommissionPlans"), utcNow(),
    )
    return extract.toRawSqlOrderRows(rows, extract.RECORD_KIND_TERRITORY, "SalesTerritoryID")


def _expectedPromotions(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    rows = extract.transformPromotions(
        spark.table("wwi_legacy_oltp.Sales.Promotions"), spark.table("wwi_legacy_oltp.Sales.PromotionLines"),
        spark.table("wwi_legacy_oltp.Sales.PromotionRedemptions"),
    )
    return extract.toRawSqlOrderRows(rows, extract.RECORD_KIND_PROMOTION, "PromotionID")


def _expectedStgCustomer(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    valid, _ = staging.splitValidCustomers(staging.transformStagedCustomer(_bronzeBatch(extract.BRONZE_CUSTOMER_MASTER)(spark, cfg)))
    return valid


def _expectedStgAddress(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    valid, _ = staging.splitValidAddresses(staging.transformStagedAddress(_bronzeBatch(extract.BRONZE_CUSTOMER_ADDRESS)(spark, cfg)))
    return valid


def _expectedStgEmployee(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return staging.transformStagedEmployee(extract.readRawRecordKind(spark, cfg, extract.RECORD_KIND_PERSON), utcNow())


def _expectedStgPromotion(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return staging.transformStagedPromotion(extract.readRawRecordKind(spark, cfg, extract.RECORD_KIND_PROMOTION)).where(F.col("discount_is_valid"))


def _expectedDedup(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    dedup, _ = deduplicateCustomers(spark.table(cfg.fqn(staging.STG_CUSTOMER)), spark.table(cfg.fqn(staging.STG_CUSTOMER_ADDRESS)))
    return dedup


def _expectedDq(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return evaluateRules(spark.table(cfg.fqn(staging.STG_CUSTOMER)).drop("dq_rule_codes"))


def _expectedDimCustomer(region: str) -> ExpectedBuilder:
    def build(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
        candidates, _ = dim_customer.buildRegionalCandidates(
            spark.table(cfg.fqn(staging.STG_CUSTOMER)), spark.table(cfg.fqn(WORK_CUSTOMER_ADDRESS_STANDARDIZED)), region, utcNow()
        )
        return candidates

    return build


def _expectedCategory(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return dim_supporting.buildCategoryCandidates(
        dim_supporting.stageCustomerCategories(spark.table("wwi_legacy_oltp.Sales.CustomerCategories"), spark.table("wwi_legacy_oltp.Sales.Customers"))
    )


def _expectedSegmentDim(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return dim_supporting.buildSegmentCandidates(
        dim_supporting.stageCustomerSegments(spark.table("wwi_legacy_oltp.Sales.CustomerSegments"), extract.readRawRecordKind(spark, cfg, extract.RECORD_KIND_SEGMENT))
    )


def _expectedEmployeeDim(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return dim_supporting.buildEmployeeCandidates(spark.table(cfg.fqn(staging.STG_EMPLOYEE)), utcNow())


def _expectedSalespersonDim(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return dim_supporting.buildSalespersonCandidates(spark.table(cfg.fqn(staging.STG_SALESPERSON)), spark.table(cfg.fqn(dim_supporting.DIM_SALES_TERRITORY)))


def _expectedTerritoryDim(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    return dim_supporting.buildTerritoryCandidates(spark.table(cfg.fqn(staging.STG_SALES_TERRITORY)))


def _expectedPromotionDim(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    valid, _ = dim_supporting.buildPromotionCandidates(spark.table(cfg.fqn(staging.STG_PROMOTION)), utcNow())
    return valid


def _expectedRekey(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Expected released rows = queue rows whose business key now exists in a current member."""
    queue = spark.table(cfg.fqn(WORK_LATE_ARRIVING_QUEUE))
    return queue.where(F.col("status") == "RELEASED").select("dimension_name", "business_key")


def _currentRegion(region: str) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.where(F.col("is_current_row") & (F.col("region") == region) & (F.col("customer_key") >= 0))


def _currentMembers(keyCol: str) -> Callable[[DataFrame], DataFrame]:
    return lambda df: df.where(F.col("is_current_row") & (F.col(keyCol) >= 0))


def _releasedRekeys(df: DataFrame) -> DataFrame:
    return df.where(F.col("status") == "RELEASED").select("dimension_name", "business_key")


def buildPackageRecons(cfg: PipelineConfig) -> tuple[PackageRecon, ...]:
    stg = f"{LEGACY_STAGING}"
    dw = f"{LEGACY_DW}"
    return (
        PackageRecon("EXT_ORA_CustomerMaster", f"{stg}.raw.OracleCustomerMaster", extract.BRONZE_CUSTOMER_MASTER,
                     ("cust_id", "cust_nbr", "cust_name", "region_cd", "country_cd", "cust_status_cd", "credit_limit_amt", "tax_registration_nbr", "delete_flag"),
                     _expectedCustomerMaster, lambda df: df.where(F.col("batch_id") == cfg.batchId)),
        PackageRecon("EXT_ORA_CustomerAddress", f"{stg}.raw.OracleCustomerAddress", extract.BRONZE_CUSTOMER_ADDRESS,
                     ("cust_addr_id", "cust_id", "addr_type_cd", "addr_line_1_std", "city_std", "postal_cd_std", "country_cd", "region_cd", "primary_flg"),
                     _expectedCustomerAddress, lambda df: df.where((F.col("batch_id") == cfg.batchId) & (F.col("delete_flag") == "N"))),
        PackageRecon("EXT_SQL_CustomerSegments", f"{stg}.raw.SqlOrder", extract.BRONZE_SQL_ORDER, ("record_kind", "source_key", "payload"),
                     _expectedSegments, lambda df: df.where(F.col("record_kind") == extract.RECORD_KIND_SEGMENT), "record_kind = SEGMENT"),
        PackageRecon("EXT_SQL_People", f"{stg}.raw.SqlOrder", extract.BRONZE_SQL_ORDER, ("record_kind", "source_key", "payload"),
                     _expectedPeople, lambda df: df.where(F.col("record_kind") == extract.RECORD_KIND_PERSON), "record_kind = PERSON"),
        PackageRecon("EXT_SQL_SalesTerritories", f"{stg}.raw.SqlOrder", extract.BRONZE_SQL_ORDER, ("record_kind", "source_key", "payload"),
                     _expectedTerritories, lambda df: df.where(F.col("record_kind") == extract.RECORD_KIND_TERRITORY), "record_kind = TERRITORY"),
        PackageRecon("EXT_SQL_Promotions", f"{stg}.raw.SqlOrder", extract.BRONZE_SQL_ORDER, ("record_kind", "source_key", "payload"),
                     _expectedPromotions, lambda df: df.where(F.col("record_kind") == extract.RECORD_KIND_PROMOTION), "record_kind = PROMOTION"),
        PackageRecon("STG_Load_Customer", f"{stg}.stg.Customer", staging.STG_CUSTOMER,
                     ("customer_business_key", "customer_code", "customer_name", "customer_class_code", "credit_status_code", "country_code", "region_code", "tax_registration_number", "credit_limit_amount", "change_hash"),
                     _expectedStgCustomer),
        PackageRecon("STG_Load_CustomerAddress", f"{stg}.stg.CustomerAddress", staging.STG_CUSTOMER_ADDRESS,
                     ("source_address_id", "customer_business_key", "address_type_code", "address_line_1", "city_name", "state_province_code", "postal_code", "postal_standard", "country_code", "region_code"),
                     _expectedStgAddress),
        PackageRecon("STG_Load_Employee", f"{stg}.stg.Employee", staging.STG_EMPLOYEE,
                     ("employee_id", "full_name", "given_name", "family_name", "phone_number_last4", "role_code", "is_current_flag"),
                     _expectedStgEmployee, None, "stg.Salesperson is loaded by the same task (see stg_salesperson)"),
        PackageRecon("STG_Load_PromotionAndTerritory", f"{stg}.stg.Promotion", staging.STG_PROMOTION,
                     ("promotion_id", "promotion_code", "promotion_name", "discount_basis_code", "region_code", "discount_percent", "discount_amount", "valid_from_date", "valid_to_date"),
                     _expectedStgPromotion, None, "stg.SalesTerritory is loaded by the same task (see stg_sales_territory)"),
        PackageRecon("STG_Work_CustomerDedup", f"{stg}.work.CustomerDedup", WORK_CUSTOMER_DEDUP,
                     ("customer_business_key", "match_rule_code", "duplicate_group_id", "survivorship_score", "is_survivor_row", "survivor_business_key"),
                     _expectedDedup),
        PackageRecon("DQ_Customer_Screen", f"{stg}.err.RejectedCustomer", DQ_RULE_RESULT,
                     ("customer_business_key", "rule_code", "severity"), _expectedDq),
        PackageRecon("DIM_NA_Load_Customer", f"{dw}.Dimension.Customer", dim_customer.DIM_CUSTOMER, dim_customer.CUSTOMER_TRACKED_COLS + ("customer_business_key",),
                     _expectedDimCustomer("NA"), _currentRegion("NA"), "region = NA slice of dim_customer", "customer_business_key"),
        PackageRecon("DIM_EU_Load_Customer", f"{dw}.Dimension.Customer", dim_customer.DIM_CUSTOMER, dim_customer.CUSTOMER_TRACKED_COLS + ("customer_business_key",),
                     _expectedDimCustomer("EU"), _currentRegion("EU"), "region = EU slice of dim_customer", "customer_business_key"),
        PackageRecon("DIM_APAC_Load_Customer", f"{dw}.Dimension.Customer", dim_customer.DIM_CUSTOMER, dim_customer.CUSTOMER_TRACKED_COLS + ("customer_business_key",),
                     _expectedDimCustomer("APAC"), _currentRegion("APAC"), "region = APAC slice of dim_customer", "customer_business_key"),
        PackageRecon("DIM_Load_CustomerCategory", f"{dw}.Dimension.Customer Category", dim_supporting.DIM_CUSTOMER_CATEGORY,
                     ("wwi_customer_category_id",) + dim_supporting.CATEGORY_SPEC.trackedCols, _expectedCategory, _currentMembers("customer_category_key"), "", "wwi_customer_category_id"),
        PackageRecon("DIM_Load_CustomerSegment", f"{dw}.Dimension.Customer Segment", dim_supporting.DIM_CUSTOMER_SEGMENT,
                     ("wwi_segment_id",) + dim_supporting.SEGMENT_SPEC.trackedCols, _expectedSegmentDim, _currentMembers("customer_segment_key"), "", "wwi_segment_id"),
        PackageRecon("DIM_Load_Employee", f"{dw}.Dimension.Employee", dim_supporting.DIM_EMPLOYEE,
                     ("wwi_employee_id",) + dim_supporting.EMPLOYEE_SPEC.trackedCols, _expectedEmployeeDim, _currentMembers("employee_key"), "", "wwi_employee_id"),
        PackageRecon("DIM_Load_Salesperson", f"{dw}.Dimension.Salesperson", dim_supporting.DIM_SALESPERSON,
                     ("wwi_salesperson_id",) + dim_supporting.SALESPERSON_SPEC.trackedCols, _expectedSalespersonDim, _currentMembers("salesperson_key"), "", "wwi_salesperson_id"),
        PackageRecon("DIM_Load_SalesTerritory", f"{dw}.Dimension.Sales Territory", dim_supporting.DIM_SALES_TERRITORY,
                     ("wwi_territory_id",) + dim_supporting.TERRITORY_SPEC.trackedCols, _expectedTerritoryDim, _currentMembers("sales_territory_key"), "", "wwi_territory_id"),
        PackageRecon("DIM_Load_Promotion", f"{dw}.Dimension.Promotion", dim_supporting.DIM_PROMOTION,
                     ("wwi_promotion_id",) + dim_supporting.PROMOTION_SPEC.trackedCols, _expectedPromotionDim, _currentMembers("promotion_key"), "", "wwi_promotion_id"),
        PackageRecon("DIM_Rekey_LateArriving", f"{stg}.work.LateArrivingDimensionQueue", WORK_LATE_ARRIVING_QUEUE,
                     ("dimension_name", "business_key"), _expectedRekey, _releasedRekeys,
                     f"released queue rows; per-dimension outcome in {REKEY_RESULT}"),
    )


def reconcilePackage(spark: SparkSession, cfg: PipelineConfig, recon: PackageRecon) -> tuple[str, list[dict], str]:
    """Return (verdict, checks, summary) for one package."""
    targetFqn = cfg.fqn(recon.targetTable)
    legacyCount = legacyRowCount(spark, recon.legacyTarget)
    if not tableExists(spark, targetFqn):
        checks = [{"check": "row_count", "source": None, "target": None, "pass": False, "baseline": "source_derived", "legacy_row_count": legacyCount}]
        return "FAIL", checks, f"target table {targetFqn} does not exist"
    try:
        target = spark.table(targetFqn)
        if recon.targetFilter is not None:
            target = recon.targetFilter(target)
        expected = recon.expected(spark, cfg)
        extraChecks: list[dict] = []
        if recon.matchKeyCol is not None:
            extraChecks.append({"check": "dim_current_rows", "target": target.count(), "baseline": "source_derived"})
            target = target.join(expected.select(recon.matchKeyCol).distinct(), recon.matchKeyCol, "semi")
        expectedCount, expectedSum = checksumOf(expected, recon.businessCols)
        targetCount, targetSum = checksumOf(target, recon.businessCols)
    except Exception as exc:  # noqa: BLE001 - the failure itself is the evidence
        checks = [{"check": "row_count", "source": None, "target": None, "pass": False, "baseline": "source_derived", "error": str(exc)[:500]}]
        return "FAIL", checks, f"reconciliation could not be computed: {str(exc)[:200]}"

    countPass = expectedCount == targetCount
    sumPass = expectedSum == targetSum
    checks = [
        {"check": "row_count", "source": expectedCount, "target": targetCount, "pass": countPass, "baseline": "source_derived", "legacy_row_count": legacyCount},
        {"check": "checksum", "method": f"sum(xxhash64({','.join(recon.businessCols)}))", "source": expectedSum, "target": targetSum, "pass": sumPass, "baseline": "source_derived"},
        *extraChecks,
    ]
    if countPass and sumPass:
        verdict = "PARTIAL"
        summary = (
            f"Row count ({targetCount}) and business-column checksum match the expected result re-derived from the "
            f"source data with the package's own logic. Legacy target {recon.legacyTarget} is not populated by an SSIS run "
            f"on the baseline host (legacy_row_count={legacyCount}), so PASS cannot be claimed."
        )
        if targetCount == 0:
            summary += " This batch carried no rows (incremental window with no source changes; run with reload_full_history=true for a full comparison)."
        if recon.matchKeyCol is not None:
            summary += " Dimension rows compared are the current versions of the business keys staged in this batch."
    else:
        verdict = "FAIL"
        summary = f"Mismatch against source-derived expectation: row_count {targetCount} vs {expectedCount}, checksum match={sumPass}."
    if recon.note:
        summary += f" Note: {recon.note}."
    return verdict, checks, summary


def writeEvidence(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Append one recon_results row per package for a fresh run_id and return them."""
    runId = str(uuid.uuid4())
    runAt = utcNow()
    rows = []
    for recon in buildPackageRecons(cfg):
        verdict, checks, summary = reconcilePackage(spark, cfg, recon)
        rows.append(
            (runId, runAt, recon.package, "ssis_package", verdict, BRANCH_TAG, recon.legacyTarget, cfg.fqn(recon.targetTable),
             json.dumps(checks, default=str), summary, cfg.gitSha, ACTOR, HARNESS_VERSION)
        )
    evidence = spark.createDataFrame(rows, RECON_SCHEMA)
    evidence.write.format("delta").mode("append").saveAsTable(cfg.evidenceFqn(RECON_RESULTS))
    return spark.table(cfg.evidenceFqn(RECON_RESULTS)).where(F.col("run_id") == runId)
