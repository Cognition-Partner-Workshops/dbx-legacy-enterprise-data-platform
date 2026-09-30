"""DQ_Customer_Screen: rule-based quality screen over stg_customer (`etl.usp_EvaluateDataQualityRules`).

Rules with severity FAIL block the row from the dimension loads; WARN rows are
loaded but flagged. When the share of blocking failures exceeds
`blockingFailureThreshold`, the package fails, as the SSIS package did.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_party.config import PipelineConfig
from customer_party.staging import ERR_REJECTED_CUSTOMER, STG_CUSTOMER
from customer_party.tables import appendTable, overwriteTable, withLoadMetadata

DQ_RULE_RESULT = "dq_customer_rule_result"

REGION_COUNTRIES: dict[str, tuple[str, ...]] = {
    "NA": ("US", "USA", "CA", "CAN", "MX", "MEX"),
    "EU": ("GB", "GBR", "UK", "DE", "DEU", "FR", "FRA", "NL", "NLD", "IE", "IRL", "ES", "ESP", "IT", "ITA", "BE", "BEL",
           "PL", "POL", "SE", "SWE", "DK", "DNK", "AT", "AUT", "PT", "PRT", "FI", "FIN", "CZ", "CZE", "LU", "LUX"),
    "APAC": ("AU", "AUS", "NZ", "NZL", "SG", "SGP", "JP", "JPN", "HK", "HKG", "MO", "MAC", "IN", "IND", "MY", "MYS",
             "KR", "KOR", "CN", "CHN", "TW", "TWN", "TH", "THA", "ID", "IDN", "PH", "PHL", "VN", "VNM"),
}


@dataclass(frozen=True)
class DqRule:
    ruleCode: str
    severity: str
    description: str


RULES: tuple[DqRule, ...] = (
    DqRule("CUST_NAME_MISSING", "FAIL", "Customer name is missing"),
    DqRule("TAX_ID_MISSING_OR_SHORT", "WARN", "Tax registration number missing or shorter than 5 characters"),
    DqRule("EU_CONSENT_RETENTION_BREACH", "FAIL", "EU customer still marketable past the consent retention window"),
    DqRule("CREDIT_LIMIT_IMPLAUSIBLE", "WARN", "Credit limit negative or above 10,000,000"),
    DqRule("COUNTRY_REGION_MISMATCH", "FAIL", "Country code does not belong to the customer's region"),
)


def ruleCondition(ruleCode: str, asOfCol: Column) -> Column:
    if ruleCode == "CUST_NAME_MISSING":
        return F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == "")
    if ruleCode == "TAX_ID_MISSING_OR_SHORT":
        return F.length(F.coalesce(F.col("tax_registration_number"), F.lit(""))) < 5
    if ruleCode == "EU_CONSENT_RETENTION_BREACH":
        consentAge = F.months_between(asOfCol, F.col("consent_captured_date"))
        return (
            (F.col("region_code") == "EU")
            & (F.col("marketing_consent_flag") == "Y")
            & (F.col("consent_captured_date").isNull() | (consentAge > F.col("retention_months")))
        )
    if ruleCode == "CREDIT_LIMIT_IMPLAUSIBLE":
        return (F.col("credit_limit_amount") < 0) | (F.col("credit_limit_amount") > 10_000_000)
    if ruleCode == "COUNTRY_REGION_MISMATCH":
        cond = F.lit(False)
        for region, countries in REGION_COUNTRIES.items():
            cond = cond | ((F.col("region_code") == region) & ~F.col("country_code").isin(*countries))
        return cond & F.col("country_code").isNotNull()
    raise ValueError(f"unknown DQ rule {ruleCode}")


def evaluateRules(customerDf: DataFrame, asOfCol: Column | None = None) -> DataFrame:
    """One row per (customer, violated rule)."""
    asOf = asOfCol if asOfCol is not None else F.current_timestamp()
    results = None
    for rule in RULES:
        violated = customerDf.where(ruleCondition(rule.ruleCode, asOf)).select(
            F.col("customer_business_key"),
            F.col("region_code"),
            F.lit(rule.ruleCode).alias("rule_code"),
            F.lit(rule.severity).alias("severity"),
            F.lit(rule.description).alias("rule_description"),
        )
        results = violated if results is None else results.unionByName(violated)
    assert results is not None
    return results


def applyDqOutcome(customerDf: DataFrame, ruleResults: DataFrame) -> DataFrame:
    worst = ruleResults.groupBy(F.col("customer_business_key").alias("r_bk")).agg(
        F.max(F.when(F.col("severity") == "FAIL", 2).otherwise(1)).alias("worst"),
        F.concat_ws(",", F.sort_array(F.collect_set("rule_code"))).alias("dq_rule_codes"),
    )
    return (
        customerDf.join(worst, F.col("customer_business_key") == F.col("r_bk"), "left")
        .withColumn(
            "dq_status_code",
            F.when(F.col("worst") == 2, F.lit("FAIL"))
            .when(F.col("worst") == 1, F.lit("WARN"))
            .otherwise(F.col("dq_status_code")),
        )
        .withColumn("dq_rule_codes", F.col("dq_rule_codes"))
        .drop("r_bk", "worst")
    )


def runCustomerQualityScreen(
    spark: SparkSession, cfg: PipelineConfig, blockingFailureThreshold: float = 0.25
) -> DataFrame:
    """DQ_Customer_Screen."""
    customers = spark.table(cfg.fqn(STG_CUSTOMER)).drop("dq_rule_codes")
    results = evaluateRules(customers)
    overwriteTable(withLoadMetadata(results, cfg), cfg.fqn(DQ_RULE_RESULT))
    persisted = spark.table(cfg.fqn(DQ_RULE_RESULT))
    rejected = persisted.where(F.col("severity") == "FAIL").select(
        F.lit("DQ_Customer_Screen").alias("package_name"),
        F.lit("stg.Customer").alias("source_table"),
        F.col("customer_business_key").alias("source_key"),
        F.col("rule_code").alias("reject_reason_code"),
        F.col("rule_description").alias("reject_detail"),
        F.col("region_code"),
        F.current_timestamp().alias("rejected_at"),
    )
    appendTable(withLoadMetadata(rejected, cfg), cfg.fqn(ERR_REJECTED_CUSTOMER))
    overwriteTable(applyDqOutcome(customers, persisted), cfg.fqn(STG_CUSTOMER))

    total = customers.count()
    failing = spark.table(cfg.fqn(STG_CUSTOMER)).where(F.col("dq_status_code") == "FAIL").count()
    if total > 0 and failing / total > blockingFailureThreshold:
        raise RuntimeError(
            f"DQ_Customer_Screen blocking threshold breached: {failing}/{total} customers failed blocking rules"
        )
    return persisted
