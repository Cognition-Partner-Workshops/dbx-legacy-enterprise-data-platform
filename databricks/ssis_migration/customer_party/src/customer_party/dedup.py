"""STG_Work_CustomerDedup: party resolution (`stg.usp_DeduplicateCustomer`) and address standardisation.

Match rules, in priority order (a candidate is assigned to the first rule that applies):
  1. EXACT_TAXNUM  - identical non-empty tax registration number
  2. NAME_POSTAL   - standardised name + standardised primary postal code
  3. NAME_FUZZY    - first 12 characters of the standardised name + country, only when
                     neither candidate carries a tax number
Survivorship score = source rank + attribute completeness (2 per populated significant
attribute) + 10 for the most recently modified record in the group + 50 for an EU
record with explicit marketing opt-in; ties break on the lowest business key.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from customer_party.config import PipelineConfig
from customer_party.staging import STG_CUSTOMER, STG_CUSTOMER_ADDRESS, emptyToNull
from customer_party.tables import overwriteTable, withLoadMetadata

WORK_CUSTOMER_DEDUP = "work_customer_dedup"
WORK_CUSTOMER_ADDRESS_STANDARDIZED = "work_customer_address_standardized"

SOURCE_RANKS: dict[str, int] = {"ORA_ERP": 30, "WWI_OLTP": 20, "WWI_WEB": 10}
SIGNIFICANT_ATTRIBUTES: tuple[str, ...] = (
    "customer_name",
    "tax_registration_number",
    "country_code",
    "credit_limit_amount",
    "customer_class_code",
    "credit_status_code",
    "primary_postal_code",
)


def sourceRank(sourceCol: Column) -> Column:
    expr = F.lit(5)
    for code, rank in SOURCE_RANKS.items():
        expr = F.when(sourceCol == code, F.lit(rank)).otherwise(expr)
    return expr


def attributeCompleteness(df: DataFrame) -> Column:
    score = F.lit(0)
    for attr in SIGNIFICANT_ATTRIBUTES:
        if attr in df.columns:
            populated = F.col(attr).isNotNull() & (F.trim(F.col(attr).cast("string")) != "")
            score = score + F.when(populated, F.lit(2)).otherwise(F.lit(0))
    return score


def standardizeAddresses(addressDf: DataFrame) -> DataFrame:
    """work.CustomerAddressStandardized: one primary address per customer with a quality code."""
    ranked = addressDf.withColumn(
        "addr_rank",
        F.row_number().over(
            Window.partitionBy("customer_business_key").orderBy(
                F.col("is_primary").desc(),
                F.when(F.col("address_type_code") == "BILL", 0).when(F.col("address_type_code") == "MAIN", 1).otherwise(2),
                F.col("effective_from_date").desc_nulls_last(),
                F.col("source_address_id"),
            )
        ),
    )
    return ranked.where(F.col("addr_rank") == 1).select(
        F.col("customer_business_key"),
        F.col("source_address_id"),
        F.col("address_type_code"),
        F.upper(F.trim(F.col("address_line_1"))).alias("address_line_1_std"),
        F.upper(F.trim(F.col("city_name"))).alias("city_name_std"),
        F.col("state_province_code"),
        F.regexp_replace(F.upper(F.coalesce(F.col("postal_code"), F.lit(""))), r"[^A-Z0-9]", "").alias("postal_code_std"),
        F.col("country_code"),
        F.col("region_code"),
        F.col("postal_standard"),
        F.when(F.col("postal_code").isNull() | (F.trim(F.col("postal_code")) == ""), F.lit("NOPOST"))
        .when(~F.col("postal_is_valid"), F.lit("BADPOST"))
        .otherwise(F.lit("OK"))
        .alias("address_quality_code"),
    )


def buildMatchKeys(customerDf: DataFrame, standardizedAddressDf: DataFrame) -> DataFrame:
    postal = standardizedAddressDf.select(
        F.col("customer_business_key").alias("addr_bk"),
        F.col("postal_code_std").alias("primary_postal_code"),
        F.col("address_quality_code"),
    )
    taxKey = emptyToNull(F.upper(F.regexp_replace(F.coalesce(F.col("tax_registration_number"), F.lit("")), r"[^A-Z0-9]", "")))
    nameKey = emptyToNull(F.col("customer_name_standardized"))
    return (
        customerDf.join(postal, F.col("customer_business_key") == F.col("addr_bk"), "left")
        .drop("addr_bk")
        .withColumn("match_key_taxnum", taxKey)
        .withColumn("match_key_name", nameKey)
        .withColumn("match_key_postal", emptyToNull(F.col("primary_postal_code")))
        .withColumn(
            "match_key_fuzzy",
            F.when(
                taxKey.isNull() & nameKey.isNotNull(),
                F.concat_ws("|", F.substring(nameKey, 1, 12), F.coalesce(F.col("country_code"), F.lit(""))),
            ),
        )
    )


def assignDuplicateGroups(candidates: DataFrame) -> DataFrame:
    """Union-of-rules grouping: each customer joins the first rule group that matches it."""
    taxGroups = (
        candidates.where(F.col("match_key_taxnum").isNotNull())
        .groupBy("match_key_taxnum")
        .agg(F.count("*").alias("n"), F.min("customer_business_key").alias("g_key"))
        .where(F.col("n") > 1)
        .select(F.col("match_key_taxnum").alias("k1"), F.col("g_key").alias("group_key_1"))
    )
    withTax = candidates.join(taxGroups, F.col("match_key_taxnum") == F.col("k1"), "left").drop("k1")
    remaining = withTax.where(F.col("group_key_1").isNull())
    namePostalGroups = (
        remaining.where(F.col("match_key_name").isNotNull() & F.col("match_key_postal").isNotNull())
        .groupBy("match_key_name", "match_key_postal")
        .agg(F.count("*").alias("n"), F.min("customer_business_key").alias("g_key"))
        .where(F.col("n") > 1)
        .select(
            F.col("match_key_name").alias("k2n"), F.col("match_key_postal").alias("k2p"), F.col("g_key").alias("group_key_2")
        )
    )
    withNamePostal = withTax.join(
        namePostalGroups,
        F.col("group_key_1").isNull() & (F.col("match_key_name") == F.col("k2n")) & (F.col("match_key_postal") == F.col("k2p")),
        "left",
    ).drop("k2n", "k2p")
    remaining2 = withNamePostal.where(F.col("group_key_1").isNull() & F.col("group_key_2").isNull())
    fuzzyGroups = (
        remaining2.where(F.col("match_key_fuzzy").isNotNull())
        .groupBy("match_key_fuzzy")
        .agg(F.count("*").alias("n"), F.min("customer_business_key").alias("g_key"))
        .where(F.col("n") > 1)
        .select(F.col("match_key_fuzzy").alias("k3"), F.col("g_key").alias("group_key_3"))
    )
    grouped = withNamePostal.join(
        fuzzyGroups,
        F.col("group_key_1").isNull() & F.col("group_key_2").isNull() & (F.col("match_key_fuzzy") == F.col("k3")),
        "left",
    ).drop("k3")
    return (
        grouped.withColumn(
            "match_rule_code",
            F.when(F.col("group_key_1").isNotNull(), F.lit("EXACT_TAXNUM"))
            .when(F.col("group_key_2").isNotNull(), F.lit("NAME_POSTAL"))
            .when(F.col("group_key_3").isNotNull(), F.lit("NAME_FUZZY"))
            .otherwise(F.lit("SINGLETON")),
        )
        .withColumn(
            "duplicate_group_key",
            F.coalesce(F.col("group_key_1"), F.col("group_key_2"), F.col("group_key_3"), F.col("customer_business_key")),
        )
        .withColumn("duplicate_group_id", F.xxhash64(F.col("duplicate_group_key")))
        .drop("group_key_1", "group_key_2", "group_key_3")
    )


def scoreSurvivorship(grouped: DataFrame) -> DataFrame:
    groupWindow = Window.partitionBy("duplicate_group_id")
    scored = (
        grouped.withColumn("source_rank", sourceRank(F.col("source_system_code")))
        .withColumn("attribute_completeness", attributeCompleteness(grouped))
        .withColumn(
            "recency_bonus",
            F.when(F.col("source_modified_date") == F.max("source_modified_date").over(groupWindow), F.lit(10)).otherwise(0),
        )
        .withColumn(
            "eu_consent_bonus",
            F.when((F.col("region_code") == "EU") & (F.col("marketing_consent_flag") == "Y"), F.lit(50)).otherwise(0),
        )
    )
    scored = scored.withColumn(
        "survivorship_score",
        F.col("source_rank") + F.col("attribute_completeness") + F.col("recency_bonus") + F.col("eu_consent_bonus"),
    )
    rankWindow = groupWindow.orderBy(F.col("survivorship_score").desc(), F.col("customer_business_key"))
    scored = scored.withColumn("survivor_rank", F.row_number().over(rankWindow))
    survivorKey = F.first("customer_business_key").over(
        groupWindow.orderBy(F.col("survivorship_score").desc(), F.col("customer_business_key"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    return (
        scored.withColumn("is_survivor_row", F.col("survivor_rank") == 1)
        .withColumn("survivor_business_key", survivorKey)
        .withColumn("group_member_count", F.count("*").over(groupWindow))
        .withColumn(
            "survivorship_rule_applied",
            F.when(F.col("group_member_count") == 1, F.lit("SINGLETON"))
            .when(F.col("survivor_rank") == 1, F.concat(F.lit("SURVIVOR:"), F.col("match_rule_code")))
            .otherwise(F.concat(F.lit("RETIRED:"), F.col("match_rule_code"))),
        )
    )


def deduplicateCustomers(customerDf: DataFrame, addressDf: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (work_customer_dedup, work_customer_address_standardized)."""
    standardized = standardizeAddresses(addressDf)
    scored = scoreSurvivorship(assignDuplicateGroups(buildMatchKeys(customerDf, standardized)))
    dedup = scored.select(
        "customer_business_key",
        "source_customer_id",
        "customer_code",
        "customer_name",
        "customer_name_standardized",
        "tax_registration_number",
        "country_code",
        "region_code",
        "source_system_code",
        "source_modified_date",
        "primary_postal_code",
        "match_key_taxnum",
        "match_key_name",
        "match_key_postal",
        "match_key_fuzzy",
        "match_rule_code",
        "duplicate_group_id",
        "group_member_count",
        "source_rank",
        "attribute_completeness",
        "recency_bonus",
        "eu_consent_bonus",
        "survivorship_score",
        "survivor_rank",
        "is_survivor_row",
        "survivor_business_key",
        "survivorship_rule_applied",
    )
    return dedup, standardized


def applyDedupToStaging(customerDf: DataFrame, dedupDf: DataFrame) -> DataFrame:
    """Write the survivorship outcome back onto stg_customer (the procedure's UPDATE stg.Customer)."""
    outcome = dedupDf.select(
        F.col("customer_business_key").alias("d_bk"),
        F.col("duplicate_group_id").alias("d_group"),
        F.col("is_survivor_row").alias("d_survivor"),
        F.col("survivorship_rule_applied").alias("d_rule"),
    )
    return (
        customerDf.join(outcome, F.col("customer_business_key") == F.col("d_bk"), "left")
        .withColumn("duplicate_group_id", F.col("d_group"))
        .withColumn("is_survivor_row", F.coalesce(F.col("d_survivor"), F.lit(True)))
        .withColumn("survivorship_rule_applied", F.col("d_rule"))
        .withColumn(
            "dq_status_code",
            F.when(~F.col("is_survivor_row"), F.lit("WARN")).otherwise(F.col("dq_status_code")),
        )
        .drop("d_bk", "d_group", "d_survivor", "d_rule")
    )


def runCustomerDedup(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """STG_Work_CustomerDedup (work tables are rebuilt from scratch on every run)."""
    customers = spark.table(cfg.fqn(STG_CUSTOMER))
    addresses = spark.table(cfg.fqn(STG_CUSTOMER_ADDRESS))
    dedup, standardized = deduplicateCustomers(customers, addresses)
    overwriteTable(withLoadMetadata(dedup, cfg), cfg.fqn(WORK_CUSTOMER_DEDUP))
    overwriteTable(withLoadMetadata(standardized, cfg), cfg.fqn(WORK_CUSTOMER_ADDRESS_STANDARDIZED))
    updated = applyDedupToStaging(customers, spark.table(cfg.fqn(WORK_CUSTOMER_DEDUP)))
    overwriteTable(updated, cfg.fqn(STG_CUSTOMER))
    return spark.table(cfg.fqn(WORK_CUSTOMER_DEDUP))
