"""DIM_NA_Load_Customer / DIM_EU_Load_Customer / DIM_APAC_Load_Customer.

The three SSIS packages write the same `Dimension.Customer` table, one region at
a time, each with its own derived columns and rejection rules. They are
reproduced as three regional candidate builders feeding ONE SCD2 Delta table
(`dim_customer`) with a `region` column; region-specific attributes are NULL
for the other regions.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_party.config import PipelineConfig, utcNow
from customer_party.dedup import WORK_CUSTOMER_ADDRESS_STANDARDIZED
from customer_party.scd import ScdSpec, applyScd2, rowHash, withReservedMembers
from customer_party.staging import ERR_REJECTED_CUSTOMER, STG_CUSTOMER
from customer_party.tables import (
    appendTable,
    overwriteTable,
    tableExists,
    withLoadMetadata,
)

DIM_CUSTOMER = "dim_customer"
REGIONS: tuple[str, ...] = ("NA", "EU", "APAC")

CUSTOMER_TRACKED_COLS: tuple[str, ...] = (
    "customer",
    "customer_code",
    "customer_class_code",
    "credit_status_code",
    "customer_status_code",
    "country_code",
    "tax_registration_number",
    "credit_limit",
    "credit_currency_code",
    "is_on_credit_hold",
    "marketing_consent_flag",
    "consent_status_code",
    "billing_postal_code",
    "billing_city",
    "billing_state_province",
    "tax_nexus_code",
    "postal_code_plus_four",
    "credit_limit_usd",
    "vat_number_is_well_formed",
    "is_pseudonymised",
    "postal_code_normalised",
    "credit_limit_eur",
    "gst_registration_clean",
    "gst_registration_is_valid",
    "customer_name_roman",
    "consent_regime_code",
    "distributor_tier_code",
    "risk_band_code",
)
CUSTOMER_SPEC = ScdSpec(keyCol="customer_key", businessKeyCol="customer_business_key", trackedCols=CUSTOMER_TRACKED_COLS)

DEFAULT_TAX_NEXUS = "US-XX"
DEFAULT_RETENTION_MONTHS = 84
FISCAL_YEAR_START_MONTH = 4


def consentStatusCode(regionCol: Column, flagCol: Column, capturedCol: Column) -> Column:
    return (
        F.when(regionCol == "EU", F.when(flagCol == "Y", F.lit("GDPR-OPTIN")).when(capturedCol.isNotNull(), F.lit("GDPR-WITHDRAWN")).otherwise(F.lit("GDPR-NONE")))
        .when(regionCol == "NA", F.when(flagCol == "N", F.lit("CCPA-OUT")).otherwise(F.lit("OPTIN")))
        .otherwise(F.when(flagCol == "N", F.lit("OPTOUT")).otherwise(F.lit("OPTIN")))
    )


def distributorTierCode(creditLimitCol: Column) -> Column:
    return (
        F.when(creditLimitCol >= 1_000_000, F.lit("TIER1"))
        .when(creditLimitCol >= 250_000, F.lit("TIER2"))
        .when(creditLimitCol > 0, F.lit("TIER3"))
        .otherwise(F.lit("SMB"))
    )


def riskBandCode(taxExemptCol: Column, creditStatusCol: Column) -> Column:
    return F.when(F.coalesce(taxExemptCol, F.lit("N")) == "Y", F.lit("EXEM")).otherwise(F.coalesce(creditStatusCol, F.lit("NEW")))


def fiscalYearLabel(asOf: datetime, fiscalYearStartMonth: int = FISCAL_YEAR_START_MONTH) -> str:
    year = asOf.year + 1 if asOf.month >= fiscalYearStartMonth else asOf.year
    return f"FY{year}"


def baseCandidates(stagedDf: DataFrame, billingDf: DataFrame) -> DataFrame:
    """Survivors that passed the quality screen, joined to their standardised billing address."""
    billing = billingDf.select(
        F.col("customer_business_key").alias("b_bk"),
        F.col("postal_code_std").alias("billing_postal_code"),
        F.col("city_name_std").alias("billing_city"),
        F.col("state_province_code").alias("billing_state_province"),
        F.col("country_code").alias("billing_country_code"),
    )
    eligible = stagedDf.where(F.col("is_survivor_row") & (F.col("dq_status_code") != "FAIL"))
    return eligible.join(billing, F.col("customer_business_key") == F.col("b_bk"), "left").drop("b_bk")


def buildNaCandidates(baseDf: DataFrame) -> tuple[DataFrame, DataFrame]:
    """NA: tax nexus from billing state, ZIP+4, CCPA opt-out, zero credit while on hold."""
    rows = baseDf.where(F.col("region_code") == "NA")
    state = F.trim(F.coalesce(F.col("billing_state_province"), F.lit("")))
    postal = F.coalesce(F.col("billing_postal_code"), F.lit(""))
    postalDigits = F.regexp_replace(postal, "-", "")
    consent = consentStatusCode(F.lit("NA"), F.col("marketing_consent_flag"), F.col("consent_captured_date"))
    derived = (
        rows.withColumn("consent_status_code", consent)
        .withColumn("tax_nexus_code", F.when(F.length(state) == 0, F.lit(DEFAULT_TAX_NEXUS)).otherwise(F.concat(F.lit("US-"), F.upper(F.substring(state, 1, 2)))))
        .withColumn(
            "postal_code_plus_four",
            F.when(postal.contains("-"), postal)
            .when(F.length(postalDigits) == 9, F.concat(F.substring(postalDigits, 1, 5), F.lit("-"), F.substring(postalDigits, 6, 4)))
            .otherwise(postal),
        )
        .withColumn("postal_code_is_valid", F.length(postalDigits).isin(5, 9))
        .withColumn("consent_opt_out", F.col("consent_status_code").isin("CCPA-OUT", "DNS"))
        .withColumn("credit_limit_usd", F.when(F.col("is_on_credit_hold"), F.lit(0)).otherwise(F.col("credit_limit_amount")).cast("decimal(18,2)"))
        .withColumn("customer", F.col("customer_name"))
    )
    rejected = derived.where(~F.col("postal_code_is_valid"))
    return derived.where(F.col("postal_code_is_valid")), rejected.withColumn("reject_reason_code", F.lit("REJECTED_ADDRESS"))


def buildEuCandidates(baseDf: DataFrame, asOf: datetime, retentionMonths: int = DEFAULT_RETENTION_MONTHS) -> tuple[DataFrame, DataFrame]:
    """EU: VAT well-formedness, GDPR erasure / retention pseudonymisation, EUR-only credit reporting."""
    rows = baseDf.where(F.col("region_code") == "EU")
    vat = F.trim(F.coalesce(F.col("vat_registration_number"), F.col("tax_registration_number"), F.lit("")))
    countryIso = F.substring(F.upper(F.col("country_code")), 1, 2)
    postal = F.trim(F.coalesce(F.col("billing_postal_code"), F.lit("")))
    postalNoHyphen = F.regexp_replace(postal, "-", "")
    now = F.lit(asOf).cast("timestamp")
    consent = consentStatusCode(F.lit("EU"), F.col("marketing_consent_flag"), F.col("consent_captured_date"))
    erasureRequested = F.col("retention_until_date").isNotNull() & (F.col("retention_until_date") <= now)
    retentionExpired = F.months_between(now, F.coalesce(F.col("last_order_date"), F.col("source_created_date"))) > retentionMonths
    derived = (
        rows.withColumn("consent_status_code", consent)
        .withColumn("risk_band_code", riskBandCode(F.col("tax_exempt_flag"), F.col("credit_status_code")))
        .withColumn("vat_number_is_well_formed", (F.length(vat) >= 8) & (F.upper(F.substring(vat, 1, 2)) == countryIso))
        .withColumn("is_erasure_requested", erasureRequested)
        .withColumn("retention_expired", retentionExpired)
        .withColumn("is_pseudonymised", erasureRequested | retentionExpired | (F.col("consent_status_code") == "GDPR-WITHDRAWN"))
        .withColumn(
            "customer",
            F.when(erasureRequested | (F.col("consent_status_code") == "GDPR-WITHDRAWN"),
                   F.concat(F.lit("REDACTED-"), F.substring(F.col("customer_business_key"), -6, 6)))
            .otherwise(F.col("customer_name")),
        )
        .withColumn(
            "postal_code_normalised",
            F.when(countryIso == "NL", F.upper(F.regexp_replace(postal, " ", "")))
            .when(countryIso == "PL", F.concat(F.substring(postalNoHyphen, 1, 2), F.lit("-"), F.substring(postalNoHyphen, 3, 3)))
            .otherwise(F.upper(postal)),
        )
        .withColumn("credit_limit_eur", F.when(F.col("credit_currency_code") == "EUR", F.col("credit_limit_amount")).cast("decimal(18,2)"))
    )
    rejectVat = ~F.col("vat_number_is_well_formed") & (F.col("risk_band_code") != "EXEM")
    return derived.where(~rejectVat), derived.where(rejectVat).withColumn("reject_reason_code", F.lit("REJECTED_VAT_NUMBER"))


def buildApacCandidates(baseDf: DataFrame, asOf: datetime) -> tuple[DataFrame, DataFrame]:
    """APAC: GST/ABN cleanup by country, romanised name, postal requirement, fiscal-year label, consent regime."""
    rows = baseDf.where(~F.col("region_code").isin("NA", "EU"))
    gst = F.trim(F.coalesce(F.col("gst_registration_number"), F.col("tax_registration_number"), F.lit("")))
    gstNoSpace = F.regexp_replace(gst, " ", "")
    countryIso = F.substring(F.upper(F.col("country_code")), 1, 2)
    postal = F.trim(F.coalesce(F.col("billing_postal_code"), F.lit("")))
    consent = consentStatusCode(F.lit("APAC"), F.col("marketing_consent_flag"), F.col("consent_captured_date"))
    derived = (
        rows.withColumn("consent_status_code", consent)
        .withColumn("gst_registration_clean", F.regexp_replace(gstNoSpace, "-", ""))
        .withColumn(
            "gst_registration_is_valid",
            F.when(countryIso == "AU", F.length(gstNoSpace) == 11)
            .when(countryIso == "IN", F.length(gst) == 15)
            .when(countryIso == "SG", F.length(gst) >= 9)
            .otherwise(F.length(gst) > 0),
        )
        .withColumn(
            "customer_name_roman",
            F.when(F.length(F.trim(F.coalesce(F.col("customer_name"), F.lit("")))) == 0,
                   F.concat(F.lit("UNROMANISED-"), F.col("customer_business_key")))
            .otherwise(F.upper(F.trim(F.col("customer_name")))),
        )
        .withColumn("customer", F.col("customer_name"))
        .withColumn("postal_code_required", ~countryIso.isin("HK", "MO"))
        .withColumn("fiscal_year_label", F.lit(fiscalYearLabel(asOf)))
        .withColumn(
            "consent_regime_code",
            F.when(countryIso == "JP", F.lit("APPI")).when(countryIso == "SG", F.lit("PDPA")).when(countryIso == "AU", F.lit("APP")).otherwise(F.lit("LOCAL")),
        )
        .withColumn("distributor_tier_code", distributorTierCode(F.col("credit_limit_amount")))
    )
    rejectGst = ~F.col("gst_registration_is_valid") & (F.col("distributor_tier_code") != "SMB")
    rejectAddress = F.col("postal_code_required") & (F.length(postal) == 0)
    rejected = derived.where(rejectGst | rejectAddress).withColumn(
        "reject_reason_code", F.when(rejectGst, F.lit("REJECTED_REGISTRATION")).otherwise(F.lit("REJECTED_ADDRESS"))
    )
    return derived.where(~rejectGst & ~rejectAddress), rejected


DIM_COLUMNS: tuple[str, ...] = (
    "customer_business_key",
    "source_customer_id",
    "customer_code",
    "customer",
    "customer_name",
    "trading_name",
    "region",
    "country_code",
    "customer_class_code",
    "credit_status_code",
    "customer_status_code",
    "buying_group_code",
    "payment_terms_code",
    "account_manager_code",
    "tax_registration_number",
    "credit_limit",
    "credit_currency_code",
    "is_on_credit_hold",
    "marketing_consent_flag",
    "consent_status_code",
    "billing_postal_code",
    "billing_city",
    "billing_state_province",
    "tax_nexus_code",
    "postal_code_plus_four",
    "consent_opt_out",
    "credit_limit_usd",
    "vat_number_is_well_formed",
    "is_erasure_requested",
    "retention_expired",
    "is_pseudonymised",
    "postal_code_normalised",
    "credit_limit_eur",
    "risk_band_code",
    "gst_registration_clean",
    "gst_registration_is_valid",
    "customer_name_roman",
    "fiscal_year_label",
    "consent_regime_code",
    "distributor_tier_code",
    "source_system_code",
    "source_row_hash",
)


def shapeCandidates(df: DataFrame, region: str) -> DataFrame:
    shaped = df.withColumn("region", F.lit(region)).withColumn("credit_limit", F.col("credit_limit_amount").cast("decimal(18,2)"))
    for c in DIM_COLUMNS:
        if c not in shaped.columns and c != "source_row_hash":
            shaped = shaped.withColumn(c, F.lit(None).cast("string"))
    return shaped.withColumn("source_row_hash", rowHash(CUSTOMER_TRACKED_COLS)).select(*DIM_COLUMNS)


def buildRegionalCandidates(stagedDf: DataFrame, billingDf: DataFrame, region: str, asOf: datetime) -> tuple[DataFrame, DataFrame]:
    base = baseCandidates(stagedDf, billingDf)
    if region == "NA":
        accepted, rejected = buildNaCandidates(base)
    elif region == "EU":
        accepted, rejected = buildEuCandidates(base, asOf)
    else:
        accepted, rejected = buildApacCandidates(base, asOf)
    return shapeCandidates(accepted, region), rejected


def loadRegionalCustomerDimension(spark: SparkSession, cfg: PipelineConfig, region: str) -> DataFrame:
    """DIM_<region>_Load_Customer: SCD2 merge of one region's candidates into `dim_customer`."""
    asOf = utcNow()
    staged = spark.table(cfg.fqn(STG_CUSTOMER))
    billing = spark.table(cfg.fqn(WORK_CUSTOMER_ADDRESS_STANDARDIZED))
    candidates, rejected = buildRegionalCandidates(staged, billing, region, asOf)

    fqn = cfg.fqn(DIM_CUSTOMER)
    existing = spark.table(fqn).drop("batch_id", "load_ts") if tableExists(spark, fqn) else None
    merged = applyScd2(existing, candidates, CUSTOMER_SPEC, F.lit(asOf).cast("timestamp"))
    merged = withReservedMembers(spark, merged, CUSTOMER_SPEC, ("customer", "customer_name"), {"region": region})
    overwriteTable(withLoadMetadata(merged, cfg), fqn)

    rejectRows = rejected.select(
        F.lit(f"DIM_{region}_Load_Customer").alias("package_name"),
        F.lit("stg.Customer").alias("source_table"),
        F.col("customer_business_key").alias("source_key"),
        F.col("reject_reason_code"),
        F.concat_ws(" ", F.col("country_code"), F.col("billing_postal_code")).alias("reject_detail"),
        F.lit(region).alias("region_code"),
        F.current_timestamp().alias("rejected_at"),
    )
    appendTable(withLoadMetadata(rejectRows, cfg), cfg.fqn(ERR_REJECTED_CUSTOMER))
    return spark.table(fqn).where(F.col("region") == region)
