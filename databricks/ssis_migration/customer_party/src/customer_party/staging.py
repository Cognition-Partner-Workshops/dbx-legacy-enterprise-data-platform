"""STG_Load_* packages: truncate-and-reload staging tables from the bronze landing tables.

Each function is the data flow of one package expressed as set-based Spark.
Row-level conditional splits become filters; the "rejected" output of each split
is written to the group's error table (`err_rejected_customer`) with the same
reason codes the SSIS `etl.usp_LogRejectedRecord` calls used.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from customer_party.config import LEGACY_STAGING, PipelineConfig, utcNow
from customer_party.extract import (
    BRONZE_CUSTOMER_ADDRESS,
    BRONZE_CUSTOMER_MASTER,
    RECORD_KIND_PERSON,
    RECORD_KIND_PROMOTION,
    RECORD_KIND_TERRITORY,
    readRawRecordKind,
)
from customer_party.tables import appendTable, overwriteTable, withLoadMetadata

STG_CUSTOMER = "stg_customer"
STG_CUSTOMER_ADDRESS = "stg_customer_address"
STG_EMPLOYEE = "stg_employee"
STG_SALESPERSON = "stg_salesperson"
STG_PROMOTION = "stg_promotion"
STG_SALES_TERRITORY = "stg_sales_territory"
ERR_REJECTED_CUSTOMER = "err_rejected_customer"

REJECT_SCHEMA = (
    "package_name string, source_table string, source_key string, reject_reason_code string, "
    "reject_detail string, region_code string, rejected_at timestamp"
)

LEGAL_SUFFIXES: tuple[str, ...] = ("PTY LTD", "PTE LTD", "GMBH", "SARL", "LTD", "LLC", "INC", "BV", "KK")


def emptyToNull(col: Column) -> Column:
    return F.when(col == "", F.lit(None)).otherwise(col)


def upperTrim(col: Column, default: str | None = None) -> Column:
    base = F.upper(F.trim(col))
    if default is None:
        return base
    return F.coalesce(emptyToNull(base), F.lit(default))


def standardizeCustomerName(col: Column) -> Column:
    """`stg.usp_NormalizeCustomer`: casing, punctuation, `&` -> AND, collapsed spaces, legal suffix stripped."""
    upper = F.upper(F.trim(F.coalesce(col, F.lit(""))))
    noApostrophe = F.regexp_replace(upper, r"'", "")
    hyphens = F.regexp_replace(noApostrophe, r"-", " ")
    ampersand = F.regexp_replace(hyphens, r"&", " AND ")
    punctuation = F.regexp_replace(ampersand, r"[.,/()]", " ")
    collapsed = F.trim(F.regexp_replace(punctuation, r"\s+", " "))
    result = collapsed
    for suffix in LEGAL_SUFFIXES:
        result = F.regexp_replace(result, rf"^(.+?)\s+{suffix}$", "$1")
    return result


def buildChangeHash(*cols: Column) -> Column:
    """The SSIS ChangeHash expression: upper-trimmed business attributes joined by `|`, hashed."""
    parts = [F.upper(F.trim(F.coalesce(c.cast("string"), F.lit("")))) for c in cols]
    return F.sha2(F.concat_ws("|", *parts), 256)


def retentionMonthsFor(regionCol: Column) -> Column:
    return F.when(regionCol == "EU", F.lit(24)).when(regionCol == "APAC", F.lit(60)).otherwise(F.lit(84))


def deriveMarketingConsent(regionCol: Column, consentCol: Column) -> Column:
    """EU defaults to N (opt-in regime); other regions default to Y (opt-out regime)."""
    upper = F.upper(F.trim(consentCol))
    return F.when(regionCol == "EU", F.when(upper == "Y", F.lit("Y")).otherwise(F.lit("N"))).otherwise(
        F.when(upper == "N", F.lit("N")).otherwise(F.lit("Y"))
    )


def transformStagedCustomer(rawDf: DataFrame, defaultCurrencyCode: str = "USD") -> DataFrame:
    """STG_Load_Customer data flow (`Conform Customer` derived column + hash)."""
    region = upperTrim(F.col("region_cd"), "NA")
    customerName = F.trim(F.regexp_replace(F.col("cust_name"), "  ", " "))
    customerClass = upperTrim(F.col("classification_cd"), "UNCL")
    creditStatus = upperTrim(F.col("credit_rating_cd"), "NEW")
    country = upperTrim(F.col("country_cd"))
    taxReg = F.upper(F.regexp_replace(F.trim(F.col("tax_registration_nbr")), r"[ \-]", ""))
    creditLimit = F.coalesce(F.col("credit_limit_amt"), F.lit(0)).cast("decimal(18,2)")
    return rawDf.where(F.col("delete_flag") == "N").select(
        F.col("cust_id").alias("source_customer_id"),
        upperTrim(F.col("cust_nbr")).alias("customer_code"),
        F.concat(F.lit("ORA:"), F.col("cust_id").cast("string")).alias("customer_business_key"),
        customerName.alias("customer_name"),
        F.when(F.trim(F.col("cust_name")) == F.trim(F.coalesce(F.col("trading_name"), F.lit(""))), F.lit(None))
        .otherwise(F.trim(F.col("trading_name")))
        .alias("trading_name"),
        standardizeCustomerName(customerName).alias("customer_name_standardized"),
        customerClass.alias("customer_class_code"),
        creditStatus.alias("credit_status_code"),
        upperTrim(F.col("cust_status_cd"), "AC").alias("customer_status_code"),
        country.alias("country_code"),
        region.alias("region_code"),
        taxReg.alias("tax_registration_number"),
        F.col("vat_registration_nbr").alias("vat_registration_number"),
        F.col("gst_registration_nbr").alias("gst_registration_number"),
        F.col("tax_exempt_flg").alias("tax_exempt_flag"),
        deriveMarketingConsent(region, F.col("marketing_consent_flg")).alias("marketing_consent_flag"),
        F.col("consent_captured_dt").cast("timestamp").alias("consent_captured_date"),
        F.col("consent_source_cd").alias("consent_source_code"),
        F.col("retention_until_dt").cast("timestamp").alias("retention_until_date"),
        retentionMonthsFor(region).alias("retention_months"),
        creditLimit.alias("credit_limit_amount"),
        F.coalesce(upperTrim(F.col("credit_ccy")), F.lit(defaultCurrencyCode)).alias("credit_currency_code"),
        (F.coalesce(F.col("credit_hold_flg"), F.lit("N")) == "Y").alias("is_on_credit_hold"),
        F.col("buying_group_cd").alias("buying_group_code"),
        F.col("payment_terms_cd").alias("payment_terms_code"),
        F.col("acct_manager_cd").alias("account_manager_code"),
        F.col("first_order_dt").cast("timestamp").alias("first_order_date"),
        F.col("last_order_dt").cast("timestamp").alias("last_order_date"),
        F.coalesce(F.col("created_dt"), F.lit("1900-01-01")).cast("timestamp").alias("source_created_date"),
        F.coalesce(F.col("last_update_dt"), F.col("created_dt")).cast("timestamp").alias("source_modified_date"),
        F.col("source_system_code"),
        buildChangeHash(customerName, customerClass, creditStatus, country, taxReg, creditLimit).alias("change_hash"),
        F.lit(None).cast("bigint").alias("duplicate_group_id"),
        F.lit(True).alias("is_survivor_row"),
        F.lit(None).cast("string").alias("survivorship_rule_applied"),
        F.lit("PASS").alias("dq_status_code"),
    )


def splitValidCustomers(stagedDf: DataFrame) -> tuple[DataFrame, DataFrame]:
    """`Screen Customer` conditional split: valid rows vs. missing-name / short-code rejects."""
    missingName = F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == "")
    shortCode = F.length(F.coalesce(F.col("customer_code"), F.lit(""))) < 4
    valid = stagedDf.where(~missingName & ~shortCode)
    rejected = stagedDf.where(missingName | shortCode).select(
        F.lit("STG_Load_Customer").alias("package_name"),
        F.lit("raw.OracleCustomerMaster").alias("source_table"),
        F.col("customer_business_key").alias("source_key"),
        F.when(missingName, F.lit("MISSING_NAME")).otherwise(F.lit("SHORT_CODE")).alias("reject_reason_code"),
        F.concat_ws(" ", F.lit("customer_code="), F.col("customer_code")).alias("reject_detail"),
        F.col("region_code"),
        F.current_timestamp().alias("rejected_at"),
    )
    return valid, rejected


def loadStagedCustomer(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """STG_Load_Customer."""
    raw = spark.table(cfg.fqn(BRONZE_CUSTOMER_MASTER)).where(F.col("batch_id") == cfg.batchId)
    valid, rejected = splitValidCustomers(transformStagedCustomer(raw))
    overwriteTable(withLoadMetadata(valid, cfg), cfg.fqn(STG_CUSTOMER))
    appendTable(withLoadMetadata(rejected, cfg), cfg.fqn(ERR_REJECTED_CUSTOMER))
    return spark.table(cfg.fqn(STG_CUSTOMER))


def abbreviateStreet(col: Column) -> Column:
    replaced = F.regexp_replace(F.trim(col), r"(?i)\bStreet\b", "ST")
    replaced = F.regexp_replace(replaced, r"(?i)\bAvenue\b", "AVE")
    replaced = F.regexp_replace(replaced, r"(?i)\bSuite\b", "STE")
    return F.upper(replaced)


def transformStagedAddress(rawDf: DataFrame) -> DataFrame:
    """STG_Load_CustomerAddress: the three regional data flows, unioned, with a postal validity flag."""
    common = rawDf.where(F.col("delete_flag") == "N").withColumn(
        "address_type_code", upperTrim(F.col("addr_type_cd"), "MAIN")
    )
    postalTrim = F.trim(F.coalesce(F.col("postal_cd"), F.lit("")))

    na = common.where(F.col("region_cd") == "NA").select(
        "*",
        abbreviateStreet(F.col("addr_line_1")).alias("address_line_1"),
        F.upper(F.trim(F.coalesce(F.col("addr_line_2"), F.lit("")))).alias("address_line_2"),
        F.upper(F.trim(F.col("city_txt"))).alias("city_name"),
        F.upper(F.substring(F.trim(F.coalesce(F.col("state_prov_cd"), F.lit(""))), 1, 2)).alias("state_province_code"),
        F.substring(F.regexp_replace(postalTrim, "-", ""), 1, 5).alias("postal_code"),
        F.lit("USPS").alias("postal_standard"),
        upperTrim(F.col("country_cd"), "USA").alias("country_code"),
    ).withColumn(
        "postal_is_valid",
        (F.length(F.col("postal_code")) == 5) & F.col("postal_code").rlike(r"^[0-9]+$"),
    )
    eu = common.where(F.col("region_cd") == "EU").select(
        "*",
        F.trim(F.col("addr_line_1")).alias("address_line_1"),
        F.trim(F.coalesce(F.col("addr_line_2"), F.lit(""))).alias("address_line_2"),
        F.trim(F.col("city_txt")).alias("city_name"),
        F.lit("").alias("state_province_code"),
        F.concat(upperTrim(F.col("country_cd")), F.lit("-"), F.upper(F.regexp_replace(postalTrim, " ", ""))).alias(
            "postal_code"
        ),
        F.lit("UPU").alias("postal_standard"),
        upperTrim(F.col("country_cd")).alias("country_code"),
    ).withColumn(
        "postal_is_valid", (F.length(F.col("postal_code")) >= 6) & F.col("postal_code").contains("-")
    )
    apac = common.where(~F.col("region_cd").isin("NA", "EU")).select(
        "*",
        F.trim(F.col("addr_line_1")).alias("address_line_1"),
        F.trim(F.coalesce(F.col("addr_line_2"), F.lit(""))).alias("address_line_2"),
        F.upper(F.trim(F.col("city_txt"))).alias("city_name"),
        upperTrim(F.col("state_prov_cd"), "XX").alias("state_province_code"),
        F.regexp_replace(postalTrim, r"[\- ]", "").alias("postal_code"),
        F.lit("APAC-LOCAL").alias("postal_standard"),
        upperTrim(F.col("country_cd")).alias("country_code"),
    ).withColumn(
        "postal_is_valid", (F.length(F.col("postal_code")) >= 3) & F.col("postal_code").rlike(r"^[0-9]+$")
    )
    unioned = na.unionByName(eu).unionByName(apac)
    return unioned.select(
        F.col("cust_addr_id").alias("source_address_id"),
        F.col("cust_id").alias("source_customer_id"),
        F.concat(F.lit("ORA:"), F.col("cust_id").cast("string")).alias("customer_business_key"),
        F.col("address_type_code"),
        F.coalesce(F.col("valid_from_dt"), F.lit("1900-01-01")).cast("timestamp").alias("effective_from_date"),
        F.col("valid_to_dt").cast("timestamp").alias("effective_to_date"),
        F.col("address_line_1"),
        F.col("address_line_2"),
        F.col("city_name"),
        F.col("state_province_code"),
        F.col("postal_code"),
        F.col("postal_cd").alias("postal_code_raw"),
        F.col("postal_standard"),
        F.col("country_code"),
        F.when(F.col("region_cd").isin("NA", "EU"), F.col("region_cd")).otherwise(F.lit("APAC")).alias("region_code"),
        (F.coalesce(F.col("primary_flg"), F.lit("N")) == "Y").alias("is_primary"),
        F.col("geography_lookup_status"),
        F.col("postal_is_valid"),
        F.col("source_system_code"),
        F.col("last_update_dt").cast("timestamp").alias("source_modified_date"),
    )


def splitValidAddresses(stagedDf: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = stagedDf.where(F.col("postal_is_valid"))
    rejected = stagedDf.where(~F.col("postal_is_valid")).select(
        F.lit("STG_Load_CustomerAddress").alias("package_name"),
        F.lit("raw.OracleCustomerAddress").alias("source_table"),
        F.col("source_address_id").cast("string").alias("source_key"),
        F.lit("INVALID_POSTAL").alias("reject_reason_code"),
        F.concat_ws(" ", F.col("postal_standard"), F.col("postal_code")).alias("reject_detail"),
        F.col("region_code"),
        F.current_timestamp().alias("rejected_at"),
    )
    return valid, rejected


def loadStagedCustomerAddress(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """STG_Load_CustomerAddress."""
    raw = spark.table(cfg.fqn(BRONZE_CUSTOMER_ADDRESS)).where(F.col("batch_id") == cfg.batchId)
    valid, rejected = splitValidAddresses(transformStagedAddress(raw))
    overwriteTable(withLoadMetadata(valid, cfg), cfg.fqn(STG_CUSTOMER_ADDRESS))
    appendTable(withLoadMetadata(rejected, cfg), cfg.fqn(ERR_REJECTED_CUSTOMER))
    return spark.table(cfg.fqn(STG_CUSTOMER_ADDRESS))


def payloadCol(name: str, dataType: str = "string") -> Column:
    return F.col("payload").getItem(name).cast(dataType)


def splitGivenName(fullNameCol: Column) -> Column:
    trimmed = F.trim(fullNameCol)
    return F.when(F.instr(trimmed, " ") > 0, F.substring_index(trimmed, " ", 1)).otherwise(trimmed)


def splitFamilyName(fullNameCol: Column) -> Column:
    trimmed = F.trim(fullNameCol)
    rest = F.substr(trimmed, F.instr(trimmed, " ") + 1, F.lit(50))
    return F.when(F.instr(trimmed, " ") > 0, rest).otherwise(F.lit(""))


def phoneLastFour(phoneCol: Column) -> Column:
    cleaned = F.regexp_replace(F.trim(phoneCol), r"[\- ]", "")
    return F.when(phoneCol.isNull(), F.lit("0000")).otherwise(F.substring(cleaned, -4, 4))


def transformStagedEmployee(personRaw: DataFrame, asOf: datetime) -> DataFrame:
    """STG_Load_Employee, employee branch: distinct people referenced by the raw PERSON rows."""
    now = F.lit(asOf).cast("timestamp")
    fullName = payloadCol("FullName")
    region = F.lit("NA")
    rows = personRaw.select(
        F.col("source_key").cast("int").alias("employee_id"),
        F.trim(fullName).alias("full_name"),
        F.coalesce(F.trim(payloadCol("PreferredName")), F.trim(fullName)).alias("preferred_name"),
        splitGivenName(fullName).alias("given_name"),
        splitFamilyName(fullName).alias("family_name"),
        phoneLastFour(payloadCol("PhoneNumber")).alias("phone_number_last4"),
        F.when(region == "EU", F.lit("masked@example.invalid"))
        .otherwise(F.lower(F.trim(F.coalesce(payloadCol("EmailAddress"), F.lit("")))))
        .alias("email_address"),
        payloadCol("IsEmployee", "boolean").alias("is_employee"),
        payloadCol("IsSalesperson", "boolean").alias("is_salesperson"),
        payloadCol("RoleCode").alias("role_code"),
        region.alias("region_code"),
        payloadCol("ValidFrom", "timestamp").alias("valid_from"),
        payloadCol("ValidTo", "timestamp").alias("valid_to"),
        F.when(payloadCol("ValidTo", "timestamp").isNull() | (payloadCol("ValidTo", "timestamp") > now), F.lit("Y"))
        .otherwise(F.lit("N"))
        .alias("is_current_flag"),
        F.col("source_system_code"),
    )
    dedupWindow = Window.partitionBy("employee_id").orderBy(F.col("valid_from").desc_nulls_last())
    return rows.withColumn("rn", F.row_number().over(dedupWindow)).where(F.col("rn") == 1).drop("rn")


def transformStagedSalesperson(personRaw: DataFrame, quotaDf: DataFrame, fxDf: DataFrame, asOf: datetime) -> DataFrame:
    """STG_Load_Employee, salesperson branch: quota converted to USD with the average FX rate (or 1)."""
    people = personRaw.where(payloadCol("IsSalesperson", "boolean")).select(
        F.col("source_key").cast("int").alias("employee_id"),
        F.trim(payloadCol("FullName")).alias("full_name"),
        F.col("source_system_code"),
    )
    quotas = quotaDf.select(
        F.col("SalespersonPersonID").alias("q_person"),
        F.col("SalesTerritoryID").alias("sales_territory_id"),
        F.col("QuotaAmount"),
        F.col("QuotaCurrencyCode"),
    )
    fx = fxDf.select(F.col("currency_code").alias("fx_ccy"), F.col("average_rate"))
    joined = (
        people.join(quotas, F.col("employee_id") == F.col("q_person"), "left")
        .withColumn("quota_currency_code", upperTrim(F.col("QuotaCurrencyCode"), "USD"))
        .withColumn("quota_amount", F.coalesce(F.col("QuotaAmount"), F.lit(0)).cast("decimal(18,2)"))
        .join(fx, F.col("quota_currency_code") == F.col("fx_ccy"), "left")
    )
    result = joined.select(
        F.col("employee_id"),
        F.col("full_name"),
        F.coalesce(F.col("sales_territory_id").cast("string"), F.lit("UNASSIGNED")).alias("sales_territory_code"),
        F.col("quota_currency_code"),
        F.col("quota_amount"),
        (F.col("quota_amount") * F.coalesce(F.col("average_rate"), F.lit(1))).cast("decimal(18,2)").alias("quota_amount_usd"),
        F.lit(asOf).cast("timestamp").alias("effective_from_date"),
        F.col("source_system_code"),
    )
    dedupWindow = Window.partitionBy("employee_id").orderBy(F.col("quota_amount").desc())
    return result.withColumn("rn", F.row_number().over(dedupWindow)).where(F.col("rn") == 1).drop("rn")


def loadStagedEmployee(spark: SparkSession, cfg: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    """STG_Load_Employee (stg.Employee + stg.Salesperson, both truncated first)."""
    personRaw = readRawRecordKind(spark, cfg, RECORD_KIND_PERSON)
    quotas = spark.table("wwi_legacy_oltp.Sales.SalesQuotas")
    fx = spark.createDataFrame([], "currency_code string, average_rate decimal(18,6)")
    now = utcNow()
    employees = transformStagedEmployee(personRaw, now)
    salespeople = transformStagedSalesperson(personRaw, quotas, fx, now)
    overwriteTable(withLoadMetadata(employees, cfg), cfg.fqn(STG_EMPLOYEE))
    overwriteTable(withLoadMetadata(salespeople, cfg), cfg.fqn(STG_SALESPERSON))
    return spark.table(cfg.fqn(STG_EMPLOYEE)), spark.table(cfg.fqn(STG_SALESPERSON))


def transformStagedPromotion(promoRaw: DataFrame) -> DataFrame:
    """STG_Load_PromotionAndTerritory, promotion branch."""
    discountPercent = F.coalesce(payloadCol("DiscountPercent", "decimal(9,4)"), F.lit(0)).cast("decimal(9,4)")
    return promoRaw.select(
        F.col("source_key").cast("int").alias("promotion_id"),
        upperTrim(payloadCol("PromotionCode")).alias("promotion_code"),
        F.trim(payloadCol("PromotionName")).alias("promotion_name"),
        F.when((discountPercent == 0), F.lit("AMOUNT")).otherwise(F.lit("PERCENT")).alias("discount_basis_code"),
        upperTrim(payloadCol("RegionCode"), "NA").alias("region_code"),
        payloadCol("MechanicCode").alias("mechanic_code"),
        payloadCol("CampaignReference").alias("campaign_reference"),
        discountPercent.alias("discount_percent"),
        F.coalesce(payloadCol("DiscountAmount", "decimal(18,2)"), F.lit(0)).cast("decimal(18,2)").alias("discount_amount"),
        payloadCol("StartDate", "timestamp").alias("valid_from_date"),
        F.coalesce(payloadCol("EndDate", "timestamp"), F.lit("9999-12-31").cast("timestamp")).alias("valid_to_date"),
        payloadCol("BudgetAmount", "decimal(18,2)").alias("budget_amount"),
        payloadCol("BudgetCurrencyCode").alias("budget_currency_code"),
        payloadCol("SupplierFundedPercent", "decimal(5,2)").alias("supplier_funded_percent"),
        payloadCol("PromotionStatus").alias("promotion_status"),
        payloadCol("PromotionLineCount", "int").alias("promotion_line_count"),
        payloadCol("RedemptionCount", "int").alias("redemption_count"),
        payloadCol("RedeemedValue", "decimal(18,2)").alias("redeemed_value"),
        F.lit("UNTRANSLATED").alias("source_code_translation_status"),
        F.col("source_system_code"),
    ).withColumn("discount_is_valid", (F.col("discount_percent") >= 0) & (F.col("discount_percent") <= 90))


def transformStagedTerritory(territoryRaw: DataFrame, countryRef: DataFrame) -> DataFrame:
    """STG_Load_PromotionAndTerritory, territory branch (country lookup + hierarchy)."""
    region = upperTrim(payloadCol("RegionCode"), "NA")
    countries = countryRef.select(
        F.upper(F.col("CountryCodeIso3")).alias("ref_iso3"), F.col("CountryName").alias("country_name")
    )
    rows = territoryRaw.select(
        F.col("source_key").cast("int").alias("sales_territory_id"),
        upperTrim(payloadCol("TerritoryCode")).alias("sales_territory_code"),
        F.trim(payloadCol("TerritoryName")).alias("sales_territory_name"),
        region.alias("region_code"),
        upperTrim(payloadCol("CountryISO3"), "USA").alias("country_code"),
        F.coalesce(upperTrim(payloadCol("ParentTerritoryCode")), region).alias("parent_territory_code"),
        payloadCol("ParentTerritoryID", "int").alias("parent_territory_id"),
        payloadCol("TerritoryLevel", "int").alias("territory_level"),
        payloadCol("TaxRegimeCode").alias("tax_jurisdiction_code"),
        payloadCol("FiscalCalendarCode").alias("fiscal_calendar_code"),
        payloadCol("ReportingCurrencyCode").alias("reporting_currency_code"),
        payloadCol("PostalStandardCode").alias("postal_standard_code"),
        payloadCol("ManagerPersonID", "int").alias("manager_person_id"),
        payloadCol("IsActive", "boolean").alias("is_active"),
        payloadCol("CurrentQuotaAmount", "decimal(18,2)").alias("current_quota_amount"),
        payloadCol("CommissionPlanCode").alias("commission_plan_code"),
        F.col("source_system_code"),
    )
    return (
        rows.join(countries, F.col("country_code") == F.col("ref_iso3"), "left")
        .withColumn("country_lookup_status", F.when(F.col("ref_iso3").isNotNull(), F.lit("MATCHED")).otherwise(F.lit("UNMATCHED")))
        .drop("ref_iso3")
    )


def loadStagedPromotionAndTerritory(spark: SparkSession, cfg: PipelineConfig) -> tuple[DataFrame, DataFrame]:
    """STG_Load_PromotionAndTerritory (stg.Promotion + stg.SalesTerritory, both truncated first)."""
    promotions = transformStagedPromotion(readRawRecordKind(spark, cfg, RECORD_KIND_PROMOTION))
    validPromotions = promotions.where(F.col("discount_is_valid"))
    rejectedPromotions = promotions.where(~F.col("discount_is_valid")).select(
        F.lit("STG_Load_PromotionAndTerritory").alias("package_name"),
        F.lit("raw.SqlOrder[PROMOTION]").alias("source_table"),
        F.col("promotion_code").alias("source_key"),
        F.lit("DISCOUNT_OUT_OF_RANGE").alias("reject_reason_code"),
        F.col("discount_percent").cast("string").alias("reject_detail"),
        F.col("region_code"),
        F.current_timestamp().alias("rejected_at"),
    )
    countryRef = spark.table(cfg.legacy(LEGACY_STAGING, "ref", "Country"))
    territories = transformStagedTerritory(readRawRecordKind(spark, cfg, RECORD_KIND_TERRITORY), countryRef)
    overwriteTable(withLoadMetadata(validPromotions, cfg), cfg.fqn(STG_PROMOTION))
    appendTable(withLoadMetadata(rejectedPromotions, cfg), cfg.fqn(ERR_REJECTED_CUSTOMER))
    overwriteTable(withLoadMetadata(territories, cfg), cfg.fqn(STG_SALES_TERRITORY))
    return spark.table(cfg.fqn(STG_PROMOTION)), spark.table(cfg.fqn(STG_SALES_TERRITORY))
