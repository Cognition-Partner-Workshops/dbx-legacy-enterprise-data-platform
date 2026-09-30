"""Read-only access to the legacy sources through Lakehouse Federation.

Oracle identifiers come back upper-case from the foreign catalog; every reader
here lower-cases the columns so the rest of the code can use snake_case.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from finance.config import FinanceConfig
from finance.fx import normalizeRates


def lowerColumns(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(f"`{c}`").alias(c.lower()) for c in df.columns])


def oracleTable(spark: SparkSession, cfg: FinanceConfig, schema: str, name: str) -> DataFrame:
    return lowerColumns(spark.table(cfg.oracle(schema, name)))


def supplierMaster(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_mdm", "supp_master").select(
        "supp_id",
        F.col("supp_nbr").alias("supp_num"),
        "supp_name",
        "country_cd",
        F.col("region_cd").alias("supplier_region_cd"),
        "default_curr_cd",
        F.col("payment_terms_cd").alias("supplier_payment_terms_cd"),
        "vat_reg_nbr",
        "tax_id_nbr",
        "withholding_flg",
        "form_1099_flg",
        "hold_all_flg",
    )


def paymentTerms(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "payment_terms").where(
        F.coalesce(F.col("active_flg"), F.lit("Y")) == "Y"
    )


def taxRates(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "tax_rate")


def fxRates(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return normalizeRates(spark.table(cfg.oracle("wwi_ref", "fx_rate_daily")))


def currencies(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_ref", "currency_code").select(
        "curr_cd", "minor_unit_digits", "region_cd"
    )


def regionRef(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_ref", "region_ref").select(
        "region_cd", "reporting_curr_cd", "fiscal_calendar_cd", "tax_regime_cd", "fiscal_year_start_month"
    )


def calendarFiscal(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    """Non-adjustment calendar rows keyed by (calendar_cd, calendar_dt) -> period_cd (as used by FN_FISCAL_PERIOD)."""
    cal = oracleTable(spark, cfg, "wwi_ref", "calendar_fiscal").where(
        F.coalesce(F.col("adjustment_period_flg"), F.lit("N")) == "N"
    )
    return cal.groupBy("calendar_cd", F.to_date("calendar_dt").alias("calendar_dt")).agg(
        F.min("period_cd").alias("calendar_period_cd")
    )


def periodStatus(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "gl_period_status").select(
        F.col("ledger_cd").alias("status_ledger_cd"),
        "period_cd",
        "region_cd",
        F.to_date("period_start_dt").alias("period_start_dt"),
        F.to_date("period_end_dt").alias("period_end_dt"),
        "adjustment_period_flg",
        "ap_status_cd",
        "gl_status_cd",
        F.to_date("closed_dt").alias("closed_dt"),
        F.to_date("soft_close_dt").alias("soft_close_dt"),
    )


def glAccounts(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "gl_account").select(
        "gl_account_id",
        "account_cd",
        "account_name",
        "account_type_cd",
        "account_class_cd",
        "normal_balance_cd",
        "posting_allowed_flg",
        "reconciliation_flg",
        "account_status_cd",
    )


def costCenters(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "cost_center")


def allocationRules(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    return oracleTable(spark, cfg, "wwi_fin", "cost_allocation_rule")


def legacyDateDimension(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    d = spark.table(cfg.dw("Dimension", "Date"))
    return d.select(
        F.col("`Date`").alias("date_key"),
        F.col("`Fiscal Year`").alias("fiscal_year"),
        F.col("`Fiscal Period Number`").alias("fiscal_period_number"),
        F.col("`APAC Fiscal Period Label`").alias("apac_fiscal_period_label"),
        F.col("`APAC Is Period End`").alias("apac_is_period_end"),
        F.col("`Is Month End Close Day`").alias("is_month_end_close_day"),
    )


def legacySupplierDimension(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    s = spark.table(cfg.dw("Dimension", "Supplier"))
    return s.select(
        F.col("`Supplier Key`").alias("supplier_key"),
        F.col("`WWI Supplier ID`").alias("wwi_supplier_id"),
        F.col("`Supplier`").alias("supplier_name"),
        F.col("`Withholding Tax Rate`").alias("withholding_tax_rate"),
        F.col("`Is 1099 Reportable`").alias("is_1099_reportable"),
        F.col("`Is Current Row`").alias("is_current_row"),
    )


def legacyConfiguration(spark: SparkSession, cfg: FinanceConfig) -> DataFrame:
    c = spark.table(cfg.staging("etl", "Configuration"))
    return c.select(
        F.col("ConfigurationKey").alias("configuration_key"),
        F.col("ConfigurationValue").alias("configuration_value"),
        F.col("EnvironmentCode").alias("environment_code"),
    )
