"""EXT_ORA_* and EXT_SQL_* packages: land the legacy sources as bronze Delta tables.

Every package here is a full reload of a raw landing table except EXT_ORA_FxRateDaily, which is a
half-open date-window load driven by a watermark (etl.usp_GetWatermark / usp_UpdateWatermark).
"""
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config
from ref_calendar.common import addAuditColumns, logRowCount, snakeCaseColumns, tableExists, writeTable, yesNo

ORA = config.LEGACY_ORACLE
OLTP = config.LEGACY_OLTP

WATERMARK_SCHEMA = T.StructType([
    T.StructField("source_system_code", T.StringType()),
    T.StructField("object_name", T.StringType()),
    T.StructField("watermark_type", T.StringType()),
    T.StructField("last_value", T.StringType()),
    T.StructField("previous_value", T.StringType()),
    T.StructField("last_loaded_at_utc", T.TimestampType()),
    T.StructField("last_batch_id", T.LongType()),
])


# ----------------------------------------------------------------------------- pure transforms

def shapeCurrencyExtract(currency: DataFrame) -> DataFrame:
    """WWI_REF.CURRENCY_CODE -> raw.OracleCurrency column shape (landed as NVARCHAR, as SSIS did)."""
    return currency.select(
        F.col("curr_cd").cast("string").alias("ccy_code"),
        F.col("curr_name").cast("string").alias("ccy_name"),
        F.col("curr_symbol").cast("string").alias("ccy_symbol"),
        F.col("minor_unit_digits").cast("string").alias("minor_units"),
        F.col("curr_num_cd").cast("string").alias("iso_numeric_cd"),
        F.col("rounding_rule_cd").cast("string").alias("rounding_rule_cd"),
        F.col("primary_country_cd").cast("string").alias("primary_country_cd"),
        F.col("region_cd").cast("string").alias("region_cd"),
        F.col("active_flg").cast("string").alias("active_flg"),
        F.col("trading_allowed_flg").cast("string").alias("trading_allowed_flg"),
        F.col("euro_legacy_flg").cast("string").alias("euro_legacy_flg"),
        F.col("euro_fixed_rate").cast("string").alias("legacy_fixed_rate"),
        F.col("euro_conversion_dt").cast("string").alias("euro_conversion_dt"),
        F.col("retired_dt").cast("string").alias("retired_dt"),
        F.coalesce(F.col("updated_dt"), F.col("created_dt")).cast("string").alias("last_update_dt"),
    )


def shapeGeographyExtract(country: DataFrame, region: DataFrame, city: DataFrame, postal: DataFrame,
                          language: DataFrame | None = None) -> DataFrame:
    """WWI_REF.V_GEOGRAPHY_EXTRACT rebuilt over the base tables (the view is not exposed by federation)."""
    ct, rg, ci, po = country.alias("ct"), region.alias("rg"), city.alias("ci"), postal.alias("po")
    df = (
        ct.join(rg, F.col("rg.region_cd") == F.col("ct.region_cd"), "left")
        .join(ci, F.col("ci.country_cd") == F.col("ct.country_cd"), "left")
        .join(po, F.col("po.city_id") == F.col("ci.city_id"), "left")
    )
    if language is not None:
        lg = language.alias("lg")
        df = df.join(lg, F.col("lg.language_cd") == F.col("ct.default_language_cd"), "left")
        languageCode, languageName = F.col("lg.language_cd"), F.col("lg.language_name")
    else:
        languageCode, languageName = F.col("ct.default_language_cd"), F.lit(None).cast("string")
    return df.select(
        F.col("ct.country_cd").alias("country_cd"),
        F.col("ct.country_name").alias("country_name"),
        F.col("ct.country_cd_3").alias("iso3_cd"),
        F.col("ct.default_curr_cd").alias("currency_cd"),
        F.col("ct.eu_member_flg").alias("eu_member_flag"),
        F.col("ct.eu_vat_area_flg").alias("eu_vat_area_flg"),
        F.col("ct.sanctioned_flg").alias("sanctioned_flg"),
        F.col("ct.sub_region_txt").alias("sub_region_name"),
        F.col("ct.postal_format_txt").alias("postal_format_mask"),
        F.col("ct.postal_required_flg").alias("postal_required_flg"),
        F.col("ct.state_prov_required_flg").alias("state_prov_required_flg"),
        F.coalesce(F.col("ct.region_cd"), F.col("rg.region_cd")).alias("region_cd"),
        F.col("rg.region_name").alias("region_name"),
        F.col("ci.city_id").alias("geography_id"),
        F.col("ci.city_name").alias("city_name"),
        F.col("ci.state_prov_cd").alias("state_province_cd"),
        F.col("ci.state_prov_name").alias("state_province_name"),
        F.col("ci.prefecture_txt").alias("prefecture_txt"),
        F.col("ci.timezone_txt").alias("timezone_name"),
        F.col("ci.population_cnt").cast("string").alias("population_num"),
        F.col("ci.latitude").cast("string").alias("latitude"),
        F.col("ci.longitude").cast("string").alias("longitude"),
        F.col("po.postal_cd").alias("postal_cd"),
        F.col("po.postal_cd_norm").alias("postal_cd_norm"),
        F.col("po.district_txt").alias("postal_area_name"),
        F.coalesce(F.col("po.tax_jurisdiction_cd"), F.col("ci.tax_jurisdiction_cd")).alias("tax_jurisdiction_cd"),
        F.when(F.col("po.postal_cd").isNull(), F.lit("CITY")).otherwise(F.lit("POSTAL")).alias("grain_cd"),
        languageCode.alias("language_cd"),
        languageName.alias("language_name"),
        F.when(F.col("rg.region_cd") == "NA", "CITY_STATE_ZIP")
        .when(F.col("rg.region_cd") == "EU", "ZIP_CITY")
        .when(F.col("rg.region_cd") == "APAC", "ZIP_PREFECTURE_CITY")
        .otherwise("FREEFORM").alias("address_format_cd"),
        F.col("rg.retention_months").alias("data_retention_months"),
        F.when(F.col("rg.consent_regime_cd").isNull(), "N").otherwise("Y").alias("consent_required_flag"),
        F.coalesce(F.col("ct.updated_dt"), F.col("ct.created_dt")).cast("string").alias("last_update_dt"),
        F.lit("ORAGEO").alias("record_kind"),
        F.lit(None).cast("string").alias("continent"),
        F.lit(None).cast("string").alias("sales_territory"),
        F.lit(None).cast("string").alias("postal_format_code"),
        F.lit(None).cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
    )


def regionFromContinent(continent):
    return (
        F.when(continent == "North America", "NA")
        .when(continent == "Europe", "EU")
        .when(continent.isin("Asia", "Oceania"), "APAC")
        .otherwise("ROW")
    )


def shapeCityExtract(cities: DataFrame, stateProvinces: DataFrame, countries: DataFrame) -> DataFrame:
    """EXT_SQL_Cities: Application.Cities (current + system-versioned history) joined to state and country,
    tagged RecordKind = OLTPCITY and mapped onto the raw.OracleGeography landing shape."""
    c, sp, co = cities.alias("c"), stateProvinces.alias("sp"), countries.alias("co")
    continent = F.col("co.Continent")
    return (
        c.join(sp, F.col("sp.StateProvinceID") == F.col("c.StateProvinceID"))
        .join(co, F.col("co.CountryID") == F.col("sp.CountryID"))
        .select(
            F.col("co.IsoAlpha3Code").alias("country_cd"),
            F.col("co.CountryName").alias("country_name"),
            F.col("co.IsoAlpha3Code").alias("iso3_cd"),
            F.lit(None).cast("string").alias("currency_cd"),
            F.lit(None).cast("string").alias("eu_member_flag"),
            F.lit(None).cast("string").alias("eu_vat_area_flg"),
            F.lit(None).cast("string").alias("sanctioned_flg"),
            F.col("co.Subregion").alias("sub_region_name"),
            F.lit(None).cast("string").alias("postal_format_mask"),
            F.lit(None).cast("string").alias("postal_required_flg"),
            F.lit(None).cast("string").alias("state_prov_required_flg"),
            regionFromContinent(continent).alias("region_cd"),
            continent.alias("region_name"),
            F.col("c.CityID").cast("long").alias("geography_id"),
            F.col("c.CityName").alias("city_name"),
            F.col("sp.StateProvinceCode").alias("state_province_cd"),
            F.col("sp.StateProvinceName").alias("state_province_name"),
            F.lit(None).cast("string").alias("prefecture_txt"),
            F.lit(None).cast("string").alias("timezone_name"),
            F.col("c.LatestRecordedPopulation").cast("string").alias("population_num"),
            F.lit(None).cast("string").alias("latitude"),
            F.lit(None).cast("string").alias("longitude"),
            F.lit(None).cast("string").alias("postal_cd"),
            F.lit(None).cast("string").alias("postal_cd_norm"),
            F.lit(None).cast("string").alias("postal_area_name"),
            F.lit(None).cast("string").alias("tax_jurisdiction_cd"),
            F.lit("CITY").alias("grain_cd"),
            F.lit(None).cast("string").alias("language_cd"),
            F.lit(None).cast("string").alias("language_name"),
            F.lit(None).cast("string").alias("address_format_cd"),
            F.lit(None).cast("int").alias("data_retention_months"),
            F.lit(None).cast("string").alias("consent_required_flag"),
            F.col("c.ValidFrom").cast("string").alias("last_update_dt"),
            F.lit("OLTPCITY").alias("record_kind"),
            continent.alias("continent"),
            F.col("sp.SalesTerritory").alias("sales_territory"),
            F.when(continent == "North America", "ZIP5_PLUS4")
            .when(continent == "Europe", "ALPHANUM")
            .otherwise("NUMERIC6").alias("postal_format_code"),
            F.col("c.ValidFrom").cast("timestamp").alias("valid_from"),
            F.col("c.ValidTo").cast("timestamp").alias("valid_to"),
        )
    )


def shapePaymentMethodExtract(paymentMethods: DataFrame) -> DataFrame:
    """EXT_SQL_PaymentMethods: current + archived versions, tagged RecordKind = PAYMETHOD."""
    code = F.upper(F.regexp_replace(F.col("PaymentMethodName"), r"[^A-Za-z]", ""))
    return paymentMethods.select(
        F.col("PaymentMethodID").cast("int").alias("payment_method_id"),
        F.col("PaymentMethodName").alias("payment_method_name"),
        code.alias("payment_method_code"),
        F.when(code.isin("CASH", "CHECK"), F.lit("N")).otherwise(F.lit("Y")).alias("is_electronic_flag"),
        F.when(code == "CASH", F.lit(0)).when(code == "CHECK", F.lit(5)).otherwise(F.lit(2)).alias("settlement_days"),
        F.lit("ALL").alias("region_code"),
        F.col("ValidFrom").cast("timestamp").alias("valid_from"),
        F.col("ValidTo").cast("timestamp").alias("valid_to"),
        F.lit("PAYMETHOD").alias("record_kind"),
    ).withColumn("immediate_settlement_flag", yesNo(F.col("settlement_days") == 0))


def shapeTransactionTypeExtract(transactionTypes: DataFrame) -> DataFrame:
    """EXT_SQL_TransactionTypes: current + archived versions, tagged RecordKind = TRANTYPE."""
    name = F.col("TransactionTypeName")
    return transactionTypes.select(
        F.col("TransactionTypeID").cast("int").alias("transaction_type_id"),
        name.alias("transaction_type_name"),
        F.when(name.like("%Credit%"), F.lit("CR")).otherwise(F.lit("DR")).alias("ledger_side_code"),
        F.when(name.like("%Reversal%"), F.lit(1)).otherwise(F.lit(0)).cast("int").alias("is_reversal"),
        F.col("ValidFrom").cast("timestamp").alias("valid_from"),
        F.col("ValidTo").cast("timestamp").alias("valid_to"),
        F.lit("TRANTYPE").alias("record_kind"),
    )


def shapeFxRateExtract(fx: DataFrame, windowFrom: date, windowTo: date) -> DataFrame:
    """Direct rates in the half-open window for the SPOT/CORP/AVG rate types (raw.OracleFxRate shape)."""
    return (
        fx.where((F.col("rate_dt") >= F.lit(windowFrom)) & (F.col("rate_dt") < F.lit(windowTo)))
        .where(F.col("rate_type_cd").isin(*config.FX_RATE_TYPES))
        .select(
            F.col("rate_dt").cast("date").alias("rate_dt"),
            F.col("from_curr_cd").alias("from_currency_cd"),
            F.col("to_curr_cd").alias("to_currency_cd"),
            F.col("rate_type_cd").alias("rate_type_cd"),
            F.col("rate").cast("decimal(18,8)").alias("rate"),
            F.col("inverse_rate").cast("decimal(18,8)").alias("inverse_rate"),
            F.col("bid_rate").cast("decimal(18,8)").alias("bid_rate"),
            F.col("ask_rate").cast("decimal(18,8)").alias("ask_rate"),
            F.coalesce(F.col("rate_source_cd"), F.lit("ORA_ERP")).alias("rate_source_cd"),
            F.coalesce(F.col("feed_region_cd"), F.lit("GLOBAL")).alias("region_cd"),
            F.col("interpolated_flg").alias("interpolated_flg"),
            F.col("superseded_flg").alias("superseded_flg"),
            F.col("loaded_ts").cast("timestamp").alias("last_update_dt"),
        )
    )


def triangulateFxRates(fx: DataFrame, windowFrom: date, windowTo: date) -> DataFrame:
    """Cross rates through USD for the EUR and SGD legs (the second source of EXT_ORA_FxRateDaily)."""
    base = fx.where(
        (F.col("rate_dt") >= F.lit(windowFrom)) & (F.col("rate_dt") < F.lit(windowTo))
        & F.col("rate_type_cd").isin(*config.FX_RATE_TYPES) & (F.col("to_curr_cd") == "USD")
    )
    usdFrom, usdTo = base.alias("usd_from"), base.where(F.col("from_curr_cd").isin(*config.FX_TRIANGULATION_CURRENCIES)).alias("usd_to")
    return (
        usdFrom.join(
            usdTo,
            (F.col("usd_to.rate_dt") == F.col("usd_from.rate_dt"))
            & (F.col("usd_to.rate_type_cd") == F.col("usd_from.rate_type_cd"))
            & (F.col("usd_from.from_curr_cd") != F.col("usd_to.from_curr_cd")),
        )
        .select(
            F.col("usd_from.rate_dt").cast("date").alias("rate_dt"),
            F.col("usd_from.from_curr_cd").alias("from_currency_cd"),
            F.col("usd_to.from_curr_cd").alias("to_currency_cd"),
            F.col("usd_from.rate_type_cd").alias("rate_type_cd"),
            (F.col("usd_from.rate") / F.when(F.col("usd_to.rate") == 0, None).otherwise(F.col("usd_to.rate"))).cast("decimal(18,8)").alias("rate"),
            (F.col("usd_to.rate") / F.when(F.col("usd_from.rate") == 0, None).otherwise(F.col("usd_from.rate"))).cast("decimal(18,8)").alias("inverse_rate"),
            F.lit(None).cast("decimal(18,8)").alias("bid_rate"),
            F.lit(None).cast("decimal(18,8)").alias("ask_rate"),
            F.lit("TRIANG").alias("rate_source_cd"),
            F.when(F.col("usd_to.from_curr_cd") == "EUR", "EU")
            .when(F.col("usd_to.from_curr_cd") == "SGD", "APAC")
            .otherwise("GLOBAL").alias("region_cd"),
            F.lit("N").alias("interpolated_flg"),
            F.lit("N").alias("superseded_flg"),
            F.col("usd_from.loaded_ts").cast("timestamp").alias("last_update_dt"),
        )
    )


def shapePaymentTermsExtract(terms: DataFrame) -> DataFrame:
    return terms.select(
        F.col("payment_terms_cd").cast("string").alias("terms_code"),
        F.col("terms_desc").cast("string").alias("terms_desc"),
        F.col("net_days").cast("int").alias("net_days"),
        F.col("discount_1_pct").cast("decimal(9,4)").alias("disc_pct"),
        F.col("discount_1_days").cast("int").alias("disc_days"),
        F.col("discount_2_pct").cast("decimal(9,4)").alias("disc_2_pct"),
        F.col("discount_2_days").cast("int").alias("disc_2_days"),
        F.col("due_day_of_month_nbr").cast("int").alias("day_of_month_due"),
        F.col("months_forward_nbr").cast("int").alias("months_forward"),
        F.col("term_basis_cd").cast("string").alias("calculation_basis_cd"),
        F.col("late_fee_pct").cast("decimal(9,4)").alias("late_fee_pct"),
        F.col("prompt_pay_flg").cast("string").alias("prompt_pay_flg"),
        F.col("eu_late_payment_dir_flg").cast("string").alias("eu_late_payment_dir_flg"),
        F.col("region_cd").cast("string").alias("region_cd"),
        F.col("active_flg").cast("string").alias("active_flg"),
        F.col("effective_from_dt").cast("date").alias("effective_from_dt"),
        F.coalesce(F.col("updated_dt"), F.col("created_dt")).cast("string").alias("last_update_dt"),
    )


def shapeTaxRateExtract(taxRate: DataFrame, taxJurisdiction: DataFrame) -> DataFrame:
    tr, tj = taxRate.alias("tr"), taxJurisdiction.alias("tj")
    return tr.join(tj, F.col("tj.jurisdiction_cd") == F.col("tr.jurisdiction_cd"), "left").select(
        F.col("tr.tax_rate_id").cast("long").alias("tax_rate_id"),
        F.col("tr.tax_code_cd").cast("string").alias("tax_code"),
        F.col("tr.tax_regime_cd").cast("string").alias("tax_type_cd"),
        F.col("tj.country_cd").cast("string").alias("country_cd"),
        F.col("tj.state_prov_cd").cast("string").alias("state_province_cd"),
        F.col("tj.county_txt").cast("string").alias("county_txt"),
        F.col("tj.city_txt").cast("string").alias("city_txt"),
        F.col("tj.jurisdiction_name").cast("string").alias("jurisdiction_name"),
        F.col("tj.jurisdiction_level_cd").cast("string").alias("jurisdiction_level_cd"),
        F.col("tj.parent_jurisdiction_cd").cast("string").alias("parent_jurisdiction_cd"),
        F.col("tj.stacks_with_parent_flg").cast("string").alias("stacks_with_parent_flg"),
        F.coalesce(F.col("tr.region_cd"), F.col("tj.region_cd")).cast("string").alias("region_cd"),
        F.col("tr.jurisdiction_cd").cast("string").alias("jurisdiction_cd"),
        F.col("tr.rate_category_cd").cast("string").alias("tax_class_cd"),
        F.col("tr.rate_pct").cast("decimal(9,4)").alias("rate_pct"),
        F.col("tr.compound_flg").cast("string").alias("compound_flg"),
        F.col("tr.compound_seq_nbr").cast("int").alias("compound_seq_nbr"),
        F.col("tr.recoverable_pct").cast("decimal(9,4)").alias("recoverable_pct"),
        F.when(F.col("tr.recoverable_pct") > 0, "Y").otherwise("N").alias("recoverable_flg"),
        F.col("tr.reverse_charge_flg").cast("string").alias("reverse_charge_flg"),
        F.col("tr.effective_from_dt").cast("date").alias("eff_from_dt"),
        F.col("tr.effective_to_dt").cast("date").alias("eff_to_dt"),
        F.coalesce(F.col("tr.updated_dt"), F.col("tr.created_dt")).cast("string").alias("last_update_dt"),
    )


# ----------------------------------------------------------------------------- watermark helpers

def getWatermark(spark: SparkSession, objectName: str) -> str | None:
    name = config.tbl("etl_watermark")
    if not tableExists(spark, name):
        return None
    rows = spark.table(name).where(F.col("object_name") == objectName).collect()
    return rows[0]["last_value"] if rows else None


def updateWatermark(spark: SparkSession, objectName: str, newValue: str, batchId: int, sourceSystemCode: str = config.SOURCE_SYSTEM_ORACLE) -> None:
    name = config.tbl("etl_watermark")
    previous = getWatermark(spark, objectName)
    newRow = spark.createDataFrame(
        [(sourceSystemCode, objectName, "DATE_WINDOW", newValue, previous, None, batchId)], WATERMARK_SCHEMA
    ).withColumn("last_loaded_at_utc", F.current_timestamp())
    if tableExists(spark, name):
        others = spark.table(name).where(F.col("object_name") != objectName)
        newRow = others.unionByName(newRow)
    writeTable(newRow, name)


def _asDate(value) -> date:
    return value.date() if isinstance(value, datetime) else value


def resolveFxWindow(spark: SparkSession, windowFrom: str | None, windowTo: str | None) -> tuple[date, date]:
    """etl.usp_GetWatermark semantics: [last watermark, today + 1). The very first run back-fills from the
    oldest rate on the source, which is what the SSIS estate's seeded etl.Watermark row expressed."""
    if windowFrom and windowTo:
        return date.fromisoformat(windowFrom), date.fromisoformat(windowTo)
    last = getWatermark(spark, "WWI_REF.FX_RATE_DAILY")
    if last is None:
        bounds = spark.table(f"{ORA}.wwi_ref.fx_rate_daily").agg(F.min("rate_dt"), F.max("rate_dt")).first()
        start = _asDate(bounds[0]) if bounds[0] is not None else date.today()
        end = (_asDate(bounds[1]) if bounds[1] is not None else date.today()) + timedelta(days=1)
    else:
        start = date.fromisoformat(last)
        end = date.today() + timedelta(days=1)
    if windowFrom:
        start = date.fromisoformat(windowFrom)
    if windowTo:
        end = date.fromisoformat(windowTo)
    return start, end


# ----------------------------------------------------------------------------- package entrypoints

def runExtOraCodeTranslation(spark: SparkSession, batchId: int) -> int:
    src = snakeCaseColumns(spark.table(f"{ORA}.wwi_ref.code_translation"))
    df = addAuditColumns(src, batchId, config.SOURCE_SYSTEM_ORACLE)
    writeTable(df, config.tbl("bronze_oracle_code_translation"))
    count = spark.table(config.tbl("bronze_oracle_code_translation")).count()
    logRowCount(spark, batchId, "EXT_ORA_CodeTranslation", "bronze_oracle_code_translation", source=count, target=count)
    return count


def runExtOraCurrency(spark: SparkSession, batchId: int) -> int:
    df = shapeCurrencyExtract(spark.table(f"{ORA}.wwi_ref.currency_code"))
    writeTable(addAuditColumns(df, batchId, config.SOURCE_SYSTEM_ORACLE), config.tbl("bronze_oracle_currency"))
    count = spark.table(config.tbl("bronze_oracle_currency")).count()
    logRowCount(spark, batchId, "EXT_ORA_Currency", "bronze_oracle_currency", source=count, target=count)
    return count


def runExtOraGeography(spark: SparkSession, batchId: int) -> int:
    language = spark.table(f"{ORA}.wwi_ref.language_ref") if tableExists(spark, f"{ORA}.wwi_ref.language_ref") else None
    df = shapeGeographyExtract(
        spark.table(f"{ORA}.wwi_ref.country_ref"), spark.table(f"{ORA}.wwi_ref.region_ref"),
        spark.table(f"{ORA}.wwi_ref.city_ref"), spark.table(f"{ORA}.wwi_ref.postal_ref"), language,
    )
    df = addAuditColumns(df, batchId, config.SOURCE_SYSTEM_ORACLE)
    target = config.tbl("bronze_oracle_geography")
    if tableExists(spark, target):
        writeTable(df, target, replaceWhere="record_kind = 'ORAGEO'")
    else:
        writeTable(df, target)
    count = spark.table(target).where("record_kind = 'ORAGEO'").count()
    logRowCount(spark, batchId, "EXT_ORA_Geography", "bronze_oracle_geography", source=count, target=count)
    return count


def runExtOraPaymentTerms(spark: SparkSession, batchId: int) -> int:
    df = shapePaymentTermsExtract(spark.table(f"{ORA}.wwi_fin.payment_terms"))
    writeTable(addAuditColumns(df, batchId, config.SOURCE_SYSTEM_ORACLE), config.tbl("bronze_oracle_payment_terms"))
    count = spark.table(config.tbl("bronze_oracle_payment_terms")).count()
    logRowCount(spark, batchId, "EXT_ORA_PaymentTerms", "bronze_oracle_payment_terms", source=count, target=count)
    return count


def runExtOraTaxRate(spark: SparkSession, batchId: int) -> int:
    df = shapeTaxRateExtract(spark.table(f"{ORA}.wwi_fin.tax_rate"), spark.table(f"{ORA}.wwi_fin.tax_jurisdiction"))
    writeTable(addAuditColumns(df, batchId, config.SOURCE_SYSTEM_ORACLE), config.tbl("bronze_oracle_tax_rate"))
    count = spark.table(config.tbl("bronze_oracle_tax_rate")).count()
    logRowCount(spark, batchId, "EXT_ORA_TaxRate", "bronze_oracle_tax_rate", source=count, target=count)
    return count


def runExtOraFxRateDaily(spark: SparkSession, batchId: int, windowFrom: str | None = None, windowTo: str | None = None) -> dict:
    start, end = resolveFxWindow(spark, windowFrom, windowTo)
    fx = spark.table(f"{ORA}.wwi_ref.fx_rate_daily")
    df = shapeFxRateExtract(fx, start, end).unionByName(triangulateFxRates(fx, start, end))
    df = addAuditColumns(df, batchId, config.SOURCE_SYSTEM_ORACLE)
    target = config.tbl("bronze_oracle_fx_rate")
    windowPredicate = f"rate_dt >= '{start.isoformat()}' AND rate_dt < '{end.isoformat()}'"
    if tableExists(spark, target):
        writeTable(df, target, replaceWhere=windowPredicate)
    else:
        writeTable(df, target)
    loaded = spark.table(target).where(windowPredicate).count()
    missingPairs = checkMandatoryFxPairs(spark, target, windowPredicate)
    updateWatermark(spark, "WWI_REF.FX_RATE_DAILY", end.isoformat(), batchId)
    logRowCount(spark, batchId, "EXT_ORA_FxRateDaily", "bronze_oracle_fx_rate", source=loaded, target=loaded, reject=len(missingPairs))
    return {"window_from": start.isoformat(), "window_to": end.isoformat(), "rows": loaded, "missing_pairs": missingPairs}


def checkMandatoryFxPairs(spark: SparkSession, target: str, windowPredicate: str) -> list[str]:
    """Check the mandatory rate pairs configured in legacy etl.Configuration (FxMandatoryPairs); a miss is
    logged rather than failing the load, as the package only raised a warning event."""
    cfg = spark.table(f"{config.LEGACY_STAGING}.etl.Configuration").where("ConfigurationKey = 'FxMandatoryPairs'").collect()
    if not cfg or not cfg[0]["ConfigurationValue"]:
        return []
    wanted = [p.strip().upper() for p in cfg[0]["ConfigurationValue"].split(",") if p.strip()]
    present = {
        r[0] for r in spark.table(target).where(windowPredicate)
        .select(F.concat_ws("/", "from_currency_cd", "to_currency_cd")).distinct().collect()
    }
    return [p for p in wanted if p not in present]


def _withHistory(spark: SparkSession, tableName: str) -> DataFrame:
    """Application.<table> FOR SYSTEM_TIME ALL: current rows plus the system-versioned archive."""
    current = spark.table(f"{OLTP}.Application.{tableName}")
    archiveName = f"{OLTP}.Application.{tableName}_Archive"
    if tableExists(spark, archiveName):
        archive = spark.table(archiveName).select(*current.columns)
        return current.unionByName(archive)
    return current


def runExtSqlCities(spark: SparkSession, batchId: int) -> int:
    df = shapeCityExtract(
        _withHistory(spark, "Cities"), spark.table(f"{OLTP}.Application.StateProvinces"),
        spark.table(f"{OLTP}.Application.Countries"),
    )
    df = addAuditColumns(df, batchId, config.SOURCE_SYSTEM_OLTP)
    target = config.tbl("bronze_oracle_geography")
    if tableExists(spark, target):
        writeTable(df, target, replaceWhere="record_kind = 'OLTPCITY'")
    else:
        writeTable(df, target)
    count = spark.table(target).where("record_kind = 'OLTPCITY'").count()
    logRowCount(spark, batchId, "EXT_SQL_Cities", "bronze_oracle_geography", source=count, target=count)
    return count


def runExtSqlPaymentMethods(spark: SparkSession, batchId: int) -> int:
    df = shapePaymentMethodExtract(_withHistory(spark, "PaymentMethods"))
    writeTable(addAuditColumns(df, batchId, config.SOURCE_SYSTEM_OLTP), config.tbl("bronze_sql_payment_methods"))
    count = spark.table(config.tbl("bronze_sql_payment_methods")).count()
    logRowCount(spark, batchId, "EXT_SQL_PaymentMethods", "bronze_sql_payment_methods", source=count, target=count)
    return count


def runExtSqlTransactionTypes(spark: SparkSession, batchId: int) -> int:
    df = shapeTransactionTypeExtract(_withHistory(spark, "TransactionTypes"))
    writeTable(addAuditColumns(df, batchId, config.SOURCE_SYSTEM_OLTP), config.tbl("bronze_sql_transaction_types"))
    count = spark.table(config.tbl("bronze_sql_transaction_types")).count()
    logRowCount(spark, batchId, "EXT_SQL_TransactionTypes", "bronze_sql_transaction_types", source=count, target=count)
    return count


def runAllExtracts(spark: SparkSession, batchId: int, fxWindowFrom: str | None = None, fxWindowTo: str | None = None) -> dict:
    results = {
        "EXT_ORA_CodeTranslation": runExtOraCodeTranslation(spark, batchId),
        "EXT_ORA_Currency": runExtOraCurrency(spark, batchId),
        "EXT_ORA_Geography": runExtOraGeography(spark, batchId),
        "EXT_ORA_PaymentTerms": runExtOraPaymentTerms(spark, batchId),
        "EXT_ORA_TaxRate": runExtOraTaxRate(spark, batchId),
        "EXT_ORA_FxRateDaily": runExtOraFxRateDaily(spark, batchId, fxWindowFrom, fxWindowTo),
        "EXT_SQL_Cities": runExtSqlCities(spark, batchId),
        "EXT_SQL_PaymentMethods": runExtSqlPaymentMethods(spark, batchId),
        "EXT_SQL_TransactionTypes": runExtSqlTransactionTypes(spark, batchId),
    }
    return results
