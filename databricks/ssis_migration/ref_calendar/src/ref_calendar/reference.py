"""The ref.* conformed reference layer (silver_ref_*) that the REF_Load_* and DIM_Load_City packages read.

Mirrors sqlserver/reference/ref.usp_Load*.sql: steward grids (region, postal rules, crosswalk, status and
reason codes) and the source-driven currency, FX and tax-jurisdiction conformance procedures.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config, seeds
from ref_calendar.common import nullIfBlank, safeDate, trimUpper, writeTable

LOW_DATE = "1900-01-01"


# ----------------------------------------------------------------------------- steward grids

def buildRefRegion(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(seeds.REGION_GRID).withColumn("is_active", F.lit(True))


def buildRefPostalFormatRule(spark: SparkSession) -> DataFrame:
    schema = T.StructType([
        T.StructField("country_code", T.StringType()), T.StructField("rule_set_code", T.StringType()),
        T.StructField("rule_priority", T.IntegerType()), T.StructField("strip_characters", T.StringType()),
        T.StructField("upper_case_flag", T.IntegerType()), T.StructField("format_mask", T.StringType()),
        T.StructField("min_length", T.IntegerType()), T.StructField("max_length", T.IntegerType()),
        T.StructField("truncate_to_length", T.IntegerType()), T.StructField("insert_space_after", T.IntegerType()),
        T.StructField("insert_character", T.StringType()), T.StructField("rule_note", T.StringType()),
    ])
    rows = [tuple(r[f.name] for f in schema.fields) for r in seeds.POSTAL_FORMAT_RULES]
    return spark.createDataFrame(rows, schema).withColumn("is_active", F.lit(True))


def buildRefCodeCrosswalk(spark: SparkSession) -> DataFrame:
    """First-load semantics of ref.usp_LoadCodeCrosswalk: every grid mapping open from 1900-01-01."""
    schema = T.StructType([
        T.StructField("code_domain_code", T.StringType()), T.StructField("source_system_code", T.StringType()),
        T.StructField("source_code_value", T.StringType()), T.StructField("source_code_description", T.StringType()),
        T.StructField("conformed_code_value", T.StringType()), T.StructField("region_code", T.StringType()),
        T.StructField("is_default_for_conformed", T.IntegerType()),
    ])
    rows = [tuple(r[f.name] for f in schema.fields) for r in seeds.CODE_CROSSWALK]
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("effective_from_date", F.to_date(F.lit(LOW_DATE)))
        .withColumn("effective_to_date", F.lit(None).cast("date"))
        .withColumn("is_active", F.lit(True))
        .withColumn("maintained_by", F.lit("ref.usp_LoadCodeCrosswalk steward grid"))
    )


def buildRefStatusCode(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(seeds.STATUS_CODES)
    unknown = df.select("status_domain_code").distinct().select(
        "status_domain_code", F.lit("UNKNOWN").alias("conformed_status_code"), F.lit("Unknown").alias("conformed_status_name"),
        F.lit("UNKNOWN").alias("status_group_code"), F.lit(0).alias("sort_order"), F.lit(0).alias("is_terminal"),
    )
    return df.unionByName(unknown).withColumn("is_active", F.lit(True))


def buildRefReasonCode(spark: SparkSession) -> DataFrame:
    df = spark.createDataFrame(seeds.REASON_CODES)
    unknown = df.select("reason_domain_code").distinct().select(
        "reason_domain_code", F.lit("UNKNOWN").alias("conformed_reason_code"), F.lit("Unknown").alias("conformed_reason_name"),
        F.lit("EXCEPTION").alias("reason_group_code"), F.lit(0).alias("is_customer_fault"), F.lit(0).alias("is_supplier_fault"),
        F.lit(0).alias("requires_approval"),
    )
    return df.unionByName(unknown, allowMissingColumns=True).dropDuplicates(["reason_domain_code", "conformed_reason_code"]).withColumn("is_active", F.lit(True))


# ----------------------------------------------------------------------------- source-driven ref tables

def buildRefCountry(rawGeography: DataFrame, crosswalk: DataFrame) -> DataFrame:
    """ref.usp_LoadCountry: one row per ISO alpha-2 country from the Oracle geography landing, resolving legacy
    aliases through the COUNTRY crosswalk domain and keeping the most recently updated row per code."""
    alias = crosswalk.where("code_domain_code = 'COUNTRY' AND is_active").select(
        F.col("source_code_value").alias("alias_code"), F.col("conformed_code_value").alias("resolved_code")
    )
    g = rawGeography.where("record_kind = 'ORAGEO'").withColumn("country_cd_clean", trimUpper("country_cd"))
    g = g.join(alias, g.country_cd_clean == alias.alias_code, "left").withColumn(
        "country_code", F.coalesce(F.col("resolved_code"), F.col("country_cd_clean"))
    )
    ranked = g.withColumn(
        "rn", F.row_number().over(Window.partitionBy("country_code").orderBy(safeDate(F.col("last_update_dt")).desc_nulls_last(), F.col("source_row_number").desc()))
    ).where("rn = 1")
    return ranked.select(
        F.col("country_code"),
        trimUpper("iso3_cd").alias("country_code_iso3"),
        F.trim("country_name").alias("country_name"),
        F.coalesce(trimUpper("region_cd"), F.lit("NA")).alias("region_code"),
        F.trim("sub_region_name").alias("sub_region_name"),
        trimUpper("currency_cd").alias("currency_code"),
        (F.upper(F.trim("eu_member_flag")) == "Y").alias("is_eu_member"),
        (F.upper(F.trim("eu_vat_area_flg")) == "Y").alias("is_eu_vat_area"),
        (F.upper(F.trim("sanctioned_flg")) == "Y").alias("is_sanctioned"),
        (F.coalesce(F.upper(F.trim("postal_required_flg")), F.lit("Y")) == "Y").alias("postal_code_required_flag"),
        (F.coalesce(F.upper(F.trim("state_prov_required_flg")), F.lit("N")) == "Y").alias("state_province_required_flag"),
        F.col("postal_format_mask"),
        F.lit(True).alias("is_active"),
    )


def buildRefCurrency(rawCurrency: DataFrame) -> DataFrame:
    """ref.usp_LoadCurrency: normalise, default minor units, keep the euro legacy set as superseded rows."""
    code = trimUpper("ccy_code")
    minorUnits = F.col("minor_units").cast("int")
    euroLegacy = F.upper(F.trim("euro_legacy_flg")) == "Y"
    ranked = rawCurrency.withColumn("currency_code", code).withColumn(
        "rn", F.row_number().over(Window.partitionBy("currency_code").orderBy(safeDate(F.col("last_update_dt")).desc_nulls_last(), F.col("source_row_number").desc()))
    ).where("rn = 1").where(F.length("currency_code") == 3)
    return ranked.select(
        "currency_code",
        F.trim("ccy_name").alias("currency_name"),
        nullIfBlank(F.col("ccy_symbol")).alias("currency_symbol"),
        nullIfBlank(F.col("iso_numeric_cd")).alias("currency_numeric_code"),
        F.when(minorUnits.between(0, 4), minorUnits).otherwise(F.lit(2)).alias("minor_unit_digits"),
        F.coalesce(trimUpper("rounding_rule_cd"), F.lit("HALFUP")).alias("rounding_rule_code"),
        trimUpper("primary_country_cd").alias("primary_country_code"),
        F.coalesce(trimUpper("region_cd"), F.lit("GLOBAL")).alias("region_code"),
        euroLegacy.alias("is_historical"),
        F.when(euroLegacy, F.lit("EUR")).alias("superseded_by_currency_code"),
        F.when(euroLegacy, safeDate(F.col("euro_conversion_dt"))).alias("superseded_on"),
        F.when(euroLegacy, F.col("legacy_fixed_rate").cast("decimal(18,8)")).alias("fixed_conversion_rate"),
        (F.upper(F.trim("trading_allowed_flg")) == "Y").alias("is_transactional_currency"),
        code.isin("USD", "EUR", "AUD").alias("is_reporting_currency"),
        ((F.coalesce(F.upper(F.trim("active_flg")), F.lit("Y")) == "Y") & F.col("retired_dt").isNull()).alias("is_active"),
        safeDate(F.col("retired_dt")).alias("retired_on"),
        F.col("source_system_code"),
    )


def buildRefFxRateDaily(rawFx: DataFrame, approvedOverrides: DataFrame | None = None, inverseTolerance: float = 0.0001) -> DataFrame:
    """ref.usp_LoadFxRateDaily: normalise codes, drop non-positive rates, de-duplicate one rate per
    (pair, date, type) keeping the highest conversion rate, validate/repair the inverse, let treasury
    overrides win over the feed and fill forward gaps of up to FX_FILL_FORWARD_DAYS days."""
    fx = rawFx.select(
        trimUpper("from_currency_cd").alias("from_currency_code"),
        trimUpper("to_currency_cd").alias("to_currency_code"),
        F.col("rate_dt").cast("date").alias("rate_date"),
        F.coalesce(trimUpper("rate_type_cd"), F.lit("SPOT")).alias("rate_type_code"),
        F.col("rate").cast("decimal(18,8)").alias("conversion_rate"),
        F.col("inverse_rate").cast("decimal(18,8)").alias("inverse_rate"),
        F.coalesce(trimUpper("rate_source_cd"), F.lit("ORA_ERP")).alias("rate_source_code"),
        F.lit(0).alias("source_priority"),
    ).where(F.col("conversion_rate") > 0)
    if approvedOverrides is not None:
        overrides = approvedOverrides.select(
            trimUpper("from_currency_code").alias("from_currency_code"),
            trimUpper("to_currency_code").alias("to_currency_code"),
            F.col("rate_date").cast("date").alias("rate_date"),
            F.lit("SPOT").alias("rate_type_code"),
            F.col("override_rate").cast("decimal(18,8)").alias("conversion_rate"),
            F.lit(None).cast("decimal(18,8)").alias("inverse_rate"),
            F.lit("TREASURY_OVERRIDE").alias("rate_source_code"),
            F.lit(1).alias("source_priority"),
        ).where(F.col("conversion_rate") > 0)
        fx = fx.unionByName(overrides)
    keyWindow = Window.partitionBy("from_currency_code", "to_currency_code", "rate_date", "rate_type_code").orderBy(
        F.col("source_priority").desc(), F.col("conversion_rate").desc()
    )
    deduped = fx.withColumn("rn", F.row_number().over(keyWindow)).where("rn = 1").drop("rn", "source_priority")
    computedInverse = (F.lit(1) / F.col("conversion_rate")).cast("decimal(18,8)")
    inverseOk = F.col("inverse_rate").isNotNull() & (F.abs(F.col("conversion_rate") * F.col("inverse_rate") - 1) <= inverseTolerance)
    validated = deduped.withColumn("inverse_rate", F.when(inverseOk, F.col("inverse_rate")).otherwise(computedInverse)).withColumn(
        "is_fill_forward", F.lit(False)
    )
    pairWindow = Window.partitionBy("from_currency_code", "to_currency_code", "rate_type_code").orderBy("rate_date")
    gaps = validated.withColumn("next_rate_date", F.lead("rate_date").over(pairWindow)).withColumn(
        "gap_days", F.datediff("next_rate_date", "rate_date")
    ).where((F.col("gap_days") > 1) & (F.col("gap_days") <= config.FX_FILL_FORWARD_DAYS + 1))
    filled = gaps.withColumn(
        "rate_date", F.explode(F.expr("sequence(date_add(rate_date, 1), date_sub(next_rate_date, 1), interval 1 day)"))
    ).withColumn("is_fill_forward", F.lit(True)).drop("next_rate_date", "gap_days")
    return validated.unionByName(filled)


def buildRefTaxJurisdiction(rawTaxRate: DataFrame) -> DataFrame:
    """ref.usp_LoadTaxJurisdiction: one row per jurisdiction with its current standard rate; NA jurisdictions
    stack components (state + county + city), EU is country VAT, APAC is national/state GST."""
    t = rawTaxRate.withColumn("jurisdiction_code", trimUpper("jurisdiction_cd")).withColumn(
        "region_code", F.coalesce(trimUpper("region_cd"), F.lit("NA"))
    )
    currentRates = t.where(F.col("eff_to_dt").isNull() | (F.col("eff_to_dt") >= F.current_date()))
    rateWindow = Window.partitionBy("jurisdiction_code").orderBy(F.col("eff_from_dt").desc_nulls_last(), F.col("tax_rate_id").desc())
    latest = currentRates.withColumn("rn", F.row_number().over(rateWindow)).where("rn = 1")
    stacked = currentRates.groupBy("jurisdiction_code").agg(F.sum("rate_pct").alias("stacked_rate_pct"), F.count("*").alias("component_count"))
    return latest.join(stacked, "jurisdiction_code").select(
        "jurisdiction_code",
        F.trim("jurisdiction_name").alias("jurisdiction_name"),
        "region_code",
        trimUpper("country_cd").alias("country_code"),
        trimUpper("state_province_cd").alias("state_province_code"),
        F.trim("county_txt").alias("county_or_district_name"),
        F.upper(F.trim("city_txt")).alias("city_name"),
        F.coalesce(trimUpper("jurisdiction_level_cd"), F.lit("COUNTRY")).alias("jurisdiction_level_code"),
        F.when(F.col("region_code") == "EU", "VAT").when(F.col("region_code") == "APAC", "GST").otherwise("SALESTAX").alias("tax_regime_code"),
        F.col("rate_pct").cast("decimal(9,4)").alias("standard_rate_pct"),
        F.when(F.col("region_code") == "NA", F.col("stacked_rate_pct")).otherwise(F.col("rate_pct")).cast("decimal(9,4)").alias("effective_rate_pct"),
        F.col("component_count"),
        trimUpper("parent_jurisdiction_cd").alias("parent_jurisdiction_code"),
        (F.upper(F.trim("stacks_with_parent_flg")) == "Y").alias("stacks_with_parent"),
        F.when(F.col("region_code") == "EU", F.lit(True)).otherwise(F.col("recoverable_pct") > 0).alias("is_recoverable"),
        (F.upper(F.trim("reverse_charge_flg")) == "Y").alias("reverse_charge_eligible"),
        F.lit(True).alias("is_active"),
    )


def translateCode(df: DataFrame, crosswalk: DataFrame, domain: str, sourceSystemCode: str, sourceCol: str, regionCol: str | None, outputCol: str) -> DataFrame:
    """WWI_REF.FN_TRANSLATE_CODE: region-specific active mapping, then the region-agnostic ('ALL') mapping,
    then pass the source code through unchanged; NULL in gives NULL out."""
    xw = crosswalk.where((F.col("code_domain_code") == domain) & (F.col("source_system_code") == sourceSystemCode) & F.col("is_active"))
    regional = xw.where(F.col("region_code").isNotNull()).select(
        F.col("source_code_value").alias("_src_r"), F.col("region_code").alias("_reg_r"), F.col("conformed_code_value").alias("_conf_r")
    )
    global_ = xw.where(F.col("region_code").isNull()).select(F.col("source_code_value").alias("_src_g"), F.col("conformed_code_value").alias("_conf_g"))
    src = F.upper(F.trim(F.col(sourceCol)))
    out = df.join(global_, src == F.col("_src_g"), "left")
    if regionCol is not None:
        out = out.join(regional, (src == F.col("_src_r")) & (F.col(regionCol) == F.col("_reg_r")), "left")
    else:
        out = out.withColumn("_conf_r", F.lit(None).cast("string")).withColumn("_src_r", F.lit(None)).withColumn("_reg_r", F.lit(None))
    return out.withColumn(outputCol, F.when(F.col(sourceCol).isNull(), F.lit(None)).otherwise(F.coalesce("_conf_r", "_conf_g", src))).drop(
        "_src_r", "_reg_r", "_conf_r", "_src_g", "_conf_g"
    )


def runReferenceLayer(spark: SparkSession, batchId: int) -> dict:
    crosswalk = buildRefCodeCrosswalk(spark)
    rawGeography = spark.table(config.tbl("bronze_oracle_geography"))
    overridesTable = config.tbl("silver_fx_override_approved")
    overrides = spark.table(overridesTable) if spark.catalog.tableExists(overridesTable) else None
    outputs = {
        "silver_ref_region": buildRefRegion(spark),
        "silver_ref_postal_format_rule": buildRefPostalFormatRule(spark),
        "silver_ref_code_crosswalk": crosswalk,
        "silver_ref_status_code": buildRefStatusCode(spark),
        "silver_ref_reason_code": buildRefReasonCode(spark),
        "silver_ref_country": buildRefCountry(rawGeography, crosswalk),
        "silver_ref_currency": buildRefCurrency(spark.table(config.tbl("bronze_oracle_currency"))),
        "silver_ref_fx_rate_daily": buildRefFxRateDaily(spark.table(config.tbl("bronze_oracle_fx_rate")), overrides),
        "silver_ref_tax_jurisdiction": buildRefTaxJurisdiction(spark.table(config.tbl("bronze_oracle_tax_rate"))),
    }
    counts = {}
    for name, df in outputs.items():
        writeTable(df.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn("loaded_at_utc", F.current_timestamp()), config.tbl(name))
        counts[name] = spark.table(config.tbl(name)).count()
    return counts


