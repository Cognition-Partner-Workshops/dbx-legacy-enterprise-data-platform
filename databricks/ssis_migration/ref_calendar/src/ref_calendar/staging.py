"""STG_Load_Currency, STG_Load_Geography and STG_Load_TaxAndTerms: truncate/reload of the stg.* layer.

Each package truncates its stg tables, conforms the raw landing rows through derived columns, lookups and
conditional splits, and routes failures to err.* tables (modelled here as err_rejected_* Delta tables).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ref_calendar import config
from ref_calendar.common import cleanString, nullIfBlank, rowHash, trimUpper, writeTable
from ref_calendar.reference import translateCode

REJECT_COLUMNS = ["batch_id", "package_name", "object_name", "business_key", "reject_reason_code", "reject_reason", "record_payload"]


def rejectRows(df: DataFrame, batchId: int, packageName: str, objectName: str, keyCol, reasonCode: str, reason: str, payloadCols: list[str]) -> DataFrame:
    return df.select(
        F.lit(batchId).cast("bigint").alias("batch_id"), F.lit(packageName).alias("package_name"), F.lit(objectName).alias("object_name"),
        keyCol.cast("string").alias("business_key"), F.lit(reasonCode).alias("reject_reason_code"), F.lit(reason).alias("reject_reason"),
        F.concat_ws("|", *[F.col(c).cast("string") for c in payloadCols]).alias("record_payload"),
    )


# ----------------------------------------------------------------------------- STG_Load_Currency

def conformCurrency(rawCurrency: DataFrame) -> tuple[DataFrame, DataFrame]:
    """DFT Conform Currency: cleanse, default minor units to 2 and active flag to Y; a code that is not three
    characters long is an invalid currency."""
    df = rawCurrency.select(
        trimUpper("ccy_code").alias("currency_code"),
        F.trim("ccy_name").alias("currency_name"),
        F.coalesce(F.col("minor_units").cast("int"), F.lit(2)).alias("minor_units"),
        trimUpper("region_cd").alias("region_code"),
        F.coalesce(F.upper(F.trim("active_flg")), F.lit("Y")).alias("is_active_flag"),
        "batch_id", "source_row_number",
    )
    valid = F.length("currency_code") == 3
    return df.where(valid), df.where(~valid | F.col("currency_code").isNull())


def conformFxRate(rawFx: DataFrame, fxOverrides: DataFrame | None = None) -> DataFrame:
    """DFT Conform FX Rate: union the Oracle window with the approved treasury overrides, normalise, sort and
    de-duplicate on (from, to, effective from) with the override winning, then triangulate through USD."""
    fx = rawFx.where(F.col("rate") > 0).select(
        F.col("from_currency_cd").alias("from_ccy"), F.col("to_currency_cd").alias("to_ccy"),
        F.col("rate_dt").cast("date").alias("eff_from_dt"), F.lit(None).cast("date").alias("eff_to_dt"),
        F.col("rate").cast("decimal(18,8)").alias("rate"), F.col("rate_type_cd").alias("rate_type_cd"),
        F.col("rate_source_cd").alias("src_system_cd"), F.lit(0).alias("priority"),
    )
    if fxOverrides is not None:
        fx = fx.unionByName(
            fxOverrides.select(
                F.col("from_currency_code").alias("from_ccy"), F.col("to_currency_code").alias("to_ccy"),
                F.col("rate_date").cast("date").alias("eff_from_dt"), F.lit(None).cast("date").alias("eff_to_dt"),
                F.col("override_rate").cast("decimal(18,8)").alias("rate"), F.lit("OVERRIDE").alias("rate_type_cd"),
                F.lit("FILE_FX").alias("src_system_cd"), F.lit(1).alias("priority"),
            )
        )
    normalised = fx.select(
        trimUpper("from_ccy").alias("from_currency_code"),
        trimUpper("to_ccy").alias("to_currency_code"),
        F.coalesce(trimUpper("rate_type_cd"), F.lit("SPOT")).alias("rate_type_code"),
        F.col("eff_from_dt").alias("effective_from_date"),
        F.coalesce(F.col("eff_to_dt"), F.to_date(F.lit(config.HIGH_DATE))).alias("effective_to_date"),
        F.col("rate").alias("exchange_rate"),
        F.when(F.col("rate") == 0, F.lit(0)).otherwise(F.lit(1) / F.col("rate")).cast("decimal(18,8)").alias("inverse_rate"),
        F.coalesce(trimUpper("src_system_cd"), F.lit("ORA_ERP")).alias("rate_source_code"),
        "priority",
    )
    dedupWindow = Window.partitionBy("from_currency_code", "to_currency_code", "effective_from_date").orderBy(F.col("priority").desc(), F.col("rate_type_code"))
    deduped = normalised.withColumn("rn", F.row_number().over(dedupWindow)).where("rn = 1").drop("rn", "priority")
    usdCross = deduped.where((F.col("rate_type_code") == "SPOT") & (F.col("to_currency_code") == "USD")).groupBy(
        F.col("from_currency_code").alias("cross_currency_code")
    ).agg(F.max_by("exchange_rate", "effective_from_date").alias("usd_cross_rate"))
    return (
        deduped.join(usdCross, deduped.to_currency_code == usdCross.cross_currency_code, "left")
        .withColumn("usd_equivalent_rate", F.when(F.col("usd_cross_rate").isNull(), F.col("exchange_rate")).otherwise(F.col("exchange_rate") * F.col("usd_cross_rate")).cast("decimal(18,8)"))
        .drop("cross_currency_code")
    )


def runStgLoadCurrency(spark: SparkSession, batchId: int) -> dict:
    overridesTable = config.tbl("silver_fx_override_approved")
    overrides = spark.table(overridesTable) if spark.catalog.tableExists(overridesTable) else None
    currency, invalid = conformCurrency(spark.table(config.tbl("bronze_oracle_currency")))
    fx = conformFxRate(spark.table(config.tbl("bronze_oracle_fx_rate")), overrides)
    writeTable(currency.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_currency"))
    writeTable(fx.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_fx_rate"))
    rejects = rejectRows(invalid, batchId, "STG_Load_Currency", "stg.Currency", F.col("currency_code"), "INVALID_CURRENCY", "Currency code is not three characters.", ["currency_code", "currency_name"])
    writeTable(rejects, config.tbl("err_rejected_constraint_violation"), mode="append")
    return {name: spark.table(config.tbl(name)).count() for name in ("silver_stg_currency", "silver_stg_fx_rate")}


# ----------------------------------------------------------------------------- STG_Load_Geography

def conformGeography(rawGeography: DataFrame, salesTerritory: DataFrame | None = None) -> tuple[DataFrame, DataFrame]:
    """DFT Conform Geography + stg.usp_TruncateAndReload_Geography: cleanse, attach the sales territory, reject
    implausible coordinates, then collapse to one row per (country, state, city) keeping the most complete row."""
    df = rawGeography.select(
        F.concat_ws("-", trimUpper("country_cd"), F.col("geography_id").cast("string")).alias("geography_code"),
        cleanString(F.col("city_name")).alias("city_name"),
        F.coalesce(trimUpper("state_province_cd"), F.lit("")).alias("state_province_code"),
        F.coalesce(F.trim("state_province_name"), F.lit("")).alias("state_province_name"),
        trimUpper("country_cd").alias("country_code"),
        F.coalesce(trimUpper("region_cd"), F.lit("NA")).alias("region_code"),
        F.coalesce(trimUpper("sales_territory"), F.lit("UNASSIGNED")).alias("sales_territory_code"),
        F.coalesce(F.col("latitude").cast("decimal(9,6)"), F.lit(0)).cast("decimal(9,6)").alias("latitude"),
        F.coalesce(F.col("longitude").cast("decimal(9,6)"), F.lit(0)).cast("decimal(9,6)").alias("longitude"),
        F.coalesce(F.col("population_num").cast("bigint"), F.lit(0)).alias("population_count"),
        nullIfBlank(F.col("postal_cd")).alias("postal_code"),
        nullIfBlank(F.col("timezone_name")).alias("time_zone_name"),
        nullIfBlank(F.col("tax_jurisdiction_cd")).alias("tax_jurisdiction_code"),
        "record_kind", "geography_id", "source_row_number",
    ).withColumn("city_state_key", F.concat_ws("|", F.upper("city_name"), F.col("state_province_code")))
    df = df.withColumn("change_hash", rowHash("city_name", "state_province_code", "country_code", "sales_territory_code", "population_count"))
    if salesTerritory is not None:
        st = salesTerritory.select(F.col("SalesTerritoryCode").alias("_st_code"), F.col("SalesTerritoryName").alias("sales_territory_name"), F.col("RegionCode").alias("territory_region_code"))
        df = df.join(st, df.sales_territory_code == st._st_code, "left").drop("_st_code")
    else:
        df = df.withColumn("sales_territory_name", F.lit(None).cast("string")).withColumn("territory_region_code", F.lit(None).cast("string"))
    plausible = F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
    outOfRange = df.where(~plausible)
    completeness = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in ["postal_code", "time_zone_name", "tax_jurisdiction_code", "state_province_name"])
    dedupWindow = Window.partitionBy("country_code", "state_province_code", F.upper("city_name")).orderBy(completeness.desc(), F.col("population_count").desc(), F.col("source_row_number").desc())
    conformed = df.where(plausible & F.col("city_name").isNotNull()).withColumn("rn", F.row_number().over(dedupWindow)).where("rn = 1").drop("rn")
    return conformed, outOfRange


def runStgLoadGeography(spark: SparkSession, batchId: int) -> dict:
    territoryTable = f"{config.LEGACY_STAGING}.stg.SalesTerritory"
    territory = spark.table(territoryTable) if spark.catalog.tableExists(territoryTable) else None
    conformed, outOfRange = conformGeography(spark.table(config.tbl("bronze_oracle_geography")), territory)
    writeTable(conformed.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_geography"))
    rejects = rejectRows(outOfRange, batchId, "STG_Load_Geography", "stg.Geography", F.col("geography_code"), "OUT_OF_RANGE", "Latitude or longitude outside the valid range.", ["geography_code", "latitude", "longitude"])
    writeTable(rejects, config.tbl("err_rejected_constraint_violation"), mode="append")
    return {"silver_stg_geography": spark.table(config.tbl("silver_stg_geography")).count()}


# ----------------------------------------------------------------------------- STG_Load_TaxAndTerms

def conformTaxRate(rawTax: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Route tax by region: NA sales tax 0 <= r < 20 (never recoverable), EU VAT 0 < r <= 27 (always
    recoverable), APAC GST 0 < r <= 15; anything else is an implausible rate."""
    region = F.coalesce(trimUpper("region_cd"), F.lit("NA"))
    df = rawTax.select(
        trimUpper("tax_code").alias("tax_code"),
        region.alias("region_code"),
        F.when(region == "EU", "VAT").when(region == "APAC", "GST").otherwise("SALESTAX").alias("tax_type_code"),
        F.coalesce(trimUpper("jurisdiction_cd"), trimUpper("country_cd"), F.lit("UNKNOWN")).alias("jurisdiction_code"),
        trimUpper("country_cd").alias("country_code"),
        F.coalesce(F.col("rate_pct").cast("decimal(9,4)"), F.lit(0)).cast("decimal(9,4)").alias("rate_percent"),
        F.when(region == "EU", F.lit("Y")).otherwise(F.coalesce(F.upper(F.trim("recoverable_flg")), F.lit("N"))).alias("is_recoverable_flag"),
        F.col("eff_from_dt").alias("effective_from_date"),
        F.coalesce(F.col("eff_to_dt"), F.to_date(F.lit(config.HIGH_DATE))).alias("effective_to_date"),
        "tax_rate_id", "tax_class_cd", "compound_flg", "reverse_charge_flg",
    )
    r = F.col("rate_percent")
    plausible = (
        ((F.col("region_code") == "NA") & (r >= 0) & (r < 20))
        | ((F.col("region_code") == "EU") & (r > 0) & (r <= 27))
        | ((F.col("region_code") == "APAC") & (r > 0) & (r <= 15))
    )
    return df.where(plausible), df.where(~plausible)


def conformPaymentTerms(rawTerms: DataFrame, crosswalk: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cleanse the terms and translate the legacy code through the PAYMENT_TERMS crosswalk (full-cache lookup,
    no-match rows are redirected to err.RejectedLookupFailure)."""
    df = rawTerms.select(
        F.upper(F.regexp_replace(F.trim("terms_code"), " ", "")).alias("payment_terms_code"),
        F.coalesce(F.trim("terms_desc"), F.lit("")).alias("payment_terms_description"),
        F.when(F.col("net_days").isNull() | (F.col("net_days") < 0), F.lit(30)).otherwise(F.col("net_days")).cast("int").alias("net_days"),
        F.coalesce(F.col("disc_pct").cast("decimal(9,4)"), F.lit(0)).cast("decimal(9,4)").alias("discount_percent"),
        F.coalesce(F.col("disc_days").cast("int"), F.lit(0)).alias("discount_days"),
        F.coalesce(trimUpper("region_cd"), F.lit("NA")).alias("region_code"),
        F.lit(True).alias("is_current"),
        "calculation_basis_cd", "day_of_month_due", "active_flg",
    )
    mapping = crosswalk.where("code_domain_code = 'PAYMENT_TERMS' AND is_active").select(
        F.col("source_code_value").alias("_src"), F.col("conformed_code_value").alias("conformed_terms_code")
    ).dropDuplicates(["_src"])
    joined = df.join(mapping, df.payment_terms_code == mapping._src, "left").drop("_src")
    return joined.where(F.col("conformed_terms_code").isNotNull()), joined.where(F.col("conformed_terms_code").isNull())


def runStgLoadTaxAndTerms(spark: SparkSession, batchId: int) -> dict:
    crosswalk = spark.table(config.tbl("silver_ref_code_crosswalk"))
    tax, implausible = conformTaxRate(spark.table(config.tbl("bronze_oracle_tax_rate")))
    terms, unmapped = conformPaymentTerms(spark.table(config.tbl("bronze_oracle_payment_terms")), crosswalk)
    terms = translateCode(terms, crosswalk, "PAYMENT_TERMS", config.SOURCE_SYSTEM_ORACLE, "payment_terms_code", "region_code", "translated_terms_code")
    writeTable(tax.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_tax_rate"))
    writeTable(terms.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_payment_terms"))
    writeTable(rejectRows(implausible, batchId, "STG_Load_TaxAndTerms", "stg.TaxRate", F.col("tax_code"), "IMPLAUSIBLE_RATE", "Tax rate outside the regional plausible band.", ["tax_code", "region_code", "rate_percent"]), config.tbl("err_rejected_constraint_violation"), mode="append")
    writeTable(rejectRows(unmapped, batchId, "STG_Load_TaxAndTerms", "stg.PaymentTerms", F.col("payment_terms_code"), "LOOKUP_MISS", "Payment terms code has no PAYMENT_TERMS crosswalk mapping.", ["payment_terms_code", "region_code", "net_days"]), config.tbl("err_rejected_lookup_failure"), mode="append")
    return {name: spark.table(config.tbl(name)).count() for name in ("silver_stg_tax_rate", "silver_stg_payment_terms")}
