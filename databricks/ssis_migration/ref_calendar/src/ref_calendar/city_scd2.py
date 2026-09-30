"""DIM_Load_City: stg.City -> Dimension.City as a Type 2 slowly changing dimension.

Mirrors stg.usp_ConformCityForDimension (conformance + de-duplication) and
Integration.usp_MigrateStagedCityData (hash-driven SCD2 with inferred-member promotion and reserved keys).
"""
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config
from ref_calendar.common import cleanString, rowHash, trimUpper, writeTable

HIGH_TS = "9999-12-31 23:59:59.999"
TYPE2_COLUMNS = [
    "city", "state_province", "country_code", "sales_territory_code", "latest_recorded_population",
    "postcode_standardized", "county_fips_code", "nuts_level_3_code", "prefecture_or_province", "tax_jurisdiction_code",
]
ATTRIBUTE_COLUMNS = [
    "city", "state_province", "country", "continent", "sales_territory", "region", "subregion", "latest_recorded_population",
    "region_code", "country_code", "sales_territory_code", "postcode_standardized", "county_fips_code", "nuts_level_3_code",
    "prefecture_or_province", "tax_jurisdiction_code", "postal_format_code", "time_zone_name", "source_system_code", "row_hash_type_2",
]
CITY_SCHEMA = T.StructType(
    [T.StructField("city_key", T.IntegerType()), T.StructField("wwi_city_id", T.IntegerType())]
    + [T.StructField(c, T.LongType() if c == "latest_recorded_population" else T.StringType()) for c in ATTRIBUTE_COLUMNS]
    + [
        T.StructField("valid_from", T.TimestampType()), T.StructField("valid_to", T.TimestampType()),
        T.StructField("effective_from", T.TimestampType()), T.StructField("effective_to", T.TimestampType()),
        T.StructField("is_current_row", T.BooleanType()), T.StructField("version_number", T.IntegerType()),
        T.StructField("is_inferred_member", T.BooleanType()), T.StructField("lineage_key", T.LongType()),
        T.StructField("last_load_batch_id", T.LongType()),
    ]
)


def conformCityForDimension(bronzeGeography: DataFrame, refCountry: DataFrame) -> DataFrame:
    """stg.usp_ConformCityForDimension over the OLTPCITY rows: one row per (city id, version), cleansed,
    de-duplicated on (country, state, city, valid from) keeping the most recently updated / most populous row,
    enriched with the conformed country/region and the Type 2 hash."""
    g = bronzeGeography.where("record_kind = 'OLTPCITY'").where(cleanString(F.col("city_name")).isNotNull())
    country = refCountry.select(
        F.col("country_code_iso3").alias("_iso3"), F.col("country_code").alias("conformed_country_code"), F.col("region_code").alias("conformed_region_code")
    ).dropDuplicates(["_iso3"])
    j = g.join(country, trimUpper("iso3_cd") == country._iso3, "left")
    dedupWindow = Window.partitionBy(
        trimUpper("country_cd"), F.upper(F.coalesce(F.trim("state_province_cd"), F.lit(""))), F.upper(F.trim("city_name")), F.col("valid_from")
    ).orderBy(F.col("population_num").cast("bigint").desc_nulls_last(), F.col("source_row_number").desc())
    ranked = j.withColumn("rn", F.row_number().over(dedupWindow)).where("rn = 1")
    staged = ranked.select(
        F.col("geography_id").cast("int").alias("wwi_city_id"),
        cleanString(F.col("city_name")).alias("city"),
        F.coalesce(cleanString(F.col("state_province_name")), F.lit("N/A")).alias("state_province"),
        F.coalesce(cleanString(F.col("country_name")), F.lit("N/A")).alias("country"),
        F.coalesce(cleanString(F.col("continent")), F.lit("N/A")).alias("continent"),
        F.coalesce(cleanString(F.col("sales_territory")), F.lit("N/A")).alias("sales_territory"),
        F.coalesce(cleanString(F.col("region_name")), F.lit("N/A")).alias("region"),
        F.coalesce(cleanString(F.col("sub_region_name")), F.lit("N/A")).alias("subregion"),
        F.coalesce(F.col("population_num").cast("bigint"), F.lit(0)).alias("latest_recorded_population"),
        F.coalesce(F.col("conformed_region_code"), trimUpper("region_cd"), F.lit("ROW")).alias("region_code"),
        F.coalesce(F.col("conformed_country_code"), trimUpper("iso3_cd")).alias("country_code"),
        F.upper(F.regexp_replace(F.coalesce(F.col("sales_territory"), F.lit("UNASSIGNED")), r"[^A-Za-z0-9]", "")).alias("sales_territory_code"),
        F.lit(None).cast("string").alias("postcode_standardized"),
        F.lit(None).cast("string").alias("county_fips_code"),
        F.lit(None).cast("string").alias("nuts_level_3_code"),
        F.when(F.coalesce(F.col("conformed_region_code"), trimUpper("region_cd")) == "APAC", cleanString(F.col("state_province_name"))).alias("prefecture_or_province"),
        F.col("tax_jurisdiction_cd").alias("tax_jurisdiction_code"),
        F.col("postal_format_code"),
        F.col("timezone_name").alias("time_zone_name"),
        F.lit(config.SOURCE_SYSTEM_OLTP).alias("source_system_code"),
        F.col("valid_from").cast("timestamp").alias("valid_from"),
        F.coalesce(F.col("valid_to").cast("timestamp"), F.to_timestamp(F.lit(HIGH_TS))).alias("valid_to"),
    )
    return staged.withColumn("row_hash_type_2", rowHash(*TYPE2_COLUMNS))


def reservedCityRows(spark: SparkSession) -> DataFrame:
    def row(key, label, other, validFrom):
        return {
            "city_key": key, "wwi_city_id": key, "city": label, "state_province": other, "country": other, "continent": other,
            "sales_territory": other, "region": other, "subregion": other, "latest_recorded_population": 0, "region_code": "GLOBAL",
            "country_code": None, "sales_territory_code": None, "postcode_standardized": None, "county_fips_code": None,
            "nuts_level_3_code": None, "prefecture_or_province": None, "tax_jurisdiction_code": None, "postal_format_code": None,
            "time_zone_name": None, "source_system_code": "SYSTEM", "row_hash_type_2": None,
            "valid_from": validFrom, "valid_to": datetime.fromisoformat("9999-12-31 23:59:59.999"),
            "effective_from": validFrom, "effective_to": datetime.fromisoformat("9999-12-31 23:59:59.999"),
            "is_current_row": True, "version_number": 1, "is_inferred_member": False, "lineage_key": 0, "last_load_batch_id": 0,
        }

    low, epoch = datetime(1900, 1, 1), datetime(2013, 1, 1)
    rows = [row(-2, "Not Applicable", "N/A", low), row(-1, "Unknown", "Unknown", low), row(0, "Unknown", "N/A", epoch)]
    return spark.createDataFrame([tuple(r[f.name] for f in CITY_SCHEMA.fields) for r in rows], CITY_SCHEMA)


def _asDimensionRows(df: DataFrame, batchId: int, isInferred: bool = False) -> DataFrame:
    return df.select(
        F.col("city_key").cast("int"), F.col("wwi_city_id").cast("int"), *ATTRIBUTE_COLUMNS,
        "valid_from", "valid_to", F.col("valid_from").alias("effective_from"), F.col("valid_to").alias("effective_to"),
        (F.col("valid_to") >= F.to_timestamp(F.lit("9999-01-01"))).alias("is_current_row"),
        F.lit(1).alias("version_number"), F.lit(isInferred).alias("is_inferred_member"),
        F.lit(batchId).cast("bigint").alias("lineage_key"), F.lit(batchId).cast("bigint").alias("last_load_batch_id"),
    )


def applyCityScd2(spark: SparkSession, existing: DataFrame | None, staged: DataFrame, batchId: int, now: datetime | None = None) -> DataFrame:
    """Integration.usp_MigrateStagedCityData:
    * reserved keys -2/-1/0 are always present;
    * historical source versions not yet in the dimension are back-filled as closed rows;
    * a city with no current row is inserted; an inferred member is promoted in place (same key);
    * a current row whose Type 2 hash differs is closed at the source change timestamp (or now) and a new
      version is inserted; unchanged rows are left alone.
    Keys are allocated from max(city_key) + 1 in (valid_from, wwi_city_id) order; version numbers are the
    rank of each row inside its city."""
    nowTs = F.lit(now or datetime.utcnow()).cast("timestamp")
    highTs = F.to_timestamp(F.lit("9999-01-01"))
    if existing is None:
        existing = reservedCityRows(spark)
    reserved = existing.where("city_key <= 0")
    if reserved.count() == 0:
        reserved = reservedCityRows(spark)
    positive = existing.where("city_key > 0")
    current = positive.where("is_current_row")

    latestWindow = Window.partitionBy("wwi_city_id").orderBy(F.col("valid_from").desc())
    curStaged = staged.where(F.col("valid_to") >= highTs).withColumn("rn", F.row_number().over(latestWindow)).where("rn = 1").drop("rn")
    histStaged = staged.where(F.col("valid_to") < highTs)
    newHist = histStaged.join(positive.select("wwi_city_id", "valid_from"), ["wwi_city_id", "valid_from"], "left_anti")

    s, d = curStaged.alias("s"), current.alias("d")
    j = s.join(d, F.col("s.wwi_city_id") == F.col("d.wwi_city_id"), "left")
    isNew = F.col("d.city_key").isNull()
    isPromote = ~isNew & F.col("d.is_inferred_member")
    isChanged = ~isNew & ~F.col("d.is_inferred_member") & (F.col("d.row_hash_type_2") != F.col("s.row_hash_type_2"))

    stagedCols = [F.col(f"s.{c}").alias(c) for c in ["wwi_city_id", *ATTRIBUTE_COLUMNS, "valid_from", "valid_to"]]
    newRows = j.where(isNew).select(*stagedCols)
    closeTs = F.when(F.col("s.valid_from") > F.col("d.valid_from"), F.col("s.valid_from")).otherwise(nowTs)
    changedNew = j.where(isChanged).select(*[F.col(f"s.{c}").alias(c) for c in ["wwi_city_id", *ATTRIBUTE_COLUMNS, "valid_to"]], closeTs.alias("valid_from"))
    touchedKeys = j.where(isChanged | isPromote).select(F.col("d.city_key").alias("city_key"))
    closed = j.where(isChanged).select(
        *[F.col(f"d.{c}").alias(c) for c in existing.columns if c not in ("valid_to", "effective_to", "is_current_row", "last_load_batch_id")],
        closeTs.alias("valid_to"), closeTs.alias("effective_to"), F.lit(False).alias("is_current_row"), F.lit(batchId).cast("bigint").alias("last_load_batch_id"),
    )
    promoted = j.where(isPromote).select(
        F.col("d.city_key").alias("city_key"), F.col("s.wwi_city_id").alias("wwi_city_id"), *[F.col(f"s.{c}").alias(c) for c in ATTRIBUTE_COLUMNS],
        F.col("d.valid_from").alias("valid_from"), F.col("d.valid_to").alias("valid_to"), F.col("d.effective_from").alias("effective_from"), F.col("d.effective_to").alias("effective_to"),
        F.lit(True).alias("is_current_row"), F.col("d.version_number").alias("version_number"), F.lit(False).alias("is_inferred_member"),
        F.col("d.lineage_key").alias("lineage_key"), F.lit(batchId).cast("bigint").alias("last_load_batch_id"),
    )
    untouched = positive.join(touchedKeys, "city_key", "left_anti")

    inserts = newRows.unionByName(changedNew).unionByName(newHist.select(*[F.col(c) for c in ["wwi_city_id", *ATTRIBUTE_COLUMNS, "valid_from", "valid_to"]]))
    maxKey = existing.agg(F.coalesce(F.max("city_key"), F.lit(0))).first()[0]
    inserts = inserts.withColumn("city_key", F.lit(maxKey) + F.row_number().over(Window.orderBy("valid_from", "wwi_city_id")))
    insertRows = _asDimensionRows(inserts, batchId)

    result = untouched.select(*existing.columns).unionByName(closed.select(*existing.columns)).unionByName(promoted.select(*existing.columns)).unionByName(insertRows.select(*existing.columns))
    versionWindow = Window.partitionBy("wwi_city_id").orderBy("valid_from", "city_key")
    result = result.withColumn("version_number", F.row_number().over(versionWindow).cast("int"))
    return reserved.select(*existing.columns).unionByName(result).orderBy("city_key")


def insertInferredCityMembers(spark: SparkSession, existing: DataFrame, cityIds: list[int], batchId: int, now: datetime | None = None) -> DataFrame:
    """Integration.usp_InsertInferredMember for City: a fact arrives for a city id the dimension has not seen,
    so a placeholder row is created with the reserved 'Unknown' attributes and Is Inferred Member = 1; the
    next DIM_Load_City run promotes it in place."""
    known = {r[0] for r in existing.select("wwi_city_id").distinct().collect()}
    missing = sorted(set(cityIds) - known)
    if not missing:
        return existing
    nowTs = now or datetime.utcnow()
    maxKey = existing.agg(F.coalesce(F.max("city_key"), F.lit(0))).first()[0]
    rows = []
    for i, cityId in enumerate(missing, start=1):
        rows.append({
            "city_key": maxKey + i, "wwi_city_id": cityId, "city": "Unknown", "state_province": "Unknown", "country": "Unknown", "continent": "Unknown",
            "sales_territory": "Unknown", "region": "Unknown", "subregion": "Unknown", "latest_recorded_population": 0, "region_code": "GLOBAL",
            "country_code": None, "sales_territory_code": None, "postcode_standardized": None, "county_fips_code": None, "nuts_level_3_code": None,
            "prefecture_or_province": None, "tax_jurisdiction_code": None, "postal_format_code": None, "time_zone_name": None,
            "source_system_code": "INFERRED", "row_hash_type_2": None, "valid_from": nowTs, "valid_to": datetime.fromisoformat("9999-12-31 23:59:59.999"),
            "effective_from": nowTs, "effective_to": datetime.fromisoformat("9999-12-31 23:59:59.999"), "is_current_row": True, "version_number": 1,
            "is_inferred_member": True, "lineage_key": batchId, "last_load_batch_id": batchId,
        })
    inferred = spark.createDataFrame([tuple(r[f.name] for f in CITY_SCHEMA.fields) for r in rows], CITY_SCHEMA)
    return existing.unionByName(inferred)


def runDimLoadCity(spark: SparkSession, batchId: int) -> dict:
    staged = conformCityForDimension(spark.table(config.tbl("bronze_oracle_geography")), spark.table(config.tbl("silver_ref_country")))
    writeTable(staged.withColumn("batch_id", F.lit(batchId).cast("bigint")), config.tbl("silver_stg_city"))
    target = config.tbl("gold_dim_city")
    existing = spark.table(target).select(*[f.name for f in CITY_SCHEMA.fields]) if spark.catalog.tableExists(target) else None
    result = applyCityScd2(spark, existing, spark.table(config.tbl("silver_stg_city")), batchId)
    writeTable(result, target)
    out = spark.table(target)
    return {"gold_dim_city": out.count(), "current_rows": out.where("is_current_row AND city_key > 0").count()}
