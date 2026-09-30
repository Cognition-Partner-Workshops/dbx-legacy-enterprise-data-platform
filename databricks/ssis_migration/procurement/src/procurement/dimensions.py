"""DIM_Load_Supplier (hybrid Type 1 / Type 2) and DIM_Load_VendorContract (SCD2 amendments).

The dimension is rebuilt as a whole DataFrame from (existing versions, incoming snapshot) which
keeps the logic pure and locally testable; the result is written back with a full overwrite of
the Delta table (the table is small: tens to hundreds of rows)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procurement import io
from procurement.config import HIGH_TS, LEGACY_DW, LOW_TS, SOURCE_SYSTEM_ORACLE, qualified
from procurement.staging import (
    SILVER_SUPPLIER,
    SILVER_VENDOR_CONTRACT,
    SUPPLIER_TYPE1_COLUMNS,
    SUPPLIER_TYPE2_COLUMNS,
)

GOLD_DIM_SUPPLIER = "gold_dim_supplier"
GOLD_DIM_VENDOR_CONTRACT = "gold_dim_vendor_contract"
LEGACY_SEED_SOURCE = "WWI_DW_SEED"

SUPPLIER_DIM_SCHEMA = T.StructType(
    [
        T.StructField("supplier_key", T.IntegerType()),
        T.StructField("wwi_supplier_id", T.LongType()),
        T.StructField("supplier_business_key", T.StringType()),
        T.StructField("source_system_code", T.StringType()),
        T.StructField("supplier_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("primary_contact", T.StringType()),
        T.StructField("supplier_reference", T.StringType()),
        T.StructField("payment_days", T.IntegerType()),
        T.StructField("postal_code", T.StringType()),
        T.StructField("supplier_status_code", T.StringType()),
        T.StructField("payment_terms_code", T.StringType()),
        T.StructField("payment_method_code", T.StringType()),
        T.StructField("transaction_currency_code", T.StringType()),
        T.StructField("region_code", T.StringType()),
        T.StructField("country_code", T.StringType()),
        T.StructField("preferred_supplier_flag", T.StringType()),
        T.StructField("lead_time_days", T.IntegerType()),
        T.StructField("tax_identifier", T.StringType()),
        T.StructField("vat_registration_number", T.StringType()),
        T.StructField("certification_expired_flag", T.StringType()),
        T.StructField("withholding_applies", T.StringType()),
        T.StructField("strategic_tier_code", T.StringType()),
        T.StructField("type2_hash", T.StringType()),
        T.StructField("type1_hash", T.StringType()),
        T.StructField("row_version", T.IntegerType()),
        T.StructField("valid_from", T.TimestampType()),
        T.StructField("valid_to", T.TimestampType()),
        T.StructField("is_current_row", T.BooleanType()),
        T.StructField("lineage_key", T.LongType()),
    ]
)
DIM_COLUMNS = [f.name for f in SUPPLIER_DIM_SCHEMA.fields]


def emptySupplierDimension(spark):
    return spark.createDataFrame([], SUPPLIER_DIM_SCHEMA)


def seedFromLegacySupplierDimension(legacy: DataFrame) -> DataFrame:
    """The existing WideWorldImportersDW.Dimension.Supplier rows (including the -2/-1/0 special
    members) are the starting state of the dimension: keys are preserved so facts that were
    keyed on the legacy host resolve identically."""
    lg = legacy.select([F.col(f"`{c}`").alias(c) for c in legacy.columns])
    return lg.select(
        F.col("Supplier Key").cast("int").alias("supplier_key"),
        F.col("WWI Supplier ID").cast("long").alias("wwi_supplier_id"),
        F.concat(F.lit("WWI:"), F.col("WWI Supplier ID").cast("string")).alias("supplier_business_key"),
        F.lit(LEGACY_SEED_SOURCE).alias("source_system_code"),
        F.col("Supplier").alias("supplier_name"),
        F.col("Category").alias("category"),
        F.col("Primary Contact").alias("primary_contact"),
        F.col("Supplier Reference").alias("supplier_reference"),
        F.col("Payment Days").cast("int").alias("payment_days"),
        F.col("Postal Code").alias("postal_code"),
        F.lit(None).cast("string").alias("supplier_status_code"),
        F.lit(None).cast("string").alias("payment_terms_code"),
        F.lit(None).cast("string").alias("payment_method_code"),
        F.lit(None).cast("string").alias("transaction_currency_code"),
        F.lit(None).cast("string").alias("region_code"),
        F.lit(None).cast("string").alias("country_code"),
        F.lit(None).cast("string").alias("preferred_supplier_flag"),
        F.lit(None).cast("int").alias("lead_time_days"),
        F.lit(None).cast("string").alias("tax_identifier"),
        F.lit(None).cast("string").alias("vat_registration_number"),
        F.lit(None).cast("string").alias("certification_expired_flag"),
        F.lit(None).cast("string").alias("withholding_applies"),
        F.lit(None).cast("string").alias("strategic_tier_code"),
        F.lit(None).cast("string").alias("type2_hash"),
        F.lit(None).cast("string").alias("type1_hash"),
        F.lit(1).alias("row_version"),
        F.col("Valid From").cast("timestamp").alias("valid_from"),
        F.col("Valid To").cast("timestamp").alias("valid_to"),
        (F.col("Valid To").cast("timestamp") >= F.lit(HIGH_TS).cast("timestamp")).alias("is_current_row"),
        F.col("Lineage Key").cast("long").alias("lineage_key"),
    )


def incomingSupplierVersions(silverSupplier: DataFrame) -> DataFrame:
    """The staged snapshot shaped like the dimension (without keys / validity)."""
    s = silverSupplier.where(F.col("is_survivor_row") & (F.coalesce(F.col("dq_status_code"), F.lit("PASS")) != "FAIL"))
    return s.select(
        F.col("source_supplier_id").alias("wwi_supplier_id"),
        "supplier_business_key",
        F.lit(SOURCE_SYSTEM_ORACLE).alias("source_system_code"),
        "supplier_name",
        F.coalesce(F.col("strategic_tier_code"), F.lit("Other")).alias("category"),
        F.lit(None).cast("string").alias("primary_contact"),
        F.col("supplier_business_key").alias("supplier_reference"),
        F.col("payment_days").cast("int").alias("payment_days"),
        F.lit(None).cast("string").alias("postal_code"),
        *[F.col(c) for c in SUPPLIER_TYPE2_COLUMNS if c != "supplier_name"],
        *[F.col(c) for c in SUPPLIER_TYPE1_COLUMNS],
        F.col("change_hash").alias("type2_hash"),
        F.col("type1_hash"),
    )


def applyHybridScd(existing: DataFrame, incoming: DataFrame, batchId, asOf) -> DataFrame:
    """Hybrid SCD (Integration.usp_MigrateStagedSupplierDataV2 + DIM_Load_Supplier split):
    - new business key           -> insert version 1
    - type2_hash changed         -> close current row (valid_to = asOf - 1s), insert version n+1
    - type1_hash changed only    -> overwrite Type 1 attributes on every version of the key
    - unchanged                  -> untouched
    Returns the complete new dimension content."""
    asOfTs = F.lit(asOf).cast("timestamp")
    current = existing.where("is_current_row").select(
        "supplier_business_key",
        F.col("supplier_key").alias("_cur_key"),
        F.col("type2_hash").alias("_cur_t2"),
        F.col("type1_hash").alias("_cur_t1"),
        F.col("row_version").alias("_cur_version"),
    )
    classified = incoming.join(current, "supplier_business_key", "left").withColumn(
        "_change",
        F.when(F.col("_cur_key").isNull(), "NEW")
        .when(F.coalesce(F.col("_cur_t2"), F.lit("")) != F.col("type2_hash"), "TYPE2")
        .when(F.coalesce(F.col("_cur_t1"), F.lit("")) != F.col("type1_hash"), "TYPE1")
        .otherwise("UNCHANGED"),
    )
    maxKey = existing.agg(F.coalesce(F.max("supplier_key"), F.lit(0))).collect()[0][0]
    newVersions = classified.where(F.col("_change").isin("NEW", "TYPE2"))
    keyWindow = Window.orderBy("supplier_business_key")
    inserts = (
        newVersions.withColumn("supplier_key", (F.row_number().over(keyWindow) + F.lit(int(maxKey))).cast("int"))
        .withColumn("row_version", F.coalesce(F.col("_cur_version") + 1, F.lit(1)).cast("int"))
        .withColumn("valid_from", F.when(F.col("_change") == "NEW", F.lit(LOW_TS).cast("timestamp")).otherwise(asOfTs))
        .withColumn("valid_to", F.lit(HIGH_TS).cast("timestamp"))
        .withColumn("is_current_row", F.lit(True))
        .withColumn("lineage_key", F.lit(int(batchId)).cast("long"))
        .select(*DIM_COLUMNS)
    )
    type2Keys = classified.where(F.col("_change") == "TYPE2").select("supplier_business_key").distinct()
    closed = (
        existing.join(type2Keys, "supplier_business_key", "left_semi")
        .where("is_current_row")
        .withColumn("valid_to", asOfTs - F.expr("INTERVAL 1 SECOND"))
        .withColumn("is_current_row", F.lit(False))
    )
    untouched = existing.join(type2Keys, "supplier_business_key", "left_anti").unionByName(
        existing.join(type2Keys, "supplier_business_key", "left_semi").where(~F.col("is_current_row"))
    )
    # Type 1 overwrite is applied to ALL versions of the business key (SSIS: @ApplyType1 = 1)
    type1Values = classified.where(F.col("_change").isin("TYPE1", "TYPE2")).select(
        "supplier_business_key", *[F.col(c).alias(f"_new_{c}") for c in SUPPLIER_TYPE1_COLUMNS], F.col("type1_hash").alias("_new_type1_hash")
    )
    retained = untouched.unionByName(closed).join(type1Values, "supplier_business_key", "left")
    for c in SUPPLIER_TYPE1_COLUMNS:
        retained = retained.withColumn(c, F.coalesce(F.col(f"_new_{c}"), F.col(c))).drop(f"_new_{c}")
    retained = retained.withColumn("type1_hash", F.coalesce(F.col("_new_type1_hash"), F.col("type1_hash"))).drop("_new_type1_hash")
    return retained.select(*DIM_COLUMNS).unionByName(inserts)


def runDimSupplier(spark, batchId, asOf=None):
    packageName = "DIM_Load_Supplier"
    target = qualified(GOLD_DIM_SUPPLIER)
    asOf = asOf or spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    if io.tableExists(spark, target):
        existing = alignToSchema(spark.table(target), SUPPLIER_DIM_SCHEMA)
    else:
        existing = seedFromLegacySupplierDimension(spark.table(f"{LEGACY_DW}.Dimension.Supplier"))
    incoming = incomingSupplierVersions(spark.table(qualified(SILVER_SUPPLIER)))
    before = existing.count()
    io.writeThroughWork(spark, applyHybridScd(existing, incoming, batchId, asOf), target)
    after = spark.table(target).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=incoming.count(), rowsInserted=after - before,
                     message=f"as_of={asOf}")
    return after


# --------------------------------------------------------------------------------------
# DIM_Load_VendorContract  (SCD2 amendment versions)
# --------------------------------------------------------------------------------------
VENDOR_CONTRACT_BUSINESS_COLUMNS = [
    "contract_number", "contract_business_key", "supplier_business_key", "source_supplier_id", "contract_type_code", "source_status_code",
    "region_code", "contract_currency_code", "contract_start_date", "contract_end_date", "auto_renew_flag",
    "notice_period_days", "committed_amount", "committed_amount_usd", "rebate_percent", "price_protection_flag",
    "payment_terms_code", "signed_date", "contract_band_code",
]
VENDOR_CONTRACT_DIM_SCHEMA = T.StructType(
    [T.StructField("vendor_contract_key", T.IntegerType())]
    + [
        T.StructField(c, t)
        for c, t in [
            ("contract_number", T.StringType()), ("contract_business_key", T.LongType()), ("supplier_business_key", T.StringType()), ("source_supplier_id", T.LongType()),
            ("contract_type_code", T.StringType()), ("source_status_code", T.StringType()), ("region_code", T.StringType()),
            ("contract_currency_code", T.StringType()), ("contract_start_date", T.DateType()), ("contract_end_date", T.DateType()),
            ("auto_renew_flag", T.StringType()), ("notice_period_days", T.IntegerType()), ("committed_amount", T.DecimalType(19, 4)),
            ("committed_amount_usd", T.DecimalType(18, 2)), ("rebate_percent", T.DecimalType(9, 4)), ("price_protection_flag", T.StringType()),
            ("payment_terms_code", T.StringType()), ("signed_date", T.DateType()), ("contract_band_code", T.StringType()),
        ]
    ]
    + [
        T.StructField("supplier_key", T.IntegerType()),
        T.StructField("amendment_number", T.IntegerType()),
        T.StructField("fx_collar_lower_rate", T.DecimalType(18, 8)),
        T.StructField("fx_collar_upper_rate", T.DecimalType(18, 8)),
        T.StructField("row_hash_type2", T.StringType()),
        T.StructField("valid_from", T.TimestampType()),
        T.StructField("valid_to", T.TimestampType()),
        T.StructField("is_current_row", T.BooleanType()),
        T.StructField("lineage_key", T.LongType()),
    ]
)
VC_DIM_COLUMNS = [f.name for f in VENDOR_CONTRACT_DIM_SCHEMA.fields]


def alignToSchema(df: DataFrame, schema: T.StructType) -> DataFrame:
    """Existing dimension rows widened to the current dimension schema (new attribute columns are null
    on historical versions)."""
    for f in schema.fields:
        if f.name not in df.columns:
            df = df.withColumn(f.name, F.lit(None).cast(f.dataType))
    return df.select(*[f.name for f in schema.fields])


def emptyVendorContractDimension(spark):
    return spark.createDataFrame([], VENDOR_CONTRACT_DIM_SCHEMA)


def incomingContractVersions(silverContract: DataFrame, dimSupplier: DataFrame) -> DataFrame:
    """Staged contracts shaped like the dimension. Duplicate amendments (same contract, same hash,
    several rows in a batch) collapse to one; the supplier key is the current dimension row for the
    supplier (unknown member 0 when absent - late-arriving supplier)."""
    supplierKeys = dimSupplier.where("is_current_row").select("supplier_business_key", F.col("supplier_key").alias("_sk"))
    c = silverContract.join(supplierKeys, "supplier_business_key", "left")
    dedupWindow = Window.partitionBy("contract_number", "row_hash").orderBy(F.col("source_modified_date").desc_nulls_last())
    c = c.withColumn("_rn", F.row_number().over(dedupWindow)).where("_rn = 1").drop("_rn")
    return c.select(
        *[F.col(x).cast(VENDOR_CONTRACT_DIM_SCHEMA[x].dataType) for x in VENDOR_CONTRACT_BUSINESS_COLUMNS],
        F.coalesce(F.col("_sk"), F.lit(0)).cast("int").alias("supplier_key"),
        # FX collar only applies when the contract currency differs from the supplier settlement currency
        F.when(F.col("contract_currency_code") == F.col("supplier_currency_code"), F.lit(None).cast("decimal(18,8)"))
        .otherwise(F.lit(0.95).cast("decimal(18,8)")).alias("fx_collar_lower_rate"),
        F.when(F.col("contract_currency_code") == F.col("supplier_currency_code"), F.lit(None).cast("decimal(18,8)"))
        .otherwise(F.lit(1.05).cast("decimal(18,8)")).alias("fx_collar_upper_rate"),
        F.col("row_hash").alias("row_hash_type2"),
    )


def applyScd2(existing: DataFrame, incoming: DataFrame, batchId, asOf) -> DataFrame:
    """Plain SCD2 on contract_number keyed by row_hash_type2. Every change creates a new
    amendment version and closes the previous one."""
    asOfTs = F.lit(asOf).cast("timestamp")
    current = existing.where("is_current_row").select(
        "contract_number", F.col("vendor_contract_key").alias("_cur_key"), F.col("row_hash_type2").alias("_cur_hash"),
        F.col("amendment_number").alias("_cur_amendment"),
    )
    classified = incoming.join(current, "contract_number", "left").withColumn(
        "_change",
        F.when(F.col("_cur_key").isNull(), "NEW")
        .when(F.col("_cur_hash") != F.col("row_hash_type2"), "TYPE2")
        .otherwise("UNCHANGED"),
    )
    maxKey = existing.agg(F.coalesce(F.max("vendor_contract_key"), F.lit(0))).collect()[0][0]
    changed = classified.where(F.col("_change").isin("NEW", "TYPE2"))
    inserts = (
        changed.withColumn("vendor_contract_key", (F.row_number().over(Window.orderBy("contract_number")) + F.lit(int(maxKey))).cast("int"))
        .withColumn("amendment_number", F.coalesce(F.col("_cur_amendment") + 1, F.lit(1)).cast("int"))
        .withColumn("valid_from", F.when(F.col("_change") == "NEW", F.lit(LOW_TS).cast("timestamp")).otherwise(asOfTs))
        .withColumn("valid_to", F.lit(HIGH_TS).cast("timestamp"))
        .withColumn("is_current_row", F.lit(True))
        .withColumn("lineage_key", F.lit(int(batchId)).cast("long"))
        .select(*VC_DIM_COLUMNS)
    )
    changedKeys = classified.where(F.col("_change") == "TYPE2").select("contract_number").distinct()
    closed = (
        existing.join(changedKeys, "contract_number", "left_semi")
        .where("is_current_row")
        .withColumn("valid_to", asOfTs - F.expr("INTERVAL 1 SECOND"))
        .withColumn("is_current_row", F.lit(False))
    )
    untouched = existing.join(changedKeys, "contract_number", "left_anti").unionByName(
        existing.join(changedKeys, "contract_number", "left_semi").where(~F.col("is_current_row"))
    )
    return untouched.unionByName(closed).select(*VC_DIM_COLUMNS).unionByName(inserts)


def runDimVendorContract(spark, batchId, asOf=None):
    packageName = "DIM_Load_VendorContract"
    target = qualified(GOLD_DIM_VENDOR_CONTRACT)
    asOf = asOf or spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS ts").collect()[0]["ts"]
    existing = alignToSchema(spark.table(target), VENDOR_CONTRACT_DIM_SCHEMA) if io.tableExists(spark, target) else emptyVendorContractDimension(spark)
    incoming = incomingContractVersions(spark.table(qualified(SILVER_VENDOR_CONTRACT)), spark.table(qualified(GOLD_DIM_SUPPLIER)))
    before = existing.count()
    io.writeThroughWork(spark, applyScd2(existing, incoming, batchId, asOf), target)
    after = spark.table(target).count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=incoming.count(), rowsInserted=after - before, message=f"as_of={asOf}")
    return after
