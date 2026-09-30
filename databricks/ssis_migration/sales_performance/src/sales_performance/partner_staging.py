"""STG_Load_PartnerSale: raw partner file rows -> silver_partner_sale (incremental append)."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_performance import config
from sales_performance.common import readOltp, readStaging, readTableOrEmpty, saveTable, withAudit
from sales_performance.partner_files import RAW_TABLE

PACKAGE = "STG_Load_PartnerSale"
STAGE_TABLE = "silver_partner_sale"
REJECT_TABLE = "silver_partner_sale_reject"
WATERMARK_TABLE = "etl_watermark"
COUNTRY_TABLE = "silver_ref_country"
CROSSWALK_TABLE = "silver_partner_customer_crosswalk"
PARTNER_CUSTOMER_CODE_SET = "PARTNER_CUSTOMER"


def normalizePartnerText(df: DataFrame) -> DataFrame:
    """The 'Normalize Partner Text' + 'Type Partner Measures' components."""
    saleDate = F.trim(F.col("sale_date_text"))
    isoDate = F.when(
        F.instr(saleDate, "/") > 0,
        F.when(
            F.substring(saleDate, 3, 1) == "/",
            F.concat(F.substring(saleDate, 7, 4), F.lit("-"), F.substring(saleDate, 4, 2), F.lit("-"), F.substring(saleDate, 1, 2)),
        ).otherwise(F.regexp_replace(F.substring(saleDate, 1, 10), "/", "-")),
    ).otherwise(F.substring(saleDate, 1, 10))
    amountText = F.regexp_replace(F.regexp_replace(F.trim(F.col("amount_text")), "[,$€£]", ""), " ", "")
    return (
        df.withColumn("partner_code", F.upper(F.trim(F.col("partner_code"))))
        .withColumn("partner_order_ref", F.upper(F.trim(F.col("partner_order_ref"))))
        .withColumn("customer_ref", F.upper(F.trim(F.col("customer_ref"))))
        .withColumn("item_ref", F.upper(F.regexp_replace(F.trim(F.col("item_ref")), " ", "")))
        .withColumn("sale_date_iso", isoDate)
        .withColumn("sale_date", F.coalesce(F.col("transaction_date"), F.to_date(F.col("sale_date_iso"), "yyyy-MM-dd")))
        .withColumn(
            "quantity_typed", F.coalesce(F.col("quantity"), F.regexp_replace(F.trim(F.col("quantity_text")), ",", "").cast("decimal(18,4)"))
        )
        .withColumn("gross_amount_typed", F.coalesce(F.col("gross_amount"), amountText.cast("decimal(19,4)")))
        .withColumn(
            "partner_currency_code",
            F.upper(F.substring(F.coalesce(F.nullif(F.trim(F.col("currency_text")), F.lit("")), F.lit("USD")), 1, 3)),
        )
        .withColumn("country_name", F.upper(F.trim(F.col("country_text"))))
    )


def builtinCountryReference(spark: SparkSession) -> DataFrame:
    rows = [(name, iso2, region, iso3) for iso2, (name, region, iso3) in config.COUNTRY_MAP.items()]
    return spark.createDataFrame(rows, "country_name string, country_code string, region_code string, country_code_iso3 string")


def loadCountryReference(spark: SparkSession) -> DataFrame:
    """ref.Country (legacy) unioned with the built-in map; legacy rows win on conflicts."""
    builtin = builtinCountryReference(spark)
    try:
        legacy = readStaging(spark, "ref", "Country").select(
            F.upper(F.col("CountryName")).alias("country_name"),
            F.col("CountryCode").alias("country_code"),
            F.col("RegionCode").alias("region_code"),
            F.col("CountryCodeIso3").alias("country_code_iso3"),
        )
        return legacy.unionByName(builtin.join(legacy, "country_name", "left_anti"))
    except Exception:
        return builtin


def loadPartnerCustomerCrosswalk(spark: SparkSession, seed: DataFrame = None) -> DataFrame:
    """ref.CodeCrosswalk rows for code set PARTNER_CUSTOMER plus the committed seed crosswalk."""
    frames = []
    try:
        legacy = readStaging(spark, "ref", "CodeCrosswalk")
        frames.append(
            legacy.filter(F.col("CodeDomainCode") == PARTNER_CUSTOMER_CODE_SET).select(
                F.upper(F.col("SourceCodeValue")).alias("customer_ref"),
                F.col("ConformedCodeValue").alias("customer_code"),
            )
        )
    except Exception:
        pass
    if seed is not None:
        frames.append(seed.select(F.upper(F.col("customer_ref")).alias("customer_ref"), F.col("customer_code")))
    if not frames:
        return spark.createDataFrame([], "customer_ref string, customer_code string")
    out = frames[0]
    for other in frames[1:]:
        out = out.unionByName(other.join(out, "customer_ref", "left_anti"))
    return out.dropDuplicates(["customer_ref"])


def stagePartnerSales(raw: DataFrame, countryRef: DataFrame, crosswalk: DataFrame, batchId: int):
    """Pure transformation: returns (stagedRows, rejectedRows)."""
    normalized = normalizePartnerText(raw)
    withCountry = normalized.join(
        countryRef.select("country_name", F.col("country_code").alias("ref_country_code"), F.col("region_code").alias("ref_region_code")),
        "country_name",
        "left",
    )
    withCustomer = withCountry.join(crosswalk, "customer_ref", "left")
    screened = withCustomer.withColumn(
        "dq_status_code",
        F.when(F.col("ref_country_code").isNull(), "UNKNOWN_COUNTRY")
        .when(F.col("customer_code").isNull(), "UNKNOWN_CUSTOMER")
        .when(F.col("sale_date").isNull() | F.col("quantity_typed").isNull() | F.col("gross_amount_typed").isNull(), "CONVERSION_ERROR")
        .when(F.col("gross_amount_typed") <= 0, "UNPARSABLE_AMOUNT")
        .when(F.col("quantity_typed") <= 0, "UNPARSABLE_AMOUNT")
        .when(F.length(F.col("partner_order_ref")) == 0, "MISSING_ORDER_REFERENCE")
        .otherwise("VALID"),
    )
    staged = screened.filter(F.col("dq_status_code") == "VALID").select(
        F.concat_ws("|", F.col("partner_code"), F.col("partner_order_ref"), F.col("item_ref")).alias("partner_sale_business_key"),
        F.lit(config.SOURCE_SYSTEM_PARTNER).alias("source_system_code"),
        F.col("partner_code"),
        F.col("partner_outlet_code"),
        F.date_format(F.col("sale_date"), "yyyy-MM").alias("reporting_period_code"),
        F.col("partner_order_ref").alias("transaction_reference"),
        F.col("sale_date").alias("transaction_date"),
        F.col("item_ref").alias("partner_product_code"),
        F.col("item_ref").alias("stock_item_business_key"),
        F.when(F.col("barcode").isNotNull() & (F.length(F.col("barcode")) > 0), "BARCODE")
        .otherwise("PARTNERCODE")
        .alias("product_match_method_code"),
        F.col("barcode"),
        F.col("customer_ref").alias("partner_customer_ref"),
        F.col("customer_code"),
        F.col("quantity_typed").alias("quantity_sold"),
        F.lit("EA").alias("uom_code"),
        F.col("quantity_typed").alias("quantity_base_uom"),
        F.col("gross_amount_typed").alias("gross_amount"),
        F.lit(0).cast("decimal(19,4)").alias("discount_amount"),
        F.coalesce(F.col("tax_amount"), F.lit(0)).cast("decimal(19,4)").alias("tax_amount"),
        F.coalesce(F.col("net_amount"), F.col("gross_amount_typed") - F.coalesce(F.col("tax_amount"), F.lit(0)))
        .cast("decimal(19,4)")
        .alias("net_amount"),
        F.col("tax_treatment_code"),
        F.col("partner_currency_code").alias("transaction_currency_code"),
        F.col("ref_country_code").alias("country_code"),
        F.col("ref_region_code").alias("region_code"),
        F.lit("PARTNER").alias("sales_channel_code"),
        F.col("marketable_flag"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.lit("VALID").alias("dq_status_code"),
    )
    staged = staged.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "|",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit(""))
                    for c in (
                        "partner_sale_business_key",
                        "transaction_date",
                        "quantity_sold",
                        "gross_amount",
                        "transaction_currency_code",
                        "country_code",
                    )
                ],
            ),
            256,
        ),
    )
    rejected = screened.filter(F.col("dq_status_code") != "VALID").select(
        F.lit(PACKAGE).alias("package_name"),
        F.concat_ws("|", F.col("partner_code"), F.col("partner_order_ref"), F.col("item_ref")).alias("business_key"),
        F.col("dq_status_code").alias("reject_reason_code"),
        F.col("source_file_name"),
        F.col("source_row_number"),
        F.col("partner_code"),
        F.col("customer_ref"),
        F.col("country_name"),
        F.col("amount_text"),
        F.col("quantity_text"),
        F.col("sale_date_text"),
    )
    return withAudit(staged, PACKAGE, batchId), withAudit(rejected, PACKAGE, batchId)


def normalizeCustomerNames(staged: DataFrame, customers: DataFrame) -> DataFrame:
    """stg.usp_NormalizeCustomer equivalent: attach the WWI customer for a resolved customer code."""
    return staged.join(
        customers.select(
            F.col("CustomerID").cast("string").alias("customer_code"),
            F.upper(F.trim(F.col("CustomerName"))).alias("customer_name_normalized"),
        ),
        "customer_code",
        "left",
    )


def readWatermark(spark: SparkSession, unit: str) -> int:
    wm = readTableOrEmpty(spark, WATERMARK_TABLE, "unit string, watermark_value bigint, updated_at_utc timestamp")
    rows = wm.filter(F.col("unit") == unit).agg(F.max("watermark_value")).collect()
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


def writeWatermark(spark: SparkSession, unit: str, value: int):
    current = readTableOrEmpty(spark, WATERMARK_TABLE, "unit string, watermark_value bigint, updated_at_utc timestamp")
    updated = current.filter(F.col("unit") != unit).unionByName(
        spark.createDataFrame([(unit, int(value))], "unit string, watermark_value bigint").withColumn(
            "updated_at_utc", F.current_timestamp()
        )
    )
    saveTable(updated, WATERMARK_TABLE)


def runStaging(spark: SparkSession, batchId: int, seedCrosswalk: DataFrame = None):
    raw = spark.table(config.tableName(RAW_TABLE))
    lastBatch = readWatermark(spark, PACKAGE)
    pending = raw.filter(F.col("batch_id") > lastBatch)
    maxBatch = pending.agg(F.max("batch_id")).collect()[0][0]
    countryRef = loadCountryReference(spark)
    crosswalk = loadPartnerCustomerCrosswalk(spark, seedCrosswalk)
    saveTable(withAudit(countryRef, PACKAGE, batchId), COUNTRY_TABLE)
    saveTable(withAudit(crosswalk, PACKAGE, batchId), CROSSWALK_TABLE)
    staged, rejected = stagePartnerSales(pending, countryRef, crosswalk, batchId)
    staged = normalizeCustomerNames(staged, readOltp(spark, "Sales", "Customers"))
    stagedCount = staged.count()
    rejectedCount = rejected.count()
    saveTable(staged, STAGE_TABLE, mode="append")
    if rejectedCount:
        saveTable(rejected, REJECT_TABLE, mode="append")
    if maxBatch is not None:
        writeWatermark(spark, PACKAGE, int(maxBatch))
    return {"rows_read": pending.count(), "rows_inserted": stagedCount, "rows_rejected": rejectedCount, "watermark": maxBatch}
