"""ING_FILE_SupplierCatalog: pipe-delimited supplier price lists (HDR/DTL/TRL records) read with
`read_files`/CSV over the Unity Catalog volume `landing`, validated row by row, reconciled against
the TRL footer (row count + price checksum) and landed as bronze_file_supplier_catalog.

Files whose footer does not reconcile are quarantined (recorded in ctl_landing_file with status
Quarantined and their rows are NOT landed), mirroring the SSIS Foreach loop outcome."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from procurement import io
from procurement.config import LANDING_VOLUME, SOURCE_SYSTEM_FILE, qualified, volumePath

BRONZE_CATALOG = "bronze_file_supplier_catalog"
LANDING_FILE_TABLE = "ctl_landing_file"
REJECTED_FILE_ROW = "err_rejected_file_row"
FEED_SUBDIR = "inbound/supplier"

RAW_COLUMNS = [
    "RecordType", "SupplierCode", "SupplierItemCode", "ManufacturerPartNumber", "ItemDescription", "UomCode",
    "PackSizeText", "ListPriceText", "NetPriceText", "CurrencyCode", "MinimumOrderQuantityText",
    "LeadTimeDaysText", "EffectiveFromText", "EffectiveToText", "HazardClassCode",
]

RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in RAW_COLUMNS])


def readCatalogFiles(spark, path) -> DataFrame:
    """Auto Loader/`read_files` equivalent in batch mode: every *.psv under the landing path."""
    df = (
        spark.read.format("csv")
        .schema(RAW_SCHEMA)
        .option("sep", "|")
        .option("header", "false")
        .option("encoding", "ISO-8859-1")
        .option("mode", "PERMISSIVE")
        .load(f"{path}/*.psv")
        .select("*", F.col("_metadata.file_path").alias("source_file_path"))
    )
    return df.withColumn("source_file_name", F.element_at(F.split(F.col("source_file_path"), "/"), -1))


def parseDetailRows(raw: DataFrame) -> DataFrame:
    """Derived-column component: typed values + validity flag for DTL rows."""
    dtl = raw.where(F.col("RecordType") == "DTL")
    toDate = lambda c: F.when(  # noqa: E731
        F.length(F.trim(F.col(c))) == 8,
        F.to_date(F.trim(F.col(c)), "yyyyMMdd"),
    )
    typed = (
        dtl.withColumn("effective_from_date", toDate("EffectiveFromText"))
        .withColumn("effective_to_date", toDate("EffectiveToText"))
        .withColumn("list_price", F.col("ListPriceText").cast("decimal(19,4)"))
        .withColumn("net_price", F.col("NetPriceText").cast("decimal(19,4)"))
        .withColumn("pack_size", F.col("PackSizeText").cast("decimal(18,3)"))
        .withColumn("minimum_order_quantity", F.col("MinimumOrderQuantityText").cast("decimal(18,3)"))
        .withColumn("lead_time_days", F.col("LeadTimeDaysText").cast("int"))
        .withColumn("hazardous_flag", F.when(F.length(F.trim(F.coalesce(F.col("HazardClassCode"), F.lit("")))) > 0, "Y").otherwise("N"))
    )
    valid = (
        (F.length(F.trim(F.coalesce(F.col("SupplierItemCode"), F.lit("")))) > 0)
        & (F.col("net_price") >= 0)
        & (F.col("list_price") >= F.col("net_price"))
        & (F.length(F.trim(F.coalesce(F.col("EffectiveFromText"), F.lit("")))) == 8)
        & F.col("effective_from_date").isNotNull()
    )
    return typed.withColumn("is_valid", F.coalesce(valid, F.lit(False))).withColumn(
        "reject_reason_code",
        F.when(F.length(F.trim(F.coalesce(F.col("SupplierItemCode"), F.lit("")))) == 0, "MISSING_ITEM_CODE")
        .when(F.col("net_price").isNull() | (F.col("net_price") < 0), "INVALID_NET_PRICE")
        .when(F.col("list_price").isNull() | (F.col("list_price") < F.col("net_price")), "LIST_BELOW_NET")
        .when(F.col("effective_from_date").isNull(), "INVALID_EFFECTIVE_FROM")
        .otherwise(F.lit(None).cast("string")),
    )


def reconcileFooter(raw: DataFrame, detail: DataFrame) -> DataFrame:
    """Per-file reconciliation: landed valid rows vs TRL row count, and
    SUM(CAST(NetPrice*100 AS bigint)) % 1000000 vs the TRL checksum."""
    footer = raw.where(F.col("RecordType") == "TRL").select(
        "source_file_name",
        F.col("SupplierCode").cast("long").alias("footer_row_count"),
        F.col("SupplierItemCode").cast("long").alias("footer_checksum"),
    )
    landed = detail.groupBy("source_file_name").agg(
        F.sum(F.when(F.col("is_valid"), 1).otherwise(0)).alias("landed_row_count"),
        F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("malformed_row_count"),
        (F.coalesce(F.sum(F.when(F.col("is_valid"), (F.col("net_price") * 100).cast("long"))), F.lit(0)) % 1000000).alias("price_checksum"),
    )
    files = raw.select("source_file_name", "source_file_path").distinct()
    result = files.join(landed, "source_file_name", "left").join(footer, "source_file_name", "left")
    return result.withColumn(
        "file_status",
        F.when(F.col("footer_row_count").isNull(), "Quarantined")
        .when(F.col("footer_row_count") != F.coalesce(F.col("landed_row_count"), F.lit(0)), "Quarantined")
        .when(F.col("footer_checksum") != F.coalesce(F.col("price_checksum"), F.lit(0)), "Quarantined")
        .otherwise("Processed"),
    ).withColumn(
        "status_reason",
        F.when(F.col("footer_row_count").isNull(), "MISSING_TRL")
        .when(F.col("footer_row_count") != F.coalesce(F.col("landed_row_count"), F.lit(0)), "ROW_COUNT_MISMATCH")
        .when(F.col("footer_checksum") != F.coalesce(F.col("price_checksum"), F.lit(0)), "CHECKSUM_MISMATCH")
        .otherwise(F.lit(None).cast("string")),
    )


def transformSupplierCatalog(raw: DataFrame, batchId):
    """Returns (landedRows, rejectedRows, fileStatus)."""
    detail = parseDetailRows(raw)
    files = reconcileFooter(raw, detail)
    processedFiles = files.where(F.col("file_status") == "Processed").select("source_file_name")
    landed = detail.join(processedFiles, "source_file_name", "inner").where(F.col("is_valid")).select(
        F.col("SupplierCode").alias("supplier_code"),
        F.col("SupplierItemCode").alias("supplier_item_code"),
        F.col("ManufacturerPartNumber").alias("manufacturer_part_number"),
        F.col("ItemDescription").alias("supplier_item_desc"),
        F.col("UomCode").alias("pack_uom"),
        "pack_size", "list_price", "net_price",
        F.col("CurrencyCode").alias("currency_code"),
        "minimum_order_quantity", "lead_time_days", "effective_from_date", "effective_to_date",
        F.col("HazardClassCode").alias("hazard_class_code"), "hazardous_flag",
        "source_file_name",
    ).withColumn("source_system_code", F.lit(SOURCE_SYSTEM_FILE)).withColumn("batch_id", F.lit(int(batchId)).cast("long")).withColumn("loaded_at_utc", F.current_timestamp())
    rejected = detail.where(~F.col("is_valid")).select(
        "source_file_name", "reject_reason_code", *[F.col(c) for c in RAW_COLUMNS if c != "RecordType"]
    ).withColumn("batch_id", F.lit(int(batchId)).cast("long")).withColumn("rejected_at", F.current_timestamp())
    return landed, rejected, files.withColumn("batch_id", F.lit(int(batchId)).cast("long")).withColumn("processed_at", F.current_timestamp())


def runSupplierCatalog(spark, batchId):
    packageName = "ING_FILE_SupplierCatalog"
    path = f"{volumePath(LANDING_VOLUME)}/{FEED_SUBDIR}"
    raw = readCatalogFiles(spark, path)
    alreadyProcessed = None
    fileTable = qualified(LANDING_FILE_TABLE)
    if io.tableExists(spark, fileTable):
        alreadyProcessed = spark.table(fileTable).where(F.col("file_status") == "Processed").select("source_file_name").distinct()
        raw = raw.join(alreadyProcessed, "source_file_name", "left_anti")
    landed, rejected, files = transformSupplierCatalog(raw, batchId)
    landedCount = landed.count()
    rejectedCount = rejected.count()
    target = qualified(BRONZE_CATALOG)
    if io.tableExists(spark, target):
        io.appendDelta(landed, target)
    else:
        io.writeDelta(landed, target)
    if rejectedCount:
        io.appendDelta(rejected, qualified(REJECTED_FILE_ROW))
    if io.tableExists(spark, fileTable):
        io.appendDelta(files, fileTable)
    else:
        io.writeDelta(files, fileTable)
    quarantined = files.where(F.col("file_status") == "Quarantined").count()
    io.logPackageRun(spark, batchId, packageName, "Succeeded", rowsRead=landedCount + rejectedCount, rowsInserted=landedCount,
                     rowsRejected=rejectedCount, message=f"quarantined_files={quarantined}")
    return landedCount
