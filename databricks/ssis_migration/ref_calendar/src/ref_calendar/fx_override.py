"""ING_FILE_FxOverride: treasury FX override CSV feed -> raw.FileFxOverride (bronze_file_fx_override).

Files land in the Unity Catalog volume <schema>.landing/inbound/treasury as fx_override_{yyyyMMdd}_{seq3}.csv
(Windows-1252, comma, header). Detail rows carry RecordType = FXO, the last row is the control record
(RecordType = CTL) whose count and hash total must reconcile to the detail rows or the file is quarantined.
"""
import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ref_calendar import config
from ref_calendar.common import writeTable, yesNo

FEED_COLUMNS = [
    "RecordType", "RateDateText", "FromCurrencyCode", "ToCurrencyCode", "OverrideRateText", "OverrideReasonCode",
    "RequestedByUser", "ApprovedByUser", "ApprovalTicketNumber", "SourceDeskCode", "Comment",
]
FEED_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in FEED_COLUMNS])
INBOUND = "inbound/treasury"
ARCHIVE = "archive/treasury"
QUARANTINE = "quarantine/treasury"


def readFeed(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("csv").option("header", "true").option("encoding", "windows-1252").option("mode", "PERMISSIVE")
        .schema(FEED_SCHEMA).load(path).withColumn("source_file_name", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )


def parseFeed(raw: DataFrame, publishedRates: DataFrame) -> dict[str, DataFrame]:
    """Data flow of the package: split detail/control, parse, derive the pair and four-eyes flag, look up the
    published SPOT rate, compute the deviation and validate the override."""
    detail = raw.where(F.col("RecordType") == "FXO")
    control = raw.where(F.col("RecordType") != "FXO")
    parsed = detail.select(
        "source_file_name",
        F.to_date(F.col("RateDateText"), "yyyy-MM-dd").alias("rate_date"),
        F.upper(F.trim("FromCurrencyCode")).alias("from_currency_code"),
        F.upper(F.trim("ToCurrencyCode")).alias("to_currency_code"),
        F.col("OverrideRateText").cast("decimal(18,8)").alias("override_rate"),
        F.col("OverrideReasonCode").alias("override_reason_code"),
        F.col("RequestedByUser").alias("requested_by_user"),
        F.col("ApprovedByUser").alias("approved_by_user"),
        F.col("ApprovalTicketNumber").alias("approval_ticket_number"),
        F.col("SourceDeskCode").alias("source_desk_code"),
        F.col("Comment").alias("comment"),
        F.col("RateDateText").alias("rate_date_text"), F.col("OverrideRateText").alias("override_rate_text"),
    )
    conversionError = parsed.where(F.col("rate_date").isNull() | F.col("override_rate").isNull())
    parsed = parsed.where(F.col("rate_date").isNotNull() & F.col("override_rate").isNotNull()).withColumn(
        "rate_pair_code", F.concat_ws("/", "from_currency_code", "to_currency_code")
    ).withColumn(
        "four_eyes_flag", yesNo((F.length(F.trim(F.coalesce(F.col("approved_by_user"), F.lit("")))) > 0) & (F.col("approved_by_user") != F.col("requested_by_user")))
    )
    published = publishedRates.select(
        F.col("from_currency_code").alias("_pub_from"), F.col("to_currency_code").alias("_pub_to"), F.col("published_rate")
    )
    looked = parsed.join(published, (parsed.from_currency_code == published._pub_from) & (parsed.to_currency_code == published._pub_to), "left").drop("_pub_from", "_pub_to")
    unknownPair = looked.where(F.col("published_rate").isNull())
    looked = looked.where(F.col("published_rate").isNotNull()).withColumn(
        "deviation_basis_points",
        F.when(F.col("published_rate") == 0, F.lit(0)).otherwise(F.abs(F.col("override_rate") - F.col("published_rate")) / F.col("published_rate") * 10000).cast("decimal(18,4)"),
    )
    approvedCond = (F.col("four_eyes_flag") == "Y") & (F.length(F.trim(F.coalesce(F.col("approval_ticket_number"), F.lit("")))) > 0) & (F.col("deviation_basis_points") <= config.FX_OVERRIDE_MAX_DEVIATION_BPS)
    approved = looked.where(approvedCond)
    refused = looked.where(~approvedCond).withColumn(
        "reject_reason_code",
        F.when(F.col("four_eyes_flag") != "Y", "FOUR_EYES").when(F.length(F.trim(F.coalesce(F.col("approval_ticket_number"), F.lit("")))) == 0, "NO_TICKET").otherwise("TOLERANCE"),
    )
    return {
        "detail": parsed, "control": control, "approved": approved, "refused": refused,
        "unknown_pair": unknownPair.withColumn("reject_reason_code", F.lit("UNKNOWN_PAIR")),
        "conversion_error": conversionError.withColumn("reject_reason_code", F.lit("CONVERSION")),
    }


def controlTotals(detail: DataFrame, control: DataFrame) -> DataFrame:
    """Per file: detail count and hash total (sum of override rates) versus the control record's declared values
    (RateDateText carries the count, OverrideRateText the hash total)."""
    actual = detail.groupBy("source_file_name").agg(F.count("*").alias("actual_count"), F.sum("override_rate").alias("actual_hash_total"))
    declared = control.groupBy("source_file_name").agg(
        F.max(F.col("RateDateText").cast("long")).alias("declared_count"), F.max(F.col("OverrideRateText").cast("decimal(18,8)")).alias("declared_hash_total")
    )
    return declared.join(actual, "source_file_name", "full").select(
        "source_file_name", "declared_count", F.coalesce("actual_count", F.lit(0)).alias("actual_count"), "declared_hash_total",
        F.coalesce("actual_hash_total", F.lit(0)).cast("decimal(18,8)").alias("actual_hash_total"),
    ).withColumn(
        "is_reconciled",
        F.col("declared_count").isNotNull() & (F.col("declared_count") == F.col("actual_count")) & (F.abs(F.coalesce(F.col("declared_hash_total"), F.lit(-1)) - F.col("actual_hash_total")) < 0.000001),
    )


def publishedSpotRates(spark: SparkSession) -> DataFrame:
    """Lookup Published Rate: latest SPOT rate per pair from the landed Oracle window."""
    fx = spark.table(config.tbl("bronze_oracle_fx_rate")).where("rate_type_cd = 'SPOT'")
    return fx.groupBy(F.col("from_currency_cd").alias("from_currency_code"), F.col("to_currency_cd").alias("to_currency_code")).agg(
        F.max_by("rate", "rate_dt").cast("decimal(18,8)").alias("published_rate")
    )


def runIngFileFxOverride(spark: SparkSession, batchId: int, volumePath: str = config.LANDING_VOLUME_PATH) -> dict:
    inboundPath = os.path.join(volumePath, INBOUND)
    files = sorted(f for f in os.listdir(inboundPath) if f.startswith("fx_override_") and f.endswith(".csv")) if os.path.isdir(inboundPath) else []
    if not files:
        return {"files": 0, "approved": 0, "rejected": 0, "quarantined": []}
    raw = readFeed(spark, [os.path.join(inboundPath, f) for f in files])
    parts = parseFeed(raw, publishedSpotRates(spark))
    totals = controlTotals(parts["detail"], parts["control"])
    reconciled = [r["source_file_name"] for r in totals.where("is_reconciled").collect()]
    quarantined = [f for f in files if f not in reconciled]
    okFiles = F.col("source_file_name").isin(reconciled) if reconciled else F.lit(False)
    approved = parts["approved"].where(okFiles).withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn("loaded_at_utc", F.current_timestamp())
    writeTable(approved, config.tbl("bronze_file_fx_override"), mode="append")
    writeTable(spark.table(config.tbl("bronze_file_fx_override")).dropDuplicates(["from_currency_code", "to_currency_code", "rate_date"]), config.tbl("silver_fx_override_approved"))
    rejectCols = ["source_file_name", "rate_date_text", "from_currency_code", "to_currency_code", "override_rate_text", "reject_reason_code"]
    rejected = None
    for key in ("refused", "unknown_pair", "conversion_error"):
        part = parts[key].select(*rejectCols)
        rejected = part if rejected is None else rejected.unionByName(part)
    rejected = rejected.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn("package_name", F.lit("ING_FILE_FxOverride"))
    writeTable(rejected, config.tbl("err_rejected_file_row"), mode="append")
    registry = totals.withColumn("batch_id", F.lit(batchId).cast("bigint")).withColumn(
        "file_status", F.when(F.col("is_reconciled"), "PROCESSED").otherwise("QUARANTINED")
    ).withColumn("processed_at_utc", F.current_timestamp())
    writeTable(registry, config.tbl("etl_file_registry"), mode="append")
    for f in files:
        dest = ARCHIVE if f in reconciled else QUARANTINE
        os.makedirs(os.path.join(volumePath, dest), exist_ok=True)
        shutil.move(os.path.join(inboundPath, f), os.path.join(volumePath, dest, f))
    return {"files": len(files), "approved": approved.count(), "rejected": rejected.count(), "quarantined": quarantined}
