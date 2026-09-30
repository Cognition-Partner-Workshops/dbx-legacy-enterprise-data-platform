"""ING_FILE_CarrierScan: carrier scan event files -> bronze_file_carrier_scan.

SSIS control flow: Foreach file in <InboundFileRoot>\\carrier -> Register Inbound File (etl.FileIngestionLog)
-> Read Sidecar Control Count (etl.FileControlTotal) -> Data Flow (Flat File Source -> Parse Scan Event ->
Validate Scan Rows -> raw.FileCarrierScan | err.RejectedFileRow) -> duplicate-scan count -> control-total check
-> mark file Processed / Quarantined -> archive.

Databricks shape: the drop folder is a Unity Catalog volume (``/Volumes/<catalog>/<schema>/landing/inbound/carrier``),
files are read with the CSV reader (Windows-1252, header row) and idempotency comes from ``ctl_file_ingestion_log``
(a file already marked Processed is skipped) instead of moving files to an archive folder.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_returns.common import appendTable, ensureTable, loadMetadata, logRowCount, tableExists
from logistics_returns.config import SOURCE_SYSTEM_CARRIER, RunContext, Tables

PACKAGE_NAME = "ING_FILE_CarrierScan"
FILE_PATTERN = re.compile(r"^carrier_scan_(\d{8})_(\d{3})\.csv$", re.IGNORECASE)
FILE_ENCODING = "windows-1252"
DELIVERED_CODES = ("DLV", "POD")

# Flat File Source columns, in file order (the layout the SSIS connection manager declares).
FILE_COLUMNS = [
    "CarrierCode",
    "TrackingNumber",
    "ShipmentReference",
    "ScanStatusCode",
    "ScanStatusDescription",
    "ScanTimestampText",
    "ScanLocationCode",
    "ScanCountryCode",
    "ExceptionReasonCode",
    "SignedByName",
    "PieceCount",
    "WeightValue",
    "WeightUomCode",
]

FILE_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in FILE_COLUMNS])

FILE_INGESTION_LOG_SCHEMA = T.StructType(
    [
        T.StructField("file_ingestion_log_id", T.StringType()),
        T.StructField("package_execution_id", T.LongType()),
        T.StructField("batch_id", T.LongType()),
        T.StructField("object_name", T.StringType()),
        T.StructField("file_name", T.StringType()),
        T.StructField("file_path", T.StringType()),
        T.StructField("received_at_utc", T.TimestampType()),
        T.StructField("completed_at_utc", T.TimestampType()),
        T.StructField("status", T.StringType()),
        T.StructField("detail_row_count", T.LongType()),
        T.StructField("reject_row_count", T.LongType()),
        T.StructField("sidecar_row_count", T.LongType()),
        T.StructField("duplicate_scan_count", T.LongType()),
        T.StructField("control_totals_match", T.BooleanType()),
    ]
)


@dataclass
class FileResult:
    fileName: str
    status: str
    detailRows: int
    rejectRows: int
    sidecarRows: int
    duplicateScans: int


# ---------------------------------------------------------------------------------------------
# Pure transformations (unit-tested on local Spark)
# ---------------------------------------------------------------------------------------------


def parseScanEvent(df: DataFrame) -> DataFrame:
    """The "Parse Scan Event" derived column.

    SSIS took ``SUBSTRING(ScanTimestampText, 1, 19)`` as the *UTC* timestamp and dropped the offset sign; the
    file carries carrier-local time with an ISO offset (``2025-01-06T08:15:00+01:00``), so here the local value is
    kept as ``scan_timestamp_local`` and ``scan_timestamp_utc`` is derived by applying the signed offset
    (deliberate deviation, see README).
    """
    text = F.col("ScanTimestampText")
    localTs = F.to_timestamp(F.substring(text, 1, 19), "yyyy-MM-dd'T'HH:mm:ss")
    sign = F.when(F.substring(text, 20, 1) == "-", F.lit(-1)).otherwise(F.lit(1))
    offsetMinutes = (F.substring(text, 21, 2).cast("int") * 60 + F.substring(text, 24, 2).cast("int")) * sign
    offsetMinutes = F.when(F.length(text) >= 25, offsetMinutes).otherwise(F.lit(0))
    return (
        df.withColumn("scan_timestamp_local", localTs)
        .withColumn("scan_offset_minutes", offsetMinutes)
        .withColumn("scan_timestamp_utc", (F.unix_timestamp(localTs) - offsetMinutes * 60).cast("timestamp"))
        .withColumn("scan_time_zone_abbrev", F.when(F.length(text) >= 25, F.substring(text, 20, 6)))
        .withColumn(
            "exception_flag",
            F.when(F.length(F.trim(F.coalesce(F.col("ExceptionReasonCode"), F.lit("")))) > 0, F.lit("Y")).otherwise("N"),
        )
        .withColumn(
            "delivered_flag",
            F.when(F.upper(F.trim(F.col("ScanStatusCode"))).isin(*DELIVERED_CODES), F.lit("Y")).otherwise(F.lit("N")),
        )
        .withColumn("region_code", F.lit("GLOBAL"))
    )


def validScanRow() -> F.Column:
    """ "Validate Scan Rows" conditional split expression."""
    return (
        (F.length(F.trim(F.coalesce(F.col("TrackingNumber"), F.lit("")))) > 0)
        & (F.length(F.trim(F.coalesce(F.col("ScanStatusCode"), F.lit("")))) > 0)
        & (F.length(F.trim(F.coalesce(F.col("ScanTimestampText"), F.lit("")))) >= 19)
        & F.col("scan_timestamp_local").isNotNull()
    )


def toBronzeColumns(df: DataFrame) -> DataFrame:
    """Map the flat-file layout onto the raw.FileCarrierScan contract (snake_case)."""
    return df.select(
        F.upper(F.trim(F.col("CarrierCode"))).alias("carrier_code"),
        F.trim(F.col("TrackingNumber")).alias("tracking_number"),
        F.trim(F.col("ShipmentReference")).alias("shipment_reference"),
        F.upper(F.trim(F.col("ScanStatusCode"))).alias("scan_event_code"),
        F.col("ScanStatusDescription").alias("scan_event_description"),
        F.col("scan_timestamp_local"),
        F.col("scan_time_zone_abbrev"),
        F.col("scan_timestamp_utc"),
        F.col("scan_offset_minutes"),
        F.upper(F.trim(F.col("ScanLocationCode"))).alias("depot_code"),
        F.lit(None).cast("string").alias("depot_city"),
        F.upper(F.trim(F.col("ScanCountryCode"))).alias("depot_country_code"),
        F.upper(F.trim(F.col("ExceptionReasonCode"))).alias("exception_reason_code"),
        F.col("SignedByName").alias("signatory_name"),
        F.col("PieceCount").cast("int").alias("piece_count"),
        F.col("WeightValue").cast(T.DecimalType(18, 3)).alias("weight_value"),
        F.upper(F.trim(F.col("WeightUomCode"))).alias("weight_uom_code"),
        F.col("exception_flag"),
        F.col("delivered_flag"),
        F.col("region_code"),
        F.col("source_file_name"),
        F.col("source_row_number"),
    )


def countDuplicateScans(df: DataFrame) -> int:
    """SELECT COUNT(*) FROM (... GROUP BY TrackingNumber, ScanStatusCode, ScanTimestampUtc HAVING COUNT(*) > 1)."""
    return df.groupBy("tracking_number", "scan_event_code", "scan_timestamp_utc").count().where(F.col("count") > 1).count()


def readSidecarCount(sidecarText: str | None) -> int:
    """The ``.ctl`` sidecar holds ``ROWCOUNT=<n>`` (fallback: a bare integer). Missing sidecar -> 0."""
    if not sidecarText:
        return 0
    match = re.search(r"ROWCOUNT\s*=\s*(\d+)", sidecarText, re.IGNORECASE)
    if match:
        return int(match.group(1))
    stripped = sidecarText.strip()
    return int(stripped) if stripped.isdigit() else 0


def controlTotalsMatch(landedRows: int, rejectedRows: int, sidecarRows: int) -> bool:
    """SSIS only checked ``@Landed >= 0`` (always true). Here a sidecar count, when present, must equal
    landed + rejected rows; files without a sidecar pass (deliberate tightening, see README)."""
    if sidecarRows == 0:
        return True
    return landedRows + rejectedRows == sidecarRows


# ---------------------------------------------------------------------------------------------
# File-level driver
# ---------------------------------------------------------------------------------------------


def listInboundFiles(inboundDir: str) -> list[str]:
    if not os.path.isdir(inboundDir):
        return []
    return sorted(f for f in os.listdir(inboundDir) if FILE_PATTERN.match(f))


def processedFileNames(ctx: RunContext) -> set[str]:
    name = ctx.table(Tables.ctlFileIngestionLog)
    if not tableExists(ctx.spark, name):
        return set()
    rows = ctx.spark.table(name).where(F.col("status") == "Processed").select("file_name").distinct().collect()
    return {r[0] for r in rows}


def readCarrierFile(spark: SparkSession, filePath: str) -> DataFrame:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", FILE_ENCODING)
        .option("sep", ",")
        .option("quote", '"')
        .option("mode", "PERMISSIVE")
        .schema(FILE_SCHEMA)
        .load(filePath)
    )
    return df.withColumn("source_row_number", F.monotonically_increasing_id() + 1).withColumn(
        "source_file_name", F.lit(os.path.basename(filePath))
    )


def ingestFile(ctx: RunContext, filePath: str, sidecarText: str | None) -> FileResult:
    spark = ctx.spark
    fileName = os.path.basename(filePath)
    parsed = parseScanEvent(readCarrierFile(spark, filePath))
    valid = parsed.where(validScanRow())
    rejected = parsed.where(~validScanRow())

    bronze = toBronzeColumns(valid).select("*", *loadMetadata(ctx, SOURCE_SYSTEM_CARRIER))
    detailRows = bronze.count()
    rejectRows = rejected.count()
    if detailRows:
        appendTable(bronze, ctx.table(Tables.bronzeFileCarrierScan))
    if rejectRows:
        rejects = rejected.select(
            F.lit(ctx.batchId).cast("long").alias("batch_id"),
            F.lit(ctx.packageExecutionId).cast("long").alias("package_execution_id"),
            F.lit(SOURCE_SYSTEM_CARRIER).alias("source_system_code"),
            F.col("source_file_name"),
            F.col("source_row_number"),
            F.concat_ws(",", *[F.coalesce(F.col(c), F.lit("")) for c in FILE_COLUMNS]).alias("raw_row_text"),
            F.lit(len(FILE_COLUMNS)).alias("expected_column_count"),
            F.lit(",").alias("delimiter_used"),
            F.when(F.length(F.trim(F.coalesce(F.col("TrackingNumber"), F.lit("")))) == 0, F.lit("MISSING_TRACKING"))
            .when(F.length(F.trim(F.coalesce(F.col("ScanStatusCode"), F.lit("")))) == 0, F.lit("MISSING_STATUS"))
            .otherwise(F.lit("BAD_TIMESTAMP"))
            .alias("reject_reason_code"),
            F.lit("Validate Scan Rows").alias("reject_stage"),
            F.lit(ctx.startedAtUtc).cast("timestamp").alias("rejected_at_utc"),
        )
        appendTable(rejects, ctx.table(Tables.errRejectedFileRow))

    duplicateScans = countDuplicateScans(bronze)
    sidecarRows = readSidecarCount(sidecarText)
    matched = controlTotalsMatch(detailRows, rejectRows, sidecarRows)
    status = "Processed" if matched else "Quarantined"
    parsed.unpersist()

    logRow = spark.createDataFrame(
        [
            (
                f"{ctx.packageExecutionId}:{fileName}",
                ctx.packageExecutionId,
                ctx.batchId,
                "raw.FileCarrierScan",
                fileName,
                filePath,
                ctx.startedAtUtc,
                ctx.startedAtUtc,
                status,
                detailRows,
                rejectRows,
                sidecarRows,
                duplicateScans,
                matched,
            )
        ],
        FILE_INGESTION_LOG_SCHEMA,
    )
    appendTable(logRow, ctx.table(Tables.ctlFileIngestionLog))
    return FileResult(fileName, status, detailRows, rejectRows, sidecarRows, duplicateScans)


def readSidecar(filePath: str) -> str | None:
    sidecarPath = re.sub(r"\.csv$", ".ctl", filePath, flags=re.IGNORECASE)
    if not os.path.isfile(sidecarPath):
        return None
    with open(sidecarPath, encoding=FILE_ENCODING) as handle:
        return handle.read()


def runCarrierScanIngestion(ctx: RunContext, inboundDir: str | None = None) -> list[FileResult]:
    inboundDir = inboundDir or ctx.volumePath("inbound", "carrier")
    ensureTable(ctx.spark, ctx.table(Tables.ctlFileIngestionLog), FILE_INGESTION_LOG_SCHEMA)
    alreadyDone = processedFileNames(ctx)
    results: list[FileResult] = []
    for fileName in listInboundFiles(inboundDir):
        if fileName in alreadyDone and not ctx.reloadFullHistory:
            continue
        filePath = f"{inboundDir}/{fileName}"
        results.append(ingestFile(ctx, filePath, readSidecar(filePath)))
    if not tableExists(ctx.spark, ctx.table(Tables.bronzeFileCarrierScan)):
        emptyBronze = toBronzeColumns(
            parseScanEvent(
                ctx.spark.createDataFrame([], FILE_SCHEMA)
                .withColumn("source_row_number", F.lit(0).cast("long"))
                .withColumn("source_file_name", F.lit(""))
            )
        )
        ensureTable(
            ctx.spark,
            ctx.table(Tables.bronzeFileCarrierScan),
            emptyBronze.select("*", *loadMetadata(ctx, SOURCE_SYSTEM_CARRIER)).schema,
        )
    logRowCount(
        ctx,
        PACKAGE_NAME,
        Tables.bronzeFileCarrierScan,
        {
            "files_processed": len(results),
            "rows_inserted": sum(r.detailRows for r in results),
            "rows_rejected": sum(r.rejectRows for r in results),
            "files_quarantined": sum(1 for r in results if r.status == "Quarantined"),
            "duplicate_scans": sum(r.duplicateScans for r in results),
        },
    )
    return results
