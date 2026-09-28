"""Bronze ingestion - the lakehouse equivalent of the legacy ``raw.*`` landing.

``run(spark, cfg)`` reads every registered mock CSV under ``cfg.mockDataRoot``
with its DDL-derived schema and lands it, source-shaped, in
``sales_bronze.<system>_<schema>_<table>`` plus the five ``_`` metadata
columns. Rows that fail to parse are quarantined (rule ``BRONZE_PARSE``),
nothing is dropped silently, and every table run - including missing source
files - is recorded in ``sales_quality.load_log``.

Replaces: ssis/01_oracle_extract EXT_ORA_* and ssis/02_sqlserver_extract
EXT_SQL_* packages, sqlserver/staging/tables/10_raw_tables_oracle.sql and
11_raw_tables_sqlserver.sql (landing shape), Integration.ChangeTrackingWatermark
(``sales_bronze._watermark``) and the etl.usp_GetWatermark / etl.LoadLog
bookkeeping (``sales_quality.load_log``).
"""
from __future__ import annotations

import csv
import datetime as dt
import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_lakehouse.bronze.registry import BY_SOURCE_OBJECT, REGISTRY, SOURCE_TABLES
from sales_lakehouse.bronze.source_table import SourceTable
from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.spark import cacheIfSupported, unpersistQuietly
from sales_lakehouse.common.tables import appendBatch

log = logging.getLogger(__name__)

CORRUPT_RECORD_COLUMN = "_corrupt_record"
WATERMARK_TABLE = "_watermark"
LOAD_LOG_TABLE = "load_log"
PARSE_RULE_CODE = "BRONZE_PARSE"
PROCESS_NAME = "sales_lakehouse.bronze.ingest"

METADATA_COLUMNS = ("_source_system", "_source_object", "_source_file", "_load_ts", "_batch_id")

# LEGACY QUIRK: SQL Server BIT arrives as 0/1 in the extract files (the SSIS
# packages cast it with (DT_BOOL)); "true"/"false" are accepted for hand-written fixtures.
BIT_TRUE_VALUES = ("1", "true", "TRUE", "True")
BIT_FALSE_VALUES = ("0", "false", "FALSE", "False")

STATUS_SUCCESS = "SUCCESS"
STATUS_MISSING = "MISSING_SOURCE"
STATUS_FAILED = "FAILED"

LOAD_LOG_SCHEMA = T.StructType(
    [
        T.StructField("batch_id", T.LongType()),
        T.StructField("source_object", T.StringType()),
        T.StructField("rows_read", T.LongType()),
        T.StructField("rows_loaded", T.LongType()),
        T.StructField("rows_rejected", T.LongType()),
        T.StructField("status", T.StringType()),
        T.StructField("started_at_utc", T.TimestampType()),
        T.StructField("finished_at_utc", T.TimestampType()),
        T.StructField("message", T.StringType()),
    ]
)

# Mirrors Integration.ChangeTrackingWatermark (one row per consumer/source table).
WATERMARK_SCHEMA = T.StructType(
    [
        T.StructField("source_object", T.StringType()),
        T.StructField("watermark_column", T.StringType()),
        T.StructField("last_watermark_value", T.StringType()),
        T.StructField("previous_watermark_value", T.StringType()),
        T.StructField("overlap_minutes", T.IntegerType()),
        T.StructField("last_run_batch_id", T.LongType()),
        T.StructField("last_row_count", T.LongType()),
        T.StructField("last_updated_utc", T.TimestampType()),
        T.StructField("updated_by_process", T.StringType()),
    ]
)


@dataclass
class LoadResult:
    sourceObject: str
    bronzeTable: str
    status: str
    rowsRead: int = 0
    rowsLoaded: int = 0
    rowsRejected: int = 0
    message: str = ""


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def run(spark: SparkSession, cfg: PipelineConfig, tables: list[str] | None = None) -> list[LoadResult]:
    """Load the registered source tables (all, or the subset named in ``tables``).

    ``tables`` accepts bronze table names (``sqlserver_sales_orders``) or source
    object names (``Sales.Orders``). Per-table failures are logged and recorded
    in ``sales_quality.load_log``; a ``RuntimeError`` summarising them is raised
    after every table has been attempted.
    """
    selected = resolveTables(tables)
    ensureControlTables(spark, cfg)
    loadTs = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    results: list[LoadResult] = []
    for source in selected:
        startedAt = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        try:
            result = loadTable(spark, cfg, source, loadTs)
        except Exception as exc:  # noqa: BLE001 - recorded in load_log, re-raised after the loop
            log.exception("bronze load failed for %s", source.sourceObject)
            result = LoadResult(source.sourceObject, source.bronzeTable, STATUS_FAILED, message=f"{type(exc).__name__}: {exc}"[:4000])
        finishedAt = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
        writeLoadLog(spark, cfg, result, startedAt, finishedAt)
        results.append(result)
    failed = [r for r in results if r.status == STATUS_FAILED]
    if failed:
        raise RuntimeError("bronze load failed for: " + ", ".join(f"{r.sourceObject} ({r.message})" for r in failed))
    return results


def resolveTables(tables: list[str] | None) -> list[SourceTable]:
    if not tables:
        return list(SOURCE_TABLES)
    selected: list[SourceTable] = []
    for name in tables:
        if name in REGISTRY:
            selected.append(REGISTRY[name])
        elif name in BY_SOURCE_OBJECT:
            selected.append(BY_SOURCE_OBJECT[name])
        else:
            raise KeyError(f"unknown bronze table {name!r}; use a registry key or Schema.Table")
    return selected


# --------------------------------------------------------------------------- #
# Per-table load
# --------------------------------------------------------------------------- #
def loadTable(spark: SparkSession, cfg: PipelineConfig, source: SourceTable, loadTs: dt.datetime) -> LoadResult:
    path = cfg.sourcePath(source.system, source.schema, source.table)
    bronzeFqn = cfg.fqn("bronze", source.bronzeTable)
    result = LoadResult(source.sourceObject, source.bronzeTable, STATUS_SUCCESS)

    if not pathExists(spark, path):
        log.warning("bronze source missing for %s: %s", source.sourceObject, path)
        result.status = STATUS_MISSING
        result.message = f"source file not found: {path}"
        return result

    notes: list[str] = []
    raw = readSourceCsv(spark, source, path, notes)
    raw = cacheIfSupported(raw)
    result.rowsRead = raw.count()

    failCondition = parseFailureCondition(source, raw)
    passed = quarantine(
        spark,
        cfg,
        raw,
        PARSE_RULE_CODE,
        bronzeFqn,
        failCondition,
        f"row failed to parse against the {source.sourceObject} DDL schema",
    )
    result.rowsRejected = raw.filter(failCondition).count()

    shaped = shapeToRegistry(source, passed)

    if source.loadMode == "incremental":
        shaped, watermarkNote = applyWatermark(spark, cfg, source, shaped)
        notes.append(watermarkNote)

    shaped = cacheIfSupported(shaped)
    result.rowsLoaded = shaped.count()
    newWatermark = None
    if source.loadMode == "incremental":
        newWatermark = shaped.agg(F.max(F.col(source.watermarkColumn))).first()[0]

    final = addMetadata(shaped, source, path, loadTs, cfg.batchId)
    appendBatch(final, bronzeFqn, "_batch_id", cfg.batchId)

    if source.loadMode == "incremental":
        updateWatermark(spark, cfg, source, newWatermark, result.rowsLoaded)

    result.message = "; ".join(n for n in notes if n)
    log.info(
        "bronze %s -> %s: read=%d loaded=%d rejected=%d %s",
        source.sourceObject, bronzeFqn, result.rowsRead, result.rowsLoaded, result.rowsRejected, result.message,
    )
    unpersistQuietly(raw)
    unpersistQuietly(shaped)
    return result


def pathExists(spark: SparkSession, path: str) -> bool:
    if "://" not in path and not path.startswith("dbfs:"):
        return os.path.exists(path)
    jvm = spark._jvm  # noqa: SLF001 - Hadoop FS is the only portable way to probe dbfs:/abfss: paths
    hadoopPath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoopPath.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    return fs.exists(hadoopPath)


def readHeader(spark: SparkSession, path: str) -> list[str]:
    first = spark.read.text(path).first()
    if first is None:
        return []
    return next(csv.reader([first["value"].lstrip("\ufeff")]))


def readSchemaFor(source: SourceTable, header: list[str], notes: list[str]) -> T.StructType:
    """Explicit read schema in *file* column order.

    Registered columns get their DDL type (BIT is read as string and cast after
    validation); columns in the file but not in the DDL are read as string and
    reported; registered columns absent from the file are added as typed NULLs.
    """
    registered = dict(source.columns)
    fields: list[T.StructField] = []
    unknown: list[str] = []
    for name in header:
        if name in registered:
            kind = registered[name]
            fields.append(T.StructField(name, T.StringType() if kind == "boolean" else source.sparkType(name), True))
        else:
            unknown.append(name)
            fields.append(T.StructField(name, T.StringType(), True))
    missing = [c for c in source.columnNames if c not in header]
    if unknown:
        notes.append(f"columns in file but not in DDL (not landed): {', '.join(unknown)}")
    if missing:
        notes.append(f"DDL columns absent from file (landed as NULL): {', '.join(missing)}")
    fields.append(T.StructField(CORRUPT_RECORD_COLUMN, T.StringType(), True))
    return T.StructType(fields)


def readSourceCsv(spark: SparkSession, source: SourceTable, path: str, notes: list[str]) -> DataFrame:
    header = readHeader(spark, path)
    schema = readSchemaFor(source, header, notes)
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        # LEGACY QUIRK: the raw.* landing tables keep empty extract fields as
        # NULL (no empty-string sentinels), so '' -> NULL for every type.
        .option("nullValue", "")
        .option("multiLine", "true")
        .option("escape", '"')
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .load(path)
    )


def bitColumnsIn(source: SourceTable, df: DataFrame) -> list[str]:
    return [name for name, kind in source.columns if kind == "boolean" and name in df.columns]


def parseFailureCondition(source: SourceTable, raw: DataFrame) -> F.Column:
    condition = F.col(CORRUPT_RECORD_COLUMN).isNotNull()
    for name in bitColumnsIn(source, raw):
        condition = condition | (F.col(name).isNotNull() & ~F.col(name).isin(*BIT_TRUE_VALUES, *BIT_FALSE_VALUES))
    return condition


def shapeToRegistry(source: SourceTable, df: DataFrame) -> DataFrame:
    """Project to the registry columns, in DDL order, with DDL types."""
    selected: list[F.Column] = []
    for name, kind in source.columns:
        if name not in df.columns:
            selected.append(F.lit(None).cast(source.sparkType(name)).alias(name))
        elif kind == "boolean":
            selected.append(
                F.when(F.col(name).isin(*BIT_TRUE_VALUES), F.lit(True))
                .when(F.col(name).isin(*BIT_FALSE_VALUES), F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
                .alias(name)
            )
        else:
            selected.append(F.col(name))
    return df.select(*selected)


def addMetadata(df: DataFrame, source: SourceTable, path: str, loadTs: dt.datetime, batchId: int) -> DataFrame:
    return (
        df.withColumn("_source_system", F.lit(source.sourceSystemLabel))
        .withColumn("_source_object", F.lit(source.sourceObject))
        .withColumn("_source_file", F.lit(path))
        .withColumn("_load_ts", F.lit(loadTs).cast("timestamp"))
        .withColumn("_batch_id", F.lit(batchId).cast("bigint"))
    )


# --------------------------------------------------------------------------- #
# Control tables: sales_bronze._watermark and sales_quality.load_log
# --------------------------------------------------------------------------- #
def ensureControlTables(spark: SparkSession, cfg: PipelineConfig) -> None:
    for fqn, schema in (
        (cfg.fqn("bronze", WATERMARK_TABLE), WATERMARK_SCHEMA),
        (cfg.fqn("quality", LOAD_LOG_TABLE), LOAD_LOG_SCHEMA),
    ):
        if not spark.catalog.tableExists(fqn):
            spark.createDataFrame([], schema).write.format("delta").mode("ignore").saveAsTable(fqn)


def readWatermarkRow(spark: SparkSession, cfg: PipelineConfig, source: SourceTable):
    return (
        spark.table(cfg.fqn("bronze", WATERMARK_TABLE))
        .filter(F.col("source_object") == source.sourceObject)
        .first()
    )


def applyWatermark(spark: SparkSession, cfg: PipelineConfig, source: SourceTable, df: DataFrame) -> tuple[DataFrame, str]:
    """Keep only rows whose watermark column is beyond the stored watermark.

    A re-run of the batch that produced the stored watermark re-extracts the
    same window (``previous_watermark_value``), so appendBatch's replaceWhere
    replaces that batch with identical content instead of emptying it.
    """
    column = source.watermarkColumn
    row = readWatermarkRow(spark, cfg, source)
    if row is None:
        return df, f"incremental on {column}: no watermark yet, full extract"
    if row["last_run_batch_id"] == cfg.batchId:
        lowerBound = row["previous_watermark_value"]
        note = f"incremental on {column}: same batch re-run, replaying from {lowerBound!r}"
    else:
        lowerBound = row["last_watermark_value"]
        note = f"incremental on {column}: rows > {lowerBound!r}"
    if lowerBound is None:
        return df, note
    kind = source.sparkType(column)
    bound = F.lit(lowerBound).cast(kind)
    overlap = row["overlap_minutes"] or 0
    if overlap and isinstance(kind, (T.TimestampType, T.DateType)):
        # LEGACY QUIRK: timestamp watermarks re-read an overlap window for
        # late-arriving edits (ChangeTrackingWatermark.OverlapMinutes, the
        # 240-minute DATEADD in EXT_SQL_StockItems). Overlapping rows land
        # again in the new batch; silver de-duplicates on the natural key.
        bound = (bound.cast("timestamp") - F.expr(f"INTERVAL {int(overlap)} MINUTES")).cast(kind)
        note += f" (overlap {overlap} min)"
    return df.filter(F.col(column) > bound), note


def updateWatermark(spark: SparkSession, cfg: PipelineConfig, source: SourceTable, newValue, rowCount: int) -> None:
    from delta.tables import DeltaTable

    fqn = cfg.fqn("bronze", WATERMARK_TABLE)
    existing = readWatermarkRow(spark, cfg, source)
    sameBatch = existing is not None and existing["last_run_batch_id"] == cfg.batchId
    if existing is None:
        previous = None
    elif sameBatch:
        previous = existing["previous_watermark_value"]
    else:
        previous = existing["last_watermark_value"]
    carried = existing["last_watermark_value"] if existing and not sameBatch else None
    candidates = [v for v in (formatWatermark(newValue), carried) if v is not None]
    kind = source.sparkType(source.watermarkColumn)
    lastValue = maxWatermark(candidates, kind)
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    update = spark.createDataFrame(
        [
            (
                source.sourceObject,
                source.watermarkColumn,
                lastValue,
                previous,
                int(source.overlapMinutes),
                int(cfg.batchId),
                int(rowCount),
                now,
                PROCESS_NAME,
            )
        ],
        WATERMARK_SCHEMA,
    )
    (
        DeltaTable.forName(spark, fqn)
        .alias("t")
        .merge(update.alias("u"), "t.source_object = u.source_object")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def formatWatermark(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value)


def maxWatermark(values: list[str], kind: T.DataType) -> str | None:
    if not values:
        return None
    if isinstance(kind, (T.TimestampType, T.DateType, T.StringType)):
        return max(values)
    if isinstance(kind, T.DecimalType):
        from decimal import Decimal

        return formatWatermark(max(Decimal(v) for v in values))
    return formatWatermark(max(int(v) for v in values))


def writeLoadLog(spark: SparkSession, cfg: PipelineConfig, result: LoadResult, startedAt: dt.datetime, finishedAt: dt.datetime) -> None:
    row = spark.createDataFrame(
        [
            (
                int(cfg.batchId),
                result.sourceObject,
                int(result.rowsRead),
                int(result.rowsLoaded),
                int(result.rowsRejected),
                result.status,
                startedAt,
                finishedAt,
                result.message or None,
            )
        ],
        LOAD_LOG_SCHEMA,
    )
    row.write.format("delta").mode("append").saveAsTable(cfg.fqn("quality", LOAD_LOG_TABLE))
