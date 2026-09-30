"""ETL control: watermarks, package execution log and reject capture.

Databricks-native equivalents of `etl.usp_GetWatermark`/`etl.usp_SetWatermark`,
`etl.usp_LogPackageStart`/`usp_LogPackageEnd`/`usp_LogRowCount` and the `err.*`
reject tables. Everything lands in the group's own schema.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from product_inventory.config import PipelineConfig
from product_inventory.tables import appendTable, readTableOrEmpty

WATERMARK_TABLE = "etl_watermark"
EXECUTION_TABLE = "etl_package_execution"
REJECT_TABLE = "err_rejected_record"
LATE_ARRIVING_TABLE = "work_late_arriving_dimension_queue"

WATERMARK_SCHEMA = StructType(
    [
        StructField("source_system_code", StringType()),
        StructField("object_name", StringType()),
        StructField("watermark_type", StringType()),
        StructField("watermark_from", StringType()),
        StructField("watermark_to", StringType()),
        StructField("batch_id", LongType()),
        StructField("updated_at_utc", TimestampType()),
    ]
)

REJECT_SCHEMA = StructType(
    [
        StructField("batch_id", LongType()),
        StructField("package_name", StringType()),
        StructField("object_name", StringType()),
        StructField("reject_stage", StringType()),
        StructField("reject_reason_code", StringType()),
        StructField("reject_reason", StringType()),
        StructField("business_key", StringType()),
        StructField("payload_json", StringType()),
        StructField("logged_at_utc", TimestampType()),
    ]
)


def utcNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------- watermarks
def resolveTimestampWindow(
    lastValue: Optional[datetime],
    reloadFullHistory: bool,
    lookbackMinutes: int,
    now: datetime,
    floor: datetime = datetime(1900, 1, 1),
) -> Tuple[datetime, datetime]:
    """SSIS `etl.usp_GetWatermark` semantics for timestamp watermarks.

    Returns the half-open window [from, to). The lookback is applied by the
    packages on top of the stored watermark (`DATEADD(minute, -240, ?)`), which is
    why it is folded in here for both branches.
    """
    if reloadFullHistory or lastValue is None:
        return floor, now
    return lastValue - timedelta(minutes=lookbackMinutes), now


def resolveKeyWindow(lastKey: Optional[int], maxKey: Optional[int], reloadFullHistory: bool) -> Tuple[int, int]:
    """Numeric-key watermark window (`WHERE key > from AND key <= to`)."""
    upper = maxKey if maxKey is not None else 0
    if reloadFullHistory or lastKey is None:
        return 0, upper
    return lastKey, max(lastKey, upper)


def getWatermark(spark: SparkSession, cfg: PipelineConfig, sourceSystem: str, objectName: str) -> Optional[str]:
    df = readTableOrEmpty(spark, cfg.fqn(WATERMARK_TABLE), WATERMARK_SCHEMA)
    row = (
        df.where((F.col("source_system_code") == sourceSystem) & (F.col("object_name") == objectName))
        .orderBy(F.col("updated_at_utc").desc())
        .select("watermark_to")
        .limit(1)
        .collect()
    )
    return row[0][0] if row else None


def setWatermark(
    spark: SparkSession,
    cfg: PipelineConfig,
    sourceSystem: str,
    objectName: str,
    watermarkType: str,
    watermarkFrom: str,
    watermarkTo: str,
) -> None:
    df = spark.createDataFrame(
        [(sourceSystem, objectName, watermarkType, watermarkFrom, watermarkTo, cfg.batchId, utcNow())],
        WATERMARK_SCHEMA,
    )
    appendTable(df, cfg.fqn(WATERMARK_TABLE))


# ------------------------------------------------------------- execution log
@dataclass
class PackageRun:
    packageName: str
    startedAt: datetime
    rowsRead: int = 0
    rowsInserted: int = 0
    rowsUpdated: int = 0
    rowsRejected: int = 0


def startPackage(packageName: str) -> PackageRun:
    return PackageRun(packageName=packageName, startedAt=utcNow())


def endPackage(spark: SparkSession, cfg: PipelineConfig, run: PackageRun, status: str, message: str = "") -> None:
    schema = StructType(
        [
            StructField("batch_id", LongType()),
            StructField("package_name", StringType()),
            StructField("started_at_utc", TimestampType()),
            StructField("ended_at_utc", TimestampType()),
            StructField("status", StringType()),
            StructField("rows_read", LongType()),
            StructField("rows_inserted", LongType()),
            StructField("rows_updated", LongType()),
            StructField("rows_rejected", LongType()),
            StructField("message", StringType()),
            StructField("git_sha", StringType()),
        ]
    )
    df = spark.createDataFrame(
        [
            (
                cfg.batchId,
                run.packageName,
                run.startedAt,
                utcNow(),
                status,
                run.rowsRead,
                run.rowsInserted,
                run.rowsUpdated,
                run.rowsRejected,
                message,
                cfg.gitSha,
            )
        ],
        schema,
    )
    appendTable(df, cfg.fqn(EXECUTION_TABLE))


# ------------------------------------------------------------------ rejects
def shapeRejects(
    df: DataFrame,
    batchId: int,
    packageName: str,
    objectName: str,
    rejectStage: str,
    reasonCode: str,
    reason: str,
    businessKeyCol: str,
) -> DataFrame:
    """Project any reject stream onto the generic `err_rejected_record` shape."""
    payload = F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    return df.select(
        F.lit(batchId).cast("long").alias("batch_id"),
        F.lit(packageName).alias("package_name"),
        F.lit(objectName).alias("object_name"),
        F.lit(rejectStage).alias("reject_stage"),
        F.lit(reasonCode).alias("reject_reason_code"),
        F.lit(reason).alias("reject_reason"),
        F.col(businessKeyCol).cast("string").alias("business_key"),
        payload.alias("payload_json"),
        F.current_timestamp().alias("logged_at_utc"),
    )


def writeRejects(
    spark: SparkSession,
    cfg: PipelineConfig,
    df: DataFrame,
    packageName: str,
    objectName: str,
    rejectStage: str,
    reasonCode: str,
    reason: str,
    businessKeyCol: str,
) -> int:
    shaped = shapeRejects(df, cfg.batchId, packageName, objectName, rejectStage, reasonCode, reason, businessKeyCol)
    rejected = shaped.count()
    if rejected > 0:
        appendTable(shaped, cfg.fqn(REJECT_TABLE))
    return rejected


def queueLateArrivingMembers(
    spark: SparkSession, cfg: PipelineConfig, df: DataFrame, dimensionName: str, businessKeyCol: str, packageName: str
) -> int:
    """`work.LateArrivingDimensionQueue`: business keys seen by a fact before the dimension."""
    queued = (
        df.select(F.col(businessKeyCol).cast("string").alias("business_key"))
        .distinct()
        .select(
            F.lit(cfg.batchId).cast("long").alias("batch_id"),
            F.lit(dimensionName).alias("dimension_name"),
            F.col("business_key"),
            F.lit(packageName).alias("queued_by_package"),
            F.current_timestamp().alias("queued_at_utc"),
            F.lit(False).alias("is_resolved"),
        )
    )
    count = queued.count()
    if count > 0:
        appendTable(queued, cfg.fqn(LATE_ARRIVING_TABLE))
    return count
