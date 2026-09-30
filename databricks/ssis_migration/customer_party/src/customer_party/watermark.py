"""Incremental watermark handling (etl.usp_GetWatermark / etl.usp_SetWatermark)."""
from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from customer_party.config import LOW_DATE, PipelineConfig, utcNow
from customer_party.tables import tableExists

WATERMARK_TABLE = "etl_watermark"
WATERMARK_SCHEMA = (
    "source_system_code string, object_name string, watermark_value timestamp, "
    "batch_id bigint, updated_ts timestamp"
)


def computeWatermarkWindow(
    lastWatermark: datetime | None,
    now: datetime,
    lookbackMinutes: int,
    reloadFullHistory: bool,
) -> tuple[datetime, datetime]:
    """`usp_GetWatermark`: [last - lookback, now); 1900-01-01 when reloading or first run."""
    if reloadFullHistory or lastWatermark is None:
        return LOW_DATE, now
    return lastWatermark - timedelta(minutes=lookbackMinutes), now


def getWatermark(
    spark: SparkSession, cfg: PipelineConfig, sourceSystemCode: str, objectName: str
) -> tuple[datetime, datetime]:
    fqn = cfg.fqn(WATERMARK_TABLE)
    last: datetime | None = None
    if tableExists(spark, fqn):
        row = (
            spark.table(fqn)
            .where((F.col("source_system_code") == sourceSystemCode) & (F.col("object_name") == objectName))
            .agg(F.max("watermark_value").alias("wm"))
            .collect()[0]
        )
        last = row["wm"]
    return computeWatermarkWindow(last, utcNow(), cfg.lookbackMinutes, cfg.reloadFullHistory)


def setWatermark(
    spark: SparkSession,
    cfg: PipelineConfig,
    sourceSystemCode: str,
    objectName: str,
    watermarkTo: datetime,
) -> None:
    """`usp_SetWatermark`: upsert the object's high-water mark after a successful extract."""
    fqn = cfg.fqn(WATERMARK_TABLE)
    incoming = spark.createDataFrame(
        [(sourceSystemCode, objectName, watermarkTo, cfg.batchId, utcNow())], WATERMARK_SCHEMA
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} ({WATERMARK_SCHEMA}) USING DELTA")
    incoming.createOrReplaceTempView("_cp_watermark_incoming")
    spark.sql(
        f"""
        MERGE INTO {fqn} AS t
        USING _cp_watermark_incoming AS s
          ON t.source_system_code = s.source_system_code AND t.object_name = s.object_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
