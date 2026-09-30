"""Delta table helpers, watermark control and batch/reject logging.

Everything here is I/O; transformation logic lives in the other modules so it
can be unit-tested on a local SparkSession without Delta.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from finance.config import FinanceConfig

WATERMARK_TABLE = "etl_watermark"
BATCH_TABLE = "etl_batch"
REJECT_TABLE = "err_rejected_record"
WATERMARK_EPOCH = "1900-01-01 00:00:00"


@dataclass
class PackageResult:
    package: str
    rowsRead: int = 0
    rowsWritten: int = 0
    rowsRejected: int = 0
    status: str = "SUCCEEDED"
    notes: dict = field(default_factory=dict)

    def asRow(self) -> dict:
        return {
            "package_name": self.package,
            "rows_read": self.rowsRead,
            "rows_written": self.rowsWritten,
            "rows_rejected": self.rowsRejected,
            "status": self.status,
            "notes": json.dumps(self.notes, default=str),
        }


def tableExists(spark: SparkSession, fqName: str) -> bool:
    return spark.catalog.tableExists(fqName)


def readTable(spark: SparkSession, fqName: str) -> DataFrame:
    return spark.table(fqName)


def readTableOrEmpty(spark: SparkSession, fqName: str, schema: T.StructType | str) -> DataFrame:
    if tableExists(spark, fqName):
        return spark.table(fqName)
    return spark.createDataFrame([], schema)


def overwriteTable(df: DataFrame, fqName: str) -> int:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqName))
    return df.sparkSession.table(fqName).count()


def appendTable(df: DataFrame, fqName: str) -> int:
    cnt = df.count()
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqName)
    return cnt


def mergeTable(spark: SparkSession, df: DataFrame, fqName: str, keys: list[str]) -> int:
    """Upsert `df` into `fqName` on `keys` (creates the table when missing)."""
    if not tableExists(spark, fqName):
        return overwriteTable(df, fqName)
    view = "src_" + fqName.replace(".", "_")
    df.createOrReplaceTempView(view)
    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    spark.sql(
        f"MERGE INTO {fqName} AS t USING {view} AS s ON {cond} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    return df.count()


def deleteInsert(spark: SparkSession, df: DataFrame, fqName: str, predicate: str) -> int:
    """Replace the slice of `fqName` matching `predicate` with `df` (date-window / period rebuilds)."""
    if not tableExists(spark, fqName):
        return overwriteTable(df, fqName)
    spark.sql(f"DELETE FROM {fqName} WHERE {predicate}")
    return appendTable(df, fqName)


def ensureControlTables(spark: SparkSession, cfg: FinanceConfig) -> None:
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {cfg.table(WATERMARK_TABLE)} (
            package_name STRING, watermark_column STRING, watermark_value STRING,
            rows_extracted BIGINT, updated_at TIMESTAMP) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {cfg.table(BATCH_TABLE)} (
            batch_id BIGINT, package_name STRING, business_date DATE, accounting_period STRING,
            started_at TIMESTAMP, completed_at TIMESTAMP, status STRING,
            rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT, notes STRING) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {cfg.table(REJECT_TABLE)} (
            package_name STRING, stage STRING, source_object STRING, business_key STRING,
            reject_reason_code STRING, reject_detail STRING, payload STRING,
            batch_id BIGINT, rejected_at TIMESTAMP) USING DELTA"""
    )


def getWatermark(spark: SparkSession, cfg: FinanceConfig, package: str) -> str:
    rows = (
        spark.table(cfg.table(WATERMARK_TABLE))
        .where(F.col("package_name") == package)
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["watermark_value"] if rows else WATERMARK_EPOCH


def setWatermark(
    spark: SparkSession, cfg: FinanceConfig, package: str, column: str, value, rows: int
) -> None:
    if value is None:
        return
    df = spark.createDataFrame(
        [(package, column, str(value), int(rows), datetime.now(timezone.utc))],
        "package_name string, watermark_column string, watermark_value string, rows_extracted bigint, updated_at timestamp",
    )
    df.write.format("delta").mode("append").saveAsTable(cfg.table(WATERMARK_TABLE))


def nextBatchId(spark: SparkSession, cfg: FinanceConfig) -> int:
    row = spark.table(cfg.table(BATCH_TABLE)).agg(F.max("batch_id").alias("m")).collect()[0]
    return int(row["m"] or 0) + 1


def logBatch(
    spark: SparkSession, cfg: FinanceConfig, batchId: int, startedAt: datetime, result: PackageResult
) -> None:
    row = result.asRow()
    df = spark.createDataFrame(
        [
            (
                batchId,
                row["package_name"],
                cfg.businessDate,
                cfg.accountingPeriod,
                startedAt,
                datetime.now(timezone.utc),
                row["status"],
                row["rows_read"],
                row["rows_written"],
                row["rows_rejected"],
                row["notes"],
            )
        ],
        "batch_id bigint, package_name string, business_date date, accounting_period string, started_at timestamp, "
        "completed_at timestamp, status string, rows_read bigint, rows_written bigint, rows_rejected bigint, notes string",
    )
    df.write.format("delta").mode("append").saveAsTable(cfg.table(BATCH_TABLE))


def writeRejects(
    spark: SparkSession,
    cfg: FinanceConfig,
    df: DataFrame,
    package: str,
    stage: str,
    sourceObject: str,
    keyCol: str,
    reasonCol: str,
    detailCol: str | None = None,
    batchId: int = 0,
) -> int:
    """Append rejected rows (payload kept as JSON) to the shared reject table and return the count."""
    if df.isEmpty():
        return 0
    payloadCols = [c for c in df.columns if c not in ("_reject_reason", "_reject_detail")]
    out = df.select(
        F.lit(package).alias("package_name"),
        F.lit(stage).alias("stage"),
        F.lit(sourceObject).alias("source_object"),
        F.col(keyCol).cast("string").alias("business_key"),
        F.col(reasonCol).cast("string").alias("reject_reason_code"),
        (F.col(detailCol).cast("string") if detailCol else F.lit(None).cast("string")).alias("reject_detail"),
        F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in payloadCols])).alias("payload"),
        F.lit(batchId).cast("bigint").alias("batch_id"),
        F.current_timestamp().alias("rejected_at"),
    )
    spark.sql(f"DELETE FROM {cfg.table(REJECT_TABLE)} WHERE package_name = '{package}' AND stage = '{stage}'")
    return appendTable(out, cfg.table(REJECT_TABLE))
