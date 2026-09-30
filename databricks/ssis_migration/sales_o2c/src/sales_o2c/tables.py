"""Delta table helpers: idempotent create, MERGE upsert, delete-by-window, audit columns."""
import time
from collections.abc import Callable
from typing import TypeVar

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import RunContext


def withAudit(df: DataFrame, ctx: RunContext) -> DataFrame:
    return (
        df.withColumn("batch_id", F.lit(ctx.batchId).cast("bigint"))
        .withColumn("package_execution_id", F.lit(ctx.packageExecutionId).cast("bigint"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )


def tableExists(spark: SparkSession, fullName: str) -> bool:
    return spark.catalog.tableExists(fullName)

T = TypeVar("T")


def _retryConcurrent(action: Callable[[], T], attempts: int = 5) -> T:
    """Parallel package tasks share control tables; retry Delta concurrency/create races."""
    for attempt in range(attempts):
        try:
            return action()
        except Exception as exc:  # noqa: BLE001
            text = str(exc)
            if attempt == attempts - 1 or not any(k in text for k in ("ALREADY_EXISTS", "Concurrent", "CONCURRENT")):
                raise
            time.sleep(2 + attempt * 3)
    raise RuntimeError("unreachable")


def ensureTable(spark: SparkSession, fullName: str, df: DataFrame) -> None:
    """Create the Delta table from the DataFrame schema when missing (tolerates a concurrent creator)."""
    if not tableExists(spark, fullName):
        try:
            df.limit(0).write.format("delta").mode("overwrite").saveAsTable(fullName)
        except Exception as exc:  # noqa: BLE001
            if "ALREADY_EXISTS" not in str(exc):
                raise


def mergeUpsert(spark: SparkSession, fullName: str, df: DataFrame, keys, updateWhen: str | None = None) -> int:
    """MERGE df into fullName on keys. Returns the number of rows in df (rows read)."""
    ensureTable(spark, fullName, df)
    view = f"src_{abs(hash(fullName)) % 10_000_000}"
    df.createOrReplaceTempView(view)
    onClause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    matched = f"WHEN MATCHED {('AND ' + updateWhen) if updateWhen else ''} THEN UPDATE SET *"
    _retryConcurrent(
        lambda: spark.sql(
            f"""
            MERGE WITH SCHEMA EVOLUTION INTO {fullName} AS t
            USING {view} AS s ON {onClause}
            {matched}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    )
    return df.count()


def appendRows(spark: SparkSession, fullName: str, df: DataFrame) -> int:
    ensureTable(spark, fullName, df)
    _retryConcurrent(lambda: df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fullName))
    return df.count()


def deleteWhere(spark: SparkSession, fullName: str, predicate: str) -> int:
    if not tableExists(spark, fullName):
        return 0
    n = spark.table(fullName).filter(predicate).count()
    if n:
        spark.sql(f"DELETE FROM {fullName} WHERE {predicate}")
    return n


def replaceWhere(spark: SparkSession, fullName: str, df: DataFrame, predicate: str) -> int:
    """Delete-by-window then insert: the legacy 'DELETE ... WHERE date BETWEEN' + INSERT pattern."""
    ensureTable(spark, fullName, df)
    deleteWhere(spark, fullName, predicate)
    return appendRows(spark, fullName, df)
