"""Delta table IO helpers (thin wrappers so layers stay declarative)."""
from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def tableExists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def readTable(spark: SparkSession, name: str) -> DataFrame:
    return spark.table(name)


def readTableOrEmpty(spark: SparkSession, name: str, schema: StructType) -> DataFrame:
    if tableExists(spark, name):
        return spark.table(name)
    return spark.createDataFrame([], schema)


def overwriteTable(df: DataFrame, name: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)


def appendTable(df: DataFrame, name: str, mergeSchema: bool = True) -> None:
    writer = df.write.format("delta").mode("append")
    if mergeSchema:
        writer = writer.option("mergeSchema", "true")
    writer.saveAsTable(name)


def replaceWhere(spark: SparkSession, df: DataFrame, name: str, condition: str) -> None:
    """Delete-and-insert a partition of rows (the SSIS `DELETE ... WHERE date = ?; INSERT` idiom)."""
    if not tableExists(spark, name):
        overwriteTable(df, name)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", condition)
        .option("mergeSchema", "true")
        .saveAsTable(name)
    )


def overwriteFromSelf(spark: SparkSession, df: DataFrame, name: str) -> None:
    """Overwrite `name` with a DataFrame that itself reads from `name`.

    The result is materialised in a side table first so the read snapshot is never
    invalidated by the overwrite.
    """
    sideName = f"{name}__rebuild"
    overwriteTable(df, sideName)
    overwriteTable(spark.table(sideName), name)
    spark.sql(f"DROP TABLE IF EXISTS {sideName}")


def truncateReload(df: DataFrame, name: str) -> None:
    overwriteTable(df, name)


def countRows(df: Optional[DataFrame]) -> int:
    return 0 if df is None else df.count()


def selectColumns(df: DataFrame, columns: Sequence[str]) -> DataFrame:
    return df.select(*columns)


def writeBatch(spark: SparkSession, df: DataFrame, name: str, batchId: int, fullReload: bool) -> None:
    """Land one extraction batch: a full-history reload replaces the whole table so a
    re-run does not stack duplicate batches; a watermark run replaces only its own batch."""
    if fullReload:
        overwriteTable(df, name)
    else:
        replaceWhere(spark, df, name, f"batch_id = {batchId}")
