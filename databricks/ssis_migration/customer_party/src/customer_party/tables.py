"""Delta table write helpers (set-based replacements for SSIS OLE DB destinations)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_party.config import PipelineConfig


def tableExists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)


def withLoadMetadata(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    """Append the batch/load audit columns every SSIS destination carried."""
    return df.withColumn("batch_id", F.lit(cfg.batchId).cast("bigint")).withColumn(
        "load_ts", F.current_timestamp()
    )


def overwriteTable(df: DataFrame, fqn: str) -> None:
    """Truncate-and-reload semantics (SSIS `TRUNCATE TABLE` + OLE DB fast load)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)


def appendTable(df: DataFrame, fqn: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)


def replacePartition(df: DataFrame, fqn: str, column: str, value: str, spark: SparkSession) -> None:
    """`DELETE ... WHERE col = value` followed by an append, as one atomic replaceWhere."""
    if not tableExists(spark, fqn):
        df.write.format("delta").mode("overwrite").saveAsTable(fqn)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{column} = '{value}'")
        .option("mergeSchema", "true")
        .saveAsTable(fqn)
    )


def readTable(spark: SparkSession, fqn: str) -> DataFrame:
    return spark.table(fqn)


def readTableOrEmpty(spark: SparkSession, fqn: str, schemaDdl: str) -> DataFrame:
    if tableExists(spark, fqn):
        return spark.table(fqn)
    return spark.createDataFrame([], schemaDdl)
