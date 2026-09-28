"""Delta table helpers shared by every layer."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from sales_lakehouse.common.config import PipelineConfig


def tableExists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)


def overwriteTable(df: DataFrame, fqn: str, partitionBy: list[str] | None = None) -> None:
    """Full rebuild pattern (used by Fact.Sales Margin, aggregates, dims snapshots)."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partitionBy:
        writer = writer.partitionBy(*partitionBy)
    writer.saveAsTable(fqn)


def appendBatch(df: DataFrame, fqn: str, batchCol: str, batchId: int) -> None:
    """Idempotent append: re-running the same batch id replaces that batch."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{batchCol} = {batchId}")
        .saveAsTable(fqn)
    )


def readTable(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame:
    return spark.table(cfg.fqn(layer, table))
