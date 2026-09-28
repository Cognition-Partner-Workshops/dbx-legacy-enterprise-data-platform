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


def mergeByKey(
    spark: SparkSession, df: DataFrame, fqn: str, keyCols: list[str], changeCol: str | None = None
) -> None:
    """Upsert ``df`` into the Delta table ``fqn`` on ``keyCols`` (whole-row update).

    Creates the table on first use. When ``changeCol`` (typically ``row_hash``)
    is given, matched rows are only rewritten when that column differs, so an
    unchanged row keeps the batch that last changed it and re-runs are no-ops.
    """
    from delta.tables import DeltaTable

    if not tableExists(spark, fqn):
        df.write.format("delta").mode("overwrite").saveAsTable(fqn)
        return
    target = DeltaTable.forName(spark, fqn)
    condition = " AND ".join(f"t.`{c}` <=> s.`{c}`" for c in keyCols)
    updateCondition = None if changeCol is None else f"NOT (t.`{changeCol}` <=> s.`{changeCol}`)"
    (
        target.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll(condition=updateCondition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def softDeleteByKey(spark: SparkSession, fqn: str, keys: DataFrame, keyCol: str, batchId: int) -> None:
    """Flag rows whose ``keyCol`` appears in ``keys`` as ``is_deleted`` for ``batchId``.

    Rows already flagged keep their original ``deleted_batch_id`` (idempotent).
    """
    from delta.tables import DeltaTable

    if not tableExists(spark, fqn):
        return
    target = DeltaTable.forName(spark, fqn)
    (
        target.alias("t")
        .merge(keys.select(keyCol).distinct().alias("s"), f"t.`{keyCol}` = s.`{keyCol}` AND NOT t.is_deleted")
        .whenMatchedUpdate(set={"is_deleted": "true", "deleted_batch_id": str(int(batchId))})
        .execute()
    )
