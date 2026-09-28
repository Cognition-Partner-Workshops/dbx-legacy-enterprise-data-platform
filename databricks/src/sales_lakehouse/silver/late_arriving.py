"""Late-arriving dimension queue - port of ``work.usp_QueueLateArrivingDimensions``.

Legacy: sqlserver/staging/procedures/work.usp_QueueLateArrivingDimensions.sql
and sqlserver/staging/tables/30_work_tables.sql (work.LateArrivingDimensionQueue).
The intent (proc header lines 12-29): a fact row that references a dimension
member not yet staged is *not* rejected; the missing key is queued, the fact
row is loaded with ``DqStatusCode = 'WARN'`` against the ``-1`` unknown member,
and the row is re-keyed once the dimension arrives (work.FactRekeyQueue).

Queue semantics reproduced here:
  * merge key is ``(entity_type, business_key)`` (lines 148-174);
  * an unresolved key seen again increments ``occurrence_count``;
  * a key whose dimension has now arrived gets ``resolved_batch_id`` (lines 178-199).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import mergeByKey, tableExists

QUEUE_TABLE = "late_arriving_dimension_queue"
QUEUE_KEY_COLS = ["entity_type", "business_key"]


def flagMissingReferences(df: DataFrame, keyCol: str, dimensionKeys: DataFrame, flagCol: str) -> DataFrame:
    """Add boolean ``flagCol``: True when ``df[keyCol]`` is non-null and absent
    from ``dimensionKeys`` (a one-column DataFrame named ``business_key``)."""
    dims = dimensionKeys.select(F.col("business_key").alias("_dim_key")).distinct()
    joined = df.join(dims, F.col(keyCol) == F.col("_dim_key"), "left")
    return joined.withColumn(flagCol, F.col(keyCol).isNotNull() & F.col("_dim_key").isNull()).drop("_dim_key")


def collectMissing(df: DataFrame, entityType: str, keyCol: str, flagCol: str, sourceObjectName: str) -> DataFrame:
    """Aggregate the missing keys of one entity type into queue shape."""
    return (
        df.filter(F.col(flagCol))
        .groupBy(F.col(keyCol).alias("business_key"))
        .agg(
            F.first("source_system_code", ignorenulls=True).alias("source_system_code"),
            F.count(F.lit(1)).cast("bigint").alias("occurrence_count"),
        )
        .select(
            F.lit(entityType).alias("entity_type"),
            "business_key",
            "source_system_code",
            F.lit(sourceObjectName).alias("first_seen_object_name"),
            "occurrence_count",
        )
    )


def mergeQueue(
    spark: SparkSession,
    cfg: PipelineConfig,
    missing: DataFrame,
    availableKeys: DataFrame,
) -> None:
    """Upsert ``missing`` into the queue and resolve entries whose dimension arrived.

    ``availableKeys`` is a DataFrame of ``(entity_type, business_key)`` present
    in the bronze dimension sources for this run.
    """
    fqn = cfg.fqn("silver", QUEUE_TABLE)
    batchId = F.lit(cfg.batchId).cast("bigint")
    incoming = missing.select(
        "entity_type",
        "business_key",
        "source_system_code",
        "first_seen_object_name",
        "occurrence_count",
        batchId.alias("first_seen_batch_id"),
        batchId.alias("last_seen_batch_id"),
        F.lit(None).cast("bigint").alias("resolved_batch_id"),
        F.lit(False).alias("is_resolved"),
    )
    if tableExists(spark, fqn):
        existing = spark.table(fqn)
        # LEGACY QUIRK: re-running the same batch must not double count, so the
        # occurrence increment only applies to entries first seen in an earlier batch.
        merged = (
            incoming.alias("i")
            .join(existing.alias("e"), QUEUE_KEY_COLS, "left")
            .select(
                F.col("i.entity_type"),
                F.col("i.business_key"),
                F.coalesce(F.col("e.source_system_code"), F.col("i.source_system_code")).alias("source_system_code"),
                F.coalesce(F.col("e.first_seen_object_name"), F.col("i.first_seen_object_name")).alias(
                    "first_seen_object_name"
                ),
                F.when(
                    F.col("e.business_key").isNotNull() & (F.col("e.last_seen_batch_id") != cfg.batchId),
                    F.col("e.occurrence_count") + F.col("i.occurrence_count"),
                )
                .when(F.col("e.business_key").isNotNull(), F.col("e.occurrence_count"))
                .otherwise(F.col("i.occurrence_count"))
                .cast("bigint")
                .alias("occurrence_count"),
                F.coalesce(F.col("e.first_seen_batch_id"), F.col("i.first_seen_batch_id")).alias("first_seen_batch_id"),
                F.col("i.last_seen_batch_id"),
                F.lit(None).cast("bigint").alias("resolved_batch_id"),
                F.lit(False).alias("is_resolved"),
            )
        )
        # Lines 178-199: queued keys that now exist in the dimension are resolved.
        resolved = (
            existing.filter(~F.col("is_resolved"))
            .join(availableKeys.select(*QUEUE_KEY_COLS).distinct(), QUEUE_KEY_COLS, "left_semi")
            .withColumn("resolved_batch_id", batchId)
            .withColumn("is_resolved", F.lit(True))
        )
        upsert = merged.unionByName(resolved.select(*merged.columns))
    else:
        upsert = incoming
    upsert = upsert.withColumn("batch_id", batchId).withColumn("loaded_at_utc", F.current_timestamp())
    mergeByKey(spark, upsert, fqn, QUEUE_KEY_COLS)
