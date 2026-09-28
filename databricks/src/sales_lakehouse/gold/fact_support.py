"""Shared helpers for the gold fact loaders.

Dimension lookups (SCD2 as-of and current), unknown-member defaulting to -1,
deterministic surrogate keys, Delta MERGE / replaceWhere / accumulating
snapshot patterns, and the ``batch_id`` / ``loaded_at_utc`` load metadata
every fact carries.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import tableExists

UNKNOWN_KEY = -1
MONEY = "decimal(19,4)"
RATE = "decimal(19,8)"


def readOptional(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame | None:
    """Read a table that another workstream may not have landed yet."""
    fqn = cfg.fqn(layer, table)
    return spark.table(fqn) if tableExists(spark, fqn) else None


def withLoadMetadata(df: DataFrame, cfg: PipelineConfig) -> DataFrame:
    return df.withColumn("batch_id", F.lit(cfg.batchId).cast(LongType())).withColumn(
        "loaded_at_utc", F.current_timestamp()
    )


def surrogateKey(businessKeyCol: str) -> Column:
    """Deterministic bigint replacement for the legacy IDENTITY surrogate."""
    return F.xxhash64(F.col(businessKeyCol)).cast(LongType())


def unknownIfNull(col: Column) -> Column:
    return F.coalesce(col, F.lit(UNKNOWN_KEY)).cast(LongType())


def lookupScd2Key(
    df: DataFrame,
    dim: DataFrame | None,
    dfKeyCol: str,
    asOfCol: str,
    outCol: str,
    dimBusinessKeyCol: str,
    dimSurrogateCol: str,
    validFromCol: str = "valid_from",
    validToCol: str = "valid_to",
) -> DataFrame:
    """SCD2 as-of lookup: ``valid_from <= asOf < valid_to`` (open row = null valid_to).

    Unknown or unmatched members get ``-1``.
    """
    if dim is None:
        return df.withColumn(outCol, F.lit(UNKNOWN_KEY).cast(LongType()))
    d = dim.select(
        F.col(dimBusinessKeyCol).alias("_bk"),
        F.col(dimSurrogateCol).cast(LongType()).alias("_sk"),
        F.col(validFromCol).cast("date").alias("_vf"),
        F.col(validToCol).cast("date").alias("_vt"),
    )
    asOf = F.col(asOfCol).cast("date")
    cond = (F.col(dfKeyCol) == F.col("_bk")) & (F.col("_vf") <= asOf) & (F.col("_vt").isNull() | (asOf < F.col("_vt")))
    return df.join(d, cond, "left").withColumn(outCol, unknownIfNull(F.col("_sk"))).drop("_bk", "_sk", "_vf", "_vt")


def lookupCurrentKey(
    df: DataFrame,
    dim: DataFrame | None,
    dfKeyCol: str,
    outCol: str,
    dimBusinessKeyCol: str,
    dimSurrogateCol: str,
    isCurrentCol: str | None = None,
) -> DataFrame:
    """Type-1 style lookup on the current dimension row; unmatched -> -1."""
    if dim is None or dfKeyCol not in df.columns:
        return df.withColumn(outCol, F.lit(UNKNOWN_KEY).cast(LongType()))
    d = dim
    if isCurrentCol and isCurrentCol in dim.columns:
        d = d.filter(F.col(isCurrentCol) == F.lit(True))
    d = d.select(
        F.col(dimBusinessKeyCol).alias("_bk"),
        F.col(dimSurrogateCol).cast(LongType()).alias("_sk"),
    ).dropDuplicates(["_bk"])
    return (
        df.join(d, F.col(dfKeyCol) == F.col("_bk"), "left")
        .withColumn(outCol, unknownIfNull(F.col("_sk")))
        .drop("_bk", "_sk")
    )


def dedupeLatest(df: DataFrame, keyCols: Sequence[str], orderCols: Sequence[Column]) -> DataFrame:
    """Keep one row per key (first by ``orderCols``); rejects are the caller's job."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*keyCols).orderBy(*orderCols)
    return df.withColumn("_rn", F.row_number().over(w))


def mergeOnKeys(
    spark: SparkSession,
    df: DataFrame,
    fqn: str,
    keyCols: Sequence[str],
    updateCondition: Column | None = None,
    extraSet: dict[str, Column] | None = None,
) -> None:
    """Idempotent upsert on ``keyCols`` (whenMatchedUpdateAll / whenNotMatchedInsertAll).

    ``extraSet`` overrides individual target columns on update (e.g. restatement
    counters); ``updateCondition`` restricts the update to changed rows.
    """
    if not tableExists(spark, fqn):
        df.write.format("delta").mode("overwrite").saveAsTable(fqn)
        return
    target = DeltaTable.forName(spark, fqn)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keyCols)
    builder = target.alias("t").merge(df.alias("s"), cond)
    if extraSet:
        setMap = {c: F.col(f"s.`{c}`") for c in df.columns}
        setMap.update(extraSet)
        builder = builder.whenMatchedUpdate(condition=updateCondition, set=setMap)
    else:
        builder = builder.whenMatchedUpdateAll(condition=updateCondition)
    builder.whenNotMatchedInsertAll().execute()


def replaceSnapshotDates(df: DataFrame, fqn: str, dateCol: str, snapshotDates: Sequence[str]) -> None:
    """Periodic snapshot write: replace exactly the snapshot dates in this batch."""
    if not snapshotDates:
        return
    inList = ", ".join(f"'{d}'" for d in snapshotDates)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{dateCol} IN ({inList})")
        .option("overwriteSchema", "true")
        .saveAsTable(fqn)
    )


def mergeAccumulating(
    spark: SparkSession,
    incoming: DataFrame,
    fqn: str,
    keyCols: Sequence[str],
    milestoneCols: Sequence[str],
    finalize: Callable[[DataFrame], DataFrame],
) -> None:
    """Accumulating snapshot: milestone dates are COALESCE(existing, incoming) so a
    later load never erases a milestone; ``finalize`` recomputes lag / status
    columns from the coalesced milestones on every merge.
    """
    if tableExists(spark, fqn):
        existing = spark.table(fqn).select(*keyCols, *[F.col(m).alias(f"_ex_{m}") for m in milestoneCols])
        merged = incoming.join(existing, list(keyCols), "left")
        for m in milestoneCols:
            merged = merged.withColumn(m, F.coalesce(F.col(f"_ex_{m}"), F.col(m))).drop(f"_ex_{m}")
    else:
        merged = incoming
    mergeOnKeys(spark, finalize(merged), fqn, keyCols)


def daysBetween(startCol: str, endCol: str) -> Column:
    return F.datediff(F.col(endCol).cast("date"), F.col(startCol).cast("date")).cast("int")


def money(col: Column) -> Column:
    return F.round(col, 4).cast(MONEY)


def pickColumn(df: DataFrame, candidates: Sequence[str], castTo: str | None = None) -> Column:
    """First candidate column present on ``df`` (null literal when none is)."""
    for c in candidates:
        if c in df.columns:
            return F.col(c).cast(castTo) if castTo else F.col(c)
    return F.lit(None).cast(castTo or "string")


def optionalColumn(df: DataFrame, name: str, castTo: str) -> Column:
    return F.col(name).cast(castTo) if name in df.columns else F.lit(None).cast(castTo)
