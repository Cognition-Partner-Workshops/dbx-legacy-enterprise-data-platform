"""Generic SCD1 / SCD2 dimension maintenance and reserved (unknown) members.

Replaces the SSIS pattern of Lookup (current row) -> Conditional Split
(new / type-2 change / unchanged) -> OLE DB Command (close prior row) ->
OLE DB Destination (insert new version) with a single set-based pass that
returns the full new dimension content.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType
from pyspark.sql.window import Window

from customer_party.config import HIGH_DATE

RESERVED_MEMBERS: tuple[tuple[int, str], ...] = (
    (-1, "Unknown"),
    (-2, "Not Applicable"),
    (-3, "Invalid"),
    (-4, "Inferred Pending"),
    (-9, "Error"),
)
UNKNOWN_KEY = -1
INFERRED_PENDING_KEY = -4


@dataclass(frozen=True)
class ScdSpec:
    keyCol: str
    businessKeyCol: str
    trackedCols: tuple[str, ...]
    validFromCol: str = "valid_from"
    validToCol: str = "valid_to"
    isCurrentCol: str = "is_current_row"
    rowVersionCol: str = "row_version"
    hashCol: str = "source_row_hash"


def rowHash(cols: tuple[str, ...]) -> Column:
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)


def reservedMembers(spark: SparkSession, schema: StructType, spec: ScdSpec, nameCols: tuple[str, ...], extra: dict[str, str] | None = None) -> DataFrame:
    """`Integration.usp_EnsureUnknownMembers`: the fixed negative-key members for a dimension."""
    rows = []
    for key, name in RESERVED_MEMBERS:
        row = {}
        for field in schema.fields:
            row[field.name] = None
        row[spec.keyCol] = key
        row[spec.businessKeyCol] = None
        for c in nameCols:
            row[c] = name
        row[spec.validFromCol] = None
        row[spec.validToCol] = None
        row[spec.isCurrentCol] = True
        row[spec.rowVersionCol] = 1
        row["is_inferred_member"] = False
        if extra:
            row.update(extra)
        rows.append(row)
    nullable = StructType([StructField(f.name, f.dataType, True) for f in schema.fields])
    df = spark.createDataFrame(rows, nullable)
    return df.withColumn(spec.validFromCol, F.lit("1900-01-01 00:00:00").cast("timestamp")).withColumn(
        spec.validToCol, F.lit(HIGH_DATE).cast("timestamp")
    )


def applyScd2(existing: DataFrame | None, incoming: DataFrame, spec: ScdSpec, effectiveTs: Column) -> DataFrame:
    """Return the complete new content of an SCD2 dimension.

    `incoming` carries one row per business key (the staged candidates, already
    hashed into `spec.hashCol`). New keys get a new surrogate; keys whose hash
    differs from the current version get a new version and the prior version is
    closed at `effectiveTs - 1 second`. Reserved members (key < 0) are preserved.
    """
    incoming = incoming.withColumn(spec.hashCol, rowHash(spec.trackedCols)) if spec.hashCol not in incoming.columns else incoming
    if existing is None or existing.rdd.isEmpty():
        maxKey = 0
        current = incoming.limit(0).select(
            F.col(spec.businessKeyCol).alias("_bk"), F.lit(None).cast("string").alias("_hash"), F.lit(None).cast("int").alias("_ver"),
            F.lit(None).cast("boolean").alias("_inferred"),
        )
        history = None
    else:
        maxKey = existing.agg(F.max(spec.keyCol)).collect()[0][0] or 0
        currentRows = existing.where(F.col(spec.isCurrentCol) & (F.col(spec.keyCol) >= 0))
        current = currentRows.select(
            F.col(spec.businessKeyCol).alias("_bk"),
            F.col(spec.hashCol).alias("_hash"),
            F.col(spec.rowVersionCol).alias("_ver"),
            F.coalesce(F.col("is_inferred_member"), F.lit(False)).alias("_inferred"),
        )
        history = existing

    joined = incoming.join(current, incoming[spec.businessKeyCol] == current["_bk"], "left")
    isNew = F.col("_bk").isNull()
    isChanged = F.col("_bk").isNotNull() & ((F.col("_hash") != F.col(spec.hashCol)) | F.col("_inferred"))
    candidates = joined.where(isNew | isChanged)
    versioned = (
        candidates.withColumn(spec.rowVersionCol, F.coalesce(F.col("_ver") + 1, F.lit(1)))
        .withColumn(spec.validFromCol, effectiveTs)
        .withColumn(spec.validToCol, F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn(spec.isCurrentCol, F.lit(True))
        .withColumn("is_inferred_member", F.lit(False))
        .withColumn("_new_key", F.row_number().over(Window.orderBy(spec.businessKeyCol)) + F.lit(maxKey))
        .withColumn(spec.keyCol, F.col("_new_key").cast("bigint"))
        .drop("_bk", "_hash", "_ver", "_inferred", "_new_key")
    )
    if history is None:
        return versioned

    changedKeys = versioned.select(F.col(spec.businessKeyCol).alias("_cbk"), F.col(spec.validFromCol).alias("_new_from"))
    closed = (
        history.join(changedKeys, (history[spec.businessKeyCol] == F.col("_cbk")) & F.col(spec.isCurrentCol) & (F.col(spec.keyCol) >= 0), "left")
        .withColumn(
            spec.validToCol,
            F.when(F.col("_cbk").isNotNull(), F.col("_new_from") - F.expr("INTERVAL 1 SECOND")).otherwise(F.col(spec.validToCol)),
        )
        .withColumn(spec.isCurrentCol, F.when(F.col("_cbk").isNotNull(), F.lit(False)).otherwise(F.col(spec.isCurrentCol)))
        .drop("_cbk", "_new_from")
    )
    return closed.unionByName(versioned.select(*closed.columns), allowMissingColumns=True)


def applyScd1(existing: DataFrame | None, incoming: DataFrame, spec: ScdSpec) -> DataFrame:
    """Return the complete new content of an SCD1 dimension: overwrite in place, insert new keys."""
    incoming = incoming.withColumn(spec.hashCol, rowHash(spec.trackedCols)) if spec.hashCol not in incoming.columns else incoming
    if existing is None or existing.rdd.isEmpty():
        maxKey = 0
        keyed = incoming.withColumn("_existing_key", F.lit(None).cast("bigint"))
        reserved = None
    else:
        maxKey = existing.agg(F.max(spec.keyCol)).collect()[0][0] or 0
        keys = existing.where(F.col(spec.keyCol) >= 0).select(
            F.col(spec.businessKeyCol).alias("_bk"), F.col(spec.keyCol).alias("_existing_key")
        )
        keyed = incoming.join(keys, incoming[spec.businessKeyCol] == keys["_bk"], "left").drop("_bk")
        reserved = existing.where(F.col(spec.keyCol) < 0)
    assigned = (
        keyed.withColumn(
            "_new_key", F.row_number().over(Window.partitionBy(F.col("_existing_key").isNull()).orderBy(spec.businessKeyCol)) + F.lit(maxKey)
        )
        .withColumn(spec.keyCol, F.coalesce(F.col("_existing_key"), F.col("_new_key")).cast("bigint"))
        .withColumn(spec.isCurrentCol, F.lit(True))
        .withColumn(spec.rowVersionCol, F.lit(1))
        .withColumn("is_inferred_member", F.lit(False))
        .drop("_existing_key", "_new_key")
    )
    if reserved is None:
        return assigned
    return reserved.unionByName(assigned, allowMissingColumns=True)


def withReservedMembers(spark: SparkSession, dimension: DataFrame, spec: ScdSpec, nameCols: tuple[str, ...], extra: dict[str, str] | None = None) -> DataFrame:
    """Ensure every reserved member is present exactly once."""
    reserved = reservedMembers(spark, dimension.schema, spec, nameCols, extra)
    existingReserved = dimension.where(F.col(spec.keyCol) < 0).select(spec.keyCol)
    missing = reserved.join(existingReserved, spec.keyCol, "left_anti")
    return dimension.unionByName(missing.select(*dimension.columns))
