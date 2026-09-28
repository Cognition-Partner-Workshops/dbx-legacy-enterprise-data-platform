"""Deterministic surrogate key allocation (port of Integration.DimensionKeyRegistry,
Integration.usp_AllocateDimensionKeyRange and the Sequences.* sequences).

Design decision (documented in docs/migration/07_dimensions-package-mapping.md):
Delta `GENERATED ALWAYS AS IDENTITY` columns cannot hold the reserved band
(-9..0) that every fact load points at, cannot be pre-assigned to inferred
members before the fact insert, and cannot be given explicit values inside a
MERGE. The legacy estate already had the answer - a registry row per dimension
with `Next Key` starting at 1 above the reserved band - so the registry is kept
as `silver.int_dimension_key_registry` and a contiguous block is handed out per
load with the same read-and-advance semantics as the T-SQL procedure.
"""

from typing import Iterable, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wwi_dimensions import specs

REGISTRY_TABLE = "int_dimension_key_registry"
REGISTRY_SCHEMA = "silver"


def registryTableName(catalog: str) -> str:
    return f"{catalog}.{REGISTRY_SCHEMA}.{REGISTRY_TABLE}"


def ensureKeyRegistry(spark: SparkSession, catalog: str) -> None:
    """Create the registry and seed the rows from 01_dimension_key_registry.sql (idempotent)."""
    table = registryTableName(catalog)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            DimensionName      STRING    NOT NULL,
            TableName          STRING    NOT NULL,
            KeyColumnName      STRING    NOT NULL,
            AllocationMethod   STRING    NOT NULL,
            SequenceName       STRING,
            ReservedKeyLow     INT       NOT NULL,
            ReservedKeyHigh    INT       NOT NULL,
            NextKey            BIGINT    NOT NULL,
            BlockSize          INT       NOT NULL,
            SupportsInferred   BOOLEAN   NOT NULL,
            ScdPattern         STRING    NOT NULL,
            LastAllocatedAt    TIMESTAMP,
            LastAllocatedBy    STRING,
            LastAllocatedCount INT
        ) USING DELTA
        """
    )
    rows = [
        (
            s.name, s.tableName, s.keyColumn, "Registry", s.keyColumn,
            specs.RESERVED_KEY_LOW, specs.RESERVED_KEY_HIGH, specs.FIRST_REAL_KEY,
            specs.DEFAULT_BLOCK_SIZE, s.supportsInferred, s.scdPattern,
        )
        for s in specs.SPECS.values()
    ]
    seed = spark.createDataFrame(
        rows,
        "DimensionName STRING, TableName STRING, KeyColumnName STRING, AllocationMethod STRING, "
        "SequenceName STRING, ReservedKeyLow INT, ReservedKeyHigh INT, NextKey BIGINT, BlockSize INT, "
        "SupportsInferred BOOLEAN, ScdPattern STRING",
    )
    seed.createOrReplaceTempView("_key_registry_seed")
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING _key_registry_seed AS s
          ON t.DimensionName = s.DimensionName
        WHEN NOT MATCHED THEN INSERT (
            DimensionName, TableName, KeyColumnName, AllocationMethod, SequenceName, ReservedKeyLow,
            ReservedKeyHigh, NextKey, BlockSize, SupportsInferred, ScdPattern)
        VALUES (
            s.DimensionName, s.TableName, s.KeyColumnName, s.AllocationMethod, s.SequenceName,
            s.ReservedKeyLow, s.ReservedKeyHigh, s.NextKey, s.BlockSize, s.SupportsInferred, s.ScdPattern)
        """
    )


def synchroniseNextKey(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec) -> int:
    """Move NextKey above any key already present in the dimension.

    The legacy sequences could drift ahead of the table but never behind it; a
    restored table or a rerun that appended rows before the registry write was
    committed must never hand out an existing key.
    """
    table = registryTableName(catalog)
    dimTable = dimensionSpec.fullTableName(catalog)
    maxKey = spark.sql(f"SELECT COALESCE(MAX({dimensionSpec.keyColumn}), 0) AS k FROM {dimTable}").collect()[0]["k"]
    floorKey = max(int(maxKey) + 1, specs.FIRST_REAL_KEY)
    spark.sql(
        f"UPDATE {table} SET NextKey = {floorKey} "
        f"WHERE DimensionName = '{dimensionSpec.name}' AND NextKey < {floorKey}"
    )
    return floorKey


def allocateKeyRange(
    spark: SparkSession,
    catalog: str,
    dimensionName: str,
    requestedCount: int,
    requestedBy: str = None,
) -> Tuple[int, int]:
    """Hand out a contiguous block [firstKey, lastKey] and advance the registry.

    Mirrors usp_AllocateDimensionKeyRange: a request of < 1 is treated as 1, the
    reserved band is never handed out, and the counter is read and advanced in
    one statement (Delta gives serialisable single-table writes, so concurrent
    allocators for the same dimension cannot both see the same NextKey).
    """
    if requestedCount is None or requestedCount < 1:
        requestedCount = 1
    table = registryTableName(catalog)
    row = spark.sql(
        f"SELECT NextKey, ReservedKeyHigh FROM {table} WHERE DimensionName = '{dimensionName}'"
    ).collect()
    if not row:
        raise ValueError(
            f"Dimension {dimensionName} is not present in {table}; no key range can be allocated."
        )
    nextKey = max(int(row[0]["NextKey"]), int(row[0]["ReservedKeyHigh"]) + 1)
    firstKey = nextKey
    lastKey = nextKey + requestedCount - 1
    by = (requestedBy or "").replace("'", "''")
    spark.sql(
        f"""
        UPDATE {table}
           SET NextKey = {lastKey + 1},
               LastAllocatedAt = current_timestamp(),
               LastAllocatedBy = '{by}',
               LastAllocatedCount = {requestedCount}
         WHERE DimensionName = '{dimensionName}' AND NextKey = {int(row[0]["NextKey"])}
        """
    )
    check = spark.sql(f"SELECT NextKey FROM {table} WHERE DimensionName = '{dimensionName}'").collect()[0]["NextKey"]
    if int(check) != lastKey + 1:
        # Somebody else advanced the counter between our read and write: retry once from the new value.
        return allocateKeyRange(spark, catalog, dimensionName, requestedCount, requestedBy)
    return firstKey, lastKey


def assignSurrogateKeys(df: DataFrame, keyColumn: str, firstKey: int, orderColumns: Iterable[str]) -> DataFrame:
    """Number the rows deterministically (ordered by the business key) from firstKey upwards."""
    orderCols = [F.col(c) for c in orderColumns]
    w = Window.orderBy(*orderCols)
    return df.withColumn(keyColumn, (F.lit(firstKey) + F.row_number().over(w) - F.lit(1)).cast("int"))
