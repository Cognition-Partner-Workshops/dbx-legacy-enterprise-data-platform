"""Reusable SCD1 / SCD2 / hybrid dimension load on Delta.

Port of the change detection in Integration.usp_MigrateStaged*Data:

* the source extract is deduplicated by business key first (the legacy
  procedures explicitly avoided one combined MERGE because a source extract can
  carry several rows for the same key);
* Type 2 attributes are compared through a SHA-256 hash of the exact CONCAT_WS
  list the procedure hashed (`RowHashType2`); Type 1 attributes through
  `RowHashType1`;
* a Type 2 change closes the current row (`IsCurrentRow = 0`, `EffectiveTo` and
  `ValidTo` set to the change timestamp) and inserts a new version with
  `VersionNumber + 1`; a change on the same effective date increments
  `EffectiveSequence` instead of producing overlapping windows;
* Type 1 changes are written through to every historical version of the member
  (hybrid dimensions) - `UPDATE ... WHERE [Business Key] = ...`;
* an inferred member that finally arrives is enriched in place (keeps its key,
  `IsInferredMember = 0`, `EnrichedOn` stamped) - usp_InsertInferredMember;
* SCD1 dimensions overwrite the single row per business key.

All writes are Delta MERGE / append statements against
`${catalog}.gold.dim_*`, so a rerun for the same BatchId / BusinessDate finds
matching hashes and changes nothing.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from wwi_dimensions import keys, specs, tables

CHANGE_TYPE_COLUMN = "_ChangeType"
INFERRED_LABEL_PREFIX = "Inferred: "


@dataclass
class ScdResult:
    rowsRead: int = 0
    rowsInserted: int = 0            # brand-new members
    rowsType2Versioned: int = 0      # new versions of existing members
    rowsType1Updated: int = 0        # members whose Type 1 attributes were overwritten
    rowsClosedOut: int = 0
    rowsInferredEnriched: int = 0
    rowsUnchanged: int = 0
    firstAllocatedKey: Optional[int] = None
    lastAllocatedKey: Optional[int] = None
    extra: Dict[str, int] = field(default_factory=dict)

    @property
    def rowsWritten(self) -> int:
        return self.rowsInserted + self.rowsType2Versioned

    @property
    def rowsUpdated(self) -> int:
        return self.rowsType1Updated + self.rowsClosedOut + self.rowsInferredEnriched


def rowHash(columns: Sequence[str]) -> Column:
    """SHA-256 over CONCAT_WS('|', ISNULL(CONVERT(NVARCHAR, col), '')) - same shape as the T-SQL."""
    if not columns:
        return F.lit(None).cast("string")
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def deduplicateSource(df: DataFrame, businessKeyColumn: str, orderColumns: Optional[Iterable[str]] = None) -> DataFrame:
    """Keep exactly one row per business key: the latest by orderColumns (desc), ties broken by hash."""
    ordering = [F.col(c).desc_nulls_last() for c in (orderColumns or [])]
    ordering.append(F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns]), 256))
    w = Window.partitionBy(businessKeyColumn).orderBy(*ordering)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def conformSource(df: DataFrame, dimensionSpec: specs.DimensionSpec) -> DataFrame:
    """Project the source onto the dimension's attribute columns (missing -> NULL, cast to target type)."""
    existing = {c.lower(): c for c in df.columns}
    projected = []
    for name, dtype in dimensionSpec.attributeColumns:
        if name.lower() in existing:
            projected.append(F.col(existing[name.lower()]).cast(dtype).alias(name))
        else:
            projected.append(F.lit(None).cast(dtype).alias(name))
    passthrough = [c for c in df.columns if c.startswith("_") or c == dimensionSpec.changeTimestampColumn]
    passthrough = [c for c in passthrough if c.lower() not in {n.lower() for n, _ in dimensionSpec.attributeColumns}]
    return df.select(*projected, *[F.col(c) for c in passthrough])


def _sqlTimestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _changeTimestamp(df: DataFrame, dimensionSpec: specs.DimensionSpec, now: datetime) -> Column:
    col = dimensionSpec.changeTimestampColumn
    if col and col in df.columns:
        return F.coalesce(F.col(col).cast("timestamp"), F.lit(_sqlTimestamp(now)).cast("timestamp"))
    return F.lit(_sqlTimestamp(now)).cast("timestamp")


def _tempView(spark: SparkSession, df: DataFrame, name: str) -> str:
    df.createOrReplaceTempView(name)
    return name


def applyScd(
    spark: SparkSession,
    catalog: str,
    dimensionSpec: specs.DimensionSpec,
    sourceDf: DataFrame,
    batchId: int,
    packageExecutionId: int,
    loadTimestamp: Optional[datetime] = None,
    lineageKey: Optional[int] = None,
) -> ScdResult:
    """Load one dimension from a conformed staging DataFrame. Returns row counts for logging."""
    now = loadTimestamp or datetime.utcnow().replace(microsecond=0)
    lineageKey = packageExecutionId if lineageKey is None else lineageKey
    tables.ensureDimensionTable(spark, catalog, dimensionSpec)
    keys.ensureKeyRegistry(spark, catalog)
    if dimensionSpec.scdPattern == "SCD1":
        return _applyScd1(spark, catalog, dimensionSpec, sourceDf, batchId, packageExecutionId, now, lineageKey)
    return _applyScd2(spark, catalog, dimensionSpec, sourceDf, batchId, packageExecutionId, now, lineageKey)


# --------------------------------------------------------------------------------------------
# SCD2 / Hybrid
# --------------------------------------------------------------------------------------------

def classifyChanges(sourceDf: DataFrame, currentDf: DataFrame, dimensionSpec: specs.DimensionSpec, now: datetime) -> DataFrame:
    """Join the (deduplicated, hashed) source onto the current dimension rows and label every row.

    Returns the source columns plus `_ChangeType` (NEW / INFERRED / TYPE2 / TYPE1 / UNCHANGED),
    `_Type1Changed`, `_CurrentKey`, `_CurrentVersion`, `_CurrentEffectiveFrom`,
    `_CurrentEffectiveFromDate`, `_CurrentEffectiveSequence` and `_ChangeTimestamp`.
    """
    bk = dimensionSpec.businessKeyColumn
    s = sourceDf.alias("s")
    t = currentDf.select(
        F.col(bk).alias("_t_bk"),
        F.col(dimensionSpec.keyColumn).alias("_CurrentKey"),
        F.col("VersionNumber").alias("_CurrentVersion"),
        F.col("EffectiveFrom").alias("_CurrentEffectiveFrom"),
        F.col("EffectiveFromDate").alias("_CurrentEffectiveFromDate"),
        F.col("EffectiveSequence").alias("_CurrentEffectiveSequence"),
        F.col("RowHashType2").alias("_CurrentHash2"),
        F.col("RowHashType1").alias("_CurrentHash1"),
        F.col("IsInferredMember").alias("_CurrentIsInferred"),
    ).alias("t")
    joined = s.join(t, F.col(f"s.{bk}") == F.col("t._t_bk"), "left")
    changeTs = _changeTimestamp(sourceDf, dimensionSpec, now)
    hash2Changed = F.coalesce(F.col("_CurrentHash2") != F.col("RowHashType2"), F.lit(True))
    hash1Changed = (
        F.coalesce(F.col("_CurrentHash1") != F.col("RowHashType1"), F.lit(True))
        if dimensionSpec.scd1Columns else F.lit(False)
    )
    changeType = (
        F.when(F.col("_CurrentKey").isNull(), F.lit("NEW"))
        .when(F.col("_CurrentIsInferred") == True, F.lit("INFERRED"))  # noqa: E712
        .when(hash2Changed, F.lit("TYPE2"))
        .when(hash1Changed, F.lit("TYPE1"))
        .otherwise(F.lit("UNCHANGED"))
    )
    return (
        joined.withColumn("_ChangeTimestamp", changeTs)
        .withColumn(CHANGE_TYPE_COLUMN, changeType)
        .withColumn("_Type1Changed", F.col("_CurrentKey").isNotNull() & (F.col("_CurrentIsInferred") == False) & hash1Changed)  # noqa: E712
        .drop("_t_bk", "_CurrentHash2", "_CurrentHash1", "_CurrentIsInferred")
    )


def _applyScd2(spark, catalog, dimensionSpec, sourceDf, batchId, packageExecutionId, now, lineageKey) -> ScdResult:
    table = dimensionSpec.fullTableName(catalog)
    bk = dimensionSpec.businessKeyColumn
    key = dimensionSpec.keyColumn
    result = ScdResult(rowsRead=sourceDf.count())

    src = conformSource(sourceDf, dimensionSpec)
    src = deduplicateSource(src, bk, [dimensionSpec.changeTimestampColumn] if dimensionSpec.changeTimestampColumn in src.columns else None)
    src = src.withColumn("RowHashType2", rowHash(dimensionSpec.scd2Columns))
    src = src.withColumn("RowHashType1", rowHash(dimensionSpec.scd1Columns))

    current = spark.table(table).where((F.col("IsCurrentRow") == True) & (F.col(key) > 0))  # noqa: E712
    classified = classifyChanges(src, current, dimensionSpec, now).localCheckpoint()

    counts = {r[CHANGE_TYPE_COLUMN]: r["n"] for r in classified.groupBy(CHANGE_TYPE_COLUMN).count().withColumnRenamed("count", "n").collect()}
    result.rowsUnchanged = int(counts.get("UNCHANGED", 0))
    result.rowsInferredEnriched = int(counts.get("INFERRED", 0))
    newCount = int(counts.get("NEW", 0))
    type2Count = int(counts.get("TYPE2", 0))
    nowLit = f"TIMESTAMP '{_sqlTimestamp(now)}'"
    audit = f"LastLoadBatchId = {int(batchId)}, LastLoadPackageExecutionId = {int(packageExecutionId)}"

    # 1. Type 1 write-through across every version of the member (hybrid dimensions only).
    if dimensionSpec.scd1Columns:
        type1 = classified.where(F.col("_Type1Changed"))
        result.rowsType1Updated = type1.count()
        if result.rowsType1Updated:
            view = _tempView(spark, type1, "_scd_type1")
            sets = ", ".join(f"t.{c} = s.{c}" for c in dimensionSpec.scd1Columns)
            spark.sql(
                f"""
                MERGE INTO {table} AS t
                USING {view} AS s ON t.{bk} = s.{bk} AND t.{key} > 0
                WHEN MATCHED THEN UPDATE SET {sets}, t.RowHashType1 = s.RowHashType1, t.{audit.replace(', ', ', t.')}
                """
            )

    # 2. Inferred members that have now arrived: enrich in place, keep the key.
    if result.rowsInferredEnriched:
        inferred = classified.where(F.col(CHANGE_TYPE_COLUMN) == "INFERRED")
        view = _tempView(spark, inferred, "_scd_inferred")
        sets = ", ".join(f"t.{c} = s.{c}" for c in dimensionSpec.attributeNames if c != bk)
        spark.sql(
            f"""
            MERGE INTO {table} AS t
            USING {view} AS s ON t.{key} = s._CurrentKey
            WHEN MATCHED THEN UPDATE SET {sets}, t.RowHashType2 = s.RowHashType2, t.RowHashType1 = s.RowHashType1,
                 t.IsInferredMember = false, t.EnrichedOn = {nowLit}, t.LineageKey = {int(lineageKey)},
                 t.{audit.replace(', ', ', t.')}
            """
        )

    # 3. Close the superseded current version of every Type 2 change.
    if type2Count:
        type2 = classified.where(F.col(CHANGE_TYPE_COLUMN) == "TYPE2")
        view = _tempView(spark, type2, "_scd_closeout")
        spark.sql(
            f"""
            MERGE INTO {table} AS t
            USING {view} AS s ON t.{key} = s._CurrentKey
            WHEN MATCHED THEN UPDATE SET
                 t.IsCurrentRow = false,
                 t.EffectiveTo = GREATEST(s._ChangeTimestamp, s._CurrentEffectiveFrom),
                 t.ValidTo = GREATEST(s._ChangeTimestamp, s._CurrentEffectiveFrom),
                 t.{audit.replace(', ', ', t.')}
            """
        )
        result.rowsClosedOut = type2Count

    # 4. Insert brand-new members and the new versions in one deterministic, key-ordered append.
    insertCount = newCount + type2Count
    if insertCount:
        firstKey, lastKey = keys.allocateKeyRange(spark, catalog, dimensionSpec.name, insertCount, f"pkgexec:{packageExecutionId}")
        result.firstAllocatedKey, result.lastAllocatedKey = firstKey, lastKey
        toInsert = classified.where(F.col(CHANGE_TYPE_COLUMN).isin("NEW", "TYPE2"))
        effectiveFrom = F.when(F.col("_CurrentEffectiveFrom").isNotNull(),
                               F.greatest(F.col("_ChangeTimestamp"), F.col("_CurrentEffectiveFrom"))).otherwise(F.col("_ChangeTimestamp"))
        toInsert = (
            toInsert.withColumn("EffectiveFrom", effectiveFrom)
            .withColumn("EffectiveTo", F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"))
            .withColumn("EffectiveFromDate", F.to_date(F.col("EffectiveFrom")))
            .withColumn("EffectiveSequence",
                        F.when(F.col("_CurrentEffectiveFromDate") == F.to_date(F.col("EffectiveFrom")),
                               F.coalesce(F.col("_CurrentEffectiveSequence"), F.lit(0)) + 1).otherwise(F.lit(1)).cast("smallint"))
            .withColumn("IsCurrentRow", F.lit(True))
            .withColumn("VersionNumber", (F.coalesce(F.col("_CurrentVersion"), F.lit(0)) + 1).cast("int"))
            .withColumn("IsInferredMember", F.lit(False))
            .withColumn("InferredCreatedOn", F.lit(None).cast("timestamp"))
            .withColumn("EnrichedOn", F.lit(None).cast("timestamp"))
            .withColumn("ValidFrom", F.col("EffectiveFrom"))
            .withColumn("ValidTo", F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"))
            .withColumn("LineageKey", F.lit(int(lineageKey)).cast("bigint"))
            .withColumn("LastLoadBatchId", F.lit(int(batchId)).cast("bigint"))
            .withColumn("LastLoadPackageExecutionId", F.lit(int(packageExecutionId)).cast("bigint"))
        )
        toInsert = keys.assignSurrogateKeys(toInsert, key, firstKey, [bk])
        _appendRows(toInsert, table, dimensionSpec)
        result.rowsInserted = newCount
        result.rowsType2Versioned = type2Count

    classified.unpersist()
    return result


def _appendRows(df: DataFrame, table: str, dimensionSpec: specs.DimensionSpec) -> None:
    ordered = [F.col(name).cast(dtype).alias(name) for name, dtype in dimensionSpec.allColumns]
    df.select(*ordered).write.format("delta").mode("append").saveAsTable(table)


# --------------------------------------------------------------------------------------------
# SCD1
# --------------------------------------------------------------------------------------------

def _applyScd1(spark, catalog, dimensionSpec, sourceDf, batchId, packageExecutionId, now, lineageKey) -> ScdResult:
    table = dimensionSpec.fullTableName(catalog)
    bk = dimensionSpec.businessKeyColumn
    key = dimensionSpec.keyColumn
    result = ScdResult(rowsRead=sourceDf.count())

    src = conformSource(sourceDf, dimensionSpec)
    src = deduplicateSource(src, bk)
    src = src.withColumn("RowHashType1", rowHash(dimensionSpec.scd1Columns))
    current = spark.table(table).where(F.col(key) > 0).select(
        F.col(bk).alias("_t_bk"), F.col(key).alias("_CurrentKey"), F.col("RowHashType1").alias("_CurrentHash1"))
    classified = src.join(current, src[bk] == current["_t_bk"], "left").withColumn(
        CHANGE_TYPE_COLUMN,
        F.when(F.col("_CurrentKey").isNull(), F.lit("NEW"))
        .when(F.coalesce(F.col("_CurrentHash1") != F.col("RowHashType1"), F.lit(True)), F.lit("TYPE1"))
        .otherwise(F.lit("UNCHANGED")),
    ).drop("_t_bk", "_CurrentHash1").localCheckpoint()

    counts = {r[CHANGE_TYPE_COLUMN]: r["n"] for r in classified.groupBy(CHANGE_TYPE_COLUMN).count().withColumnRenamed("count", "n").collect()}
    result.rowsUnchanged = int(counts.get("UNCHANGED", 0))
    newCount = int(counts.get("NEW", 0))
    changedCount = int(counts.get("TYPE1", 0))
    audit = f"t.LastLoadBatchId = {int(batchId)}, t.LastLoadPackageExecutionId = {int(packageExecutionId)}"

    if changedCount:
        view = _tempView(spark, classified.where(F.col(CHANGE_TYPE_COLUMN) == "TYPE1"), "_scd1_changed")
        sets = ", ".join(f"t.{c} = s.{c}" for c in dimensionSpec.attributeNames if c != bk)
        spark.sql(
            f"""
            MERGE INTO {table} AS t
            USING {view} AS s ON t.{key} = s._CurrentKey
            WHEN MATCHED THEN UPDATE SET {sets}, t.RowHashType1 = s.RowHashType1, t.LineageKey = {int(lineageKey)}, {audit}
            """
        )
        result.rowsType1Updated = changedCount

    if newCount:
        firstKey, lastKey = keys.allocateKeyRange(spark, catalog, dimensionSpec.name, newCount, f"pkgexec:{packageExecutionId}")
        result.firstAllocatedKey, result.lastAllocatedKey = firstKey, lastKey
        toInsert = (
            classified.where(F.col(CHANGE_TYPE_COLUMN) == "NEW")
            .withColumn("IsCurrentRow", F.lit(True))
            .withColumn("ValidFrom", F.lit(_sqlTimestamp(now)).cast("timestamp"))
            .withColumn("ValidTo", F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"))
            .withColumn("LineageKey", F.lit(int(lineageKey)).cast("bigint"))
            .withColumn("LastLoadBatchId", F.lit(int(batchId)).cast("bigint"))
            .withColumn("LastLoadPackageExecutionId", F.lit(int(packageExecutionId)).cast("bigint"))
        )
        toInsert = keys.assignSurrogateKeys(toInsert, key, firstKey, [bk])
        _appendRows(toInsert, table, dimensionSpec)
        result.rowsInserted = newCount

    classified.unpersist()
    return result


# --------------------------------------------------------------------------------------------
# Targeted overwrites / expiry / inferred stubs
# --------------------------------------------------------------------------------------------

def applyType1Overwrite(
    spark: SparkSession,
    catalog: str,
    dimensionSpec: specs.DimensionSpec,
    df: DataFrame,
    columns: Sequence[str],
    batchId: int,
    packageExecutionId: int,
    currentOnly: bool = False,
) -> int:
    """Overwrite `columns` on every (or only the current) version of the members in df (matched on business key)."""
    bk = dimensionSpec.businessKeyColumn
    key = dimensionSpec.keyColumn
    table = dimensionSpec.fullTableName(catalog)
    rows = deduplicateSource(df.select(bk, *columns), bk)
    n = rows.count()
    if not n:
        return 0
    view = _tempView(spark, rows, "_scd_overwrite")
    sets = ", ".join(f"t.{c} = s.{c}" for c in columns)
    scope = " AND t.IsCurrentRow = true" if currentOnly else ""
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING {view} AS s ON t.{bk} = s.{bk} AND t.{key} > 0{scope}
        WHEN MATCHED THEN UPDATE SET {sets}, t.LastLoadBatchId = {int(batchId)}, t.LastLoadPackageExecutionId = {int(packageExecutionId)}
        """
    )
    return n


def expireCurrentRows(
    spark: SparkSession,
    catalog: str,
    dimensionSpec: specs.DimensionSpec,
    predicateSql: str,
    closeTimestamp: datetime,
    batchId: int,
    packageExecutionId: int,
    extraSetSql: str = "",
) -> int:
    """Close current rows matching predicateSql (evaluated on alias t) - e.g. lapsed contracts."""
    table = dimensionSpec.fullTableName(catalog)
    key = dimensionSpec.keyColumn
    where = f"t.IsCurrentRow = true AND t.{key} > 0 AND ({predicateSql})"
    n = spark.sql(f"SELECT COUNT(*) AS c FROM {table} AS t WHERE {where}").collect()[0]["c"]
    if n:
        ts = f"TIMESTAMP '{_sqlTimestamp(closeTimestamp)}'"
        extra = f", {extraSetSql}" if extraSetSql else ""
        spark.sql(
            f"""
            UPDATE {table} AS t
               SET t.IsCurrentRow = false,
                   t.ValidTo = LEAST(t.ValidTo, {ts}),
                   t.EffectiveTo = LEAST(t.EffectiveTo, {ts}),
                   t.LastLoadBatchId = {int(batchId)},
                   t.LastLoadPackageExecutionId = {int(packageExecutionId)}{extra}
             WHERE {where}
            """
        )
    return int(n)


def insertInferredMembers(
    spark: SparkSession,
    catalog: str,
    dimensionSpec: specs.DimensionSpec,
    businessKeys: DataFrame,
    batchId: int,
    packageExecutionId: int,
    loadTimestamp: Optional[datetime] = None,
    sourceSystemCode: Optional[str] = None,
) -> DataFrame:
    """Port of Integration.usp_InsertInferredMember: create a positive-key stub per missing business key.

    `businessKeys` must carry the business key column and may carry `RegionCode`.
    Returns a DataFrame (businessKey, surrogateKey) for the members that exist after the call
    (existing current members are returned as-is; only genuinely missing keys are inserted).
    """
    if not dimensionSpec.supportsInferred:
        raise ValueError(f"{dimensionSpec.name} does not support inferred members (see 01_dimension_key_registry.sql)")
    now = loadTimestamp or datetime.utcnow().replace(microsecond=0)
    table = tables.ensureDimensionTable(spark, catalog, dimensionSpec)
    keys.ensureKeyRegistry(spark, catalog)
    bk = dimensionSpec.businessKeyColumn
    key = dimensionSpec.keyColumn
    wanted = businessKeys.select(F.col(bk).cast("string").alias(bk), *[c for c in businessKeys.columns if c == "RegionCode"]).dropDuplicates([bk])
    current = spark.table(table).where((F.col("IsCurrentRow") == True) & (F.col(key) > 0)).select(F.col(bk).alias("_t_bk"), F.col(key).alias("_ExistingKey"))  # noqa: E712
    joined = wanted.join(current, wanted[bk] == current["_t_bk"], "left").drop("_t_bk")
    missing = joined.where(F.col("_ExistingKey").isNull()).drop("_ExistingKey")
    missingCount = missing.count()
    if missingCount:
        firstKey, _ = keys.allocateKeyRange(spark, catalog, dimensionSpec.name, missingCount, f"inferred:{packageExecutionId}")
        stub = missing
        for name, dtype in dimensionSpec.attributeColumns:
            if name == bk or name in stub.columns:
                stub = stub.withColumn(name, F.col(name).cast(dtype))
            elif name in dimensionSpec.labelColumns:
                stub = stub.withColumn(name, F.concat(F.lit(INFERRED_LABEL_PREFIX), F.col(bk)).cast(dtype))
            elif name == "SourceSystemCode":
                stub = stub.withColumn(name, F.lit(sourceSystemCode).cast(dtype))
            else:
                stub = stub.withColumn(name, F.lit(None).cast(dtype))
        nowCol = F.lit(_sqlTimestamp(now)).cast("timestamp")
        stub = (
            stub.withColumn("EffectiveFrom", nowCol)
            .withColumn("EffectiveTo", F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"))
            .withColumn("EffectiveFromDate", F.to_date(nowCol))
            .withColumn("EffectiveSequence", F.lit(1).cast("smallint"))
            .withColumn("IsCurrentRow", F.lit(True))
            .withColumn("VersionNumber", F.lit(1))
            .withColumn("RowHashType2", F.lit(None).cast("string"))
            .withColumn("RowHashType1", F.lit(None).cast("string"))
            .withColumn("IsInferredMember", F.lit(True))
            .withColumn("InferredCreatedOn", nowCol)
            .withColumn("EnrichedOn", F.lit(None).cast("timestamp"))
            .withColumn("ValidFrom", nowCol)
            .withColumn("ValidTo", F.lit(specs.OPEN_ENDED_TIMESTAMP).cast("timestamp"))
            .withColumn("LineageKey", F.lit(int(packageExecutionId)).cast("bigint"))
            .withColumn("LastLoadBatchId", F.lit(int(batchId)).cast("bigint"))
            .withColumn("LastLoadPackageExecutionId", F.lit(int(packageExecutionId)).cast("bigint"))
        )
        stub = keys.assignSurrogateKeys(stub, key, firstKey, [bk])
        _appendRows(stub, table, dimensionSpec)
    refreshed = spark.table(table).where((F.col("IsCurrentRow") == True) & (F.col(key) > 0)).select(bk, key, "IsInferredMember")  # noqa: E712
    return wanted.select(bk).join(refreshed, bk, "inner")


def currentMembers(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec) -> DataFrame:
    """Current real members (key > 0) - the lookup the SSIS packages used against `Is Current Row = 1`."""
    table = dimensionSpec.fullTableName(catalog)
    return spark.table(table).where((F.col("IsCurrentRow") == True) & (F.col(dimensionSpec.keyColumn) > 0))  # noqa: E712
