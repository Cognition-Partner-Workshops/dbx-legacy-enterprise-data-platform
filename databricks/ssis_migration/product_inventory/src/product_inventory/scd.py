"""Pure slowly-changing-dimension and lookup algorithms.

These take and return DataFrames only (no IO) so the SCD2 / SCD1 / as-of lookup
behaviour of the SSIS packages can be unit-tested on local Spark.

Metadata columns shared by every dimension produced here:
  valid_from, valid_to, is_current_row, row_version, type1_hash, type2_hash,
  is_inferred_member, is_reserved_member, lineage_key
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Mapping, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DateType, DecimalType, DoubleType, FloatType, StringType, StructField, StructType, TimestampType

from product_inventory.config import FAR_FUTURE

META_COLUMNS = [
    "valid_from",
    "valid_to",
    "is_current_row",
    "row_version",
    "type1_hash",
    "type2_hash",
    "is_inferred_member",
    "is_reserved_member",
    "lineage_key",
]


def rowHash(columns: Sequence[str]) -> F.Column:
    """`HASHBYTES('SHA2_256', CONCAT_WS('|', ISNULL(col,'') ...))` equivalent."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def dedupeLatest(df: DataFrame, keyCols: Sequence[str], orderCols: Sequence[F.Column]) -> DataFrame:
    """Keep one row per key: the last one according to `orderCols` (ascending)."""
    window = Window.partitionBy(*keyCols).orderBy(*[c.desc() for c in orderCols])
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def reservedMemberRows(template: DataFrame, members: Sequence[Mapping[str, object]]) -> DataFrame:
    """Build reserved (unknown / not-applicable) member rows shaped like `template`."""
    spark = template.sparkSession
    rows = []
    for member in members:
        rows.append(tuple(_coerceLiteral(member.get(field.name), field.dataType) for field in template.schema.fields))
    # Timestamps travel as ISO strings: far-future literals (9999-12-31) overflow the
    # client-side Arrow conversion in Spark Connect when shifted into the session zone.
    transportSchema = StructType(
        [StructField(f.name, StringType() if isinstance(f.dataType, TimestampType) else f.dataType, True) for f in template.schema.fields]
    )
    df = spark.createDataFrame(rows, transportSchema)
    for f in template.schema.fields:
        if isinstance(f.dataType, TimestampType):
            df = df.withColumn(f.name, F.to_timestamp(F.col(f.name), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
    return df.select(*[F.col(f.name).cast(f.dataType) for f in template.schema.fields])


def _coerceLiteral(value: object, dataType: DataType) -> object:
    """Spark Connect's Arrow conversion is strict about Python types; align literals."""
    if value is None:
        return None
    if isinstance(dataType, DecimalType) and not isinstance(value, Decimal):
        return Decimal(str(value))
    if isinstance(dataType, (DoubleType, FloatType)) and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if isinstance(dataType, DateType) and isinstance(value, datetime):
        return value.date()
    if isinstance(dataType, TimestampType) and isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return value


def applyHybridScd2(
    existingDf: DataFrame,
    incomingDf: DataFrame,
    naturalKey: str,
    surrogateKey: str,
    type1Cols: Sequence[str],
    type2Cols: Sequence[str],
    lineageKey: int,
    farFuture: datetime = FAR_FUTURE,
) -> DataFrame:
    """Hybrid SCD (DIM_Load_StockItem) over a set of source *versions*.

    `incomingDf` must carry `naturalKey`, the attribute columns and `valid_from`.
    Several versions of the same key may arrive in one run (full-history reload from
    the temporal source); they are processed in `valid_from` order:

      * new key                     -> insert, row_version 1
      * type-2 hash differs         -> close current row (valid_to = new valid_from - 1s),
                                       insert new version (row_version + 1)
      * only type-1 hash differs    -> overwrite type-1 attributes on every version
      * current row is inferred     -> enrich in place (keep key, clear inferred flag)
      * reserved members (<= 0)     -> passed through untouched
    """
    attrCols = [c for c in incomingDf.columns if c not in (naturalKey, "valid_from") and c not in META_COLUMNS]
    outputCols = [surrogateKey, naturalKey, *attrCols, *META_COLUMNS]

    reserved = existingDf.where(F.col("is_reserved_member")).select(*outputCols)
    history = existingDf.where(~F.col("is_reserved_member"))

    incoming = (
        incomingDf.withColumn("type1_hash", rowHash(type1Cols))
        .withColumn("type2_hash", rowHash(type2Cols))
        .withColumn("_seq", F.lit(1))
        .withColumn(surrogateKey, F.lit(None).cast("long"))
        .withColumn("row_version", F.lit(None).cast("int"))
        .withColumn("is_inferred_member", F.lit(False))
        .withColumn("_is_anchor", F.lit(False))
    )
    incoming = dedupeLatest(incoming, [naturalKey, "valid_from"], [F.col("type2_hash"), F.col("type1_hash")])

    anchorCols = [surrogateKey, naturalKey, *attrCols, "valid_from", "row_version", "type1_hash", "type2_hash", "is_inferred_member"]
    anchor = (
        history.where(F.col("is_current_row"))
        .select(*anchorCols)
        .withColumn("_seq", F.lit(0))
        .withColumn("_is_anchor", F.lit(True))
    )
    anchorInfo = anchor.select(
        F.col(naturalKey).alias("_k"),
        F.col("valid_from").alias("_anchor_valid_from"),
        F.col("is_inferred_member").alias("_anchor_inferred"),
        F.col(surrogateKey).alias("_anchor_key"),
        F.col("row_version").alias("_anchor_version"),
    )
    incomingScoped = (
        incoming.join(anchorInfo, incoming[naturalKey] == anchorInfo["_k"], "left")
        .where(
            F.col("_anchor_valid_from").isNull()
            | F.col("_anchor_inferred")
            | (F.col("valid_from") >= F.col("_anchor_valid_from"))
        )
        .drop("_k", "_anchor_valid_from", "_anchor_inferred", "_anchor_key", "_anchor_version")
    )
    # an inferred anchor is replaced by the first real version (enrichment in place)
    realAnchor = anchor.where(~F.col("is_inferred_member"))
    sequence = realAnchor.select(*incomingScoped.columns).unionByName(incomingScoped)

    keyWindow = Window.partitionBy(naturalKey).orderBy(F.col("_seq"), F.col("valid_from"))
    sequence = sequence.withColumn("_prev_type2", F.lag("type2_hash").over(keyWindow))
    sequence = sequence.withColumn(
        "_kept", F.col("_is_anchor") | F.col("_prev_type2").isNull() | (F.col("_prev_type2") != F.col("type2_hash"))
    )
    sequence = sequence.withColumn(
        "_group", F.sum(F.col("_kept").cast("int")).over(keyWindow.rowsBetween(Window.unboundedPreceding, 0))
    )
    groupWindow = Window.partitionBy(naturalKey, "_group").orderBy(F.col("_seq"), F.col("valid_from"))
    groupWindowAll = groupWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    collapsed = sequence.withColumn("_pos", F.row_number().over(groupWindow))
    for c in type1Cols:
        collapsed = collapsed.withColumn(c, F.last(F.col(c)).over(groupWindowAll))
    collapsed = collapsed.withColumn("type1_hash", F.last(F.col("type1_hash")).over(groupWindowAll))
    collapsed = collapsed.where(F.col("_pos") == 1).drop("_pos", "_prev_type2", "_kept")

    # valid_to / is_current across the kept versions of each key
    versionWindow = Window.partitionBy(naturalKey).orderBy(F.col("_group"))
    collapsed = collapsed.withColumn("_next_valid_from", F.lead("valid_from").over(versionWindow))
    collapsed = collapsed.withColumn(
        "valid_to",
        F.when(F.col("_next_valid_from").isNull(), F.to_timestamp(F.lit(farFuture.strftime("%Y-%m-%d %H:%M:%S")))).otherwise(
            F.col("_next_valid_from") - F.expr("INTERVAL 1 SECOND")
        ),
    ).withColumn("is_current_row", F.col("_next_valid_from").isNull())

    # row_version: anchor keeps its version, later groups increment
    collapsed = collapsed.join(anchorInfo, collapsed[naturalKey] == anchorInfo["_k"], "left").drop("_k")
    baseVersion = F.coalesce(F.col("_anchor_version"), F.lit(0))
    collapsed = collapsed.withColumn(
        "row_version",
        F.when(F.col("_is_anchor"), F.col("row_version"))
        .when(F.col("_anchor_key").isNotNull(), baseVersion + F.col("_group") - 1)
        .otherwise(F.col("_group"))
        .cast("int"),
    )
    # inferred anchors: the first real version inherits key, version and valid_from
    firstOfInferred = F.col("_anchor_inferred") & (F.col("_group") == 1)
    collapsed = (
        collapsed.withColumn(surrogateKey, F.when(firstOfInferred, F.col("_anchor_key")).otherwise(F.col(surrogateKey)))
        .withColumn("row_version", F.when(firstOfInferred, F.col("_anchor_version")).otherwise(F.col("row_version")))
        .withColumn("valid_from", F.when(firstOfInferred, F.col("_anchor_valid_from")).otherwise(F.col("valid_from")))
        .withColumn("is_inferred_member", F.lit(False))
    )

    # surrogate keys for brand-new versions
    maxKeyRow = existingDf.agg(F.max(F.col(surrogateKey))).collect()[0][0]
    maxKey = int(maxKeyRow) if maxKeyRow is not None else 0
    needsKey = F.col(surrogateKey).isNull()
    collapsed = collapsed.withColumn("_is_new_row", needsKey)
    collapsed = collapsed.withColumn(
        "_new_key", F.when(needsKey, F.row_number().over(Window.partitionBy(needsKey).orderBy(F.col(naturalKey), F.col("valid_from"))))
    )
    collapsed = collapsed.withColumn(
        surrogateKey, F.when(needsKey, F.lit(maxKey) + F.col("_new_key")).otherwise(F.col(surrogateKey)).cast("long")
    )

    changedKeys = collapsed.select(naturalKey).distinct()
    untouchedCurrent = history.where(F.col("is_current_row")).join(changedKeys, naturalKey, "left_anti")
    closedHistory = history.where(~F.col("is_current_row"))

    rebuilt = collapsed.select(
        *[F.col(c) for c in [surrogateKey, naturalKey, *attrCols, "valid_from", "valid_to", "is_current_row", "row_version", "type1_hash", "type2_hash", "is_inferred_member"]],
        F.lit(False).alias("is_reserved_member"),
        F.when(F.col("_is_new_row") | F.col("_anchor_inferred"), F.lit(lineageKey)).otherwise(F.lit(None).cast("long")).alias("lineage_key"),
        F.col("_is_anchor").alias("_from_anchor"),
    )
    anchorLineage = history.where(F.col("is_current_row")).select(F.col(naturalKey).alias("_k"), F.col("lineage_key").alias("_old_lineage"))
    rebuilt = rebuilt.join(anchorLineage, rebuilt[naturalKey] == anchorLineage["_k"], "left").withColumn(
        "lineage_key", F.coalesce(F.col("lineage_key"), F.col("_old_lineage"))
    ).drop("_k", "_old_lineage", "_from_anchor")

    result = closedHistory.select(*outputCols).unionByName(untouchedCurrent.select(*outputCols)).unionByName(rebuilt.select(*outputCols))

    # Type-1 overwrite across all versions of a key (Integration.usp_MigrateStagedStockItemData)
    latestType1 = (
        result.where(F.col("is_current_row"))
        .select(naturalKey, *[F.col(c).alias(f"_t1_{c}") for c in type1Cols], F.col("type1_hash").alias("_t1_hash"))
    )
    result = result.join(latestType1, naturalKey, "left")
    for c in type1Cols:
        result = result.withColumn(c, F.coalesce(F.col(f"_t1_{c}"), F.col(c))).drop(f"_t1_{c}")
    result = result.withColumn("type1_hash", F.coalesce(F.col("_t1_hash"), F.col("type1_hash"))).drop("_t1_hash")
    return reserved.unionByName(result.select(*outputCols))


def applyScd1(
    existingDf: DataFrame,
    incomingDf: DataFrame,
    naturalKey: str,
    surrogateKey: str,
    attributeCols: Sequence[str],
    lineageKey: int,
) -> DataFrame:
    """SCD Type 1 (DIM_Load_ProductCategory): overwrite in place, insert new keys,
    never delete. Surrogate keys of existing rows are preserved."""
    outputCols = [surrogateKey, naturalKey, *attributeCols, "row_hash", "is_reserved_member", "lineage_key", "valid_from", "updated_at"]
    reserved = existingDf.where(F.col("is_reserved_member")).select(*outputCols)
    current = existingDf.where(~F.col("is_reserved_member"))

    incoming = dedupeLatest(incomingDf, [naturalKey], [rowHash(attributeCols)]).withColumn("row_hash", rowHash(attributeCols))
    joined = incoming.alias("s").join(
        current.select(naturalKey, surrogateKey, "row_hash", "lineage_key", "valid_from").alias("d"), naturalKey, "left"
    )
    changed = F.col("d.row_hash").isNull() | (F.col("d.row_hash") != F.col("s.row_hash"))
    maxKeyRow = existingDf.agg(F.max(F.col(surrogateKey))).collect()[0][0]
    maxKey = int(maxKeyRow) if maxKeyRow is not None else 0
    newKeyWindow = Window.partitionBy(F.col(f"d.{surrogateKey}").isNull()).orderBy(F.col(naturalKey))
    merged = joined.select(
        F.when(F.col(f"d.{surrogateKey}").isNull(), F.lit(maxKey) + F.row_number().over(newKeyWindow))
        .otherwise(F.col(f"d.{surrogateKey}"))
        .cast("long")
        .alias(surrogateKey),
        F.col(f"s.{naturalKey}").alias(naturalKey),
        *[F.col(f"s.{c}").alias(c) for c in attributeCols],
        F.col("s.row_hash").alias("row_hash"),
        F.lit(False).alias("is_reserved_member"),
        F.when(changed, F.lit(lineageKey)).otherwise(F.col("d.lineage_key")).cast("long").alias("lineage_key"),
        F.coalesce(F.col("d.valid_from"), F.current_timestamp()).alias("valid_from"),
        F.when(changed, F.current_timestamp()).otherwise(F.lit(None).cast("timestamp")).alias("updated_at"),
    )
    untouched = current.join(incoming.select(naturalKey), naturalKey, "left_anti").select(*outputCols)
    return reserved.unionByName(untouched).unionByName(merged.select(*outputCols))


def lookupAsOf(
    factDf: DataFrame,
    dimDf: DataFrame,
    factKeyCol: str,
    dimNaturalKey: str,
    factTimestampCol: str,
    surrogateKey: str,
    unknownKey: int = 0,
    resultCol: str = "",
) -> DataFrame:
    """Type-2 as-of lookup: match the version whose [valid_from, valid_to] contains the
    fact timestamp; fall back to the current row (the SSIS `Is Current Row = 1` cache)
    when the fact predates every version; otherwise the unknown member.

    Adds `resultCol` (default `surrogateKey`) and `is_unknown_member`.
    """
    target = resultCol or surrogateKey
    dimSlim = dimDf.where(~F.col("is_reserved_member")).select(
        F.col(dimNaturalKey).alias("_dk"),
        F.col(surrogateKey).alias("_sk"),
        F.col("valid_from").alias("_vf"),
        F.col("valid_to").alias("_vt"),
        F.col("is_current_row").alias("_cur"),
    )
    asOf = factDf.join(
        dimSlim,
        (factDf[factKeyCol] == dimSlim["_dk"]) & (factDf[factTimestampCol] >= dimSlim["_vf"]) & (factDf[factTimestampCol] <= dimSlim["_vt"]),
        "left",
    ).withColumnRenamed("_sk", "_sk_asof").drop("_dk", "_vf", "_vt", "_cur")
    currentOnly = dimSlim.where(F.col("_cur")).select(F.col("_dk"), F.col("_sk").alias("_sk_cur"))
    resolved = asOf.join(currentOnly, asOf[factKeyCol] == currentOnly["_dk"], "left").drop("_dk")
    resolved = resolved.withColumn(target, F.coalesce(F.col("_sk_asof"), F.col("_sk_cur"), F.lit(unknownKey)).cast("long"))
    resolved = resolved.withColumn("is_unknown_member", F.col("_sk_asof").isNull() & F.col("_sk_cur").isNull())
    return resolved.drop("_sk_asof", "_sk_cur")
