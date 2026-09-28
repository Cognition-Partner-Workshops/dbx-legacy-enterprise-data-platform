"""Standard FACT_Load_* pipeline shared by the non-sale fact notebooks.

Every generated FACT_Load_* package follows the same shape (see
build_fact_packages.py::_fact_package): get watermark -> read stg window ->
validate (error output -> reject log) -> dimension Lookups (business key +
effective date, miss -> -1, optionally infer / hold / queue) -> Derived
Columns -> Row Count -> MERGE into Fact.X -> queue late arrivers -> set
watermark. Notebooks describe their package as a FactLoadSpec plus a
transform callback holding the package-specific Derived Column logic."""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dbx_etl_common import control

import fact_common as fc

SOURCE_SYSTEM_CODE = "SQLSTG"

ON_MISS_UNKNOWN = "unknown"   # ordinary Lookup miss -> -1
ON_MISS_INFER = "infer"       # create inferred member (+ queue late arriver)
ON_MISS_HOLD = "hold"         # park in Fact.[Fact Load Hold] for the rekey pass
ON_MISS_QUEUE = "queue"       # -1 now, queue for FACT_Rekey_LateArrivingDimensions


@dataclass(frozen=True)
class LookupSpec:
    dimension: str
    businessKeyCol: str
    outputCol: str
    effectiveDateCol: Optional[str] = None
    onMiss: str = ON_MISS_UNKNOWN
    extraCols: Optional[Dict[str, str]] = None
    notApplicableWhenNull: bool = False


@dataclass(frozen=True)
class FactLoadSpec:
    packageName: str
    objectName: str                 # legacy Fact.X name used for watermark / row-count logging
    targetTable: str                # gold.<targetTable>
    sourceTable: str                # silver.<sourceTable>
    sourceDateCol: str              # business date column used for the load window
    sourceTimestampCol: str         # watermark column (LoadedAtUtc / SourceModifiedDate)
    businessKeyCol: str             # legacy fact business key (natural key) column in the source
    naturalKeyCols: Sequence[str]   # columns hashed into natural_key_hash
    surrogateKeyCol: str
    dateKeyCol: str                 # target date-key column used for clustering
    lookups: Sequence[LookupSpec] = field(default_factory=list)
    regionCol: str = "RegionCode"
    watermarkObjectName: Optional[str] = None
    sourceFilter: Optional[Callable[[DataFrame], DataFrame]] = None
    validation: Optional[Callable[[DataFrame], Column]] = None
    rejectReasonCode: str = fc.REJECT_FACT_VALIDATION
    rowVersionCol: Optional[str] = None
    stepName: str = "Load Facts"
    updateCols: Optional[Sequence[str]] = None
    insertOnly: bool = False
    extraClusterCols: Sequence[str] = ()


def readSource(spark: SparkSession, catalog: str, spec: FactLoadSpec, p: dict, wmFrom, wmTo) -> DataFrame:
    df = fc.readTable(spark, catalog, "silver", spec.sourceTable)
    if not p["reloadFullHistory"]:
        df = fc.inWindow(df, spec.sourceTimestampCol, wmFrom, wmTo)
    if spec.sourceFilter is not None:
        df = spec.sourceFilter(df)
    return df.withColumn("natural_key_hash", fc.naturalKeyHash(*[F.col(c) for c in spec.naturalKeyCols]))


def applyLookups(spark: SparkSession, catalog: str, df: DataFrame, spec: FactLoadSpec) -> DataFrame:
    for lk in spec.lookups:
        dimSpec = fc.DIMENSIONS[lk.dimension]
        dimName = fc.tableName(catalog, "gold", dimSpec.table)
        if not fc.tableExists(spark, dimName):
            df = df.withColumn(lk.outputCol, F.lit(fc.UNKNOWN_KEY)).withColumn(fc.missFlag(lk.outputCol), F.col(lk.businessKeyCol).isNotNull())
            for _, alias in (lk.extraCols or {}).items():
                df = df.withColumn(alias, F.lit(None))
        else:
            df = fc.lookupDimension(df, spark.table(dimName), dimSpec, lk.businessKeyCol, lk.outputCol, lk.effectiveDateCol, extraCols=lk.extraCols)
        if lk.notApplicableWhenNull:
            df = df.withColumn(lk.outputCol, F.when(F.col(lk.businessKeyCol).isNull(), F.lit(fc.NOT_APPLICABLE_KEY)).otherwise(F.col(lk.outputCol)))
    return df


def resolveMisses(spark: SparkSession, catalog: str, df: DataFrame, spec: FactLoadSpec, batchId: int, packageExecutionId: int) -> Dict[str, int]:
    """Inferred members and late-arriving queue entries for every Lookup that asks for them."""
    counts = {"inferred": 0, "queued": 0}
    for lk in spec.lookups:
        if lk.onMiss not in (ON_MISS_INFER, ON_MISS_QUEUE):
            continue
        missing = df.where(F.col(fc.missFlag(lk.outputCol)))
        if missing.limit(1).count() == 0:
            continue
        # queue before inferring: `missing` is a lazy plan over the dimension, so once the stubs are
        # inserted it would re-evaluate to empty
        counts["queued"] += fc.queueLateArrivers(spark, catalog, missing, lk.dimension, lk.businessKeyCol, spec.packageName, batchId, packageExecutionId, sourceSystemCode=SOURCE_SYSTEM_CODE)
        if lk.onMiss == ON_MISS_INFER:
            counts["inferred"] += fc.inferMembers(spark, catalog, fc.DIMENSIONS[lk.dimension], missing, lk.businessKeyCol, batchId, packageExecutionId, SOURCE_SYSTEM_CODE, regionCol=spec.regionCol)
    return counts


def holdMisses(spark: SparkSession, catalog: str, df: DataFrame, spec: FactLoadSpec, batchId: int, packageExecutionId: int):
    holdLookups = [lk for lk in spec.lookups if lk.onMiss == ON_MISS_HOLD]
    if not holdLookups:
        return df, df.limit(0), 0
    missCols = [fc.missFlag(lk.outputCol) for lk in holdLookups]
    fresh = df.where(F.col("fact_load_hold_key").isNull())
    held = 0
    for lk in holdLookups:
        held += fc.holdRows(
            spark, catalog, fresh.where(F.col(fc.missFlag(lk.outputCol))), spec.objectName, lk.dimension, lk.businessKeyCol,
            fc.HOLD_REASON_DIM_NOT_KEYED, batchId, packageExecutionId, regionCol=spec.regionCol, businessDateCol=spec.sourceDateCol,
            naturalKeyHashCol="natural_key_hash", naturalKeyCols=spec.naturalKeyCols, sourceSystemCode=SOURCE_SYSTEM_CODE,
        )
    loadable, stillHeld = fc.splitHeld(df, missCols)
    anyMiss = F.lit(False)
    for c in missCols:
        anyMiss = anyMiss | F.coalesce(F.col(c), F.lit(False))
    loadable = loadable.where(~(F.col("fact_load_hold_key").isNull() & anyMiss))
    return loadable, stillHeld, held


def withHoldColumns(df: DataFrame, regionCol: str) -> DataFrame:
    return (
        df.withColumn("fact_load_hold_key", F.lit(None).cast("bigint"))
        .withColumn("retry_count", F.lit(0))
        .withColumn("max_retry_count", fc.retryLimitColumn(F.col(regionCol)))
    )


def run(
    spark: SparkSession, catalog: str, p: dict, spec: FactLoadSpec, runCtx, batchId: int,
    transform: Callable[[SparkSession, str, DataFrame], DataFrame],
    afterMerge: Optional[Callable[[SparkSession, str, DataFrame, Dict[str, int]], None]] = None,
) -> Dict[str, int]:
    batchId = int(batchId)
    packageExecutionId = int(runCtx.packageExecutionId)
    wmObject = spec.watermarkObjectName or spec.objectName
    wmFrom, wmTo = fc.loadWindow(spark, catalog, SOURCE_SYSTEM_CODE, wmObject, p)

    source = readSource(spark, catalog, spec, p, wmFrom, wmTo)
    rowsRead = source.count()
    if spec.rowVersionCol:
        source = fc.dedupByRowVersion(source, ["natural_key_hash"], spec.rowVersionCol)

    if spec.validation is not None:
        bad = spec.validation(source)
        valid, invalid = source.where(~F.coalesce(bad, F.lit(False))), source.where(F.coalesce(bad, F.lit(False)))
    else:
        valid, invalid = source, source.limit(0)
    rejected = fc.rejectRows(spark, catalog, invalid, spec.objectName, spec.rejectReasonCode, batchId, packageExecutionId,
                             businessKeyColumn=spec.businessKeyCol, sourceSystemCode=SOURCE_SYSTEM_CODE)

    hasHolds = any(lk.onMiss == ON_MISS_HOLD for lk in spec.lookups)
    working = withHoldColumns(valid, spec.regionCol)
    if hasHolds:
        working = fc.coalesceHeldReplays(working.unionByName(fc.readHeldRows(spark, catalog, spec.objectName, valid.schema)))

    working = applyLookups(spark, catalog, working, spec)
    missCounts = resolveMisses(spark, catalog, working, spec, batchId, packageExecutionId)
    if missCounts["inferred"] > 0:
        outputs = [c for lk in spec.lookups for c in ([lk.outputCol, fc.missFlag(lk.outputCol)] + list((lk.extraCols or {}).values()))]
        working = applyLookups(spark, catalog, working.drop(*outputs), spec)

    loadable, stillHeld, held = holdMisses(spark, catalog, working, spec, batchId, packageExecutionId)

    factRows = transform(spark, catalog, loadable)
    factRows = fc.loadAuditColumns(factRows, batchId, packageExecutionId)
    fullName = fc.tableName(catalog, "gold", spec.targetTable)
    factRows = fc.assignSurrogateKeys(spark, fullName, factRows, spec.surrogateKeyCol, ["natural_key_hash"], matchCols=["natural_key_hash"])
    merged = fc.mergeFact(spark, fullName, factRows, [spec.surrogateKeyCol], clusterCols=[spec.dateKeyCol, *spec.extraClusterCols],
                          updateCols=spec.updateCols, insertOnly=spec.insertOnly)

    holds = {"released": 0, "retried": 0, "abandoned": 0}
    if hasHolds:
        releasedKeys = loadable.where(F.col("fact_load_hold_key").isNotNull()).select("fact_load_hold_key", F.lit(None).cast("bigint").alias("released_fact_key"))
        retriedKeys = stillHeld.where(F.col("fact_load_hold_key").isNotNull()).select("fact_load_hold_key")
        holds = fc.settleHolds(spark, catalog, releasedKeys, retriedKeys, batchId, packageExecutionId)
        control.logRowCount(spark, catalog, packageExecutionId, "Fact.Fact Load Hold", sourceRowCount=held, insertRowCount=held, updateRowCount=holds["released"] + holds["retried"])

    if afterMerge is not None:
        afterMerge(spark, catalog, factRows, merged)

    fc.logFactRowCounts(spark, catalog, packageExecutionId, spec.objectName, rowsRead, fullName, merged, rejected)
    control.setWatermark(spark, catalog, SOURCE_SYSTEM_CODE, wmObject, fc.watermarkValue(wmTo), packageExecutionId=packageExecutionId)
    fc.setRunCounts(runCtx, rowsRead, merged, rejected)
    return {"rowsRead": rowsRead, "rejected": rejected, "held": held, **missCounts, **merged, **holds}
