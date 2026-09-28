"""Notebook-facing helpers shared by every DIM_* notebook (staging reads, lookups, reject splitting, count logging).

The etl.* framework calls themselves stay in dbx_etl_common; this module only prepares DataFrames and
shapes the numbers those calls receive.
"""

from datetime import datetime
from typing import Iterable, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wwi_dimensions import keys, scd, specs, tables, unknown

PROJECT_NAME = "WWI_Dimensions"
STEP_NAME = "Load Dimensions"
REJECTED_DQ_STATUSES = ("REJECTED", "ERROR", "INVALID", "QUARANTINED")


def stagingTable(catalog: str, name: str) -> str:
    return f"{catalog}.silver.{name}"


def readStaging(spark: SparkSession, catalog: str, name: str, batchId: int, reloadFullHistory: bool = False) -> DataFrame:
    """Read silver.stg_* for the running batch (staging is rebuilt per batch by the STG_* packages).

    BatchId 0 (ad-hoc run) or ReloadFullHistory reads the whole staging table, as the legacy procedures did
    when @BatchId was not supplied.
    """
    df = spark.table(stagingTable(catalog, name))
    if "BatchId" in df.columns and int(batchId) > 0 and not reloadFullHistory:
        df = df.where(F.col("BatchId") == int(batchId))
    return df


def readStagingIfExists(spark: SparkSession, catalog: str, name: str, batchId: int, reloadFullHistory: bool = False) -> Optional[DataFrame]:
    return readStaging(spark, catalog, name, batchId, reloadFullHistory) if tables.tableExists(spark, stagingTable(catalog, name)) else None


def splitRejects(df: DataFrame, rejectColumn: str = "RejectReasonCode") -> Tuple[DataFrame, DataFrame]:
    """Split staged rows into (valid, rejected): DqStatusCode rejects plus an explicit RejectReasonCode when present."""
    conditions = []
    if "DqStatusCode" in df.columns:
        conditions.append(F.upper(F.col("DqStatusCode")).isin(*REJECTED_DQ_STATUSES))
    if rejectColumn in df.columns:
        conditions.append(F.col(rejectColumn).isNotNull())
    if not conditions:
        return df, df.limit(0)
    isRejected = conditions[0]
    for c in conditions[1:]:
        isRejected = isRejected | c
    rejected = df.where(isRejected)
    if rejectColumn not in rejected.columns:
        rejected = rejected.withColumn(rejectColumn, F.concat(F.lit("DQ_"), F.upper(F.col("DqStatusCode"))))
    else:
        rejected = rejected.withColumn(rejectColumn, F.coalesce(F.col(rejectColumn), F.concat(F.lit("DQ_"), F.upper(F.col("DqStatusCode"))) if "DqStatusCode" in df.columns else F.lit("DQ_REJECTED")))
    return df.where(~F.coalesce(isRejected, F.lit(False))), rejected


def prepareDimension(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec) -> str:
    """Create the Delta dimension if needed, register it in the key registry and seed the reserved members."""
    table = tables.ensureDimensionTable(spark, catalog, dimensionSpec)
    keys.ensureKeyRegistry(spark, catalog)
    unknown.ensureUnknownMembers(spark, catalog, dimensionSpec)
    return table


def lookupSurrogateKey(
    spark: SparkSession,
    catalog: str,
    df: DataFrame,
    dimensionSpec: specs.DimensionSpec,
    sourceColumn: str,
    targetColumn: str,
    defaultKey: int = specs.UNKNOWN_KEY,
    notApplicableWhenNull: bool = True,
) -> DataFrame:
    """SSIS Lookup (full cache, 'ignore failure') against the current members of another dimension.

    NULL business key -> -2 Not Applicable (when notApplicableWhenNull), unmatched -> defaultKey (-1 Unknown).
    """
    if not tables.tableExists(spark, dimensionSpec.fullTableName(catalog)):
        return df.withColumn(targetColumn, F.when(F.col(sourceColumn).isNull(), F.lit(specs.NOT_APPLICABLE_KEY if notApplicableWhenNull else defaultKey)).otherwise(F.lit(defaultKey)).cast("int"))
    members = scd.currentMembers(spark, catalog, dimensionSpec).select(
        F.col(dimensionSpec.businessKeyColumn).cast("string").alias("_lk_bk"),
        F.col(dimensionSpec.keyColumn).alias("_lk_key"),
    )
    joined = df.join(members, F.col(sourceColumn).cast("string") == F.col("_lk_bk"), "left")
    resolved = (
        F.when(F.col(sourceColumn).isNull(), F.lit(specs.NOT_APPLICABLE_KEY if notApplicableWhenNull else defaultKey))
        .otherwise(F.coalesce(F.col("_lk_key"), F.lit(defaultKey)))
    )
    return joined.withColumn(targetColumn, resolved.cast("int")).drop("_lk_bk", "_lk_key")


def queueLateArrivingMembers(
    spark: SparkSession,
    catalog: str,
    df: DataFrame,
    dimensionName: str,
    businessKeyColumn: str,
    keyColumn: str,
    batchId: int,
    packageExecutionId: int,
    sourceObjectName: str,
    now: datetime,
    sourceSystemColumn: Optional[str] = "SourceSystemCode",
) -> int:
    """Register unresolved lookups (key = -1 with a non-null business key) on work.LateArrivingDimensionQueue.

    Mirrors the SSIS lookup error output -> Integration.usp_QueueLateArrivingMember: an existing open queue row for
    the same (DimensionName, MissingBusinessKey) gets its OccurrenceCount bumped instead of a duplicate row.
    """
    missing = df.where((F.col(keyColumn) == specs.UNKNOWN_KEY) & F.col(businessKeyColumn).isNotNull()).select(
        F.col(businessKeyColumn).cast("string").alias("MissingBusinessKey"),
        (F.col(sourceSystemColumn) if sourceSystemColumn and sourceSystemColumn in df.columns else F.lit(None).cast("string")).alias("SourceSystemCode"),
    ).groupBy("MissingBusinessKey").agg(F.max("SourceSystemCode").alias("SourceSystemCode"), F.count("*").alias("Occurrences"))
    n = missing.count()
    if not n:
        return 0
    queue = tables.ensureLateArrivingQueue(spark, catalog)
    maxId = spark.sql(f"SELECT COALESCE(MAX(QueueRowId), 0) AS m FROM {queue}").collect()[0]["m"]
    nowSql = f"TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}'"
    missing = missing.withColumn("_rn", F.row_number().over(__import__("pyspark.sql.window", fromlist=["Window"]).Window.orderBy("MissingBusinessKey")))
    missing.createOrReplaceTempView("_late_arriving_src")
    spark.sql(
        f"""
        MERGE INTO {queue} AS q
        USING _late_arriving_src AS s
          ON q.DimensionName = '{dimensionName}' AND q.MissingBusinessKey = s.MissingBusinessKey AND q.ResolvedFlag = false
        WHEN MATCHED THEN UPDATE SET q.OccurrenceCount = q.OccurrenceCount + s.Occurrences
        WHEN NOT MATCHED THEN INSERT (QueueRowId, BatchId, PackageExecutionId, DimensionName, MissingBusinessKey, SourceSystemCode,
                                      FirstSeenObjectName, FirstSeenAtUtc, OccurrenceCount, InferredAttributesJson, StubCreatedFlag,
                                      StubCreatedAtUtc, PlaceholderKey, RetryCount, ResolvedFlag, ResolvedAtUtc, ResolvedByExecutionId, ResolutionNote)
             VALUES ({int(maxId)} + s._rn, {int(batchId)}, {int(packageExecutionId)}, '{dimensionName}', s.MissingBusinessKey, s.SourceSystemCode,
                     '{sourceObjectName}', {nowSql}, s.Occurrences, NULL, false, NULL, {specs.UNKNOWN_KEY}, 0, false, NULL, NULL, NULL)
        """
    )
    return int(n)


def targetRowCount(spark: SparkSession, catalog: str, dimensionSpec: specs.DimensionSpec, currentOnly: bool = True) -> int:
    df = spark.table(dimensionSpec.fullTableName(catalog)).where(F.col(dimensionSpec.keyColumn) > 0)
    if currentOnly and dimensionSpec.isType2:
        df = df.where(F.col("IsCurrentRow") == True)  # noqa: E712
    return df.count()


def legacyObjectName(dimensionSpec: specs.DimensionSpec) -> str:
    return f"Dimension.{dimensionSpec.name}"


def scdCounts(result: scd.ScdResult, rejectCount: int = 0) -> dict:
    """Numbers for control.logRowCount / packageRun in the shape usp_LogRowCount expects."""
    return {
        "sourceRowCount": result.rowsRead,
        "insertRowCount": result.rowsWritten,
        "updateRowCount": result.rowsUpdated,
        "rejectRowCount": rejectCount,
    }


def band(column, edges: Sequence[Tuple[float, str]], default: str):
    """Ordered (upper bound exclusive, label) banding used for credit limit / quota / price bands."""
    expr = None
    for upper, label in edges:
        cond = F.col(column) < F.lit(upper)
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return (expr.otherwise(F.lit(default)) if expr is not None else F.lit(default))


def filterBatch(df: DataFrame, batchId: int, reloadFullHistory: bool = False) -> DataFrame:
    """Restrict a staging DataFrame to the running batch (see readStaging)."""
    if "BatchId" in df.columns and int(batchId) > 0 and not reloadFullHistory:
        return df.where(F.col("BatchId") == int(batchId))
    return df


def configurationOrDefault(control, spark: SparkSession, catalog: str, key: str, environmentCode: Optional[str], default: str) -> str:
    """etl.usp_GetConfiguration with the SSIS package-parameter default when the key is not configured."""
    try:
        value = control.getConfiguration(spark, catalog, key, environmentCode=environmentCode)
    except Exception:  # noqa: BLE001 - missing key / missing table behaves like the SSIS default
        value = None
    return default if value is None or str(value).strip() == "" else str(value)
