"""Shared plumbing for the 08_facts notebooks: SCD2 dimension lookups with
unknown / inferred / held members, late-arriving queues, Delta MERGE and
partition replacement, effective-dated FX lookups and the sale dedup ranking.

Everything that talks to the etl.* control tables goes through
dbx_etl_common (session 00); this module never writes those tables itself."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dbx_etl_common import control, naming

PROJECT_NAME = "WWI_Facts"
BATCH_NAME = "WWI_Daily_Facts"

UNKNOWN_KEY = -1
NOT_APPLICABLE_KEY = -2
INVALID_KEY = -3
INFERRED_PENDING_KEY = -4
ERROR_KEY = -9

INFERRED_VALID_FROM = date(2013, 1, 1)
SCD2_OPEN_END = datetime(9999, 12, 31, 23, 59, 59)

CORRECTION_ORIGINAL = "ORIG"
CORRECTION_REVERSAL = "REV"
CORRECTION_RESTATEMENT = "RES"

HOLD_REASON_DIM_NOT_KEYED = "DIM_NOT_KEYED"
HOLD_REASON_FX_MISSING = "FX_RATE_MISSING"
HOLD_STATUS_HELD = "HELD"
HOLD_STATUS_RELEASED = "RELEASED"
HOLD_STATUS_ABANDONED = "ABANDONED"
HOLD_RETRY_LIMITS = {"NA": 3, "EU": 7, "APAC": 21}
DEFAULT_HOLD_RETRY_LIMIT = 5

REJECT_HOLD_EXPIRED = "HOLD_EXPIRED"
REJECT_FACT_VALIDATION = "FACT_VALIDATION"
REJECT_DUPLICATE = "DUPLICATE_NATURAL_KEY"
REJECT_CORRECTION_UNSUPPORTED = "CORR_FACT_UNSUPPORTED"

FACT_LOAD_HOLD_TABLE = "fact_fact_load_hold"
LATE_ARRIVING_QUEUE_TABLE = "work_late_arriving_dimension_queue"
FACT_REKEY_QUEUE_TABLE = "work_fact_rekey_queue"
FX_RATE_TABLE = "stg_fx_rate"
SALE_DEDUP_ARCHIVE_TABLE = "int_fact_sale_dedup_archive"


@dataclass(frozen=True)
class DimensionSpec:
    name: str
    table: str
    keyCol: str
    businessKeyCol: str
    validFromCol: str = "valid_from"
    validToCol: str = "valid_to"
    isCurrentCol: str = "is_current_row"
    isInferredCol: str = "is_inferred_member"
    isScd2: bool = True


DIMENSIONS: Dict[str, DimensionSpec] = {
    "Customer": DimensionSpec("Customer", "dim_customer", "customer_key", "wwi_customer_id"),
    "Stock Item": DimensionSpec("Stock Item", "dim_stock_item", "stock_item_key", "wwi_stock_item_id"),
    "Supplier": DimensionSpec("Supplier", "dim_supplier", "supplier_key", "wwi_supplier_id"),
    "City": DimensionSpec("City", "dim_city", "city_key", "wwi_city_id"),
    "Salesperson": DimensionSpec("Salesperson", "dim_salesperson", "salesperson_key", "wwi_employee_id"),
    "Transaction Type": DimensionSpec("Transaction Type", "dim_transaction_type", "transaction_type_key", "wwi_transaction_type_id", isScd2=False),
    "GL Account": DimensionSpec("GL Account", "dim_gl_account", "gl_account_key", "gl_account_code", isScd2=False),
    "Warehouse Site": DimensionSpec("Warehouse Site", "dim_warehouse_site", "warehouse_site_key", "warehouse_code", isScd2=False),
    "Carrier": DimensionSpec("Carrier", "dim_carrier", "carrier_key", "carrier_code", isScd2=False),
    "Return Reason": DimensionSpec("Return Reason", "dim_return_reason", "return_reason_key", "return_reason_code", isScd2=False),
    "Promotion": DimensionSpec("Promotion", "dim_promotion", "promotion_key", "promotion_code", isScd2=False),
}


# ---------------------------------------------------------------- session / parameters


def resolveBatchId(spark: SparkSession, catalog: str, p: dict, batchName: str = BATCH_NAME) -> int:
    """BatchId 0 (the job-parameter default) means "nobody started a batch for
    me", so open one the same way the SSIS master package did."""
    batchId = int(p["batchId"])
    if batchId > 0:
        return batchId
    return control.startBatch(
        spark, catalog, batchName, batchType="Daily",
        businessDate=p["businessDate"], environmentCode=p["environmentCode"], allowAdoptRunning=True,
    )


def shouldSkipForRestart(p: dict, stepName: str, stepOrder: Sequence[str]) -> bool:
    restartFrom = (p.get("restartFromStep") or "").strip()
    if not restartFrom or restartFrom not in stepOrder or stepName not in stepOrder:
        return False
    return stepOrder.index(stepName) < stepOrder.index(restartFrom)


def tableName(catalog: str, schema: str, table: str) -> str:
    return naming.table(catalog, schema, table)


def tableExists(spark: SparkSession, fullName: str) -> bool:
    return spark.catalog.tableExists(fullName)


def readTable(spark: SparkSession, catalog: str, schema: str, table: str) -> DataFrame:
    return spark.table(tableName(catalog, schema, table))


def readTableOrEmpty(spark: SparkSession, catalog: str, schema: str, table: str, schemaDef: T.StructType) -> DataFrame:
    fullName = tableName(catalog, schema, table)
    if tableExists(spark, fullName):
        return spark.table(fullName)
    return spark.createDataFrame([], schemaDef)


def readDimension(spark: SparkSession, catalog: str, spec: DimensionSpec) -> DataFrame:
    return readTable(spark, catalog, "gold", spec.table)


def configurationValue(spark: SparkSession, catalog: str, key: str, environmentCode: str, default: str) -> str:
    try:
        value = control.getConfiguration(spark, catalog, key, environmentCode=environmentCode)
    except Exception:
        value = None
    return default if value is None or str(value).strip() == "" else str(value)


# ---------------------------------------------------------------- watermarks


WATERMARK_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def parseWatermark(value) -> Optional[datetime]:
    """usp_GetWatermark hands back ISO strings (yyyy-MM-ddTHH:mm:ss.fff); accept datetimes too."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip().replace(" ", "T")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.strptime(text[:10], "%Y-%m-%d")


def watermarkValue(ts: datetime) -> str:
    return ts.strftime(WATERMARK_FORMAT)[:-3]


def loadWindow(spark: SparkSession, catalog: str, sourceSystemCode: str, objectName: str, p: dict) -> Tuple[datetime, datetime]:
    wmFrom, wmTo = control.getWatermark(spark, catalog, sourceSystemCode, objectName, reloadFullHistory=bool(p["reloadFullHistory"]))
    parsedFrom = parseWatermark(wmFrom) or datetime(1900, 1, 1)
    parsedTo = parseWatermark(wmTo) or datetime.utcnow().replace(microsecond=0)
    return parsedFrom, parsedTo


def inWindow(df: DataFrame, tsCol: str, wmFrom, wmTo) -> DataFrame:
    return df.where((F.col(tsCol) > F.lit(wmFrom)) & (F.col(tsCol) <= F.lit(wmTo)))


def backdatedStart(businessDate: date, backdatingDays: int) -> date:
    return date.fromordinal(businessDate.toordinal() - backdatingDays)


# ---------------------------------------------------------------- keys & hashing


def naturalKeyHash(*cols: Column) -> Column:
    """SHA-256 over the pipe-delimited natural key, matching HASHBYTES('SHA2_256', CONCAT_WS('|', ...))."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]), 256)


def missFlag(outputCol: str) -> str:
    return outputCol + "_miss"


def lookupDimension(
    df: DataFrame,
    dimDf: DataFrame,
    spec: DimensionSpec,
    businessKeyCol: str,
    outputCol: str,
    effectiveDateCol: Optional[str] = None,
    extraCols: Optional[Dict[str, str]] = None,
    excludeInferred: bool = False,
) -> DataFrame:
    """Legacy Lookup semantics: match on business key and, for SCD2 dims, the
    effective-date range; a miss lands on the unknown member (-1) and is
    flagged in <outputCol>_miss so the caller can infer / hold / queue it."""
    extraCols = extraCols or {}
    d = dimDf.alias("d")
    s = df.alias("s")
    cond = F.col("s." + businessKeyCol).cast("string") == F.col("d." + spec.businessKeyCol).cast("string")
    if spec.isScd2 and effectiveDateCol is not None:
        eff = F.col("s." + effectiveDateCol).cast("timestamp")
        cond = cond & (eff >= F.col("d." + spec.validFromCol).cast("timestamp")) & (eff < F.col("d." + spec.validToCol).cast("timestamp"))
    elif spec.isScd2:
        cond = cond & (F.col("d." + spec.isCurrentCol) == F.lit(True))
    if excludeInferred and spec.isInferredCol in dimDf.columns:
        cond = cond & (F.coalesce(F.col("d." + spec.isInferredCol), F.lit(False)) == F.lit(False))
    joined = s.join(d, cond, "left")
    selected = [F.col("s." + c) for c in df.columns]
    selected.append(F.coalesce(F.col("d." + spec.keyCol), F.lit(UNKNOWN_KEY)).cast("int").alias(outputCol))
    selected.append((F.col("d." + spec.keyCol).isNull() & F.col("s." + businessKeyCol).isNotNull()).alias(missFlag(outputCol)))
    for dimCol, outName in extraCols.items():
        selected.append(F.col("d." + dimCol).alias(outName))
    return joined.select(*selected)


def dedupByRowVersion(df: DataFrame, keyCols: Sequence[str], versionCol: str, tieBreakCols: Sequence[str] = ()) -> DataFrame:
    """Keep the latest source row version per natural key (legacy Sort with
    duplicate removal after ORDER BY SourceRowVersion DESC)."""
    ordering = [F.col(versionCol).desc_nulls_last()] + [F.col(c).desc() for c in tieBreakCols]
    w = Window.partitionBy(*[F.col(c) for c in keyCols]).orderBy(*ordering)
    return df.withColumn("_rv_rank", F.row_number().over(w)).where(F.col("_rv_rank") == 1).drop("_rv_rank")


def saleDuplicateRank(saleDf: DataFrame) -> DataFrame:
    """Integration.DeduplicateFactSale ranking: newest Source Row Version wins,
    ties resolved by the highest Sale Key; only ORIG rows compete."""
    w = Window.partitionBy("natural_key_hash").orderBy(
        F.coalesce(F.col("source_row_version"), F.lit(0)).desc(), F.col("sale_key").desc()
    )
    return (
        saleDf.where(F.coalesce(F.col("correction_type_code"), F.lit(CORRECTION_ORIGINAL)) == CORRECTION_ORIGINAL)
        .withColumn("duplicate_rank", F.row_number().over(w))
    )


# ---------------------------------------------------------------- FX


def fxRates(spark: SparkSession, catalog: str, rateTypeCode: str = "CLOSE", reportingCurrency: str = "USD") -> DataFrame:
    """stg.FxRate closing rates to the reporting currency, normalised to the
    column names the legacy Lookup exposed."""
    rates = readTable(spark, catalog, "silver", FX_RATE_TABLE)
    return (
        rates.where((F.col("RateTypeCode") == rateTypeCode) & (F.col("ToCurrencyCode") == reportingCurrency))
        .select(
            F.col("FromCurrencyCode").alias("CurrencyCode"),
            F.col("RateDate").cast("date").alias("RateDate"),
            F.col("ConversionRate").cast("decimal(18,8)").alias("RateToUsd"),
            F.col("RateSourceCode").alias("RateSourceCode"),
        )
    )


def lookupEffectiveFxRate(
    df: DataFrame,
    ratesDf: DataFrame,
    currencyCol: str,
    dateCol: str,
    rateCol: str = "FxRateToUsd",
    sourceCol: str = "FxRateSource",
    rateDateCol: str = "FxRateDate",
    priorDayOnly: bool = False,
    reportingCurrency: str = "USD",
) -> DataFrame:
    """Latest rate on or before the transaction date (strictly before when the
    retry path asks for the prior-day rate). Reporting-currency rows get 1.0.
    Misses are flagged in <rateCol>_miss and left NULL for hold handling."""
    rowId = "_fx_row_id"
    s = df.withColumn(rowId, F.monotonically_increasing_id()).alias("s")
    r = ratesDf.alias("r")
    dateExpr = F.col("s." + dateCol).cast("date")
    cond = (F.col("s." + currencyCol) == F.col("r.CurrencyCode"))
    cond = cond & ((F.col("r.RateDate") < dateExpr) if priorDayOnly else (F.col("r.RateDate") <= dateExpr))
    joined = s.join(r, cond, "left")
    w = Window.partitionBy(rowId).orderBy(F.col("r.RateDate").desc_nulls_last())
    picked = (
        joined.withColumn("_fx_rank", F.row_number().over(w)).where(F.col("_fx_rank") == 1)
        .select(
            *[F.col("s." + c) for c in df.columns],
            F.col("r.RateToUsd").alias("_rate"),
            F.col("r.RateSourceCode").alias("_src"),
            F.col("r.RateDate").alias("_rateDate"),
        )
    )
    isReporting = F.col(currencyCol).isNull() | (F.col(currencyCol) == reportingCurrency)
    return (
        picked.withColumn(rateCol, F.when(isReporting, F.lit(1).cast("decimal(18,8)")).otherwise(F.col("_rate")))
        .withColumn(sourceCol, F.when(isReporting, F.lit("PARITY")).otherwise(F.col("_src")))
        .withColumn(rateDateCol, F.when(isReporting, F.col(dateCol).cast("date")).otherwise(F.col("_rateDate")))
        .withColumn(missFlag(rateCol), ~isReporting & F.col("_rate").isNull())
        .drop("_rate", "_src", "_rateDate")
    )


# ---------------------------------------------------------------- Delta writes


def ensureTable(spark: SparkSession, fullName: str, df: DataFrame, partitionCols: Sequence[str] = (), clusterCols: Sequence[str] = ()) -> None:
    """Facts are liquid-clustered by their date key (daily partitions would be
    too small); snapshots are partitioned by snapshot date so replaceWhere
    swaps exactly one partition per day."""
    if tableExists(spark, fullName):
        return
    if clusterCols and not partitionCols:
        columns = ", ".join("`%s` %s" % (f.name, f.dataType.simpleString()) for f in df.schema.fields)
        spark.sql("CREATE TABLE IF NOT EXISTS %s (%s) USING DELTA CLUSTER BY (%s)" % (fullName, columns, ", ".join("`%s`" % c for c in clusterCols)))
        return
    writer = df.limit(0).write.format("delta")
    if partitionCols:
        writer = writer.partitionBy(*partitionCols)
    writer.saveAsTable(fullName)


def tableVersion(spark: SparkSession, fullName: str) -> int:
    if not tableExists(spark, fullName):
        return -1
    row = spark.sql("DESCRIBE HISTORY %s LIMIT 1" % fullName).select("version").first()
    return int(row[0]) if row is not None and row[0] is not None else -1


def lastOperationMetrics(spark: SparkSession, fullName: str, sinceVersion: Optional[int] = None) -> Dict[str, int]:
    """Metrics of the latest Delta commit. A MERGE / DELETE that touches nothing
    leaves no commit, so callers pass the version they saw before the write and
    get an empty dict when nothing was committed."""
    row = spark.sql("DESCRIBE HISTORY %s LIMIT 1" % fullName).select("version", "operationMetrics").first()
    if row is None or row[1] is None:
        return {}
    if sinceVersion is not None and int(row[0]) <= sinceVersion:
        return {}
    return {k: int(v) for k, v in row[1].items()}


def mergeFact(
    spark: SparkSession,
    fullName: str,
    sourceDf: DataFrame,
    keyCols: Sequence[str],
    partitionCols: Sequence[str] = (),
    clusterCols: Sequence[str] = (),
    updateCols: Optional[Sequence[str]] = None,
    insertOnly: bool = False,
    partitionPruneCol: Optional[str] = None,
) -> Dict[str, int]:
    """Incremental fact MERGE keyed by the legacy business key. Returns the
    inserted / updated counts from the Delta commit so they can be logged
    through control.logRowCount exactly as the SSIS Row Count did."""
    from delta.tables import DeltaTable

    ensureTable(spark, fullName, sourceDf, partitionCols, clusterCols)
    target = DeltaTable.forName(spark, fullName)
    cond = " AND ".join("t.`%s` <=> s.`%s`" % (c, c) for c in keyCols)
    if partitionPruneCol is not None:
        bounds = sourceDf.agg(F.min(partitionPruneCol).alias("lo"), F.max(partitionPruneCol).alias("hi")).first()
        if bounds is not None and bounds["lo"] is not None:
            cond += " AND t.`%s` BETWEEN '%s' AND '%s'" % (partitionPruneCol, bounds["lo"], bounds["hi"])
    builder = target.alias("t").merge(sourceDf.alias("s"), cond)
    if not insertOnly:
        cols = list(updateCols) if updateCols is not None else [c for c in sourceDf.columns if c not in keyCols]
        builder = builder.whenMatchedUpdate(set={c: "s.`%s`" % c for c in cols})
    versionBefore = tableVersion(spark, fullName)
    builder.whenNotMatchedInsertAll().execute()
    m = lastOperationMetrics(spark, fullName, versionBefore)
    return {
        "inserted": m.get("numTargetRowsInserted", 0),
        "updated": m.get("numTargetRowsUpdated", 0),
        "deleted": m.get("numTargetRowsDeleted", 0),
    }


def appendRows(spark: SparkSession, fullName: str, df: DataFrame, partitionCols: Sequence[str] = (), clusterCols: Sequence[str] = ()) -> int:
    """Count before writing: an empty append leaves no Delta commit, and a plan
    that reads the target table would re-evaluate differently after the write."""
    ensureTable(spark, fullName, df, partitionCols, clusterCols)
    n = df.count()
    if n > 0:
        df.write.format("delta").mode("append").saveAsTable(fullName)
    return n


def replaceDateRange(spark: SparkSession, fullName: str, df: DataFrame, dateCol: str, fromDate: date, toDate: date, partitionCols: Sequence[str] = ()) -> Dict[str, int]:
    """Snapshot semantics: DELETE the window then re-insert, done atomically
    with Delta replaceWhere on the snapshot-date partition column."""
    partitionCols = tuple(partitionCols) or (dateCol,)
    ensureTable(spark, fullName, df, partitionCols)
    predicate = "`%s` >= '%s' AND `%s` <= '%s'" % (dateCol, fromDate.isoformat(), dateCol, toDate.isoformat())
    versionBefore = tableVersion(spark, fullName)
    (
        df.where(predicate).write.format("delta").mode("overwrite")
        .option("replaceWhere", predicate).saveAsTable(fullName)
    )
    m = lastOperationMetrics(spark, fullName, versionBefore)
    return {"inserted": m.get("numOutputRows", 0), "deleted": m.get("numRemovedFiles", 0)}


def assignSurrogateKeys(spark: SparkSession, fullName: str, df: DataFrame, keyCol: str, orderCols: Sequence[str], matchCols: Optional[Sequence[str]] = None) -> DataFrame:
    """Replacement for the IDENTITY column: rows already in the target (matched
    on matchCols, normally the natural key hash) keep their key; the rest get
    max(existing) + dense running number."""
    start = 0
    exists = tableExists(spark, fullName)
    if exists:
        row = spark.table(fullName).agg(F.max(keyCol)).first()
        start = int(row[0]) if row is not None and row[0] is not None else 0
    if matchCols and exists:
        existing = spark.table(fullName).select(*matchCols, F.col(keyCol).alias("_existing_key")).dropDuplicates(list(matchCols))
        df = df.drop(keyCol).join(existing, list(matchCols), "left")
    else:
        df = df.drop(keyCol).withColumn("_existing_key", F.lit(None).cast("bigint"))
    w = Window.partitionBy(F.col("_existing_key").isNull()).orderBy(*[F.col(c) for c in orderCols])
    return (
        df.withColumn(keyCol, F.coalesce(F.col("_existing_key"), (F.row_number().over(w) + F.lit(start))).cast("bigint"))
        .drop("_existing_key")
    )


def deleteWhere(spark: SparkSession, fullName: str, predicate: str) -> int:
    if not tableExists(spark, fullName):
        return 0
    versionBefore = tableVersion(spark, fullName)
    spark.sql("DELETE FROM %s WHERE %s" % (fullName, predicate))
    return lastOperationMetrics(spark, fullName, versionBefore).get("numDeletedRows", 0)


def loadAuditColumns(df: DataFrame, batchId: int, packageExecutionId: int) -> DataFrame:
    return (
        df.withColumn("lineage_key", F.lit(int(packageExecutionId)).cast("bigint"))
        .withColumn("batch_id", F.lit(int(batchId)).cast("bigint"))
        .withColumn("package_execution_id", F.lit(int(packageExecutionId)).cast("bigint"))
        .withColumn("load_datetime", F.current_timestamp())
    )


# ---------------------------------------------------------------- rejects


def rejectRows(
    spark: SparkSession, catalog: str, df: DataFrame, objectName: str, reasonCode: str,
    batchId: int, packageExecutionId: int, businessKeyColumn: Optional[str] = None,
    sourceSystemCode: Optional[str] = None, rejectStage: str = "Fact",
) -> int:
    """Error-output rows go to etl.rejected_record via the shared framework
    (the legacy err.Rejected* tables collapse onto this one log)."""
    return int(
        control.logRejectedRecordSet(
            spark, catalog, objectName, df, batchId=batchId, packageExecutionId=packageExecutionId,
            sourceSystemCode=sourceSystemCode, rejectStage=rejectStage, rejectReasonCode=reasonCode,
            businessKeyColumn=businessKeyColumn,
        ) or 0
    )


# ---------------------------------------------------------------- inferred members


def inferMembers(
    spark: SparkSession, catalog: str, spec: DimensionSpec, missingDf: DataFrame, businessKeyCol: str,
    batchId: int, packageExecutionId: int, sourceSystemCode: Optional[str] = None,
    regionCol: Optional[str] = None,
) -> int:
    """Insert inferred (early-arriving) members: key = max + n, valid from
    2013-01-01 to the open end, Is Inferred Member = 1. Later dimension loads
    overwrite the attributes when the real record arrives."""
    fullName = tableName(catalog, "gold", spec.table)
    dim = spark.table(fullName)
    existing = dim.select(F.col(spec.businessKeyCol).cast("string").alias("_bk"))
    keys = (
        missingDf.select(F.col(businessKeyCol).cast("string").alias("_bk"), *( [F.col(regionCol).alias("_region")] if regionCol else []))
        .where(F.col("_bk").isNotNull()).dropDuplicates(["_bk"])
        .join(existing, "_bk", "left_anti")
    )
    if keys.limit(1).count() == 0:
        return 0
    maxKey = dim.agg(F.max(spec.keyCol)).first()[0] or 0
    keys = keys.withColumn("_new_key", F.row_number().over(Window.orderBy("_bk")) + F.lit(int(maxKey)))
    columns = []
    bkType = dim.schema[spec.businessKeyCol].dataType
    for field in dim.schema.fields:
        name = field.name
        if name == spec.keyCol:
            columns.append(F.col("_new_key").cast(field.dataType).alias(name))
        elif name == spec.businessKeyCol:
            columns.append(F.col("_bk").cast(bkType).alias(name))
        elif name == spec.validFromCol:
            columns.append(F.lit(INFERRED_VALID_FROM).cast(field.dataType).alias(name))
        elif name == spec.validToCol:
            columns.append(F.lit(SCD2_OPEN_END).cast(field.dataType).alias(name))
        elif name == spec.isCurrentCol:
            columns.append(F.lit(True).cast(field.dataType).alias(name))
        elif name == spec.isInferredCol:
            columns.append(F.lit(True).cast(field.dataType).alias(name))
        elif name == "inferred_created_on":
            columns.append(F.current_timestamp().cast(field.dataType).alias(name))
        elif name in ("lineage_key", "last_load_package_execution_id"):
            columns.append(F.lit(int(packageExecutionId)).cast(field.dataType).alias(name))
        elif name == "last_load_batch_id":
            columns.append(F.lit(int(batchId)).cast(field.dataType).alias(name))
        elif name == "source_system_code" and sourceSystemCode is not None:
            columns.append(F.lit(sourceSystemCode).cast(field.dataType).alias(name))
        elif name == "region_code" and regionCol is not None:
            columns.append(F.col("_region").cast(field.dataType).alias(name))
        elif isinstance(field.dataType, T.StringType):
            columns.append(F.lit("Unknown").alias(name))
        else:
            columns.append(F.lit(None).cast(field.dataType).alias(name))
    return appendRows(spark, fullName, keys.select(*columns))


# ---------------------------------------------------------------- late-arriving queue


def queueLateArrivers(
    spark: SparkSession, catalog: str, missingDf: DataFrame, dimensionName: str, businessKeyCol: str,
    firstSeenObjectName: str, batchId: int, packageExecutionId: int, sourceSystemCode: Optional[str] = None,
    sourceSystemCol: Optional[str] = None, attributesJsonCol: Optional[str] = None,
) -> int:
    """work.LateArrivingDimensionQueue: one open row per (dimension, business
    key); repeats bump OccurrenceCount instead of duplicating the queue row."""
    from delta.tables import DeltaTable

    fullName = tableName(catalog, "silver", LATE_ARRIVING_QUEUE_TABLE)
    srcCode = F.col(sourceSystemCol) if sourceSystemCol else F.lit(sourceSystemCode)
    attrs = F.col(attributesJsonCol) if attributesJsonCol else F.lit(None).cast("string")
    candidates = (
        missingDf.where(F.col(businessKeyCol).isNotNull())
        .groupBy(F.col(businessKeyCol).cast("string").alias("MissingBusinessKey"))
        .agg(F.count(F.lit(1)).alias("OccurrenceCount"), F.first(srcCode, ignorenulls=True).alias("SourceSystemCode"), F.first(attrs, ignorenulls=True).alias("InferredAttributesJson"))
        .withColumn("DimensionName", F.lit(dimensionName))
    )
    if candidates.limit(1).count() == 0:
        return 0
    rows = candidates.select(
        F.xxhash64(F.lit(dimensionName), F.col("MissingBusinessKey"), F.current_timestamp()).alias("QueueRowId"),
        F.lit(int(batchId)).cast("bigint").alias("BatchId"),
        F.lit(int(packageExecutionId)).cast("bigint").alias("PackageExecutionId"),
        "DimensionName",
        "MissingBusinessKey",
        "SourceSystemCode",
        F.lit(firstSeenObjectName).alias("FirstSeenObjectName"),
        F.current_timestamp().alias("FirstSeenAtUtc"),
        F.col("OccurrenceCount").cast("int").alias("OccurrenceCount"),
        "InferredAttributesJson",
        F.lit(False).alias("StubCreatedFlag"),
        F.lit(None).cast("timestamp").alias("StubCreatedAtUtc"),
        F.lit(False).alias("ResolvedFlag"),
        F.lit(None).cast("timestamp").alias("ResolvedAtUtc"),
        F.lit(None).cast("string").alias("ResolutionNote"),
    )
    ensureTable(spark, fullName, rows)
    (
        DeltaTable.forName(spark, fullName).alias("q")
        .merge(rows.alias("n"), "q.DimensionName = n.DimensionName AND q.MissingBusinessKey = n.MissingBusinessKey AND q.ResolvedFlag = false")
        .whenMatchedUpdate(set={"OccurrenceCount": "q.OccurrenceCount + n.OccurrenceCount", "BatchId": "n.BatchId", "PackageExecutionId": "n.PackageExecutionId"})
        .whenNotMatchedInsertAll()
        .execute()
    )
    return rows.count()


# ---------------------------------------------------------------- Fact Load Hold


def retryLimitFor(regionCode: Optional[str]) -> int:
    return HOLD_RETRY_LIMITS.get(regionCode or "", DEFAULT_HOLD_RETRY_LIMIT)


def retryLimitColumn(regionCol: Column) -> Column:
    expr = F.lit(DEFAULT_HOLD_RETRY_LIMIT)
    for region, limit in HOLD_RETRY_LIMITS.items():
        expr = F.when(regionCol == region, F.lit(limit)).otherwise(expr)
    return expr


def holdRows(
    spark: SparkSession, catalog: str, df: DataFrame, targetFactName: str, missingDimensionName: str,
    missingBusinessKeyCol: str, holdReasonCode: str, batchId: int, packageExecutionId: int,
    regionCol: str = "RegionCode", businessDateCol: Optional[str] = None,
    naturalKeyHashCol: Optional[str] = None, naturalKeyCols: Sequence[str] = (),
    sourceSystemCode: Optional[str] = None,
) -> int:
    """Fact.[Fact Load Hold]: park the full source row as JSON with the
    region's retry budget, instead of loading it against the unknown member."""
    fullName = tableName(catalog, "gold", FACT_LOAD_HOLD_TABLE)
    payloadCols = [c for c in df.columns if not c.endswith("_miss")]
    nkHash = F.col(naturalKeyHashCol) if naturalKeyHashCol else naturalKeyHash(*[F.col(c) for c in naturalKeyCols])
    nkText = F.concat_ws("|", *[F.col(c).cast("string") for c in naturalKeyCols]) if naturalKeyCols else F.lit(None).cast("string")
    rows = df.select(
        F.lit(targetFactName).alias("target_fact_name"),
        F.lit(sourceSystemCode).cast("string").alias("source_system_code"),
        F.col(regionCol).cast("string").alias("region_code"),
        nkHash.alias("natural_key_hash"),
        nkText.alias("natural_key_text"),
        (F.col(businessDateCol).cast("date") if businessDateCol else F.lit(None).cast("date")).alias("business_date"),
        F.lit(missingDimensionName).alias("missing_dimension_name"),
        F.col(missingBusinessKeyCol).cast("string").alias("missing_business_key"),
        F.lit(holdReasonCode).alias("hold_reason_code"),
        F.to_json(F.struct(*[F.col(c) for c in payloadCols])).alias("source_payload"),
        F.lit(0).alias("retry_count"),
        retryLimitColumn(F.col(regionCol)).alias("max_retry_count"),
        F.lit(HOLD_STATUS_HELD).alias("hold_status_code"),
        F.current_timestamp().alias("first_held_datetime"),
        F.lit(None).cast("timestamp").alias("last_retry_datetime"),
        F.lit(None).cast("timestamp").alias("released_datetime"),
        F.lit(None).cast("bigint").alias("released_fact_key"),
        F.lit(int(batchId)).cast("bigint").alias("original_batch_id"),
        F.lit(int(batchId)).cast("bigint").alias("last_batch_id"),
    )
    rows = assignSurrogateKeys(spark, fullName, rows, "fact_load_hold_key", ["natural_key_hash", "missing_business_key"])
    rows = rows.select("fact_load_hold_key", *[c for c in rows.columns if c != "fact_load_hold_key"])
    existingOpen = (
        spark.table(fullName).where((F.col("target_fact_name") == targetFactName) & (F.col("hold_status_code") == HOLD_STATUS_HELD))
        .select("natural_key_hash", "missing_dimension_name")
        if tableExists(spark, fullName) else None
    )
    if existingOpen is not None:
        rows = rows.join(existingOpen, ["natural_key_hash", "missing_dimension_name"], "left_anti")
    return appendRows(spark, fullName, rows)


def readHeldRows(spark: SparkSession, catalog: str, targetFactName: str, payloadSchema: T.StructType, regionCode: Optional[str] = None) -> DataFrame:
    """Open holds for a fact, oldest first, with the parked source row
    re-hydrated into the same columns the package's data flow expects."""
    fullName = tableName(catalog, "gold", FACT_LOAD_HOLD_TABLE)
    if not tableExists(spark, fullName):
        return spark.createDataFrame([], payloadSchema.add("fact_load_hold_key", T.LongType()).add("retry_count", T.IntegerType()).add("max_retry_count", T.IntegerType()))
    held = spark.table(fullName).where((F.col("target_fact_name") == targetFactName) & (F.col("hold_status_code") == HOLD_STATUS_HELD))
    if regionCode is not None:
        held = held.where(F.col("region_code") == regionCode)
    return (
        held.orderBy("first_held_datetime", "fact_load_hold_key")
        .select(F.from_json("source_payload", payloadSchema).alias("p"), "fact_load_hold_key", "retry_count", "max_retry_count")
        .select("p.*", "fact_load_hold_key", "retry_count", "max_retry_count")
    )


def settleHolds(spark: SparkSession, catalog: str, resolvedKeysDf: Optional[DataFrame], retriedKeysDf: Optional[DataFrame], batchId: int, packageExecutionId: int) -> Dict[str, int]:
    """Release resolved holds, bump retry counters for the rest and abandon
    the ones that just used up the region's retry budget."""
    from delta.tables import DeltaTable

    fullName = tableName(catalog, "gold", FACT_LOAD_HOLD_TABLE)
    counts = {"released": 0, "retried": 0, "abandoned": 0}
    if not tableExists(spark, fullName):
        return counts
    target = DeltaTable.forName(spark, fullName)
    if resolvedKeysDf is not None and resolvedKeysDf.limit(1).count() > 0:
        versionBefore = tableVersion(spark, fullName)
        (
            target.alias("h").merge(resolvedKeysDf.select("fact_load_hold_key", "released_fact_key").alias("r"), "h.fact_load_hold_key = r.fact_load_hold_key")
            .whenMatchedUpdate(set={
                "hold_status_code": F.lit(HOLD_STATUS_RELEASED), "released_datetime": F.current_timestamp(),
                "released_fact_key": F.col("r.released_fact_key"), "last_batch_id": F.lit(int(batchId)),
            })
            .execute()
        )
        counts["released"] = lastOperationMetrics(spark, fullName, versionBefore).get("numTargetRowsUpdated", 0)
    if retriedKeysDf is not None and retriedKeysDf.limit(1).count() > 0:
        versionBefore = tableVersion(spark, fullName)
        (
            target.alias("h").merge(retriedKeysDf.select("fact_load_hold_key").alias("r"), "h.fact_load_hold_key = r.fact_load_hold_key")
            .whenMatchedUpdate(set={
                "retry_count": F.col("h.retry_count") + 1, "last_retry_datetime": F.current_timestamp(), "last_batch_id": F.lit(int(batchId)),
                "hold_status_code": F.when(F.col("h.retry_count") + 1 >= F.col("h.max_retry_count"), F.lit(HOLD_STATUS_ABANDONED)).otherwise(F.lit(HOLD_STATUS_HELD)),
            })
            .execute()
        )
        counts["retried"] = lastOperationMetrics(spark, fullName, versionBefore).get("numTargetRowsUpdated", 0)
        abandoned = spark.table(fullName).where((F.col("hold_status_code") == HOLD_STATUS_ABANDONED) & (F.col("last_batch_id") == int(batchId)) & (F.col("released_datetime").isNull()))
        counts["abandoned"] = abandoned.count()
        if counts["abandoned"] > 0:
            rejectRows(
                spark, catalog, abandoned.select("target_fact_name", "natural_key_text", "missing_dimension_name", "missing_business_key", "retry_count"),
                "Fact.Fact Load Hold", REJECT_HOLD_EXPIRED, batchId, packageExecutionId, businessKeyColumn="natural_key_text",
            )
    return counts


def coalesceHeldReplays(df: DataFrame, keyCol: str = "natural_key_hash") -> DataFrame:
    """A held row may re-arrive from the source in the same window as its replay
    from Fact Load Hold. Keep one row per natural key - the fresh source payload
    wins - but carry the hold key / retry counters so the hold is settled."""
    w = Window.partitionBy(keyCol)
    pick = Window.partitionBy(keyCol).orderBy(F.col("fact_load_hold_key").asc_nulls_first())
    return (
        df.withColumn("_hold_key", F.max("fact_load_hold_key").over(w))
        .withColumn("_retry", F.max("retry_count").over(w))
        .withColumn("_max_retry", F.max("max_retry_count").over(w))
        .withColumn("_rn", F.row_number().over(pick))
        .where(F.col("_rn") == 1)
        .withColumn("fact_load_hold_key", F.col("_hold_key"))
        .withColumn("retry_count", F.col("_retry"))
        .withColumn("max_retry_count", F.col("_max_retry"))
        .drop("_hold_key", "_retry", "_max_retry", "_rn")
    )


def splitHeld(df: DataFrame, missCols: Sequence[str]) -> Tuple[DataFrame, DataFrame]:
    """Rows whose flagged lookups all resolved (or whose hold budget is spent)
    load now; the rest stay held. Expects the readHeldRows() columns."""
    anyMiss = F.lit(False)
    for c in missCols:
        anyMiss = anyMiss | F.coalesce(F.col(c), F.lit(False))
    budgetSpent = (F.col("retry_count") + 1) >= F.col("max_retry_count")
    loadable = df.where(~anyMiss | budgetSpent)
    stillHeld = df.where(anyMiss & ~budgetSpent)
    return loadable, stillHeld


# ---------------------------------------------------------------- fact rekey queue


def enqueueRekey(
    spark: SparkSession, catalog: str, df: DataFrame, factObjectName: str, businessKeyCol: str,
    dimensionName: str, currentKeyCol: str, correctedKeyCol: str, reasonCode: str,
    batchId: int, packageExecutionId: int, effectiveDateCol: Optional[str] = None, priority: int = 5,
) -> int:
    fullName = tableName(catalog, "silver", FACT_REKEY_QUEUE_TABLE)
    rows = df.select(
        F.xxhash64(F.lit(factObjectName), F.col(businessKeyCol).cast("string"), F.lit(dimensionName), F.current_timestamp()).alias("QueueRowId"),
        F.lit(int(batchId)).cast("bigint").alias("BatchId"),
        F.lit(int(packageExecutionId)).cast("bigint").alias("PackageExecutionId"),
        F.lit(factObjectName).alias("FactObjectName"),
        F.col(businessKeyCol).cast("string").alias("FactBusinessKey"),
        F.lit(dimensionName).alias("DimensionName"),
        F.col(currentKeyCol).cast("int").alias("CurrentSurrogateKey"),
        F.col(correctedKeyCol).cast("int").alias("CorrectedSurrogateKey"),
        F.lit(reasonCode).alias("RekeyReasonCode"),
        (F.col(effectiveDateCol).cast("date") if effectiveDateCol else F.lit(None).cast("date")).alias("EffectiveDate"),
        F.lit(priority).alias("RekeyPriority"),
        F.lit(False).alias("AppliedFlag"),
        F.lit(None).cast("timestamp").alias("AppliedAtUtc"),
        F.lit(0).alias("AttemptCount"),
        F.lit(None).cast("string").alias("LastErrorText"),
        F.current_timestamp().alias("CreatedAtUtc"),
    )
    return appendRows(spark, fullName, rows)


# ---------------------------------------------------------------- row counts


def logFactRowCounts(spark: SparkSession, catalog: str, packageExecutionId: int, objectName: str, sourceRowCount: int, targetFullName: str, merged: Dict[str, int], rejected: int = 0) -> None:
    targetRowCount = spark.table(targetFullName).count() if tableExists(spark, targetFullName) else 0
    control.logRowCount(
        spark, catalog, packageExecutionId, objectName,
        sourceRowCount=int(sourceRowCount), targetRowCount=int(targetRowCount),
        insertRowCount=int(merged.get("inserted", 0)), updateRowCount=int(merged.get("updated", 0)),
        deleteRowCount=int(merged.get("deleted", 0)), rejectRowCount=int(rejected),
    )


def setRunCounts(run, rowsRead: int = 0, merged: Optional[Dict[str, int]] = None, rejected: int = 0) -> None:
    merged = merged or {}
    run.rowsRead = int(rowsRead)
    run.rowsInserted = int(merged.get("inserted", 0))
    run.rowsUpdated = int(merged.get("updated", 0))
    run.rowsDeleted = int(merged.get("deleted", 0))
    run.rowsRejected = int(rejected)
