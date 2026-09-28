"""DIM_Rekey_LateArriving: late-arriving dimension queue -> inferred members -> fact rekey.

Port of the generated package (ssis/07_dimensions/build_dimension_packages.py,
build_dim_rekey_late_arriving) and Integration.usp_RekeyLateArrivingDimensions:

1. Classify every open `work.LateArrivingDimensionQueue` row:
   `QueueAgeDays > MaxQueueAgeDays OR RetryCount >= 5 (regional retry limit)`
   -> ESCALATE / MANUAL, otherwise RETRY. Escalated rows are rejected
   (`err.RejectedLookupFailure` -> etl.rejected_record via
   control.logRejectedRecordSet) and closed with an ABANDONED note; the legacy
   `HOLD_EXPIRED` error code is preserved.
2. ASSIGN phase - every RETRY row is resolved against the current, non-inferred
   member with the same business key. Missing members of dimensions that support
   inferred members get a positive-key stub (Integration.usp_InsertInferredMember)
   so that the fact can point at a real key instead of the -4 "Inferred Pending"
   placeholder. The queue row is written to `work.FactRekeyQueue`.
3. REPOINT phase - fact rows that still carry the placeholder key (-1 Unknown,
   -4 Inferred Pending or the stub key recorded on the queue row) are re-pointed at
   the corrected key with a Delta MERGE per (fact table, key column). Facts that
   have not been created yet (other sessions own gold.fact_*) are skipped and
   reported, not failed.
4. Verify: count fact rows still pointing at the reserved band for the rekeyed
   dimensions; close resolved queue rows; increment RetryCount on the rest.

Regional retry limits come from usp_RekeyLateArrivingDimensions: NA 3, EU 7,
APAC 21, default 5.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wwi_dimensions import scd, specs, tables

REGION_RETRY_LIMIT = {"NA": 3, "EU": 7, "APAC": 21}
DEFAULT_RETRY_LIMIT = 5
DEFAULT_MAX_QUEUE_AGE_DAYS = 30
INFERRED_PENDING_KEY = -4
UNKNOWN_KEY = -1
ESCALATION_ERROR_CODE = "HOLD_EXPIRED"


@dataclass
class RekeyResult:
    queueDepth: int = 0
    rowsEscalated: int = 0
    rowsResolved: int = 0
    rowsStubbed: int = 0
    rowsRetried: int = 0
    factRowsRekeyed: int = 0
    remainingUnknownFacts: int = 0
    skippedFactTables: List[str] = field(default_factory=list)


def retryLimitColumn(regionCol):
    expr = F.lit(DEFAULT_RETRY_LIMIT)
    for region, limit in REGION_RETRY_LIMIT.items():
        expr = F.when(F.upper(regionCol) == region, F.lit(limit)).otherwise(expr)
    return expr


def classifyQueue(queueDf: DataFrame, businessDate, maxQueueAgeDays: int = DEFAULT_MAX_QUEUE_AGE_DAYS) -> DataFrame:
    """Add QueueAgeDays, RetryLimit and EscalationCode (ESCALATE / MANUAL / RETRY) to open queue rows.

    A region is derived from SourceSystemCode / InferredAttributesJson when present so the
    regional retry limits of usp_RekeyLateArrivingDimensions apply; otherwise the default (5)
    from the generated package is used.
    """
    regionCol = F.coalesce(
        F.get_json_object(F.col("InferredAttributesJson"), "$.RegionCode"),
        F.regexp_extract(F.upper(F.coalesce(F.col("SourceSystemCode"), F.lit(""))), "(NA|EU|APAC)", 1),
    )
    age = F.datediff(F.lit(str(businessDate)).cast("date"), F.to_date(F.col("FirstSeenAtUtc")))
    return (
        queueDf.withColumn("RegionCode", regionCol)
        .withColumn("QueueAgeDays", age)
        .withColumn("RetryLimit", retryLimitColumn(regionCol))
        .withColumn(
            "EscalationCode",
            F.when(F.col("QueueAgeDays") > F.lit(int(maxQueueAgeDays)), F.lit("ESCALATE"))
            .when(F.col("RetryCount") >= F.col("RetryLimit"), F.lit("MANUAL"))
            .otherwise(F.lit("RETRY")),
        )
    )


def _spec(dimensionName: str) -> Optional[specs.DimensionSpec]:
    return specs.SPECS.get(dimensionName)


def resolveQueue(
    spark: SparkSession,
    catalog: str,
    classified: DataFrame,
    batchId: int,
    packageExecutionId: int,
    now: datetime,
) -> DataFrame:
    """ASSIGN phase. Returns RETRY rows with CorrectedSurrogateKey (NULL when still unresolved) and StubCreated flag."""
    retry = classified.where(F.col("EscalationCode") == "RETRY").localCheckpoint()
    resolvedParts = []
    for dimensionName in [r["DimensionName"] for r in retry.select("DimensionName").distinct().collect()]:
        dimensionSpec = _spec(dimensionName)
        rows = retry.where(F.col("DimensionName") == dimensionName)
        if dimensionSpec is None:
            resolvedParts.append(rows.withColumn("CorrectedSurrogateKey", F.lit(None).cast("bigint")).withColumn("_StubCreated", F.lit(False)))
            continue
        if not tables.tableExists(spark, dimensionSpec.fullTableName(catalog)):
            tables.ensureDimensionTable(spark, catalog, dimensionSpec)
        bk = dimensionSpec.businessKeyColumn
        key = dimensionSpec.keyColumn
        members = scd.currentMembers(spark, catalog, dimensionSpec).select(
            F.col(bk).cast("string").alias("_bk"), F.col(key).alias("_key"), F.col("IsInferredMember").alias("_inferred"))
        joined = rows.join(members, rows["MissingBusinessKey"] == members["_bk"], "left")
        # real (non-inferred) member arrived -> resolved
        resolved = joined.where(F.col("_key").isNotNull() & (F.col("_inferred") == False))  # noqa: E712
        resolvedParts.append(resolved.withColumn("CorrectedSurrogateKey", F.col("_key").cast("bigint"))
                             .withColumn("_StubCreated", F.lit(False)).drop("_bk", "_key", "_inferred"))
        unresolved = joined.where(F.col("_key").isNull() | (F.col("_inferred") == True)).drop("_bk", "_key", "_inferred")  # noqa: E712
        if dimensionSpec.supportsInferred:
            needStub = unresolved.where(F.col("StubCreatedFlag") == False)  # noqa: E712
            stubKeys = scd.insertInferredMembers(
                spark, catalog, dimensionSpec,
                needStub.select(F.col("MissingBusinessKey").alias(bk), F.col("RegionCode")),
                batchId, packageExecutionId, loadTimestamp=now,
            ).select(F.col(bk).cast("string").alias("_bk"), F.col(key).alias("_stubKey"))
            stubbed = unresolved.join(stubKeys, unresolved["MissingBusinessKey"] == stubKeys["_bk"], "left")
            resolvedParts.append(
                stubbed.withColumn("CorrectedSurrogateKey", F.col("_stubKey").cast("bigint"))
                .withColumn("_StubCreated", (F.col("StubCreatedFlag") == False) & F.col("_stubKey").isNotNull())  # noqa: E712
                .drop("_bk", "_stubKey"))
        else:
            resolvedParts.append(unresolved.withColumn("CorrectedSurrogateKey", F.lit(None).cast("bigint")).withColumn("_StubCreated", F.lit(False)))
    if not resolvedParts:
        return retry.withColumn("CorrectedSurrogateKey", F.lit(None).cast("bigint")).withColumn("_StubCreated", F.lit(False))
    out = resolvedParts[0]
    for part in resolvedParts[1:]:
        out = out.unionByName(part)
    return out


def repointFacts(spark: SparkSession, catalog: str, rekeyRows: DataFrame, batchId: int, result: RekeyResult) -> None:
    """REPOINT phase: MERGE every (fact table, key column) that carries a placeholder for a corrected member."""
    corrected = rekeyRows.where(F.col("CorrectedSurrogateKey").isNotNull()).select(
        "DimensionName", "MissingBusinessKey", "CurrentSurrogateKey", "CorrectedSurrogateKey").dropDuplicates()
    if corrected.rdd.isEmpty():
        return
    corrected.createOrReplaceTempView("_rekey_rows")
    for dimensionName, factTable, keyColumn in specs.FACT_REKEY_TARGETS:
        fullFact = f"{catalog}.gold.{factTable}"
        if not tables.tableExists(spark, fullFact):
            if fullFact not in result.skippedFactTables:
                result.skippedFactTables.append(fullFact)
            continue
        factColumns = {c.lower() for c in spark.table(fullFact).columns}
        if keyColumn.lower() not in factColumns:
            continue
        bkColumn = _factBusinessKeyColumn(dimensionName, factColumns)
        if bkColumn is None:
            continue
        before = spark.sql(
            f"""SELECT COUNT(*) AS c FROM {fullFact} f
                JOIN _rekey_rows r ON r.DimensionName = '{dimensionName}'
                 AND CAST(f.{bkColumn} AS STRING) = r.MissingBusinessKey
                 AND f.{keyColumn} IN (r.CurrentSurrogateKey, {INFERRED_PENDING_KEY}, {UNKNOWN_KEY})"""
        ).collect()[0]["c"]
        if not before:
            continue
        batchSet = ", f.LastLoadBatchId = " + str(int(batchId)) if "lastloadbatchid" in factColumns else ""
        spark.sql(
            f"""
            MERGE INTO {fullFact} AS f
            USING (SELECT * FROM _rekey_rows WHERE DimensionName = '{dimensionName}') AS r
              ON CAST(f.{bkColumn} AS STRING) = r.MissingBusinessKey
             AND f.{keyColumn} IN (r.CurrentSurrogateKey, {INFERRED_PENDING_KEY}, {UNKNOWN_KEY})
            WHEN MATCHED THEN UPDATE SET f.{keyColumn} = CAST(r.CorrectedSurrogateKey AS INT){batchSet}
            """
        )
        result.factRowsRekeyed += int(before)


_FACT_BK_CANDIDATES = {
    "Customer": ("CustomerBusinessKey", "BillToCustomerBusinessKey", "WWICustomerID"),
    "Stock Item": ("StockItemBusinessKey", "WWIStockItemID"),
    "Supplier": ("SupplierBusinessKey", "WWISupplierID"),
    "City": ("CityBusinessKey", "WWICityID"),
    "Salesperson": ("SalespersonBusinessKey", "WWISalespersonID"),
    "Promotion": ("PromotionBusinessKey", "WWIPromotionID"),
}


def _factBusinessKeyColumn(dimensionName: str, factColumns) -> Optional[str]:
    for candidate in _FACT_BK_CANDIDATES.get(dimensionName, ()):
        if candidate.lower() in factColumns:
            return candidate
    return None


def countRemainingUnknownFacts(spark: SparkSession, catalog: str) -> int:
    """'Count Remaining Unknown Member Facts' on Fact.Sale (Customer / Stock Item / City keys in the reserved band)."""
    fullFact = f"{catalog}.gold.fact_sale"
    if not tables.tableExists(spark, fullFact):
        return 0
    cols = {c.lower(): c for c in spark.table(fullFact).columns}
    preds = [f"{cols[c.lower()]} <= 0" for c in ("CustomerKey", "StockItemKey", "CityKey") if c.lower() in cols]
    if not preds:
        return 0
    return int(spark.sql(f"SELECT COUNT(*) AS c FROM {fullFact} WHERE {' OR '.join(preds)}").collect()[0]["c"])


def runRekey(
    spark: SparkSession,
    catalog: str,
    batchId: int,
    packageExecutionId: int,
    businessDate,
    maxQueueAgeDays: int = DEFAULT_MAX_QUEUE_AGE_DAYS,
    now: Optional[datetime] = None,
) -> Dict[str, object]:
    """Whole DIM_Rekey_LateArriving control flow. Returns the RekeyResult plus the escalated rows DataFrame."""
    now = now or datetime.utcnow().replace(microsecond=0)
    nowSql = f"TIMESTAMP '{now.strftime('%Y-%m-%d %H:%M:%S')}'"
    queueTable = tables.ensureLateArrivingQueue(spark, catalog)
    rekeyTable = tables.ensureFactRekeyQueue(spark, catalog)
    result = RekeyResult()

    openQueue = spark.table(queueTable).where(F.col("ResolvedFlag") == False)  # noqa: E712
    result.queueDepth = openQueue.count()
    # "Clear Fact Rekey Queue": the work table is rebuilt every run, but only for this batch so
    # a rerun for the same BatchId is idempotent and other batches' history is kept.
    spark.sql(f"DELETE FROM {rekeyTable} WHERE BatchId = {int(batchId)}")
    if not result.queueDepth:
        return {"result": result, "escalated": openQueue.limit(0)}

    classified = classifyQueue(openQueue, businessDate, maxQueueAgeDays).localCheckpoint()
    escalated = classified.where(F.col("EscalationCode").isin("ESCALATE", "MANUAL"))
    result.rowsEscalated = escalated.count()

    assigned = resolveQueue(spark, catalog, classified, batchId, packageExecutionId, now).localCheckpoint()
    result.rowsStubbed = assigned.where(F.col("_StubCreated")).count()
    resolvedRows = assigned.where(F.col("CorrectedSurrogateKey").isNotNull())
    result.rowsResolved = resolvedRows.count()

    rekeyRows = (
        assigned.select(
            F.col("QueueRowId"),
            F.lit(int(batchId)).cast("bigint").alias("BatchId"),
            F.lit(int(packageExecutionId)).cast("bigint").alias("PackageExecutionId"),
            F.coalesce(F.col("FirstSeenObjectName"), F.lit("Fact.Sale")).alias("FactObjectName"),
            F.col("MissingBusinessKey").alias("FactBusinessKey"),
            F.col("DimensionName"),
            F.coalesce(F.col("PlaceholderKey").cast("bigint"),
                       F.when(F.col("StubCreatedFlag"), F.lit(INFERRED_PENDING_KEY)).otherwise(F.lit(UNKNOWN_KEY)).cast("bigint")).alias("CurrentSurrogateKey"),
            F.col("CorrectedSurrogateKey"),
            F.when(F.col("_StubCreated"), F.lit("INFERRED_STUB")).when(F.col("CorrectedSurrogateKey").isNotNull(), F.lit("LATE_ARRIVING")).otherwise(F.lit("UNRESOLVED")).alias("RekeyReasonCode"),
            F.lit(str(businessDate)).cast("date").alias("EffectiveDate"),
            F.when(F.col("DimensionName") == "Customer", F.lit(1)).when(F.col("DimensionName") == "Stock Item", F.lit(2)).when(F.col("DimensionName") == "City", F.lit(3)).otherwise(F.lit(9)).cast("smallint").alias("RekeyPriority"),
            F.lit(False).alias("AppliedFlag"),
            F.lit(None).cast("timestamp").alias("AppliedAtUtc"),
            F.lit(0).cast("smallint").alias("AttemptCount"),
            F.lit(None).cast("string").alias("LastErrorText"),
            F.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp").alias("CreatedAtUtc"),
            F.col("MissingBusinessKey"),
        )
    ).localCheckpoint()
    rekeyRows.drop("MissingBusinessKey").write.format("delta").mode("append").saveAsTable(rekeyTable)

    repointFacts(spark, catalog, rekeyRows, batchId, result)
    spark.sql(
        f"""UPDATE {rekeyTable} SET AppliedFlag = true, AppliedAtUtc = {nowSql}, AttemptCount = AttemptCount + 1
             WHERE BatchId = {int(batchId)} AND CorrectedSurrogateKey IS NOT NULL"""
    )
    result.remainingUnknownFacts = countRemainingUnknownFacts(spark, catalog)

    # Close resolved entries, mark stubs, abandon escalated rows, bump the retry count on the rest.
    assigned.select("QueueRowId", "CorrectedSurrogateKey", "_StubCreated").createOrReplaceTempView("_rekey_assigned")
    spark.sql(
        f"""
        MERGE INTO {queueTable} AS q
        USING _rekey_assigned AS a ON q.QueueRowId = a.QueueRowId AND q.ResolvedFlag = false
        WHEN MATCHED AND a.CorrectedSurrogateKey IS NOT NULL AND NOT a._StubCreated AND COALESCE(q.StubCreatedFlag, false) = false THEN UPDATE SET
             q.ResolvedFlag = true, q.ResolvedAtUtc = {nowSql}, q.ResolvedByExecutionId = {int(packageExecutionId)},
             q.ResolutionNote = CONCAT('Resolved to surrogate key ', CAST(a.CorrectedSurrogateKey AS STRING))
        WHEN MATCHED AND a._StubCreated THEN UPDATE SET
             q.StubCreatedFlag = true, q.StubCreatedAtUtc = {nowSql}, q.PlaceholderKey = CAST(a.CorrectedSurrogateKey AS INT),
             q.RetryCount = q.RetryCount + 1
        WHEN MATCHED THEN UPDATE SET q.RetryCount = q.RetryCount + 1
        """
    )
    # A stub that was already created earlier and whose real member has now arrived resolves too.
    spark.sql(
        f"""
        MERGE INTO {queueTable} AS q
        USING (SELECT a.QueueRowId FROM _rekey_assigned a
                 JOIN {queueTable} q2 ON q2.QueueRowId = a.QueueRowId
                WHERE a.CorrectedSurrogateKey IS NOT NULL AND NOT a._StubCreated AND q2.StubCreatedFlag = true
                  AND q2.ResolvedFlag = false) AS r
           ON q.QueueRowId = r.QueueRowId
        WHEN MATCHED THEN UPDATE SET q.ResolvedFlag = true, q.ResolvedAtUtc = {nowSql},
             q.ResolvedByExecutionId = {int(packageExecutionId)}, q.ResolutionNote = 'Inferred member enriched by real member'
        """
    )
    escalated.select("QueueRowId", "EscalationCode").createOrReplaceTempView("_rekey_escalated")
    spark.sql(
        f"""
        MERGE INTO {queueTable} AS q
        USING _rekey_escalated AS e ON q.QueueRowId = e.QueueRowId AND q.ResolvedFlag = false
        WHEN MATCHED THEN UPDATE SET q.ResolvedFlag = true, q.ResolvedAtUtc = {nowSql},
             q.ResolvedByExecutionId = {int(packageExecutionId)},
             q.ResolutionNote = CONCAT('ABANDONED ({ESCALATION_ERROR_CODE}): ', e.EscalationCode)
        """
    )
    result.rowsRetried = result.queueDepth - result.rowsEscalated - assigned.where(
        F.col("CorrectedSurrogateKey").isNotNull() & ~F.col("_StubCreated")).count()
    return {"result": result, "escalated": escalated, "assigned": assigned}
