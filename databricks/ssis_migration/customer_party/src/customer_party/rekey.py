"""DIM_Rekey_LateArriving: reusable late-arriving dimension rekey routine.

Legacy `Integration.usp_RekeyLateArrivingDimensions` runs in two phases:

* ASSIGN  - retry held facts / queue rows: resolve the missing business key against the
            dimension's current rows; resolved rows are RELEASED with the real surrogate,
            unresolved rows get retry_count + 1 and are ABANDONED (reason HOLD_EXPIRED)
            once the regional retry limit is reached (NA 3, EU 7, APAC 21, other 5).
* REPOINT - facts that point at an inferred member (or the reserved Unknown/Not-yet-
            assigned keys 0 / -1 / -4) are re-pointed to the real, current member that
            now carries the same business key.

Fact-owning groups call `rekeyFactTable(...)` (Delta MERGE) or the pure
`repointFactKeys(...)` with their own fact DataFrame and one of the
`DimensionRekeySpec`s exported from `CUSTOMER_PARTY_DIMENSIONS`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_party.config import PipelineConfig, utcNow
from customer_party.dim_customer import DIM_CUSTOMER
from customer_party.dim_supporting import (
    DIM_CUSTOMER_CATEGORY,
    DIM_CUSTOMER_SEGMENT,
    DIM_EMPLOYEE,
    DIM_PROMOTION,
    DIM_SALES_TERRITORY,
    DIM_SALESPERSON,
)
from customer_party.tables import (
    appendTable,
    overwriteTable,
    readTableOrEmpty,
    withLoadMetadata,
)

WORK_LATE_ARRIVING_QUEUE = "work_late_arriving_dimension_queue"
WORK_FACT_REKEY_QUEUE = "work_fact_rekey_queue"
REKEY_RESULT = "rekey_result"

QUEUE_SCHEMA = (
    "queue_id bigint, dimension_name string, business_key string, region_code string, fact_table string, "
    "fact_row_key string, source_payload string, status string, retry_count int, queued_at timestamp, "
    "resolved_at timestamp, assigned_surrogate_key bigint, abandon_reason string"
)

RETRY_LIMITS: dict[str, int] = {"NA": 3, "EU": 7, "APAC": 21}
DEFAULT_RETRY_LIMIT = 5
MAX_QUEUE_AGE_DAYS = 30
PLACEHOLDER_KEYS: tuple[int, ...] = (0, -1, -4)


@dataclass(frozen=True)
class DimensionRekeySpec:
    dimensionName: str
    table: str
    keyCol: str
    businessKeyCol: str


CUSTOMER_PARTY_DIMENSIONS: tuple[DimensionRekeySpec, ...] = (
    DimensionRekeySpec("Customer", DIM_CUSTOMER, "customer_key", "customer_business_key"),
    DimensionRekeySpec("Customer Category", DIM_CUSTOMER_CATEGORY, "customer_category_key", "wwi_customer_category_id"),
    DimensionRekeySpec("Customer Segment", DIM_CUSTOMER_SEGMENT, "customer_segment_key", "wwi_segment_id"),
    DimensionRekeySpec("Employee", DIM_EMPLOYEE, "employee_key", "wwi_employee_id"),
    DimensionRekeySpec("Salesperson", DIM_SALESPERSON, "salesperson_key", "wwi_salesperson_id"),
    DimensionRekeySpec("Sales Territory", DIM_SALES_TERRITORY, "sales_territory_key", "wwi_territory_id"),
    DimensionRekeySpec("Promotion", DIM_PROMOTION, "promotion_key", "wwi_promotion_id"),
)


def retryLimitFor(regionCol: Column) -> Column:
    expr = F.lit(DEFAULT_RETRY_LIMIT)
    for region, limit in RETRY_LIMITS.items():
        expr = F.when(F.upper(F.trim(regionCol)) == region, F.lit(limit)).otherwise(expr)
    return expr


def escalationCode(queuedAtCol: Column, retryCountCol: Column, asOfCol: Column, maxQueueAgeDays: int = MAX_QUEUE_AGE_DAYS) -> Column:
    """The package's `Classify Queue` derived column."""
    age = F.datediff(asOfCol, queuedAtCol)
    return F.when(age > maxQueueAgeDays, F.lit("ESCALATE")).when(retryCountCol >= 5, F.lit("MANUAL")).otherwise(F.lit("RETRY"))


def currentMembers(dimDf: DataFrame, spec: DimensionRekeySpec) -> DataFrame:
    return dimDf.where(
        F.col("is_current_row") & (F.col(spec.keyCol) >= 0) & ~F.coalesce(F.col("is_inferred_member"), F.lit(False))
    ).select(F.col(spec.businessKeyCol).cast("string").alias("_bk"), F.col(spec.keyCol).alias("_resolved_key"))


def resolveQueue(queueDf: DataFrame, dimDf: DataFrame, spec: DimensionRekeySpec, asOf: datetime) -> DataFrame:
    """ASSIGN phase for one dimension: returns the queue rows for that dimension with their new state."""
    now = F.lit(asOf).cast("timestamp")
    held = queueDf.where((F.col("dimension_name") == spec.dimensionName) & F.col("status").isin("HELD", "RETRY"))
    joined = held.join(currentMembers(dimDf, spec), F.col("business_key") == F.col("_bk"), "left")
    resolved = F.col("_resolved_key").isNotNull()
    newRetry = F.col("retry_count") + 1
    expired = ~resolved & (newRetry >= retryLimitFor(F.col("region_code")))
    return (
        joined.withColumn("assigned_surrogate_key", F.when(resolved, F.col("_resolved_key")))
        .withColumn("retry_count", F.when(resolved, F.col("retry_count")).otherwise(newRetry))
        .withColumn("status", F.when(resolved, F.lit("RELEASED")).when(expired, F.lit("ABANDONED")).otherwise(F.lit("RETRY")))
        .withColumn("resolved_at", F.when(resolved, now).otherwise(F.col("resolved_at")))
        .withColumn("abandon_reason", F.when(expired, F.lit("HOLD_EXPIRED")).otherwise(F.col("abandon_reason")))
        .withColumn("escalation_code", escalationCode(F.col("queued_at"), F.col("retry_count"), now))
        .drop("_bk", "_resolved_key")
    )


def repointFactKeys(factDf: DataFrame, dimDf: DataFrame, spec: DimensionRekeySpec, factKeyCol: str, factBusinessKeyCol: str) -> DataFrame:
    """REPOINT phase (pure): rows whose `factKeyCol` is a placeholder or an inferred member get the real key.

    `factBusinessKeyCol` is the natural key the fact row carries (e.g. `customer_business_key`).
    Returns the fact DataFrame with `factKeyCol` replaced and a boolean `was_rekeyed` column.
    """
    inferredKeys = dimDf.where(F.coalesce(F.col("is_inferred_member"), F.lit(False))).select(F.col(spec.keyCol).alias("_inferred_key"))
    members = currentMembers(dimDf, spec)
    joined = (
        factDf.join(inferredKeys, F.col(factKeyCol) == F.col("_inferred_key"), "left")
        .join(members, F.col(factBusinessKeyCol).cast("string") == F.col("_bk"), "left")
    )
    needsRekey = (F.col(factKeyCol).isin(*PLACEHOLDER_KEYS) | F.col("_inferred_key").isNotNull()) & F.col("_resolved_key").isNotNull()
    return (
        joined.withColumn("was_rekeyed", needsRekey)
        .withColumn(factKeyCol, F.when(needsRekey, F.col("_resolved_key")).otherwise(F.col(factKeyCol)))
        .drop("_inferred_key", "_bk", "_resolved_key")
    )


def rekeyFactTable(
    spark: SparkSession,
    factFqn: str,
    dimFqn: str,
    spec: DimensionRekeySpec,
    factKeyCol: str,
    factBusinessKeyCol: str,
    factRowIdCol: str,
) -> int:
    """REPOINT phase for a Delta fact table owned by another group (they call this with their own names).

    Performs `MERGE INTO fact USING resolved ON fact.<rowId> = resolved.<rowId> WHEN MATCHED THEN UPDATE SET <key>`.
    Returns the number of rows repointed.
    """
    fact = spark.table(factFqn)
    dim = spark.table(dimFqn)
    rekeyed = repointFactKeys(fact, dim, spec, factKeyCol, factBusinessKeyCol).where(F.col("was_rekeyed"))
    updates = rekeyed.select(F.col(factRowIdCol).alias("_row_id"), F.col(factKeyCol).alias("_new_key"))
    count = updates.count()
    if count == 0:
        return 0
    updates.createOrReplaceTempView("_customer_party_rekey_updates")
    spark.sql(
        f"MERGE INTO {factFqn} AS f USING _customer_party_rekey_updates AS u ON f.`{factRowIdCol}` = u._row_id "
        f"WHEN MATCHED THEN UPDATE SET f.`{factKeyCol}` = u._new_key"
    )
    return count


def runLateArrivingRekey(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """DIM_Rekey_LateArriving over the group's own dimensions.

    The late-arriving queue is fed by fact loads (other groups) when a fact row
    references a business key the dimension does not know yet. Within this
    group, the queue table is created (empty) if no fact load has populated it.
    """
    asOf = utcNow()
    queueFqn = cfg.fqn(WORK_LATE_ARRIVING_QUEUE)
    queue = readTableOrEmpty(spark, queueFqn, QUEUE_SCHEMA)
    if not spark.catalog.tableExists(queueFqn):
        overwriteTable(queue, queueFqn)
        queue = spark.table(queueFqn)
    queue = queue.select(*[c.split(" ")[0] for c in QUEUE_SCHEMA.split(", ")])

    processed: DataFrame | None = None
    resultRows: list[tuple[str, int, int, int, int]] = []
    for spec in CUSTOMER_PARTY_DIMENSIONS:
        dimFqn = cfg.fqn(spec.table)
        if not spark.catalog.tableExists(dimFqn):
            continue
        dim = spark.table(dimFqn)
        resolved = resolveQueue(queue, dim, spec, asOf)
        counts = resolved.groupBy("status").count().collect()
        byStatus = {r["status"]: r["count"] for r in counts}
        resultRows.append(
            (spec.dimensionName, byStatus.get("RELEASED", 0), byStatus.get("RETRY", 0), byStatus.get("ABANDONED", 0),
             dim.where(F.coalesce(F.col("is_inferred_member"), F.lit(False))).count())
        )
        processed = resolved if processed is None else processed.unionByName(resolved)

    if processed is not None:
        touched = processed.select(F.col("queue_id").alias("_qid"))
        untouched = queue.join(touched, F.col("queue_id") == F.col("_qid"), "left_anti")
        newQueue = untouched.unionByName(processed.select(*queue.columns))
        overwriteTable(newQueue, queueFqn)
        overwriteTable(
            withLoadMetadata(processed.where(F.col("status") == "RELEASED").select(
                "queue_id", "dimension_name", "business_key", "fact_table", "fact_row_key", "assigned_surrogate_key", "source_payload"
            ), cfg),
            cfg.fqn(WORK_FACT_REKEY_QUEUE),
        )

    result = spark.createDataFrame(
        resultRows,
        "dimension_name string, released_count bigint, retry_count bigint, abandoned_count bigint, inferred_member_count bigint",
    ).withColumn("run_at", F.lit(asOf).cast("timestamp"))
    appendTable(withLoadMetadata(result, cfg), cfg.fqn(REKEY_RESULT))
    return spark.table(cfg.fqn(REKEY_RESULT)).where(F.col("batch_id") == cfg.batchId)
