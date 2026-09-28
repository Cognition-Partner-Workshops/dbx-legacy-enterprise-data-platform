"""Party crosswalk resolution: WWI OLTP customer -> surviving ERP party.

Replaces the implicit crosswalk logic of the legacy estate:

* ``WWI_MDM.PARTY_XREF`` (oracle/tables) maps a source-system key to a
  ``CUST_ID``. It has no foreign key by design - "historical rows reference
  purged parties" - so an xref may point at a party that no longer survives.
* ``WWI_MDM.MDM_MERGE_HISTORY`` records every merge as
  ``MERGED_PARTY_ID -> SURVIVOR_PARTY_ID``; the loser stays in ``CUST_MASTER``
  with ``CUST_STATUS_CD`` ``'MG'`` (``PKG_CUSTOMER_MASTER.merge_customer`` writes
  ``'M'``; the table CHECK constraint and ``FN_CUSTOMER_STATUS`` use ``'MG'`` -
  both spellings are treated as merged here). ``UNMERGE_FLG = 'Y'`` rows are
  reversed merges and are ignored, as ``FN_CUSTOMER_STATUS`` does.
* ``PRC_LOAD_CUSTOMER_INTERFACE`` only inserts an xref when none exists for the
  key, so duplicates are never resolved in the legacy code; the deterministic
  pick used here is documented on :func:`rankXrefCandidates`.

Output: ``sales_silver.party_resolution`` - exactly one row per OLTP customer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable, readTable

PARTY_RESOLUTION_TABLE = "party_resolution"
UNKNOWN_PARTY_ID = -1
MAX_MERGE_HOPS = 10

STATUS_DIRECT = "DIRECT"
STATUS_MERGED = "MERGED"
STATUS_MERGED_MULTI_HOP = "MERGED_MULTI_HOP"
STATUS_RETIRED_NO_SURVIVOR = "RETIRED_NO_SURVIVOR"
STATUS_MISSING_XREF = "MISSING_XREF"
STATUS_DUPLICATE_XREF = "DUPLICATE_XREF"

# SOURCE_SYS_CD values that denote the WideWorldImporters OLTP in PARTY_XREF.
# The Oracle seed (oracle/seed/01_source_system_ref.sql) never lists the OLTP;
# stg.usp_DeduplicateCustomer ranks the code 'WWI_OLTP' and the generator
# contract uses 'WWIOLTP', so both are accepted.
WWI_OLTP_SOURCE_SYSTEM_CODES: tuple[str, ...] = ("WWI_OLTP", "WWIOLTP")
MERGED_STATUS_CODES: tuple[str, ...] = ("MG", "M")

BRONZE_CUSTOMERS = "sqlserver_sales_customers"
BRONZE_PARTY_XREF = "oracle_wwi_mdm_party_xref"
BRONZE_MERGE_HISTORY = "oracle_wwi_mdm_mdm_merge_history"
BRONZE_CUST_MASTER = "oracle_wwi_mdm_cust_master"


def rankXrefCandidates(partyXref: DataFrame) -> DataFrame:
    """One xref per OLTP customer id, tagged with how many candidates there were.

    Deterministic pick: active rows first, then the most recently touched row
    (``UPDATED_DT`` falling back to ``CREATED_DT``), then the lowest
    ``PARTY_XREF_ID``. The legacy interface load inserted only when no xref
    existed, so it never had to choose; this ordering is the documented default.
    """
    candidates = (
        partyXref.filter(
            (F.col("PARTY_TYPE_CD") == "CUST")
            & F.upper(F.trim(F.col("SOURCE_SYS_CD"))).isin(*WWI_OLTP_SOURCE_SYSTEM_CODES)
            & F.col("CUST_ID").isNotNull()
        )
        .select(
            F.trim(F.col("SOURCE_KEY_TXT")).cast("int").alias("wwi_customer_id"),
            F.col("PARTY_XREF_ID").cast("bigint").alias("party_xref_id"),
            F.col("CUST_ID").cast("bigint").alias("raw_party_id"),
            F.upper(F.coalesce(F.col("ACTIVE_FLG"), F.lit("N"))).alias("xref_active_flag"),
            F.coalesce(F.col("UPDATED_DT"), F.col("CREATED_DT")).cast("timestamp").alias("xref_effective_ts"),
        )
        .filter(F.col("wwi_customer_id").isNotNull())
    )

    byCustomer = Window.partitionBy("wwi_customer_id")
    ordering = byCustomer.orderBy(
        F.when(F.col("xref_active_flag") == "Y", 0).otherwise(1),
        F.col("xref_effective_ts").desc_nulls_last(),
        F.col("party_xref_id").asc(),
    )
    return (
        candidates.withColumn("xref_candidate_count", F.count("*").over(byCustomer))
        .withColumn("xrefRank", F.row_number().over(ordering))
        .filter(F.col("xrefRank") == 1)
        .drop("xrefRank")
    )


def buildMergeEdges(mergeHistory: DataFrame) -> DataFrame:
    """``merged_party_id -> survivor_party_id`` with one survivor per merged party.

    Reversed merges (``UNMERGE_FLG = 'Y'``) are skipped, mirroring
    ``FN_CUSTOMER_STATUS``. If a party was merged more than once (re-merged
    after an un-merge that was never logged) the latest ``MERGE_DT`` wins.
    """
    edges = mergeHistory.filter(
        (F.col("PARTY_TYPE_CD") == "CUST")
        & (F.upper(F.coalesce(F.col("UNMERGE_FLG"), F.lit("N"))) == "N")
        & (F.col("SURVIVOR_PARTY_ID") != F.col("MERGED_PARTY_ID"))
    ).select(
        F.col("MERGED_PARTY_ID").cast("bigint").alias("merged_party_id"),
        F.col("SURVIVOR_PARTY_ID").cast("bigint").alias("survivor_party_id"),
        F.col("MERGE_DT").cast("timestamp").alias("merge_ts"),
        F.col("MERGE_ID").cast("bigint").alias("merge_id"),
    )
    ordering = Window.partitionBy("merged_party_id").orderBy(
        F.col("merge_ts").desc_nulls_last(), F.col("merge_id").desc()
    )
    return (
        edges.withColumn("edgeRank", F.row_number().over(ordering))
        .filter(F.col("edgeRank") == 1)
        .select("merged_party_id", "survivor_party_id")
    )


def followMergeChain(seeds: DataFrame, edges: DataFrame) -> DataFrame:
    """Walk ``raw_party_id`` through the merge edges until a party with no
    onward merge is reached.

    Adds ``resolved_party_id``, ``hop_count``, ``resolution_path`` (the party ids
    visited, in order) and ``cycle_detected_flag``. The walk is bounded by
    :data:`MAX_MERGE_HOPS` and stops on a party already in the path, so a merge
    cycle (A -> B -> A) can never loop; the row keeps the last party before the
    repeat and is flagged.
    """
    frontier = seeds.select(
        "*",
        F.col("raw_party_id").alias("resolved_party_id"),
        F.lit(0).alias("hop_count"),
        F.array(F.col("raw_party_id")).alias("resolution_path"),
        F.lit(False).alias("cycle_detected_flag"),
    )
    for _ in range(MAX_MERGE_HOPS):
        step = frontier.join(edges, frontier["resolved_party_id"] == edges["merged_party_id"], "left").drop(
            "merged_party_id"
        )
        step = step.withColumn(
            "hitsCycle",
            F.col("survivor_party_id").isNotNull()
            & F.array_contains(F.col("resolution_path"), F.col("survivor_party_id")),
        ).withColumn("canAdvance", F.col("survivor_party_id").isNotNull() & ~F.col("hitsCycle"))
        moved = (
            step.withColumn("cycle_detected_flag", F.col("cycle_detected_flag") | F.col("hitsCycle"))
            .withColumn("hop_count", F.when(F.col("canAdvance"), F.col("hop_count") + 1).otherwise(F.col("hop_count")))
            .withColumn(
                "resolution_path",
                F.when(
                    F.col("canAdvance"), F.array_union(F.col("resolution_path"), F.array(F.col("survivor_party_id")))
                ).otherwise(F.col("resolution_path")),
            )
            .withColumn(
                "resolved_party_id",
                F.when(F.col("canAdvance"), F.col("survivor_party_id")).otherwise(F.col("resolved_party_id")),
            )
        )
        anyAdvanced = moved.filter(F.col("canAdvance")).limit(1).count() > 0
        frontier = moved.drop("survivor_party_id", "hitsCycle", "canAdvance")
        if not anyAdvanced:
            break
    return frontier


def resolveParties(
    customers: DataFrame,
    partyXref: DataFrame,
    mergeHistory: DataFrame,
    custMaster: DataFrame,
    batchId: int,
) -> DataFrame:
    """Pure transformation: one resolution row per OLTP ``CustomerID``."""
    customerIds = customers.select(F.col("CustomerID").cast("int").alias("wwi_customer_id")).distinct()
    bestXref = rankXrefCandidates(partyXref)
    edges = buildMergeEdges(mergeHistory)
    master = custMaster.select(
        F.col("CUST_ID").cast("bigint").alias("master_party_id"),
        F.upper(F.trim(F.col("CUST_STATUS_CD"))).alias("master_status_code"),
    )

    seeds = customerIds.join(bestXref, "wwi_customer_id", "left")
    withXref = seeds.filter(F.col("raw_party_id").isNotNull())
    withoutXref = seeds.filter(F.col("raw_party_id").isNull())

    walked = followMergeChain(withXref, edges)
    walked = walked.join(master, walked["resolved_party_id"] == master["master_party_id"], "left").drop(
        "master_party_id"
    )

    resolvedIsRetired = F.col("master_status_code").isin(*MERGED_STATUS_CODES) | F.col("master_status_code").isNull()
    # A raw xref party that is retired ('MG') or purged from CUST_MASTER and has
    # no onward merge record cannot be followed anywhere: the legacy extract
    # kept the retired id (PARTY_XREF has no FK on purpose) and reported the
    # customer as MERGED via FN_CUSTOMER_STATUS. Reproduced as a WARN, not a reject.
    resolvedStatus = (
        F.when(F.col("cycle_detected_flag") | resolvedIsRetired, F.lit(STATUS_RETIRED_NO_SURVIVOR))
        .when(F.col("hop_count") == 0, F.lit(STATUS_DIRECT))
        .when(F.col("hop_count") == 1, F.lit(STATUS_MERGED))
        .otherwise(F.lit(STATUS_MERGED_MULTI_HOP))
    )
    resolvedRows = walked.select(
        "wwi_customer_id",
        "party_xref_id",
        "raw_party_id",
        "resolved_party_id",
        "hop_count",
        F.concat_ws(">", F.col("resolution_path")).alias("resolution_path"),
        "xref_candidate_count",
        F.when(F.col("xref_candidate_count") > 1, F.lit(STATUS_DUPLICATE_XREF))
        .otherwise(resolvedStatus)
        .alias("resolution_status_code"),
        resolvedStatus.alias("survivor_status_code"),
        F.col("cycle_detected_flag"),
    )
    missingRows = withoutXref.select(
        "wwi_customer_id",
        F.lit(None).cast("bigint").alias("party_xref_id"),
        F.lit(None).cast("bigint").alias("raw_party_id"),
        F.lit(UNKNOWN_PARTY_ID).cast("bigint").alias("resolved_party_id"),
        F.lit(0).alias("hop_count"),
        F.lit(None).cast("string").alias("resolution_path"),
        F.lit(0).cast("bigint").alias("xref_candidate_count"),
        F.lit(STATUS_MISSING_XREF).alias("resolution_status_code"),
        F.lit(STATUS_MISSING_XREF).alias("survivor_status_code"),
        F.lit(False).alias("cycle_detected_flag"),
    )
    combined = resolvedRows.unionByName(missingRows)
    warnStatuses = (STATUS_RETIRED_NO_SURVIVOR, STATUS_MISSING_XREF, STATUS_DUPLICATE_XREF)
    dqStatus = F.when(
        F.col("resolution_status_code").isin(*warnStatuses) | F.col("survivor_status_code").isin(*warnStatuses),
        F.lit("WARN"),
    ).otherwise(F.lit("PASS"))
    return (
        combined.select(
            "*",
            F.lit("ORACLE_WWIGERP").alias("source_system_code"),
            dqStatus.alias("dq_status_code"),
        )
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("wwi_customer_id"),
                    F.coalesce(F.col("raw_party_id"), F.lit(-1)),
                    F.col("resolved_party_id"),
                    F.col("hop_count"),
                    F.col("resolution_status_code"),
                ),
                256,
            ),
        )
        .withColumn("batch_id", F.lit(batchId).cast("bigint"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Rebuild ``sales_silver.party_resolution`` from bronze (full overwrite, idempotent)."""
    result = resolveParties(
        readTable(spark, cfg, "bronze", BRONZE_CUSTOMERS),
        readTable(spark, cfg, "bronze", BRONZE_PARTY_XREF),
        readTable(spark, cfg, "bronze", BRONZE_MERGE_HISTORY),
        readTable(spark, cfg, "bronze", BRONZE_CUST_MASTER),
        cfg.batchId,
    )
    overwriteTable(result, cfg.fqn("silver", PARTY_RESOLUTION_TABLE))
