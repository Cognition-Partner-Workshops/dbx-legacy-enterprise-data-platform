"""Party crosswalk resolution: DIRECT / MERGED / MERGED_MULTI_HOP /
RETIRED_NO_SURVIVOR / MISSING_XREF / DUPLICATE_XREF, cycle guard, idempotency."""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from sales_lakehouse.common.tables import overwriteTable
from sales_lakehouse.silver import party_resolution as pr

XREF_SCHEMA = T.StructType(
    [
        T.StructField("PARTY_XREF_ID", T.LongType()),
        T.StructField("PARTY_TYPE_CD", T.StringType()),
        T.StructField("CUST_ID", T.LongType()),
        T.StructField("SUPP_ID", T.LongType()),
        T.StructField("SOURCE_SYS_CD", T.StringType()),
        T.StructField("SOURCE_KEY_TXT", T.StringType()),
        T.StructField("SOURCE_KEY_TYPE_CD", T.StringType()),
        T.StructField("ACTIVE_FLG", T.StringType()),
        T.StructField("CREATED_DT", T.TimestampType()),
        T.StructField("UPDATED_DT", T.TimestampType()),
    ]
)
MERGE_SCHEMA = T.StructType(
    [
        T.StructField("MERGE_ID", T.LongType()),
        T.StructField("PARTY_TYPE_CD", T.StringType()),
        T.StructField("SURVIVOR_PARTY_ID", T.LongType()),
        T.StructField("MERGED_PARTY_ID", T.LongType()),
        T.StructField("MERGE_REASON_CD", T.StringType()),
        T.StructField("MERGED_BY_CD", T.StringType()),
        T.StructField("MERGE_DT", T.TimestampType()),
        T.StructField("UNMERGE_FLG", T.StringType()),
    ]
)
MASTER_SCHEMA = T.StructType(
    [
        T.StructField("CUST_ID", T.LongType()),
        T.StructField("CUST_NBR", T.StringType()),
        T.StructField("CUST_NAME", T.StringType()),
        T.StructField("REGION_CD", T.StringType()),
        T.StructField("COUNTRY_CD", T.StringType()),
        T.StructField("CUST_STATUS_CD", T.StringType()),
    ]
)

D1 = datetime(2020, 1, 1)
D2 = datetime(2021, 1, 1)


def xref(xrefId, custId, key, active="Y", created=D1, updated=None, sourceSys="WWI_OLTP"):
    return (xrefId, "CUST", custId, None, sourceSys, str(key), "ID", active, created, updated)


def merge(mergeId, survivor, merged, unmerge="N", when=D1):
    return (mergeId, "CUST", survivor, merged, "DUP", "STEWARD", when, unmerge)


def master(custId, status="AC"):
    return (custId, f"C-NA-{custId:07d}", f"Customer {custId}", "NA", "US", status)


def buildInputs(spark):
    customers = spark.createDataFrame([(cid,) for cid in range(1, 9)], ["CustomerID"])
    partyXref = spark.createDataFrame(
        [
            xref(1, 100, 1),  # direct
            xref(2, 200, 2),  # merged one hop -> 201
            xref(3, 300, 3),  # multi hop 300 -> 301 -> 302
            xref(4, 400, 4),  # retired, no merge record
            # customer 5 has no xref at all
            xref(6, 600, 6, active="N", created=D1),  # duplicate (loser: inactive)
            xref(7, 601, 6, active="Y", created=D1),  # duplicate (older active)
            xref(8, 602, 6, active="Y", created=D2),  # duplicate winner: active + latest
            xref(9, 700, 7),  # cycle 700 -> 701 -> 700
            xref(10, 800, 8, sourceSys="CRM_GLOBAL"),  # wrong source system: ignored
            xref(11, 999, 1, sourceSys="WWI_OLTP"),  # CUST type but different key type: same key -> dup? no: key '1'
        ],
        XREF_SCHEMA,
    )
    mergeHistory = spark.createDataFrame(
        [
            merge(1, 201, 200),
            merge(2, 301, 300),
            merge(3, 302, 301),
            merge(4, 701, 700),
            merge(5, 700, 701),
            merge(6, 555, 100, unmerge="Y"),  # reversed merge: ignored
        ],
        MERGE_SCHEMA,
    )
    custMaster = spark.createDataFrame(
        [
            master(100),
            master(200, "MG"),
            master(201),
            master(300, "MG"),
            master(301, "MG"),
            master(302),
            master(400, "MG"),
            master(600, "MG"),
            master(601),
            master(602),
            master(700, "MG"),
            master(701, "MG"),
        ],
        MASTER_SCHEMA,
    )
    return customers, partyXref, mergeHistory, custMaster


def resolve(spark):
    customers, partyXref, mergeHistory, custMaster = buildInputs(spark)
    # drop the deliberate duplicate for customer 1 so DIRECT is clean
    partyXref = partyXref.filter(F.col("PARTY_XREF_ID") != 11)
    df = pr.resolveParties(customers, partyXref, mergeHistory, custMaster, batchId=1)
    return {r.wwi_customer_id: r for r in df.collect()}


def test_one_row_per_customer_and_statuses(spark):
    rows = resolve(spark)
    assert sorted(rows) == list(range(1, 9))

    assert rows[1].resolution_status_code == pr.STATUS_DIRECT
    assert rows[1].resolved_party_id == 100 and rows[1].hop_count == 0 and rows[1].dq_status_code == "PASS"

    assert rows[2].resolution_status_code == pr.STATUS_MERGED
    assert rows[2].raw_party_id == 200 and rows[2].resolved_party_id == 201 and rows[2].hop_count == 1

    assert rows[3].resolution_status_code == pr.STATUS_MERGED_MULTI_HOP
    assert rows[3].resolved_party_id == 302 and rows[3].hop_count == 2
    assert rows[3].resolution_path == "300>301>302"
    assert rows[3].dq_status_code == "PASS"


def test_retired_without_survivor_keeps_id_and_warns(spark):
    rows = resolve(spark)
    row = rows[4]
    assert row.resolution_status_code == pr.STATUS_RETIRED_NO_SURVIVOR
    assert row.raw_party_id == 400 and row.resolved_party_id == 400
    assert row.dq_status_code == "WARN"


def test_missing_xref_gives_unknown_party_warn(spark):
    row = resolve(spark)[5]
    assert row.resolution_status_code == pr.STATUS_MISSING_XREF
    assert row.raw_party_id is None and row.resolved_party_id == pr.UNKNOWN_PARTY_ID
    assert row.dq_status_code == "WARN"


def test_duplicate_xref_is_deterministic(spark):
    row = resolve(spark)[6]
    assert row.resolution_status_code == pr.STATUS_DUPLICATE_XREF
    assert row.xref_candidate_count == 3
    # active first, then latest CREATED/UPDATED, then lowest xref id
    assert row.party_xref_id == 8 and row.raw_party_id == 602 and row.resolved_party_id == 602
    assert row.dq_status_code == "WARN"


def test_merge_cycle_is_guarded(spark):
    row = resolve(spark)[7]
    assert row.cycle_detected_flag is True
    assert row.resolution_status_code == pr.STATUS_RETIRED_NO_SURVIVOR
    assert row.hop_count <= pr.MAX_MERGE_HOPS
    assert row.dq_status_code == "WARN"


def test_foreign_source_system_xref_is_ignored(spark):
    row = resolve(spark)[8]
    assert row.resolution_status_code == pr.STATUS_MISSING_XREF


def test_run_is_idempotent(spark, cfg):
    customers, partyXref, mergeHistory, custMaster = buildInputs(spark)
    overwriteTable(customers, cfg.fqn("bronze", pr.BRONZE_CUSTOMERS))
    overwriteTable(partyXref, cfg.fqn("bronze", pr.BRONZE_PARTY_XREF))
    overwriteTable(mergeHistory, cfg.fqn("bronze", pr.BRONZE_MERGE_HISTORY))
    overwriteTable(custMaster, cfg.fqn("bronze", pr.BRONZE_CUST_MASTER))
    pr.run(spark, cfg)
    pr.run(spark, cfg)
    out = spark.table(cfg.fqn("silver", pr.PARTY_RESOLUTION_TABLE))
    assert out.count() == 8
    assert out.select("wwi_customer_id").distinct().count() == 8
    assert {"row_hash", "batch_id", "loaded_at_utc", "dq_status_code"} <= set(out.columns)
