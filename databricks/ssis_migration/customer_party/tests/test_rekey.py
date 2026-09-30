from datetime import datetime, timedelta, timezone

from customer_party.rekey import (
    QUEUE_SCHEMA,
    DimensionRekeySpec,
    repointFactKeys,
    resolveQueue,
)
from customer_party.scd import INFERRED_PENDING_KEY, UNKNOWN_KEY

SPEC = DimensionRekeySpec("Customer", "dim_customer", "customer_key", "customer_business_key")
NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
DIM_SCHEMA = "customer_key bigint, customer_business_key string, is_current_row boolean, is_inferred_member boolean"


def _dim(spark):
    return spark.createDataFrame(
        [
            (UNKNOWN_KEY, None, True, False),
            (INFERRED_PENDING_KEY, None, True, False),
            (7, "ORA:7", True, False),
            (8, "ORA:8", True, True),
            (9, "ORA:9", False, False),
        ],
        DIM_SCHEMA,
    )


def _queue(spark, rows):
    return spark.createDataFrame(rows, QUEUE_SCHEMA)


def test_queue_release_retry_and_abandon_by_region(spark):
    queued = NOW - timedelta(days=2)
    queue = _queue(
        spark,
        [
            (1, "Customer", "ORA:7", "NA", "Fact.Sale", "1", None, "HELD", 0, queued, None, None, None),
            (2, "Customer", "ORA:8", "NA", "Fact.Sale", "2", None, "HELD", 0, queued, None, None, None),
            (3, "Customer", "ORA:9", "NA", "Fact.Sale", "3", None, "RETRY", 2, queued, None, None, None),
            (4, "Customer", "ORA:9", "EU", "Fact.Sale", "4", None, "RETRY", 2, queued, None, None, None),
            (5, "Customer", "ORA:5", "APAC", "Fact.Sale", "5", None, "RELEASED", 0, queued, NOW, 5, None),
            (6, "Promotion", "P1", "NA", "Fact.Sale", "6", None, "HELD", 0, queued, None, None, None),
        ],
    )
    result = {r.queue_id: r for r in resolveQueue(queue, _dim(spark), SPEC, NOW).collect()}
    assert set(result) == {1, 2, 3, 4}  # already released rows and other dimensions are left alone
    assert result[1].status == "RELEASED" and result[1].assigned_surrogate_key == 7 and result[1].retry_count == 0
    assert result[2].status == "RETRY" and result[2].retry_count == 1  # inferred member is not a real resolution
    assert result[3].status == "ABANDONED" and result[3].abandon_reason == "HOLD_EXPIRED"  # NA limit 3
    assert result[4].status == "RETRY" and result[4].retry_count == 3  # EU limit 7


def test_repoint_fact_keys_for_placeholders_and_inferred_members(spark):
    facts = spark.createDataFrame(
        [
            (1, UNKNOWN_KEY, "ORA:7"),
            (2, INFERRED_PENDING_KEY, "ORA:7"),
            (3, 8, "ORA:8"),
            (4, UNKNOWN_KEY, "ORA:404"),
            (5, 7, "ORA:7"),
            (6, 0, "ORA:9"),
        ],
        "fact_id int, customer_key bigint, customer_business_key string",
    )
    result = {r.fact_id: r for r in repointFactKeys(facts, _dim(spark), SPEC, "customer_key", "customer_business_key").collect()}
    assert result[1].customer_key == 7 and result[1].was_rekeyed
    assert result[2].customer_key == 7 and result[2].was_rekeyed
    assert result[3].customer_key == 8 and not result[3].was_rekeyed  # inferred row with no real member yet
    assert result[4].customer_key == UNKNOWN_KEY and not result[4].was_rekeyed  # still unknown
    assert result[5].customer_key == 7 and not result[5].was_rekeyed
    assert result[6].customer_key == 0 and not result[6].was_rekeyed  # ORA:9 has no current version
