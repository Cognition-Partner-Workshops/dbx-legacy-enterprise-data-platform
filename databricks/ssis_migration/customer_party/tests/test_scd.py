from datetime import datetime, timezone

from pyspark.sql import functions as F

from customer_party.config import HIGH_DATE
from customer_party.scd import (
    RESERVED_MEMBERS,
    ScdSpec,
    applyScd1,
    applyScd2,
    withReservedMembers,
)

SPEC = ScdSpec(keyCol="customer_key", businessKeyCol="customer_business_key", trackedCols=("customer_name", "country_code"))
T1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 2, 1, tzinfo=timezone.utc)


def _incoming(spark, rows):
    return spark.createDataFrame(rows, "customer_business_key string, customer_name string, country_code string")


def test_scd2_initial_load_assigns_positive_keys_and_open_rows(spark):
    dim = applyScd2(None, _incoming(spark, [("ORA:1", "Alpha", "US"), ("ORA:2", "Beta", "DE")]), SPEC, F.lit(T1).cast("timestamp"))
    rows = {r.customer_business_key: r for r in dim.collect()}
    assert sorted(r.customer_key for r in rows.values()) == [1, 2]
    assert all(r.is_current_row and r.row_version == 1 for r in rows.values())
    assert all(r.valid_to.replace(tzinfo=timezone.utc) == HIGH_DATE for r in rows.values())


def test_scd2_change_closes_previous_version_and_keeps_unchanged(spark):
    first = applyScd2(None, _incoming(spark, [("ORA:1", "Alpha", "US"), ("ORA:2", "Beta", "DE")]), SPEC, F.lit(T1).cast("timestamp"))
    second = applyScd2(first, _incoming(spark, [("ORA:1", "Alpha Renamed", "US"), ("ORA:2", "Beta", "DE")]), SPEC, F.lit(T2).cast("timestamp"))
    rows = second.collect()
    assert len(rows) == 3
    alpha = sorted([r for r in rows if r.customer_business_key == "ORA:1"], key=lambda r: r.row_version)
    assert [r.is_current_row for r in alpha] == [False, True]
    assert alpha[0].valid_to.replace(tzinfo=timezone.utc) == T2.replace(second=59, minute=59, hour=23, day=31, month=1)
    assert alpha[1].customer_key == 3 and alpha[1].row_version == 2
    beta = [r for r in rows if r.customer_business_key == "ORA:2"]
    assert len(beta) == 1 and beta[0].is_current_row and beta[0].row_version == 1


def test_scd2_inferred_member_is_replaced_when_real_row_arrives(spark):
    first = applyScd2(None, _incoming(spark, [("ORA:9", "Inferred", None)]), SPEC, F.lit(T1).cast("timestamp"))
    first = first.withColumn("is_inferred_member", F.lit(True))
    second = applyScd2(first, _incoming(spark, [("ORA:9", "Inferred", None)]), SPEC, F.lit(T2).cast("timestamp"))
    current = second.where("is_current_row").collect()
    assert len(current) == 1 and current[0].row_version == 2 and not current[0].is_inferred_member


def test_reserved_members_added_once_with_required_names(spark):
    dim = applyScd2(None, _incoming(spark, [("ORA:1", "Alpha", "US")]), SPEC, F.lit(T1).cast("timestamp"))
    withReserved = withReservedMembers(spark, dim, SPEC, ("customer_name",))
    twice = withReservedMembers(spark, withReserved, SPEC, ("customer_name",))
    reserved = {r.customer_key: r.customer_name for r in twice.where("customer_key < 0").collect()}
    assert reserved == dict(RESERVED_MEMBERS)
    assert twice.count() == 1 + len(RESERVED_MEMBERS)


def test_scd1_overwrites_in_place(spark):
    first = applyScd1(None, _incoming(spark, [("T1", "North", "US")]), SPEC)
    second = applyScd1(first, _incoming(spark, [("T1", "North East", "US"), ("T2", "South", "US")]), SPEC)
    rows = {r.customer_business_key: r for r in second.collect()}
    assert rows["T1"].customer_name == "North East" and rows["T1"].customer_key == 1
    assert rows["T2"].customer_key == 2
    assert second.count() == 2
