from decimal import Decimal

import pytest
from pyspark.sql import Row

from procurement_lib import delta_io, reconciliation as rc


def test_fingerprint_is_order_independent_and_ignores_batch_columns(spark):
    a = spark.createDataFrame([Row(k=1, v="x", batch_id=1), Row(k=2, v=None, batch_id=1)])
    b = spark.createDataFrame([Row(k=2, v=None, batch_id=9), Row(k=1, v="x", batch_id=9)])
    c = spark.createDataFrame([Row(k=1, v="y", batch_id=1), Row(k=2, v=None, batch_id=1)])
    assert rc.fingerprint(a) == rc.fingerprint(b)
    assert rc.fingerprint(a)[0] == 2 and rc.fingerprint(a) != rc.fingerprint(c)


def test_compare_with_baseline():
    assert rc.compareWithBaseline(10, 5, None)["status"] == "NoBaseline"
    assert rc.compareWithBaseline(10, 5, {"row_count": 10, "digest": 5})["status"] == "Matched"
    r = rc.compareWithBaseline(10, 5, {"row_count": 12, "digest": 5})
    assert r["status"] == "CountMismatch" and r["variance"] == 2
    assert rc.compareWithBaseline(10, 5, {"row_count": 10, "digest": 6})["status"] == "HashMismatch"
    assert rc.compareWithBaseline(10, 5, {"row_count": 10})["status"] == "Matched"


def test_merge_into_is_idempotent(spark, deltaAvailable):
    if not deltaAvailable:
        pytest.skip("delta-spark jars not available offline")
    table = "prc_test_merge"
    spark.sql("DROP TABLE IF EXISTS %s" % table)
    first = spark.createDataFrame([Row(k=1, amount=Decimal("1.00"), batch_id=1), Row(k=2, amount=Decimal("2.00"), batch_id=1)])
    delta_io.mergeInto(spark, first, table, ["k"])
    delta_io.mergeInto(spark, first, table, ["k"])
    assert spark.table(table).count() == 2
    second = spark.createDataFrame([Row(k=2, amount=Decimal("5.00"), batch_id=2), Row(k=3, amount=Decimal("3.00"), batch_id=2)])
    delta_io.mergeInto(spark, second, table, ["k"])
    rows = {r.k: r for r in spark.table(table).collect()}
    assert set(rows) == {1, 2, 3} and rows[2].amount == Decimal("5.00") and rows[2].batch_id == 2
    delta_io.updateMatched(spark, spark.createDataFrame([Row(k=1, amount=Decimal("9.00")), Row(k=4, amount=Decimal("0"))]), table, ["k"], ["amount"])
    rows = {r.k: r for r in spark.table(table).collect()}
    assert set(rows) == {1, 2, 3} and rows[1].amount == Decimal("9.00")
    spark.sql("DROP TABLE %s" % table)
