from datetime import datetime

from sales_o2c.dimensions import asOfLookup, currentLookup


def _dim(spark):
    return spark.createDataFrame(
        [
            (10, 1, datetime(2013, 1, 1), datetime(2015, 6, 30), False),
            (11, 1, datetime(2015, 6, 30), datetime(9999, 12, 31), True),
            (20, 2, datetime(2013, 1, 1), datetime(9999, 12, 31), True),
        ],
        "dim_key int, dim_biz int, valid_from timestamp, valid_to timestamp, is_current boolean",
    )


def test_as_of_lookup_uses_open_closed_interval_and_defaults_to_zero(spark):
    facts = spark.createDataFrame(
        [(1, 1, datetime(2014, 1, 1)), (2, 1, datetime(2015, 6, 30)), (3, 1, datetime(2015, 7, 1)), (4, 1, datetime(2013, 1, 1)), (5, 99, datetime(2016, 1, 1))],
        "id int, biz int, lm timestamp",
    )
    out = {r.id: r.key for r in asOfLookup(facts, _dim(spark), "biz", "lm", "key").collect()}
    assert out[1] == 10  # inside first version
    assert out[2] == 10  # lm == Valid To of v1 -> v1 (<=), not v2 (>)
    assert out[3] == 11
    assert out[4] == 0  # lm == Valid From is excluded (strict >) -> unknown member
    assert out[5] == 0  # unknown business key


def test_as_of_lookup_keeps_one_row_per_fact(spark):
    facts = spark.createDataFrame([(1, 1, datetime(2014, 1, 1))], "id int, biz int, lm timestamp")
    assert asOfLookup(facts, _dim(spark), "biz", "lm", "key").count() == 1


def test_current_lookup_can_return_null_for_hold_routing(spark):
    facts = spark.createDataFrame([(1, 1), (2, 99)], "id int, biz int")
    out = {r.id: r.key for r in currentLookup(facts, _dim(spark), "biz", "key", unknown=None).collect()}
    assert out == {1: 11, 2: None}
