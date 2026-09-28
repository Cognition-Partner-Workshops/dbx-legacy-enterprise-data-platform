"""Fiscal boundaries for NA445, EUCAL and APACJUN."""
import datetime as dt

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

from sales_lakehouse.silver.rules.fiscal import naFiscalPeriodFor, regionCalendarCode, resolveFiscalPeriod
from tests.silver_rules_fixtures import buildSilverReference

D = dt.date

SCHEMA = StructType(
    [
        StructField("line_id", StringType()),
        StructField("region_code", StringType(), True),
        StructField("invoice_date", DateType(), True),
    ]
)


@pytest.fixture(scope="module", autouse=True)
def silverReference(spark, cfg):
    buildSilverReference(spark, cfg)


def _resolve(spark, cfg, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    out = resolveFiscalPeriod(df, spark, cfg, "invoice_date")
    assert out.count() == len(rows)
    return {r["line_id"]: r for r in out.collect()}


def _check(row, calendar, year, period):
    assert (row["fiscal_calendar_code"], row["fiscal_year"], row["fiscal_period"]) == (calendar, year, period)
    assert row["fiscal_period_key"] == year * 100 + period


def test_region_calendar_code_mapping(spark):
    df = spark.createDataFrame([("NA",), ("EU",), ("APAC",), (" apac ",), ("LATAM",), (None,)], ["region_code"])
    got = [r[0] for r in df.select(regionCalendarCode("region_code")).collect()]
    assert got == ["NA445", "EUCAL", "APACJUN", "APACJUN", None, None]
    assert df.select(regionCalendarCode(F.col("region_code"))).count() == 6


def test_na445_boundaries(spark, cfg):
    out = _resolve(
        spark,
        cfg,
        [
            ("fy23_last", "NA", D(2023, 12, 30)),
            ("fy24_first", "NA", D(2023, 12, 31)),
            ("p1_last", "NA", D(2024, 1, 27)),
            ("p2_first", "NA", D(2024, 1, 28)),
            ("p2_last", "NA", D(2024, 2, 24)),
            ("p3_first", "NA", D(2024, 2, 25)),
            ("p3_last", "NA", D(2024, 3, 30)),
            ("q2_first", "NA", D(2024, 3, 31)),
            ("fy24_last", "NA", D(2024, 12, 28)),
            ("fy25_first", "NA", D(2024, 12, 29)),
            ("week53", "NA", D(2026, 1, 3)),
            ("fy26_first", "NA", D(2026, 1, 4)),
        ],
    )
    _check(out["fy23_last"], "NA445", 2023, 12)
    _check(out["fy24_first"], "NA445", 2024, 1)
    _check(out["p1_last"], "NA445", 2024, 1)
    _check(out["p2_first"], "NA445", 2024, 2)
    _check(out["p2_last"], "NA445", 2024, 2)
    _check(out["p3_first"], "NA445", 2024, 3)
    _check(out["p3_last"], "NA445", 2024, 3)
    _check(out["q2_first"], "NA445", 2024, 4)
    _check(out["fy24_last"], "NA445", 2024, 12)
    _check(out["fy25_first"], "NA445", 2025, 1)
    # FY2025 is a 53-week year; week 53 stays in period 12
    _check(out["week53"], "NA445", 2025, 12)
    _check(out["fy26_first"], "NA445", 2026, 1)


def test_eucal_boundaries(spark, cfg):
    out = _resolve(
        spark,
        cfg,
        [
            ("jan_last", "EU", D(2024, 1, 31)),
            ("feb_first", "EU", D(2024, 2, 1)),
            ("dec_last", "EU", D(2024, 12, 31)),
            ("jan_first", "EU", D(2025, 1, 1)),
        ],
    )
    _check(out["jan_last"], "EUCAL", 2024, 1)
    _check(out["feb_first"], "EUCAL", 2024, 2)
    _check(out["dec_last"], "EUCAL", 2024, 12)
    _check(out["jan_first"], "EUCAL", 2025, 1)


def test_apacjun_boundaries(spark, cfg):
    out = _resolve(
        spark,
        cfg,
        [
            ("mar_last", "APAC", D(2024, 3, 31)),
            ("apr_first", "APAC", D(2024, 4, 1)),
            ("dec_last", "APAC", D(2024, 12, 31)),
            ("jan_first", "APAC", D(2025, 1, 1)),
        ],
    )
    # April-March year, named for the year it closes in
    _check(out["mar_last"], "APACJUN", 2024, 12)
    _check(out["apr_first"], "APACJUN", 2025, 1)
    _check(out["dec_last"], "APACJUN", 2025, 9)
    _check(out["jan_first"], "APACJUN", 2025, 10)


def test_same_date_three_regions_three_periods(spark, cfg):
    out = _resolve(
        spark,
        cfg,
        [("na", "NA", D(2024, 1, 30)), ("eu", "EU", D(2024, 1, 30)), ("apac", "APAC", D(2024, 1, 30))],
    )
    _check(out["na"], "NA445", 2024, 2)
    _check(out["eu"], "EUCAL", 2024, 1)
    _check(out["apac"], "APACJUN", 2024, 10)


def test_outside_dimension_falls_back_to_arithmetic_and_unmapped_region_warns(spark, cfg):
    out = _resolve(
        spark,
        cfg,
        [
            ("far_future", "NA", D(2040, 7, 4)),
            ("unmapped", "LATAM", D(2024, 1, 30)),
            ("null_region", None, D(2024, 1, 30)),
        ],
    )
    _check(out["far_future"], "NA445", 2040, 7)
    assert out["far_future"]["dq_status_code"] == "PASS"
    for key in ("unmapped", "null_region"):
        assert out[key]["fiscal_calendar_code"] is None
        assert out[key]["fiscal_period_key"] is None
        assert out[key]["dq_status_code"] == "WARN"
        assert "FISCAL_PERIOD_UNRESOLVED" in out[key]["dq_reason_codes"]


def test_na_fiscal_period_for_ignores_region(spark):
    df = spark.createDataFrame(
        [("eu", "EU", D(2024, 1, 30)), ("apac", "APAC", D(2024, 3, 31)), ("na", "NA", D(2024, 12, 29))], SCHEMA
    )
    out = {r["line_id"]: r for r in naFiscalPeriodFor(df, "invoice_date").collect()}
    _check(out["eu"], "NA445", 2024, 2)
    _check(out["apac"], "NA445", 2024, 4)
    _check(out["na"], "NA445", 2025, 1)


def test_resolve_is_idempotent(spark, cfg):
    df = spark.createDataFrame([("x", "EU", D(2024, 5, 5))], SCHEMA)
    once = resolveFiscalPeriod(df, spark, cfg, "invoice_date")
    twice = resolveFiscalPeriod(once, spark, cfg, "invoice_date")
    assert set(once.columns) == set(twice.columns)
    assert once.select(*once.columns).collect() == twice.select(*once.columns).collect()
