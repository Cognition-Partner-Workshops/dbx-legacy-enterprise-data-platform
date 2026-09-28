"""FX conversion under each regional effective-date convention."""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from sales_lakehouse.silver.rules.fx import applyFx
from tests.silver_rules_fixtures import buildSilverReference

D = dt.date

SCHEMA = StructType(
    [
        StructField("line_id", StringType()),
        StructField("region_code", StringType()),
        StructField("currency_code", StringType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("net_amount", DecimalType(19, 4)),
        StructField("tax_amount", DecimalType(19, 4)),
    ]
)


@pytest.fixture(scope="module", autouse=True)
def silverReference(spark, cfg):
    buildSilverReference(spark, cfg)


def _lines(spark, rows):
    data = [(i, r, c, d, Decimal(n), Decimal(t)) for i, r, c, d, n, t in rows]
    return spark.createDataFrame(data, SCHEMA)


def _run(spark, cfg, rows):
    df = _lines(spark, rows)
    out = applyFx(df, spark, cfg, ["net_amount", "tax_amount"])
    assert out.count() == df.count(), "applyFx must never drop or duplicate rows"
    return {r["line_id"]: r for r in out.collect()}


def test_same_currency_usd(spark, cfg):
    out = _run(spark, cfg, [("usd", "NA", "USD", D(2024, 3, 8), "123.4500", "10.0000")])
    row = out["usd"]
    assert row["fx_rate_to_reporting"] == Decimal("1.00000000")
    assert row["fx_rate_source_code"] == "SAME_CCY"
    assert row["fx_rate_effective_date"] == D(2024, 3, 8)
    assert row["net_amount_usd"] == Decimal("123.4500")
    assert row["tax_amount_usd"] == Decimal("10.0000")
    assert row["dq_status_code"] == "PASS"


def test_na_uses_transaction_date_spot_with_seven_day_fallback(spark, cfg):
    out = _run(
        spark,
        cfg,
        [
            ("na_exact", "NA", "CAD", D(2024, 3, 8), "123.4500", "0.0000"),
            ("na_saturday", "NA", "CAD", D(2024, 3, 9), "100.0000", "0.0000"),
            ("na_gap", "NA", "CAD", D(2024, 3, 20), "100.0000", "0.0000"),
            ("na_too_old", "NA", "CAD", D(2024, 4, 30), "100.0000", "0.0000"),
        ],
    )
    # 123.45 * 0.74 = 91.353 -> 91.35
    assert out["na_exact"]["fx_rate_to_reporting"] == Decimal("0.74000000")
    assert out["na_exact"]["net_amount_usd"] == Decimal("91.3500")
    assert out["na_exact"]["fx_rate_source_code"] == "BOC"
    assert out["na_exact"]["fx_rate_effective_date"] == D(2024, 3, 8)
    # weekend: the NA feed fills Saturday with Friday's rate, effective date is Friday
    assert out["na_saturday"]["fx_rate_to_reporting"] == Decimal("0.74000000")
    assert out["na_saturday"]["fx_rate_effective_date"] == D(2024, 3, 8)
    # 5 days back to 15 March is within @MaxFallbackDays = 7
    assert out["na_gap"]["fx_rate_to_reporting"] == Decimal("0.73000000")
    assert out["na_gap"]["fx_rate_effective_date"] == D(2024, 3, 15)
    # 29 days back is outside the window -> legacy default of 1.0
    assert out["na_too_old"]["fx_rate_to_reporting"] == Decimal("1.00000000")
    assert out["na_too_old"]["fx_rate_source_code"] == "DEFAULT_1"
    assert out["na_too_old"]["dq_status_code"] == "WARN"


def test_eu_uses_invoice_date_then_prior_day_without_window(spark, cfg):
    out = _run(
        spark,
        cfg,
        [
            ("eu_friday", "EU", "EUR", D(2024, 3, 8), "100.0000", "19.0000"),
            ("eu_sunday", "EU", "EUR", D(2024, 3, 10), "100.0000", "19.0000"),
            ("eu_long_gap", "EU", "GBP", D(2024, 3, 9), "100.0000", "20.0000"),
        ],
    )
    assert out["eu_friday"]["fx_rate_to_reporting"] == Decimal("1.09000000")
    assert out["eu_friday"]["net_amount_usd"] == Decimal("109.0000")
    assert out["eu_friday"]["tax_amount_usd"] == Decimal("20.7100")
    assert out["eu_friday"]["fx_rate_source_code"] == "ECB"
    # weekend: ECB publishes no Sunday fixing; the interpolated row carries Friday's rate
    assert out["eu_sunday"]["fx_rate_to_reporting"] == Decimal("1.09000000")
    assert out["eu_sunday"]["fx_rate_effective_date"] == D(2024, 3, 8)
    # "Retry Held Lines With Prior Day Rate" has no lookback limit
    assert out["eu_long_gap"]["fx_rate_to_reporting"] == Decimal("1.27000000")
    assert out["eu_long_gap"]["fx_rate_effective_date"] == D(2024, 1, 15)
    assert out["eu_long_gap"]["dq_status_code"] == "PASS"


def test_apac_uses_month_start_corporate_rate(spark, cfg):
    out = _run(
        spark,
        cfg,
        [
            ("apac_mid_month", "APAC", "AUD", D(2024, 3, 20), "100.0000", "9.1000"),
            ("apac_saturday", "APAC", "AUD", D(2024, 3, 9), "100.0000", "9.1000"),
            ("apac_weekend_first", "APAC", "AUD", D(2024, 6, 15), "100.0000", "0.0000"),
            ("apac_no_rate", "APAC", "AUD", D(2024, 9, 15), "100.0000", "0.0000"),
        ],
    )
    # the 15 March CORP row is ignored: the whole month converts at the 1 March rate
    assert out["apac_mid_month"]["fx_rate_to_reporting"] == Decimal("0.65000000")
    assert out["apac_mid_month"]["fx_rate_effective_date"] == D(2024, 3, 1)
    assert out["apac_mid_month"]["fx_rate_source_code"] == "APAC_TREASURY"
    assert out["apac_mid_month"]["net_amount_usd"] == Decimal("65.0000")
    # 9.10 * 0.65 = 5.915 -> 5.92
    assert out["apac_mid_month"]["tax_amount_usd"] == Decimal("5.9200")
    assert out["apac_saturday"]["fx_rate_to_reporting"] == Decimal("0.65000000")
    assert out["apac_saturday"]["fx_rate_effective_date"] == D(2024, 3, 1)
    # 1 June 2024 is a Saturday with no APAC row; the 31 May rate carries forward
    assert out["apac_weekend_first"]["fx_rate_to_reporting"] == Decimal("0.67000000")
    assert out["apac_weekend_first"]["fx_rate_effective_date"] == D(2024, 5, 31)
    # September: last CORP row is 31 May, more than 7 days before 1 September
    assert out["apac_no_rate"]["fx_rate_source_code"] == "DEFAULT_1"
    assert out["apac_no_rate"]["fx_rate_to_reporting"] == Decimal("1.00000000")
    assert out["apac_no_rate"]["net_amount_usd"] == Decimal("100.0000")


def test_missing_rate_never_null_never_rejected(spark, cfg):
    out = _run(
        spark,
        cfg,
        [
            ("unknown_ccy", "EU", "CHF", D(2024, 3, 8), "100.0000", "0.0000"),
            ("null_ccy", "EU", None, D(2024, 3, 8), "100.0000", "0.0000"),
            ("null_date", "NA", "CAD", None, "100.0000", "0.0000"),
        ],
    )
    for row in out.values():
        assert row["fx_rate_to_reporting"] == Decimal("1.00000000")
        assert row["fx_rate_source_code"] == "DEFAULT_1"
        assert row["net_amount_usd"] == Decimal("100.0000")
        assert row["dq_status_code"] == "WARN"
        assert "FX_RATE_DEFAULTED" in row["dq_reason_codes"]


def test_inverse_and_triangulated_quotes_are_usable(spark, cfg):
    out = _run(
        spark,
        cfg,
        [
            ("jpy", "APAC", "JPY", D(2024, 3, 12), "15000.0000", "0.0000"),
            ("nzd", "APAC", "NZD", D(2024, 3, 12), "100.0000", "0.0000"),
        ],
    )
    assert out["jpy"]["fx_rate_to_reporting"] == Decimal("0.00666667")
    assert out["jpy"]["net_amount_usd"] == Decimal("100.0000")
    # 0.92 AUD per NZD * 0.65 USD per AUD
    assert out["nzd"]["fx_rate_to_reporting"] == Decimal("0.59800000")
    assert out["nzd"]["net_amount_usd"] == Decimal("59.8000")


def test_output_types_and_rerun(spark, cfg):
    df = _lines(spark, [("x", "NA", "CAD", D(2024, 3, 8), "10.0000", "1.0000")])
    once = applyFx(df, spark, cfg, ["net_amount"])
    types = dict(once.dtypes)
    assert types["fx_rate_to_reporting"] == "decimal(19,8)"
    assert types["net_amount_usd"] == "decimal(19,4)"
    assert types["fx_rate_effective_date"] == "date"
    twice = applyFx(once, spark, cfg, ["net_amount"])
    assert set(once.columns) == set(twice.columns)
    assert once.select(*once.columns).collect() == twice.select(*once.columns).collect()
