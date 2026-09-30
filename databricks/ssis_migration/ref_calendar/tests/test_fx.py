from datetime import date, datetime
from decimal import Decimal

from ref_calendar import extracts, reference, staging

FX_SCHEMA = (
    "from_curr_cd string, to_curr_cd string, rate_dt timestamp, rate_type_cd string, rate decimal(18,8), inverse_rate decimal(18,8), "
    "bid_rate decimal(18,8), ask_rate decimal(18,8), rate_source_cd string, feed_region_cd string, interpolated_flg string, loaded_ts timestamp, superseded_flg string"
)


def fxRow(day, fromCcy, toCcy, rateType, rate):
    rate = Decimal(rate)
    return (fromCcy, toCcy, datetime(2016, 1, day), rateType, rate, Decimal(1) / rate, None, None, "ECB", "GLOBAL", "N", datetime(2016, 1, day, 6), "N")


def fxRows(spark):
    return spark.createDataFrame(
        [
            fxRow(1, "USD", "GBP", "SPOT", "0.68"), fxRow(2, "USD", "GBP", "SPOT", "0.69"), fxRow(3, "USD", "GBP", "SPOT", "0.70"),
            fxRow(2, "USD", "GBP", "FWD", "0.99"), fxRow(2, "EUR", "USD", "SPOT", "1.08"), fxRow(2, "SGD", "USD", "SPOT", "0.70"),
        ],
        FX_SCHEMA,
    )


def test_fx_window_is_half_open_and_filters_rate_types(spark):
    out = extracts.shapeFxRateExtract(fxRows(spark), date(2016, 1, 2), date(2016, 1, 3))
    rows = out.collect()
    assert {r.rate_dt for r in rows} == {date(2016, 1, 2)}
    assert all(r.rate_type_cd in extracts.config.FX_RATE_TYPES for r in rows)
    assert len(rows) == 3


def test_triangulation_derives_eur_sgd_cross_rate(spark):
    tri = extracts.triangulateFxRates(fxRows(spark), date(2016, 1, 1), date(2016, 1, 4)).collect()
    pairs = {(r.from_currency_cd, r.to_currency_cd): r.rate for r in tri}
    assert ("EUR", "SGD") in pairs and ("SGD", "EUR") in pairs
    assert abs(float(pairs[("EUR", "SGD")]) - 1.08 / 0.70) < 1e-6


def test_watermark_window_resolution_defaults_to_watermark(spark, monkeypatch):
    monkeypatch.setattr(extracts, "getWatermark", lambda s, name: "2016-01-02")
    start, end = extracts.resolveFxWindow(spark, None, None)
    assert start == date(2016, 1, 2) and end > start


def test_staging_fx_dedup_prefers_override(spark):
    raw = spark.createDataFrame(
        [(date(2016, 1, 2), "USD", "GBP", "SPOT", Decimal("0.69"), "ECB", 1), (date(2016, 1, 2), "USD", "GBP", "SPOT", Decimal("0.695"), "ECB", 2)],
        "rate_dt date, from_currency_cd string, to_currency_cd string, rate_type_cd string, rate decimal(18,8), rate_source_cd string, source_row_number int",
    )
    overrides = spark.createDataFrame(
        [("USD", "GBP", date(2016, 1, 2), Decimal("0.71"), "CHG-1", "SPOT")],
        "from_currency_code string, to_currency_code string, rate_date date, override_rate decimal(18,8), approval_ticket_number string, rate_type_code string",
    )
    out = staging.conformFxRate(raw, overrides).where("from_currency_code = 'USD' AND to_currency_code = 'GBP'").collect()
    assert len(out) == 1
    assert float(out[0].exchange_rate) == 0.71
    assert out[0].rate_source_code == "FILE_FX"


def test_reference_fx_fills_short_gaps_and_dedups(spark):
    raw = spark.createDataFrame(
        [
            (date(2016, 1, 1), "USD", "GBP", "SPOT", Decimal("0.68"), Decimal("1.47058824"), "ECB"),
            (date(2016, 1, 1), "USD", "GBP", "SPOT", Decimal("0.68"), Decimal("1.47058824"), "ECB"),
            (date(2016, 1, 4), "USD", "GBP", "SPOT", Decimal("0.70"), Decimal("1.42857143"), "ECB"),
        ],
        "rate_dt date, from_currency_cd string, to_currency_cd string, rate_type_cd string, rate decimal(18,8), inverse_rate decimal(18,8), rate_source_cd string",
    )
    out = reference.buildRefFxRateDaily(raw).where("rate_type_code = 'SPOT' AND from_currency_code = 'USD' AND to_currency_code = 'GBP'")
    rows = {r.rate_date: r for r in out.collect()}
    assert sorted(rows) == [date(2016, 1, 1), date(2016, 1, 2), date(2016, 1, 3), date(2016, 1, 4)]
    assert rows[date(2016, 1, 2)].is_fill_forward and float(rows[date(2016, 1, 2)].conversion_rate) == 0.68
    assert not rows[date(2016, 1, 1)].is_fill_forward
