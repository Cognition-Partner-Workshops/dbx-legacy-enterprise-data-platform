from datetime import date

from finance.fx import convertAmounts


def _rates(typed):
    return typed(
        [
            ("EUR", "USD", date(2024, 12, 10), 1.05, "SPOT", "N"),
            ("EUR", "USD", date(2024, 12, 2), 1.10, "SPOT", "N"),
            ("USD", "GBP", date(2024, 12, 10), 0.80, "SPOT", "N"),
            ("USD", "JPY", date(2024, 11, 20), 150.0, "SPOT", "N"),
        ],
        "from_curr_cd string, to_curr_cd string, rate_dt date, rate decimal(18,8), rate_type_cd string, superseded_flg string",
    )


def _currencies(spark):
    return spark.createDataFrame(
        [("USD", 2), ("EUR", 2), ("GBP", 2), ("JPY", 0)], "curr_cd string, minor_unit_digits int"
    )


def test_convert_amount_resolution_order(spark, typed):
    df = spark.createDataFrame(
        [
            (1, 100.0, "USD", "USD", date(2024, 12, 10)),  # same currency
            (2, 100.0, "EUR", "USD", date(2024, 12, 10)),  # exact date
            (3, 100.0, "EUR", "USD", date(2024, 12, 5)),  # fallback within 7 days -> 12-02 rate
            (4, 100.0, "GBP", "USD", date(2024, 12, 10)),  # inverse of USD->GBP
            (5, 100.0, "EUR", "GBP", date(2024, 12, 10)),  # triangulate EUR->USD->GBP
            (
                6,
                100.0,
                "EUR",
                "JPY",
                date(2024, 12, 10),
            ),  # triangulate with JPY 0 minor units (USD leg 20 days back, inside 30)
            (7, 100.0, "CHF", "USD", date(2024, 12, 10)),  # no rate
            (8, None, "EUR", "USD", date(2024, 12, 10)),  # null amount
        ],
        "id int, amt double, frm string, to string, dt date",
    )
    out = (
        convertAmounts(df, _rates(typed), _currencies(spark), "amt", "frm", "to", "dt", "usd")
        .orderBy("id")
        .collect()
    )
    got = {r["id"]: (r["usd"], r["usd_rate_missing"]) for r in out}
    assert float(got[1][0]) == 100.0
    assert float(got[2][0]) == 105.0
    assert float(got[3][0]) == 110.0
    assert float(got[4][0]) == 125.0
    assert float(got[5][0]) == 84.0
    assert float(got[6][0]) == 15750.0
    assert got[7] == (None, True)
    assert got[8][0] is None and not got[8][1]
