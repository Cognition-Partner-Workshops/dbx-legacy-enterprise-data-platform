"""Worked tax examples per region (hand-computed expected values)."""
from decimal import Decimal

from pyspark.sql.types import BooleanType, DecimalType, StringType, StructField, StructType

from sales_lakehouse.silver.rules.tax import applyTax

SCHEMA = StructType(
    [
        StructField("line_id", StringType()),
        StructField("region_code", StringType()),
        StructField("gross_amount", DecimalType(19, 4), True),
        StructField("net_amount", DecimalType(19, 4), True),
        StructField("tax_rate", DecimalType(19, 8), True),
        StructField("is_reverse_charge", BooleanType(), True),
        StructField("vat_registration_number", StringType(), True),
    ]
)


def _money(v):
    return Decimal(v) if v is not None else None


def _rate(v):
    return Decimal(v) if v is not None else None


def _lines(spark, rows):
    data = [(i, r, _money(g), _money(n), _rate(t), rc, vat) for i, r, g, n, t, rc, vat in rows]
    return spark.createDataFrame(data, SCHEMA)


def _byId(df):
    return {r["line_id"]: r for r in df.collect()}


def test_na_tax_on_discounted_net_rounds_half_up(spark):
    df = _lines(
        spark,
        [
            ("chicago", "NA", None, "100.0000", "0.10250000", None, None),
            ("half_up", "NA", None, "2.0000", "0.06250000", None, None),
            ("dallas", "NA", None, "19.9900", "0.08250000", None, None),
        ],
    )
    out = _byId(applyTax(df))
    # 100 * (6.25 + 1.75 + 1.25 + 1.00)% = 10.25, gross = net + tax
    assert out["chicago"]["tax_amount_local"] == Decimal("10.2500")
    assert out["chicago"]["gross_amount_local"] == Decimal("110.2500")
    assert out["chicago"]["net_amount_local"] == Decimal("100.0000")
    assert out["chicago"]["tax_treatment_code"] == "SALES_TAX"
    assert out["chicago"]["tax_residual_local"] == Decimal("0.0000")
    # 2.00 * 6.25% = 0.125 -> 0.13 (half-up, not banker's)
    assert out["half_up"]["tax_amount_local"] == Decimal("0.1300")
    # 19.99 * 8.25% = 1.649175 -> 1.65
    assert out["dallas"]["tax_amount_local"] == Decimal("1.6500")
    assert out["dallas"]["gross_amount_local"] == Decimal("21.6400")
    assert all(r["dq_status_code"] == "PASS" for r in out.values())


def test_eu_vat_from_net_and_from_gross(spark):
    df = _lines(
        spark,
        [
            ("from_net", "EU", None, "100.0000", "0.19000000", False, "DE123456789"),
            ("from_gross_clean", "EU", "119.0000", None, "0.19000000", False, None),
            ("from_gross_rounded", "EU", "100.0000", None, "0.19000000", False, None),
        ],
    )
    out = _byId(applyTax(df))
    assert out["from_net"]["tax_amount_local"] == Decimal("19.0000")
    assert out["from_net"]["gross_amount_local"] == Decimal("119.0000")
    assert out["from_net"]["tax_treatment_code"] == "VAT"
    assert out["from_gross_clean"]["net_amount_local"] == Decimal("100.0000")
    assert out["from_gross_clean"]["tax_amount_local"] == Decimal("19.0000")
    # 100 / 1.19 = 84.0336.. -> rounded 84.03, tax = 100 - 84.03
    assert out["from_gross_rounded"]["net_amount_local"] == Decimal("84.0300")
    assert out["from_gross_rounded"]["tax_amount_local"] == Decimal("15.9700")
    assert out["from_gross_rounded"]["gross_amount_local"] == Decimal("100.0000")


def test_eu_reverse_charge_with_and_without_vat_registration(spark):
    df = _lines(
        spark,
        [
            ("rc_registered", "EU", None, "250.0000", "0.20000000", True, "FR12345678901"),
            ("rc_no_vatreg", "EU", None, "250.0000", "0.20000000", True, None),
            ("rc_blank_vatreg", "EU", None, "250.0000", "0.20000000", True, "   "),
        ],
    )
    out = _byId(applyTax(df))
    assert len(out) == 3, "reverse-charge lines without a registration must still load"
    for key in out:
        assert out[key]["tax_amount_local"] == Decimal("0.0000")
        assert out[key]["net_amount_local"] == Decimal("250.0000")
        assert out[key]["gross_amount_local"] == Decimal("250.0000")
    assert out["rc_registered"]["tax_treatment_code"] == "REVERSE_CHARGE"
    assert out["rc_registered"]["dq_status_code"] == "PASS"
    for key in ("rc_no_vatreg", "rc_blank_vatreg"):
        assert out[key]["tax_treatment_code"] == "REVERSE_CHARGE_NO_VATREG"
        assert out[key]["dq_status_code"] == "WARN"
        assert "TAX_RC_NO_VATREG" in out[key]["dq_reason_codes"]


def test_apac_gst_inclusive_truncates_and_keeps_residual(spark):
    df = _lines(
        spark,
        [
            ("unclean", "APAC", "100.0000", None, "0.10000000", None, None),
            ("clean", "APAC", "110.0000", None, "0.10000000", None, None),
            ("nz_unclean", "APAC", "19.9500", None, "0.15000000", None, None),
            ("gst_free", "APAC", "42.0000", None, "0.00000000", None, None),
            ("exclusive", "APAC", None, "100.0000", "0.10000000", None, None),
        ],
    )
    out = _byId(applyTax(df))
    # 100 / 1.10 = 90.9090.. -> truncated 90.90 (rounding would give 90.91)
    assert out["unclean"]["net_amount_local"] == Decimal("90.9000")
    assert out["unclean"]["tax_amount_local"] == Decimal("9.1000")
    assert out["unclean"]["gross_amount_local"] == Decimal("100.0000")
    assert out["unclean"]["tax_residual_local"] == Decimal("0.0091")
    assert out["unclean"]["tax_treatment_code"] == "GST_INCLUSIVE"
    assert out["clean"]["net_amount_local"] == Decimal("100.0000")
    assert out["clean"]["tax_amount_local"] == Decimal("10.0000")
    assert out["clean"]["tax_residual_local"] == Decimal("0.0000")
    # 19.95 / 1.15 = 17.3478.. -> 17.34, tax 2.61, residual 0.0078
    assert out["nz_unclean"]["net_amount_local"] == Decimal("17.3400")
    assert out["nz_unclean"]["tax_amount_local"] == Decimal("2.6100")
    assert out["nz_unclean"]["tax_residual_local"] == Decimal("0.0078")
    assert out["gst_free"]["net_amount_local"] == Decimal("42.0000")
    assert out["gst_free"]["tax_amount_local"] == Decimal("0.0000")
    assert out["exclusive"]["tax_treatment_code"] == "GST_EXCLUSIVE"
    assert out["exclusive"]["tax_amount_local"] == Decimal("10.0000")
    assert out["exclusive"]["gross_amount_local"] == Decimal("110.0000")


def test_output_types_and_missing_rate_and_unmapped_region(spark):
    df = _lines(
        spark,
        [
            ("no_rate", "NA", None, "50.0000", None, None, None),
            ("unmapped", "LATAM", "50.0000", None, "0.10000000", None, None),
            ("lower_case_region", "apac", "110.0000", None, "0.10000000", None, None),
        ],
    )
    result = applyTax(df)
    types = dict(result.dtypes)
    for col in ("net_amount_local", "tax_amount_local", "gross_amount_local", "tax_residual_local"):
        assert types[col] == "decimal(19,4)"
    assert types["tax_treatment_code"] == "string"
    assert result.count() == 3
    out = _byId(result)
    assert out["no_rate"]["tax_amount_local"] == Decimal("0.0000")
    assert out["no_rate"]["dq_status_code"] == "WARN"
    assert "TAX_RATE_MISSING" in out["no_rate"]["dq_reason_codes"]
    assert out["unmapped"]["tax_treatment_code"] == "UNMAPPED_REGION"
    assert out["unmapped"]["dq_status_code"] == "FAIL"
    assert out["unmapped"]["net_amount_local"] == Decimal("50.0000")
    assert out["lower_case_region"]["net_amount_local"] == Decimal("100.0000")


def test_apply_tax_is_idempotent_on_rerun(spark):
    df = _lines(spark, [("x", "APAC", "100.0000", None, "0.10000000", None, None)])
    once = applyTax(df)
    twice = applyTax(once)
    assert set(once.columns) == set(twice.columns)
    assert once.select(*once.columns).collect() == twice.select(*once.columns).collect()


def test_custom_column_names(spark):
    schema = StructType(
        [
            StructField("rgn", StringType()),
            StructField("amt", DecimalType(19, 4)),
            StructField("pct", DecimalType(19, 8)),
            StructField("rc", BooleanType()),
            StructField("vat", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("EU", Decimal("100.0000"), Decimal("0.19000000"), True, None)], schema)
    row = applyTax(
        df, regionCol="rgn", grossCol="missing_gross", netCol="amt", taxRateCol="pct", isReverseChargeCol="rc", vatRegCol="vat"
    ).first()
    assert row["tax_treatment_code"] == "REVERSE_CHARGE_NO_VATREG"
    assert row["tax_amount_local"] == Decimal("0.0000")
