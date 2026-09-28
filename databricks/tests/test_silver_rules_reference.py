"""silver.reference.run builds the six reference tables from bronze."""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sales_lakehouse.silver import reference
from tests.silver_rules_fixtures import buildSilverReference, loadBronzeReference

D = dt.date


@pytest.fixture(scope="module", autouse=True)
def silverReference(spark, cfg):
    buildSilverReference(spark, cfg)


def _silver(spark, cfg, table):
    return spark.table(cfg.fqn("silver", table))


def test_all_tables_exist_as_delta(spark, cfg):
    for table in (
        reference.REF_FX_RATE,
        reference.REF_TAX_RATE_NA,
        reference.REF_TAX_RATE_EU,
        reference.REF_TAX_RATE_APAC,
        reference.DIM_FISCAL_CALENDAR,
        reference.DIM_DATE,
    ):
        fqn = cfg.fqn("silver", table)
        assert spark.catalog.tableExists(fqn)
        provider = [r for r in spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").collect() if r["col_name"] == "Provider"]
        assert provider and provider[0]["data_type"].lower() == "delta"
        assert {"batch_id", "loaded_at_utc"} <= set(spark.table(fqn).columns)


def test_ref_fx_rate_contract_and_derivations(spark, cfg):
    fx = _silver(spark, cfg, reference.REF_FX_RATE)
    types = dict(fx.dtypes)
    for col in ("currency_code", "rate_date", "effective_date", "rate_to_usd", "rate_source_code", "region_code"):
        assert col in types
    assert types["rate_to_usd"] == "decimal(19,8)"
    assert types["rate_date"] == "date" and types["effective_date"] == "date"
    rows = {(r["currency_code"], r["rate_date"]): r for r in fx.collect()}
    assert rows[("CAD", D(2024, 3, 8))]["rate_to_usd"] == Decimal("0.74000000")
    assert rows[("CAD", D(2024, 3, 8))]["region_code"] == "NA"
    # interpolated weekend rows point back at the Friday publication
    assert rows[("CAD", D(2024, 3, 10))]["effective_date"] == D(2024, 3, 8)
    assert rows[("CAD", D(2024, 3, 8))]["effective_date"] == D(2024, 3, 8)
    # USD -> JPY quote is inverted; NZD -> AUD is triangulated through AUD -> USD
    assert rows[("JPY", D(2024, 3, 1))]["rate_to_usd"] == Decimal("0.00666667")
    assert rows[("JPY", D(2024, 3, 1))]["derivation_code"] == "INVERSE"
    assert rows[("NZD", D(2024, 3, 1))]["rate_to_usd"] == Decimal("0.59800000")
    assert rows[("NZD", D(2024, 3, 1))]["derivation_code"] == "TRIANGULATED"
    # superseded row dropped, USD never appears as a currency
    assert rows[("EUR", D(2024, 3, 8))]["rate_to_usd"] == Decimal("1.09000000")
    assert fx.filter(F.col("currency_code") == "USD").count() == 0
    assert fx.groupBy("currency_code", "rate_date", "rate_type_code", "region_code").count().filter("count > 1").count() == 0


def test_ref_tax_rate_na_stacks_jurisdiction_hierarchy_by_effective_segment(spark, cfg):
    na = _silver(spark, cfg, reference.REF_TAX_RATE_NA)
    chicago = sorted(
        na.filter(
            (F.col("jurisdiction_code") == "US-IL-COOK-CHI")
            & (F.col("rate_category_code") == "STD")
            & (F.col("tax_regime_code") == "SALES")
        ).collect(),
        key=lambda r: r["effective_from"],
    )
    assert [(r["effective_from"], r["effective_to"]) for r in chicago] == [
        (D(2008, 7, 1), D(2015, 12, 31)),
        (D(2016, 1, 1), D(2016, 6, 30)),
        (D(2016, 7, 1), None),
    ]
    assert [r["combined_tax_rate"] for r in chicago] == [
        Decimal("0.06250000"),
        Decimal("0.07500000"),
        Decimal("0.09250000"),
    ]
    current = chicago[-1]
    assert current["state_tax_rate"] == Decimal("0.06250000")
    assert current["county_tax_rate"] == Decimal("0.01750000")
    assert current["city_tax_rate"] == Decimal("0.01250000")
    assert current["district_tax_rate"] == Decimal("0.00000000")
    assert (current["state_jurisdiction_code"], current["county_jurisdiction_code"], current["city_jurisdiction_code"]) == (
        "US-IL", "US-IL-COOK", "US-IL-COOK-CHI"
    )
    assert current["state_code"] == "IL" and current["county_name"] == "Cook" and current["city_name"] == "Chicago"
    rta = na.filter(
        (F.col("jurisdiction_code") == "US-IL-RTA")
        & (F.col("rate_category_code") == "STD")
        & (F.col("tax_regime_code") == "SALES")
        & F.col("effective_to").isNull()
    ).first()
    assert rta["district_tax_rate"] == Decimal("0.01000000")
    assert rta["combined_tax_rate"] == Decimal("0.09000000")
    assert rta["district_jurisdiction_code"] == "US-IL-RTA"
    dallas = na.filter((F.col("jurisdiction_code") == "US-TX-DALLAS-DAL") & (F.col("rate_category_code") == "STD")).first()
    assert dallas["combined_tax_rate"] == Decimal("0.08250000")
    assert dallas["combined_tax_rate_pct"] == Decimal("8.25000")
    resale = na.filter((F.col("jurisdiction_code") == "US-IL-COOK-CHI") & (F.col("rate_category_code") == "RESALE")).first()
    assert resale["combined_tax_rate"] == Decimal("0.00000000")


def test_ref_tax_rate_eu_and_apac(spark, cfg):
    eu = {(r["country_code"], r["tax_code"]): r for r in _silver(spark, cfg, reference.REF_TAX_RATE_EU).collect()}
    assert eu[("DE", "DE_STD")]["vat_rate"] == Decimal("0.19000000")
    assert eu[("DE", "DE_STD")]["is_standard_rate"] and not eu[("DE", "DE_STD")]["is_reduced_rate"]
    assert eu[("DE", "DE_RED")]["is_reduced_rate"] and eu[("DE", "DE_RED")]["vat_rate_pct"] == Decimal("7.00000")
    assert eu[("DE", "DE_STD_COVID")]["effective_to"] == D(2020, 12, 31) and not eu[("DE", "DE_STD_COVID")]["is_active"]
    assert eu[("NL", "NL_ICA")]["is_reverse_charge_eligible"] and eu[("NL", "NL_ICA")]["country_reverse_charge_eligible"]
    assert not eu[("FR", "FR_STD")]["is_reverse_charge_eligible"]

    apac = {(r["country_code"], r["tax_code"]): r for r in _silver(spark, cfg, reference.REF_TAX_RATE_APAC).collect()}
    assert apac[("AU", "AU_GST")]["gst_rate"] == Decimal("0.10000000") and apac[("AU", "AU_GST")]["is_price_inclusive"]
    assert apac[("AU", "AU_FREE")]["is_gst_free"]
    assert apac[("SG", "SG_GST9")]["effective_from"] == D(2024, 1, 1)
    assert apac[("SG", "SG_GST7")]["effective_to"] == D(2022, 12, 31)
    assert not apac[("JP", "JP_INPUT")]["is_price_inclusive"]


def test_dim_fiscal_calendar_periods(spark, cfg):
    cal = _silver(spark, cfg, reference.DIM_FISCAL_CALENDAR)
    assert set(r[0] for r in cal.select("calendar_code").distinct().collect()) == {"NA445", "EUCAL", "APACJUN"}
    assert cal.groupBy("calendar_code", "fiscal_year", "fiscal_period").count().filter("count > 1").count() == 0
    rows = {(r["calendar_code"], r["fiscal_year"], r["fiscal_period"]): r for r in cal.collect()}
    na1 = rows[("NA445", 2024, 1)]
    assert (na1["period_start"], na1["period_end"], na1["fiscal_quarter"]) == (D(2023, 12, 31), D(2024, 1, 27), 1)
    assert na1["period_name"] == "FY2024-P01" and na1["fiscal_period_key"] == 202401 and na1["period_days"] == 28
    na3 = rows[("NA445", 2024, 3)]
    assert (na3["period_start"], na3["period_end"], na3["period_days"]) == (D(2024, 2, 25), D(2024, 3, 30), 35)
    na12 = rows[("NA445", 2025, 12)]
    # 53-week year: period 12 is weeks 48-53
    assert (na12["period_start"], na12["period_end"], na12["period_days"]) == (D(2025, 11, 23), D(2026, 1, 3), 42)
    eu2 = rows[("EUCAL", 2024, 2)]
    assert (eu2["period_start"], eu2["period_end"], eu2["fiscal_quarter"]) == (D(2024, 2, 1), D(2024, 2, 29), 1)
    apac1 = rows[("APACJUN", 2025, 1)]
    assert (apac1["period_start"], apac1["period_end"], apac1["fiscal_quarter"]) == (D(2024, 4, 1), D(2024, 4, 30), 1)
    apac12 = rows[("APACJUN", 2024, 12)]
    assert (apac12["period_start"], apac12["period_end"], apac12["fiscal_quarter"]) == (D(2024, 3, 1), D(2024, 3, 31), 4)
    assert apac12["is_year_end_period"] and apac12["region_code"] == "APAC"


def test_dim_date_carries_all_three_calendars(spark, cfg):
    dim = _silver(spark, cfg, reference.DIM_DATE)
    assert dim.groupBy("date_key").count().filter("count > 1").count() == 0
    row = dim.filter(F.col("date_key") == 20240130).first()
    assert row["calendar_date"] == D(2024, 1, 30)
    assert (row["calendar_year"], row["calendar_month_number"], row["calendar_quarter_number"]) == (2024, 1, 1)
    assert row["day_name"] == "Tuesday" and not row["is_weekend"]
    assert row["na445_fiscal_period_key"] == 202402
    assert row["eucal_fiscal_period_key"] == 202401
    assert row["apacjun_fiscal_period_key"] == 202410
    assert row["apacjun_fiscal_period_name"] == "FY2024-P10"
    newYear = dim.filter(F.col("date_key") == 20230101).first()
    assert newYear["na445_is_holiday"] and newYear["eucal_is_holiday"] and not newYear["apacjun_is_holiday"]
    assert newYear["na445_holiday_name"] == "New Year's Day"
    assert newYear["is_weekend"]
    # spine spans the bronze calendar's years, widened a week either side
    lo, hi = dim.agg(F.min("calendar_date"), F.max("calendar_date")).first()
    assert lo == D(2022, 12, 25) and hi == D(2026, 1, 7)


def test_run_is_idempotent(spark, cfg):
    before = {t: _silver(spark, cfg, t).count() for t in (reference.REF_FX_RATE, reference.REF_TAX_RATE_NA, reference.DIM_DATE)}
    loadBronzeReference(spark, cfg)
    reference.run(spark, cfg)
    after = {t: _silver(spark, cfg, t).count() for t in before}
    assert before == after
    assert before[reference.REF_FX_RATE] > 0 and before[reference.REF_TAX_RATE_NA] > 0
