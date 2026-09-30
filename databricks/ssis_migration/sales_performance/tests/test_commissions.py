from datetime import date

from conftest import SALE_LINE_SCHEMA, d, fxRates, plans, saleLine
from sales_performance.commissions import calculateApacCommission, calculateEuCommission, calculateNaCommission


def test_na_base_rate_accelerator_and_house_account(spark):
    lines = spark.createDataFrame(
        [
            saleLine(1, "NA", date(2016, 5, 2), 1000, 80),
            saleLine(2, "NA", date(2016, 5, 3), 1000, 80),
            saleLine(3, "NA", date(2016, 5, 4), 500, 0, house=True),
            saleLine(4, "NA", date(2016, 5, 5), 100, 0, lineType="SAMPLE"),
            saleLine(5, "NA", date(2016, 5, 6), 100, 0, currency="CAD"),
            saleLine(6, "NA", date(2016, 5, 7), 100, 0, salespersonId=99, iso3="USA"),
        ],
        SALE_LINE_SCHEMA,
    )
    quotas = spark.createDataFrame(
        [("NA", 10, "FY2016-P11", d(1500))],
        "region_code string, salesperson_id bigint, fiscal_period_label string, quota_amount decimal(19,4)",
    )
    accruals, unplanned = calculateNaCommission(lines, plans(spark), quotas)
    rows = {r["sale_key"]: r for r in accruals.collect()}
    assert set(rows) == {1, 2, 3, 6}
    assert str(rows[1]["commission_amount"]) == "54.0000"  # 1080 * 5%
    assert str(rows[2]["accelerator_commission_amount"]) == "13.2000"  # (2160-1500) * 2%
    assert str(rows[2]["commission_amount"]) == "67.2000"
    assert str(rows[3]["house_account_factor"]) == "0.5000" and str(rows[3]["commission_amount"]) == "17.5000"  # (25 base + 10 accel) * 0.5
    assert rows[1]["commission_period"] == "2016-05"
    assert unplanned.count() == 0


def test_eu_vat_backout_fx_cap_and_cash_basis_hold(spark):
    lines = spark.createDataFrame(
        [
            saleLine(1, "EU", date(2016, 5, 2), 100, 19, iso3="NLD", vatRate=d(19)),
            saleLine(2, "EU", date(2016, 5, 2), 100, 19, iso3="DEU", vatRate=d(19)),
            saleLine(3, "EU", date(2016, 5, 2), 1000, 200, iso3="GBR", currency="GBP", vatRate=d(0), netReported=d(1000)),
            saleLine(4, "EU", date(2016, 5, 2), 100, 0, iso3="FRA", currency="CHF"),
        ],
        SALE_LINE_SCHEMA,
    )
    cleared = spark.createDataFrame([(1002,)], "invoice_number bigint")
    accruals, _ = calculateEuCommission(lines, plans(spark, capEu=d(40)), fxRates(spark), ("DEU", "AUT"), None, cleared)
    rows = {r["sale_key"]: r for r in accruals.collect()}
    assert str(rows[1]["basis_amount"]) == "100.0000" and str(rows[1]["commission_amount"]) == "4.0000"
    assert rows[1]["accrual_status"] == "ACCRUED" and rows[2]["accrual_status"] == "ACCRUED"  # DEU but payment cleared
    assert str(rows[3]["plan_currency_amount"]) == "1250.0000" and rows[3]["cap_applied"] and str(rows[3]["commission_amount"]) == "40.0000"
    assert rows[4]["accrual_status"] == "FX_MISSING"
    held, _ = calculateEuCommission(lines, plans(spark), fxRates(spark))
    assert {r["sale_key"]: r["accrual_status"] for r in held.collect()}[2] == "HELD_CASH_BASIS"


def test_apac_gst_exclusive_fiscal_period_fx_and_boundary(spark):
    lines = spark.createDataFrame(
        [
            saleLine(1, "APAC", date(2015, 7, 10), 200, 20),
            saleLine(2, "APAC", date(2015, 8, 1), 100, 10, currency="SGD", iso3="SGP"),
            saleLine(3, "APAC", date(2015, 7, 10), 100, 10, currency="JPY", iso3="JPN"),
        ],
        SALE_LINE_SCHEMA,
    )
    accruals, rejected, unplanned, metrics = calculateApacCommission(
        lines, plans(spark), fxRates(spark), teamSplitEnabled=True, teamSplitPercent=50
    )
    rows = {r["sale_key"]: r for r in accruals.collect()}
    assert str(rows[1]["basis_amount"]) == "200.0000" and str(rows[1]["commission_amount"]) == "6.0000"  # 200*6%*50%
    assert rows[1]["commission_period"] == "FY2016-P01" and not rows[1]["is_period_boundary_line"]
    assert rows[2]["commission_period"] == "FY2016-P02" and rows[2]["is_period_boundary_line"]  # 1 Aug is in P2 starting 29 Jul
    assert str(rows[2]["plan_currency_amount"]) == "110.0000"
    assert rejected.count() == 1 and unplanned.count() == 0 and metrics["missing_calendar_days"] == 0
