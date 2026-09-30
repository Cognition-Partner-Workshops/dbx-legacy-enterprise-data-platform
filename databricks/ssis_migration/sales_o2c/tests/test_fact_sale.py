from datetime import date
from decimal import Decimal

from sales_o2c.fact_sale import computeSaleMeasures, rankDuplicates

SCHEMA = "invoice_line_id int, quantity int, unit_price_amount decimal(18,2), tax_rate_percent decimal(18,3), unit_cost decimal(18,2), transaction_fx_rate decimal(18,6), invoice_date date, customer_tax_registration_number string, customer_country string, ship_to_country string, gst_inclusive_flag boolean"


def _lines(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_na_sales_tax_and_margin(spark):
    df = _lines(spark, [(1, 3, Decimal("10.00"), Decimal("15.000"), Decimal("4.00"), Decimal("1"), date(2016, 1, 5), None, None, None, False)])
    r = computeSaleMeasures(df, "NA").first()
    assert r.net_amount == Decimal("30.00") and r.tax_amount == Decimal("4.50") and r.total_including_tax == Decimal("34.50")
    assert r.total_cost_amount == Decimal("12.00") and r.margin_amount == Decimal("18.00") and r.margin_percent == Decimal("60.0000")
    assert r.tax_regime_code == "SALESTAX" and r.fx_rate_to_usd == Decimal("1.000000")


def test_eu_reverse_charge_zeroes_vat(spark):
    df = _lines(spark, [
        (1, 1, Decimal("100.00"), Decimal("20.000"), Decimal("0"), Decimal("1.1"), date(2016, 1, 5), "DE123", "DE", "FR", False),
        (2, 1, Decimal("100.00"), Decimal("20.000"), Decimal("0"), Decimal("1.1"), date(2016, 1, 5), None, "DE", "FR", False),
    ])
    rows = {r.invoice_line_id: r for r in computeSaleMeasures(df, "EU").collect()}
    assert rows[1].reverse_charge_flag == "Y" and rows[1].tax_amount == Decimal("0.00") and rows[1].intrastat_commodity_flag == "Y"
    assert rows[2].reverse_charge_flag == "N" and rows[2].tax_amount == Decimal("20.00")
    assert rows[2].net_amount_reporting == Decimal("110.00")


def test_apac_gst_inclusive_and_rebate_and_fiscal_year(spark):
    df = _lines(spark, [
        (1, 1, Decimal("110.00"), Decimal("10.000"), Decimal("0"), Decimal("1"), date(2016, 7, 1), None, None, None, True),
        (2, 1, Decimal("100.00"), Decimal("10.000"), Decimal("0"), Decimal("1"), date(2016, 6, 30), None, None, None, False),
    ])
    rows = {r.invoice_line_id: r for r in computeSaleMeasures(df, "APAC").collect()}
    assert rows[1].tax_amount == Decimal("10.00") and rows[1].fiscal_year == 2017 and rows[1].distributor_rebate_accrual == Decimal("2.75")
    assert rows[2].tax_amount == Decimal("10.00") and rows[2].fiscal_year == 2016


def test_dedup_survivor_is_latest_lineage_then_highest_key(spark):
    df = spark.createDataFrame(
        [
            (1, 100, 7, "Widget", 2, Decimal("5.00"), 1),
            (2, 100, 7, "Widget", 2, Decimal("5.00"), 2),
            (3, 100, 7, "Widget", 2, Decimal("5.00"), 2),
            (4, 101, 7, "Widget", 2, Decimal("5.00"), 1),
        ],
        "sale_key int, wwi_invoice_id int, stock_item_key int, description string, quantity int, unit_price decimal(18,2), lineage_key int",
    )
    ranked = {r.sale_key: r for r in rankDuplicates(df).collect()}
    assert ranked[3].duplicate_rank == 1  # same lineage as 2, higher key wins
    assert ranked[2].duplicate_rank == 2 and ranked[1].duplicate_rank == 3
    assert ranked[4].duplicate_rank == 1 and ranked[4].duplicate_count == 1
