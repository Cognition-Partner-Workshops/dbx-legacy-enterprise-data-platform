"""Hand-computed expectations for the gold aggregate grains (workstream 7).

Inputs: tests/gold_reporting_fixtures.py.  Each assertion cites the fixture
rows it is derived from (sale_key numbers = ``SALES`` list).
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from tests.gold_reporting_fixtures import AS_OF, goldRun, one, rowsOf  # noqa: F401

D = dt.date


def test_daily_sales_grain_is_date_item_territory_channel(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_daily_sales")
    keys = [(r["sales_date"], r["stock_item_key"], r["sales_territory_key"], r["sales_channel_key"]) for r in rows]
    assert len(keys) == len(set(keys))
    # 16 sale lines collapse to 15 grain rows: sale 1 + reversal 6 share 2024-02-05 / item 10 / terr 1 / ch 1
    assert len(rows) == 15


def test_daily_sales_measures_include_reversals_and_364_day_prior_year(spark, cfg, goldRun):
    r = one(spark, cfg, "gold", "agg_daily_sales",
            "sales_date = DATE'2024-02-05' AND stock_item_key = 10 AND sales_territory_key = 1 AND sales_channel_key = 1")
    # sale 1 (1000, qty 10) + reversal 6 (-100, qty -1): daily sales does NOT filter correction rows
    assert r["invoice_count"] == 2 and r["line_count"] == 2 and r["distinct_customer_count"] == 1
    assert r["quantity_sold_base_uom"] == Decimal("9.000")
    assert r["net_sales_amount"] == Decimal("900.00")
    assert r["gross_margin_amount"] == Decimal("360.00")
    assert r["margin_percent"] == 40.0
    assert r["fiscal_year"] == 2024 and r["fiscal_period"] == 2  # NA445 P2 = 2024-01-29..2024-02-25
    # LEGACY QUIRK: prior year = same grain 364 days earlier -> sale 5 on 2023-02-06 (800)
    assert r["prior_year_net_sales"] == Decimal("800.00")
    assert r["prior_year_variance_percent"] == 12.5


def test_daily_sales_promotion_discount_uses_promotion_key(spark, cfg, goldRun):
    r = one(spark, cfg, "gold", "agg_daily_sales", "sales_date = DATE'2024-02-05' AND stock_item_key = 11")
    assert r["line_discount_amount"] == Decimal("50.00")
    assert r["promotion_discount_amount"] == Decimal("50.00")  # promotion_key 7 -> discount counted as promotional
    assert r["net_sales_amount"] == Decimal("450.00") and r["margin_percent"] == 33.33


def test_monthly_sales_grain_and_na_customer_month(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_monthly_sales")
    keys = [(r["calendar_month"], r["customer_key"], r["sales_territory_key"], r["sales_channel_key"]) for r in rows]
    assert len(keys) == len(set(keys)) and len(rows) == 10
    r = one(spark, cfg, "gold", "agg_monthly_sales", "customer_key = 1 AND calendar_month = DATE'2024-02-01'")
    # sales 1, 2, 7 (reversal 6 excluded from monthly)
    assert r["order_count"] == 2 and r["invoice_count"] == 2
    assert r["gross_revenue"] == Decimal("1600.00") and r["discount_given"] == Decimal("50.00")
    assert r["net_revenue"] == Decimal("1550.00") and r["net_revenue_reporting"] == Decimal("1550.00")
    assert r["gross_margin_reporting"] == Decimal("590.00000000")
    assert r["average_order_value"] == Decimal("775.0000")
    assert r["fiscal_year"] == 2024 and r["fiscal_period"] == 2 and r["fiscal_calendar_code"] == "CY12"
    assert r["prior_period_net_revenue"] is None
    assert r["prior_year_net_revenue"] == Decimal("800.00") and r["year_over_year_percent"] == 93.75
    assert r["rolling_3_period_net_revenue"] == Decimal("1550.00")
    assert r["period_closed_flag"] is False


def test_monthly_sales_eu_credit_notes_and_reverse_charge(spark, cfg, goldRun):
    berlin = one(spark, cfg, "gold", "agg_monthly_sales", "customer_key = 3 AND calendar_month = DATE'2024-02-01'")
    # sales 8, 11, 12: local 1000 + 10000 + 200; reporting 1100 + 11000 + 254
    assert berlin["net_revenue"] == Decimal("11200.00")
    assert berlin["net_revenue_reporting"] == Decimal("12354.00")
    assert berlin["credit_notes_reporting"] == 110.0  # credit note 100 EUR x 1.1
    assert berlin["net_revenue_after_credits"] == 12244.0
    assert berlin["fiscal_calendar_code"] == "APR12"
    # LEGACY QUIRK: NA fiscal bucketing for every region -> FY2024 P2
    assert (berlin["fiscal_year"], berlin["fiscal_period"]) == (2024, 2)
    paris = one(spark, cfg, "gold", "agg_monthly_sales", "customer_key = 4 AND calendar_month = DATE'2024-02-01'")
    # sale 9: reverse-charge line -> reporting revenue net of the tax amount (440 - 76)
    assert paris["net_revenue_reporting"] == Decimal("364.00")


def test_regional_sales_performance_fx_translation_and_budget(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_regional_sales_performance")
    keys = [(r["calendar_month"], r["region_code"], r["sales_territory_key"], r["sales_channel_key"]) for r in rows]
    assert len(keys) == len(set(keys)) and len(rows) == 10
    eu = one(spark, cfg, "gold", "agg_regional_sales_performance",
             "region_code = 'EU' AND calendar_month = DATE'2024-02-01' AND sales_channel_key = 3")
    assert eu["net_sales_local"] == Decimal("11200.00")
    assert eu["net_sales_daily_rate"] == Decimal("12354.00")
    # month-average: EUR 1.10 -> 1100 + 11000; GBP->USD has no AVERAGE rate -> 1.0 (LEGACY QUIRK) -> 200
    assert eu["net_sales_monthly_average_rate"] == 12300.0
    assert eu["translation_difference"] == -54.0
    assert eu["vat_output_amount"] == Decimal("2130.00") and eu["local_currency_code"] == "EUR"
    rc = one(spark, cfg, "gold", "agg_regional_sales_performance",
             "region_code = 'EU' AND calendar_month = DATE'2024-02-01' AND sales_channel_key = 5")
    assert rc["vat_reverse_charge_amount"] == Decimal("76.00") and rc["translation_difference"] == 0.0
    na = one(spark, cfg, "gold", "agg_regional_sales_performance",
             "region_code = 'NA' AND calendar_month = DATE'2024-02-01' AND sales_channel_key = 1")
    assert na["net_sales_daily_rate"] == Decimal("1550.00") and na["sales_tax_collected"] == Decimal("124.00")
    assert na["budget_net_sales_reporting"] == Decimal("3000.00")
    assert na["budget_variance_reporting"] == Decimal("-1450.00") and na["budget_attainment_percent"] == 51.67
    assert na["prior_year_net_sales"] == Decimal("800.00") and na["year_over_year_percent"] == 93.75
    assert na["rank_in_region_by_sales"] == 1 and na["active_salesperson_count"] == 2
    mar = one(spark, cfg, "gold", "agg_regional_sales_performance",
              "region_code = 'NA' AND calendar_month = DATE'2024-03-01' AND sales_channel_key = 1")
    assert mar["year_to_date_net_sales"] == Decimal("3550.00")  # 1550 + 2000 within FY2024
    apac = one(spark, cfg, "gold", "agg_regional_sales_performance",
               "region_code = 'APAC' AND calendar_month = DATE'2024-02-01'")
    assert apac["gst_collected"] == Decimal("100.00") and apac["gst_free_sales"] == Decimal("300.00")
    assert apac["fiscal_calendar_code"] == "JUL13" and apac["local_currency_code"] == "AUD"


def test_customer_360_one_row_per_customer_full_rebuild(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_customer_360")
    assert sorted(r["customer_key"] for r in rows) == [1, 2, 3, 4, 5, 6]
    acme = one(spark, cfg, "gold", "agg_customer_360", "customer_key = 1")
    assert acme["first_order_date"] == D(2023, 2, 6) and acme["last_order_date"] == D(2024, 2, 10)
    assert acme["tenure_months"] == 13 and acme["days_since_last_order"] == 34
    assert acme["lifetime_order_count"] == 3  # ORD-NA-0, ORD-NA-1, ORD-NA-4 (reversal excluded)
    assert acme["lifetime_net_revenue"] == Decimal("2350.00")
    assert acme["lifetime_gross_margin"] == Decimal("890.00000000")
    assert acme["average_order_value"] == Decimal("783.3300")
    assert acme["average_days_to_pay"] == 23.0
    assert acme["credit_limit_reporting"] == Decimal("10000.00")
    assert acme["rfm_score"] == "4" and acme["churn_risk_score"] == Decimal("15.00") and acme["churn_risk_band"] == "LOW"
    assert acme["marketing_consent_flag"] is True and acme["anonymised_flag"] is False
    assert acme["retention_expiry_date"] == D(2034, 2, 10)  # NA: last order + 10y


def test_customer_360_eu_retention_expiry_anonymises(spark, cfg, goldRun):
    roma = one(spark, cfg, "gold", "agg_customer_360", "customer_key = 6")
    assert roma["retention_expiry_date"] == D(2023, 6, 1)  # EU: last order + 7y, already past AS_OF
    assert roma["anonymised_flag"] is True and roma["customer_name"] == "REDACTED"
    assert roma["primary_contact_email"] is None and roma["marketing_consent_flag"] is False
    assert roma["tenure_months"] == 93 and roma["churn_risk_score"] == Decimal("75.00")
    assert roma["churn_risk_band"] == "HIGH"
    sydney = one(spark, cfg, "gold", "agg_customer_360", "customer_key = 5")
    assert sydney["retention_expiry_date"] == D(2027, 3, 1)  # APAC: last order + 3y
    assert sydney["average_days_to_pay"] == 6.33  # payments 7 and 5 days on cleared invoices... (7+7+5)/3


def test_customer_rolling_12_month_offsets(spark, cfg, goldRun):
    bigbox = rowsOf(spark, cfg, "gold", "agg_customer_rolling_12_month", "customer_key = 2", "month_offset")
    assert [(r["month_offset"], r["net_revenue_reporting"]) for r in bigbox] == [
        (0, Decimal("2000.00")),  # 2024-03 sale 4
        (1, Decimal("200.00")),  # 2024-02 sale 3 (PILOT channel still counts for the customer)
    ]
    assert bigbox[0]["rolling_12_month_revenue"] == Decimal("2200.00")
    assert bigbox[0]["rolling_3_month_revenue"] == Decimal("2200.00")


def test_product_performance_abc_xyz_and_new_product_flag(spark, cfg, goldRun):
    feb10 = one(spark, cfg, "gold", "agg_product_performance", "stock_item_key = 10 AND calendar_month = DATE'2024-02-01'")
    feb11 = one(spark, cfg, "gold", "agg_product_performance", "stock_item_key = 11 AND calendar_month = DATE'2024-02-01'")
    # NA Feb category 1 revenue 1750: item 10 cum 1300 <= 80% -> A; item 11 cum 1750 > 95% -> C
    assert feb10["units_sold_base_uom"] == Decimal("13.000") and feb10["net_revenue_reporting"] == Decimal("1300.00")
    assert (feb10["abc_class"], feb10["xyz_class"], feb10["rank_in_category_by_revenue"]) == ("A", "Z", 1)
    assert (feb11["abc_class"], feb11["rank_in_category_by_revenue"], feb11["discount_depth_percent"]) == ("C", 2, 10.0)
    mar10 = one(spark, cfg, "gold", "agg_product_performance", "stock_item_key = 10 AND calendar_month = DATE'2024-03-01'")
    # LEGACY QUIRK: the only item in its category is 100% of cumulative revenue -> C
    assert mar10["abc_class"] == "C" and mar10["prior_month_abc_class"] == "A"
    # trailing 2 months of units (13, 20): mean 16.5, sample std 4.95 -> CV 0.30 -> Y
    assert mar10["xyz_class"] == "Y"
    cushion = one(spark, cfg, "gold", "agg_product_performance", "stock_item_key = 31")
    assert cushion["new_product_flag"] is True and cushion["discontinued_flag"] is False


def test_monthly_margin_analysis_bridge(spark, cfg, goldRun):
    feb = one(spark, cfg, "gold", "agg_monthly_margin_analysis",
              "region_code = 'NA' AND calendar_month = DATE'2024-02-01' AND sales_channel_key = 1")
    # margin rows INV-NA-1 lines 1+2: qty 15... plus INV-NA-4 (qty 1): 16 units, rev 1550
    assert feb["quantity_sold_base_uom"] == Decimal("16.000") and feb["net_revenue_reporting"] == Decimal("1550.00")
    assert feb["cost_of_sales_reporting"] == Decimal("960.00000000")
    assert feb["standard_cost_reporting"] == Decimal("930.00000000")
    assert feb["purchase_price_variance"] == Decimal("30.00000000")
    assert feb["gross_margin_reporting"] == Decimal("590.00")
    assert feb["contribution_margin_reporting"] == Decimal("580.00")  # 590 - freight 15 + rebate 5
    assert feb["margin_percent"] == 38.06 and feb["cost_basis_code"] == "WAVG"
    mar = one(spark, cfg, "gold", "agg_monthly_margin_analysis",
              "region_code = 'NA' AND calendar_month = DATE'2024-03-01' AND sales_channel_key = 1")
    # price (100 - 96.875) x 20 = 62.5; volume (20 - 16) x 36.875 = 147.5; cost 0; mix closes the bridge
    assert mar["price_effect_amount"] == Decimal("62.50") and mar["volume_effect_amount"] == Decimal("147.50")
    assert mar["cost_effect_amount"] == Decimal("0.00") and mar["mix_effect_amount"] == Decimal("0.00")
    assert mar["prior_period_margin_percent"] == 38.06
    eu = one(spark, cfg, "gold", "agg_monthly_margin_analysis", "region_code = 'EU'")
    apac = one(spark, cfg, "gold", "agg_monthly_margin_analysis", "region_code = 'APAC'")
    assert (eu["cost_basis_code"], apac["cost_basis_code"]) == ("FIFO", "STD")


def test_aggregates_rerun_is_idempotent(spark, cfg, goldRun):
    from sales_lakehouse.gold import aggregates

    before = {t: spark.table(cfg.fqn("gold", t)).count() for t in
              ("agg_daily_sales", "agg_monthly_sales", "agg_regional_sales_performance", "agg_customer_360")}
    aggregates.run(spark, cfg, asOfDate=AS_OF)
    after = {t: spark.table(cfg.fqn("gold", t)).count() for t in before}
    assert after == before
