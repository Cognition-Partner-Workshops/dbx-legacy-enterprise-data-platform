"""rpt_* views: same column set as the legacy Report.vw_* select lists, plus spot values."""
from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import pytest

from sales_lakehouse.gold import reporting
from tests.gold_reporting_fixtures import goldRun, one, rowsOf  # noqa: F401

LEGACY_VIEWS_DIR = Path(__file__).resolve().parents[2] / "sqlserver" / "views"


def legacySelectListAliases(viewName: str) -> list[str]:
    """Aliases of the outermost SELECT of ``sqlserver/views/<viewName>.sql``."""
    lines = (LEGACY_VIEWS_DIR / f"{viewName}.sql").read_text().splitlines()
    outerSelect = max(i for i, line in enumerate(lines) if line.startswith("SELECT"))
    aliases = []
    for line in lines[outerSelect:]:
        if line.startswith("FROM"):
            break
        match = re.search(r"AS \[([^\]]+)\]\s*,?\s*$", line)
        if match:
            aliases.append(match.group(1))
    return aliases


def toSnakeCase(alias: str) -> str:
    text = alias.replace("%", " pct")
    text = re.sub(r"-(\d)", r" minus \1", text)
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()


@pytest.mark.parametrize("legacyView,goldView", sorted(reporting.REPORT_VIEWS.items()))
def test_rpt_view_columns_match_legacy_select_list(spark, cfg, goldRun, legacyView, goldView):
    expected = [toSnakeCase(a) for a in legacySelectListAliases(legacyView)]
    assert len(expected) >= 25, legacyView
    actual = spark.table(cfg.fqn("gold", goldView)).columns
    assert actual == expected


def test_rpt_daily_sales_trend_values(spark, cfg, goldRun):
    r = one(spark, cfg, "gold", "rpt_daily_sales_trend",
            "sales_date = DATE'2024-02-05' AND territory = 'US East' AND channel = 'Direct Sales'")
    # items 10 (900 after reversal) + 11 (450)
    assert r["invoices"] == 3 and r["lines"] == 3 and r["net_sales"] == Decimal("1350.00")
    # LEGACY QUIRK: Total Discount = Line Discount + Promotion Discount (promo is already part of line discount)
    assert r["total_discount"] == Decimal("100.00") and r["indirect_tax"] == Decimal("108.00")
    assert r["margin_pct"] == Decimal("37.78")  # 510 / 1350
    assert r["prior_year_net_sales"] == Decimal("800.00") and r["prior_year_growth_pct"] == Decimal("68.75")
    assert r["region"] == "NA" and r["rank_in_region_that_day"] == 1 and r["day_of_week"] == 2  # Monday
    assert r["fiscal_year_to_date_revenue"] == Decimal("1350.00")


def test_rpt_sales_by_customer_month_labels_and_status(spark, cfg, goldRun):
    r = one(spark, cfg, "gold", "rpt_sales_by_customer_month", "customer_key = 1 AND calendar_month = DATE'2024-02-01'")
    assert r["customer_name"] == "Acme Novelties" and r["customer_category"] == "Novelty Shop"
    assert r["fiscal_period_label"] == "FY2024 P02" and r["fiscal_calendar"] == "CY12"
    assert r["net_revenue"] == Decimal("1550.00") and r["margin_pct"] == Decimal("38.06")
    assert r["year_on_year_pct"] == 93.75 and r["period_status"] == "Period To Date"
    eu = one(spark, cfg, "gold", "rpt_sales_by_customer_month", "customer_key = 3 AND calendar_month = DATE'2024-02-01'")
    assert eu["fiscal_period_label"] == "FY2024/25 P02"
    assert eu["credit_notes"] == 110.0 and eu["net_revenue_after_credits"] == 12244.0
    apac = one(spark, cfg, "gold", "rpt_sales_by_customer_month", "customer_key = 5 AND calendar_month = DATE'2024-02-01'")
    assert apac["fiscal_period_label"] == "FY2024 P03 (13P)"  # month's MAX period: 2024-02-27 sale is NA445 P3
    # Feb and Mar rows share fiscal_period P3; the legacy ROWS UNBOUNDED PRECEDING frame ordered by
    # fiscal period is tie-order dependent, so only the running total of the last peer is stable.
    apacRows = rowsOf(spark, cfg, "gold", "rpt_sales_by_customer_month", "customer_key = 5 AND fiscal_year = 2024")
    assert max(r["year_to_date_revenue"] for r in apacRows) == Decimal("1145.00")  # 650 + 195 + 300


def test_rpt_sales_by_product_month_classification(spark, cfg, goldRun):
    feb = one(spark, cfg, "gold", "rpt_sales_by_product_month", "stock_item_key = 10 AND calendar_month = DATE'2024-02-01'")
    assert (feb["product"], feb["brand"], feb["category"]) == ("Chocolate frogs", "WWI", "Novelty")
    assert feb["abc_xyz"] == "A/Z" and feb["class_movement"] == "New" and feb["top_500_flag"] == 1
    assert feb["prior_month_net_revenue"] is None and feb["month_on_month_pct"] is None
    mar = one(spark, cfg, "gold", "rpt_sales_by_product_month", "stock_item_key = 10 AND calendar_month = DATE'2024-03-01'")
    assert mar["abc_xyz"] == "C/Y" and mar["class_movement"] == "Demoted"  # 'C' > 'A'
    assert mar["prior_month_net_revenue"] == Decimal("1300.00")
    assert mar["month_on_month_pct"] == Decimal("53.85")  # (2000 - 1300) / 1300


def test_rpt_sales_by_territory_month_budget_status(spark, cfg, goldRun):
    r = one(spark, cfg, "gold", "rpt_sales_by_territory_month",
            "territory = 'US East' AND channel = 'Direct Sales' AND calendar_month = DATE'2024-02-01'")
    assert r["budget"] == Decimal("3000.00") and r["budget_attainment_pct"] == 51.67 and r["budget_status"] == "Below"
    assert r["indirect_tax_collected"] == Decimal("124.00") and r["local_currency"] == "USD"
    eu = one(spark, cfg, "gold", "rpt_sales_by_territory_month",
             "territory = 'Germany' AND channel = 'Direct Sales' AND calendar_month = DATE'2024-02-01'")
    assert eu["budget_status"] == "No Budget" and eu["translation_difference"] == -54.0
    assert eu["net_sales_at_average_rate"] == 12300.0 and eu["indirect_tax_collected"] == Decimal("2130.00")


def test_rpt_order_to_cash_cycle_assessment_and_cohort(spark, cfg, goldRun):
    rows = {r["order_number"]: r for r in rowsOf(spark, cfg, "gold", "rpt_order_to_cash_cycle")}
    assert set(rows) == {"ORD-NA-1", "ORD-NA-3", "ORD-EU-6"}
    first = rows["ORD-NA-1"]
    assert first["cycle_assessment"] == "Within Target" and first["cohort_median_cycle_days"] == 20.0
    assert first["variance_to_cohort_median"] == 0.0 and first["customer"] == "Acme Novelties"
    second = rows["ORD-NA-3"]
    assert second["cycle_assessment"] == "Breached"  # 40 > 25 + 10
    assert second["cohort_median_cycle_days"] == 30.0 and second["variance_to_cohort_median"] == 10.0
    assert second["value_still_in_pipeline"] == Decimal("0.00") and second["territory"] == "US East"
    inflight = rows["ORD-EU-6"]
    assert inflight["cycle_assessment"] == "In Flight" and inflight["value_still_in_pipeline"] == Decimal("339.00")
    assert inflight["warehouse_site"] is None  # dim_warehouse_site not present on this branch


def test_rpt_margin_by_product_category_bands(spark, cfg, goldRun):
    na = one(spark, cfg, "gold", "rpt_margin_by_product_category",
             "region = 'NA' AND calendar_month = DATE'2024-02-01' AND net_revenue = 1550")
    assert na["category"] == "Novelty" and na["territory"] == "US East" and na["cost_basis"] == "WAVG"
    assert na["margin_band"] == "Strong" and na["standard_margin_pct"] is None  # only STD basis exposes it
    assert na["margin_point_movement"] == Decimal("0.00") and na["bridge_residual"] == Decimal("0.00")
    assert na["contribution_margin"] == Decimal("580.00")
    apac = one(spark, cfg, "gold", "rpt_margin_by_product_category", "region = 'APAC'")
    assert apac["standard_margin_pct"] == 40.0 and apac["margin_band"] == "Strong"
    mar = one(spark, cfg, "gold", "rpt_margin_by_product_category", "region = 'NA' AND calendar_month = DATE'2024-03-01'")
    assert mar["margin_point_movement"] == 1.94  # 40.00 - 38.06


def test_rpt_customer_360_anonymisation_and_ranks(spark, cfg, goldRun):
    acme = one(spark, cfg, "gold", "rpt_customer_360", "customer_key = 1")
    assert acme["customer"] == "Acme Novelties" and acme["contact_email"] == "buyer@acme.example"
    assert acme["lifetime_margin_pct"] == Decimal("37.87") and acme["rank_by_lifetime_revenue"] == 1
    assert acme["rolling_12_month_revenue"] is None  # no 2024-03 activity -> no month_offset 0 row
    bigbox = one(spark, cfg, "gold", "rpt_customer_360", "customer_key = 2")
    assert bigbox["contact_email"] is None  # marketing consent withheld suppresses contact
    assert bigbox["rolling_12_month_revenue"] == Decimal("2200.00") and bigbox["revenue_decile"] == 1
    roma = one(spark, cfg, "gold", "rpt_customer_360", "customer_key = 6")
    assert roma["customer"] == "(anonymised)" and roma["contact_email"] is None and roma["anonymised_flag"] is True
    assert roma["segment"] is None and roma["loyalty_tier"] is None


def test_rpt_customer_churn_risk_filter_and_worklist(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "rpt_customer_churn_risk")
    assert [r["customer_key"] for r in rows] == [6]  # only score >= 40
    roma = rows[0]
    assert roma["customer"] == "(anonymised)" and roma["contactable_flag"] == 0
    assert roma["worklist_priority"] == "P3 - Watch" and roma["churn_risk_band"] == "HIGH"
    assert roma["revenue_current_month"] is None and roma["consecutive_inactive_months"] is None


def test_reporting_run_is_repeatable(spark, cfg, goldRun):
    reporting.run(spark, cfg)
    assert spark.table(cfg.fqn("gold", "rpt_customer_churn_risk")).count() == 1
