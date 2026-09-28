"""Every generated Spark SQL statement must parse, reference only ${catalog}-resolved tables and keep the grain."""
import datetime as dt

import pytest

import agg_common as ac
import agg_sql
import rpt_views
from agg_sql import AGGREGATE_KEY_COLUMNS

DAILY = ac.RefreshWindow(dt.date(2024, 5, 1), dt.date(2024, 5, 3))
PERIOD = ac.RefreshWindow(dt.date(2024, 4, 1), dt.date(2024, 4, 30))


def builders(t):
    return {
        "agg_daily_sales_summary": agg_sql.dailySalesSummarySql(t, DAILY),
        "agg_daily_inventory_health": agg_sql.dailyInventoryHealthSql(t, DAILY, 0),
        "agg_monthly_sales_summary": agg_sql.monthlySalesSummarySql(t, PERIOD),
        "agg_monthly_margin_analysis": agg_sql.monthlyMarginAnalysisSql(t, PERIOD, True),
        "agg_customer_360": agg_sql.customer360Sql(t, dt.date(2024, 4, 30)),
        "agg_customer_rolling_12_month": agg_sql.customerRolling12MonthSql(t, dt.date(2023, 5, 1), dt.date(2024, 4, 1), 12),
        "agg_product_performance": agg_sql.productPerformanceSql(t, PERIOD, 80, 95),
        "agg_supplier_performance": agg_sql.supplierPerformanceSql(t, PERIOD),
        "agg_regional_sales_performance": agg_sql.regionalSalesPerformanceSql(t, PERIOD, "USD"),
        "agg_finance_close_summary": agg_sql.financeCloseSummarySql(t, PERIOD, 100),
        "agg_promotion_effectiveness": agg_sql.promotionEffectivenessSql(t, PERIOD, 8),
        "agg_delivery_performance_summary": agg_sql.deliveryPerformanceSummarySql(t, DAILY, 12),
    }


def _parse(spark, sql):
    spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)


@pytest.mark.parametrize("key", sorted(AGGREGATE_KEY_COLUMNS))
def test_refresh_sql_parses_and_is_catalog_parameterised(spark, tables, key):
    sql = builders(tables)[key]
    _parse(spark, sql)
    assert "spark_catalog.gold." in sql
    assert "wwi_" not in sql
    for col in AGGREGATE_KEY_COLUMNS[key]:
        assert col in sql, f"{key} lost grain column {col}"


def test_refresh_sql_uses_only_catalog_prefixed_tables(tables):
    t = ac.resolveTables("wwi_dev", lambda c, s, n: f"{c}.{s}.{n}")
    for key, sql in builders(t).items():
        for token in ("FROM ", "JOIN "):
            for part in sql.split(token)[1:]:
                name = part.strip().split()[0]
                if name.startswith("("):
                    continue
                assert name.startswith("wwi_dev.") or name[0].islower() and "." not in name, f"{key}: {name}"


def test_window_predicates_are_embedded_in_windowed_sql(tables):
    b = builders(tables)
    assert DAILY.sqlLiteral("f.invoice_date_key") in b["agg_daily_sales_summary"]
    assert "DATE'2024-04-01'" in b["agg_monthly_sales_summary"]
    assert "DATE'2024-04-30'" in b["agg_monthly_sales_summary"]


@pytest.mark.parametrize("viewName", sorted(rpt_views.REPORT_VIEWS.values()))
def test_report_view_ddl_parses(spark, tables, viewName):
    ddl = rpt_views.viewDdl("spark_catalog", viewName, rpt_views.VIEW_BUILDERS[viewName](tables),
                            lambda c, s, n: f"{c}.{s}.{n}")
    assert ddl.startswith(f"CREATE OR REPLACE VIEW spark_catalog.gold.{viewName} AS")
    _parse(spark, ddl)


def test_every_report_view_is_mapped_and_ordered():
    assert set(rpt_views.REPORT_VIEWS.values()) == set(rpt_views.VIEW_BUILDERS)
    assert set(rpt_views.REPORT_VIEWS.values()) == set(rpt_views.REPORT_SOURCES)
    assert rpt_views.DEFAULT_PUBLICATION_ORDER and set(rpt_views.DEFAULT_PUBLICATION_ORDER) == set(rpt_views.VIEW_BUILDERS)
    assert len(rpt_views.REPORT_VIEWS) == 16


def test_resolve_publication_list_accepts_legacy_and_delta_names():
    got = rpt_views.resolvePublicationList("Report.vw_PromotionRoi, rpt_customer_360;[Report].[vw_ApAgingCurrent]")
    assert got == ["rpt_promotion_roi", "rpt_customer_360", "rpt_ap_aging_current"]
    assert rpt_views.resolvePublicationList("") == list(rpt_views.DEFAULT_PUBLICATION_ORDER)
    with pytest.raises(ValueError):
        rpt_views.resolvePublicationList("Report.vw_DoesNotExist")
