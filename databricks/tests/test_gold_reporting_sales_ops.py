"""Quota attainment (3 regional bases) and commission (3 bases, PILOT exclusion, EU cap, FX)."""
from __future__ import annotations

from decimal import Decimal

from sales_lakehouse.gold import sales_ops
from tests.gold_reporting_fixtures import AS_OF, goldRun, one, rowsOf  # noqa: F401


def test_quota_attainment_grain_and_regional_bases(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_quota_attainment", orderBy="salesperson_key, period_start_date")
    keys = [(r["salesperson_key"], r["fiscal_period_label"], r["region_code"]) for r in rows]
    assert len(keys) == len(set(keys)) and len(rows) == 5

    naP2 = one(spark, cfg, "gold", "agg_quota_attainment", "salesperson_key = 1 AND fiscal_period_label = 'FY2024-P02'")
    # NA = fact_sale net USD in 2024-01-29..2024-02-25: sales 1 (1000) + 2 (450) + 3 (200, PILOT still books)
    # LEGACY QUIRK documented in sales_ops: legacy SSIS summed ExtendedPrice + TaxAmount (gross)
    assert naP2["measure_basis_code"] == "INVOICED"
    assert naP2["actual_amount"] == Decimal("1650.00") and naP2["quota_amount"] == Decimal("2000.00")
    assert naP2["attainment_pct"] == Decimal("82.50") and naP2["attainment_band_code"] == "NEAR"
    naP3 = one(spark, cfg, "gold", "agg_quota_attainment", "salesperson_key = 1 AND fiscal_period_label = 'FY2024-P03'")
    assert naP3["actual_amount"] == Decimal("2000.00") and naP3["attainment_pct"] == Decimal("400.00")
    assert naP3["attainment_band_code"] == "OVER120"

    eu = one(spark, cfg, "gold", "agg_quota_attainment", "region_code = 'EU'")
    # EU = invoiced reporting revenue (1100 + 440 + 11000 + 254) net of credit note (100 EUR x 1.1)
    assert eu["measure_basis_code"] == "NET_OF_CREDITS"
    assert eu["actual_amount"] == Decimal("12684.00") and eu["attainment_pct"] == Decimal("126.84")
    assert eu["fiscal_calendar_code"] == "EUCAL" and eu["quota_currency_code"] == "EUR"

    apac = one(spark, cfg, "gold", "agg_quota_attainment", "region_code = 'APAC'")
    # APAC = order intake: ORD-AP-1 (520) on 2024-02-03; ORD-AP-2 falls in P3
    assert apac["measure_basis_code"] == "ORDER_INTAKE"
    assert apac["actual_amount"] == Decimal("520.00") and apac["attainment_pct"] == Decimal("65.00")
    assert apac["attainment_band_code"] == "UNDER"


def test_quota_attainment_zero_quota_and_unresolved_keys(spark, cfg, goldRun):
    zero = one(spark, cfg, "gold", "agg_quota_attainment", "salesperson_key = 4")
    # LEGACY QUIRK: zero quota reports 0% rather than NULL / divide-by-zero
    assert zero["actual_amount"] == Decimal("100.00") and zero["attainment_pct"] == Decimal("0.00")
    assert zero["attainment_band_code"] == "NOQUOTA"
    rejected = rowsOf(spark, cfg, "quality", "rejected_rows", "rule_code = 'QUOTA_UNRESOLVED_KEY'")
    assert len(rejected) == 1 and '"fiscal_period_label":"FY2024-P04"' in rejected[0]["row_json"]


def test_commission_na_invoiced_margin_bands_house_account_accelerator(spark, cfg, goldRun):
    feb = one(spark, cfg, "gold", "agg_commission", "region_code = 'NA' AND commission_period_label = '2024-02'")
    assert feb["commission_basis"] == "INVOICEDMARGIN" and feb["plan_code"] == "NA-MARGIN"
    # margin lines INV-NA-1 (400 + 150); INV-NA-2 PILOT excluded; INV-NA-4 rep has no plan
    assert feb["line_count"] == 2 and feb["basis_amount_plan_currency"] == Decimal("550.00")
    # attainment 82.5% -> band 2 (<= 100) -> 8%
    assert feb["attainment_pct"] == Decimal("82.50") and feb["commission_rate_pct"] == Decimal("8.00")
    assert feb["commission_amount"] == Decimal("44.00") and feb["accelerator_commission_amount"] == Decimal("0.00")
    mar = one(spark, cfg, "gold", "agg_commission", "region_code = 'NA' AND commission_period_label = '2024-03'")
    # 800 margin at 400% attainment -> band 3 (10%) = 80; house account (customer 2) halves it -> 40
    # accelerator: 2% x (800 - quota 500) = 6
    assert mar["commission_rate_pct"] == Decimal("10.00") and mar["raw_commission_amount"] == Decimal("80.00")
    assert mar["base_commission_amount"] == Decimal("40.00")
    assert mar["accelerator_commission_amount"] == Decimal("6.00") and mar["commission_amount"] == Decimal("46.00")
    assert mar["plan_currency_code"] == "USD"


def test_commission_eu_net_revenue_month_average_eur_and_statutory_cap(spark, cfg, goldRun):
    feb = one(spark, cfg, "gold", "agg_commission", "region_code = 'EU' AND commission_period_label = '2024-02'")
    assert feb["commission_basis"] == "NETREVENUE" and feb["plan_currency_code"] == "EUR"
    # lines: 1000 EUR, 400 EUR, 10000 EUR, 200 GBP (x 1.15 Feb AVERAGE rate = 230 EUR)
    assert feb["line_count"] == 4
    assert feb["basis_amount_local"] == Decimal("11600.00")
    assert feb["basis_amount_plan_currency"] == Decimal("11630.00")
    # attainment 126.84% -> band 3 (<= 130) -> 7%; raw = 70 + 28 + 700 + 16.10
    assert feb["commission_rate_pct"] == Decimal("7.00") and feb["raw_commission_amount"] == Decimal("814.10")
    # LEGACY QUIRK: statutory cap 300 applied per line -> 700 capped to 300
    assert feb["statutory_cap_amount"] == Decimal("300.00") and feb["capped_line_count"] == 1
    assert feb["commission_amount"] == Decimal("414.10")
    mar = one(spark, cfg, "gold", "agg_commission", "region_code = 'EU' AND commission_period_label = '2024-03'")
    # CHF line with no CHF->EUR AVERAGE rate: amount passes through x1.0 (LEGACY QUIRK); no quota -> band 1
    assert mar["basis_amount_plan_currency"] == Decimal("300.00") and mar["attainment_pct"] is None
    assert mar["commission_rate_pct"] == Decimal("5.00") and mar["commission_amount"] == Decimal("15.00")


def test_commission_apac_collected_cash_team_split_and_445_period(spark, cfg, goldRun):
    rows = rowsOf(spark, cfg, "gold", "agg_commission", "region_code = 'APAC'")
    assert len(rows) == 1
    r = rows[0]
    assert r["commission_basis"] == "COLLECTEDCASH" and r["plan_currency_code"] == "AUD"
    # only the CLEARED AUD payment (1100) counts: PENDING payment held, NZD payment has no rate
    assert r["commission_period_label"] == "FY2024-P02" and r["basis_amount_plan_currency"] == Decimal("1100.00")
    assert r["attainment_pct"] == Decimal("65.00") and r["commission_rate_pct"] == Decimal("4.00")
    assert r["split_factor"] == Decimal("0.5000") and r["commission_amount"] == Decimal("22.00")
    assert r["period_boundary_line_count"] == 0


def test_commission_rejects_are_quarantined_not_dropped(spark, cfg, goldRun):
    rejected = rowsOf(spark, cfg, "quality", "rejected_rows", "rule_code LIKE 'COMM_%'", "rule_code, row_json")
    byRule: dict[str, list[str]] = {}
    for r in rejected:
        byRule.setdefault(r["rule_code"], []).append(r["row_json"])
    assert sorted(byRule) == ["COMM_MISSING_FX", "COMM_NON_COMMISSIONABLE_CHANNEL", "COMM_NO_PLAN"]
    # LEGACY QUIRK: PILOT channel (is_commissionable = false) - both bases of INV-NA-2
    assert len(byRule["COMM_NON_COMMISSIONABLE_CHANNEL"]) == 2
    assert all('"invoice_number":"INV-NA-2"' in j for j in byRule["COMM_NON_COMMISSIONABLE_CHANNEL"])
    # rep 104 has no plan (2 bases of INV-NA-4); rep 102's 2016 line predates the assignment
    assert len(byRule["COMM_NO_PLAN"]) == 3
    assert sum('"invoice_number":"INV-NA-4"' in j for j in byRule["COMM_NO_PLAN"]) == 2
    assert sum('"invoice_number":"INV-EU-0"' in j for j in byRule["COMM_NO_PLAN"]) == 1
    assert len(byRule["COMM_MISSING_FX"]) == 1 and '"currency_code":"NZD"' in byRule["COMM_MISSING_FX"][0]


def test_plan_rate_bands_are_inclusive_upper_bounds(spark):
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [(79.99,), (80.0,), (100.0,), (120.0,), (120.01,), (None,)], "attainment_pct DOUBLE"
    ).withColumn("Band1UpperPercent", F.lit(80.0)).withColumn("Band1RatePercent", F.lit(5.0)) \
        .withColumn("Band2UpperPercent", F.lit(100.0)).withColumn("Band2RatePercent", F.lit(8.0)) \
        .withColumn("Band3UpperPercent", F.lit(120.0)).withColumn("Band3RatePercent", F.lit(10.0))
    rates = [r[0] for r in df.select(sales_ops._planRate(F.col("attainment_pct"))).collect()]
    # LEGACY QUIRK: above band 3 keeps paying the band 3 rate; NULL attainment pays band 1
    assert rates == [5.0, 5.0, 8.0, 10.0, 10.0, 5.0]


def test_sales_ops_rerun_is_idempotent(spark, cfg, goldRun):
    before = spark.table(cfg.fqn("gold", "agg_commission")).count()
    sales_ops.run(spark, cfg, asOfDate=AS_OF)
    assert spark.table(cfg.fqn("gold", "agg_commission")).count() == before
    assert spark.table(cfg.fqn("gold", "agg_quota_attainment")).count() == 5
