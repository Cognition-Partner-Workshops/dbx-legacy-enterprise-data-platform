from datetime import date
from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.gold import fact_daily_snapshots, fact_order, fact_sale
from tests.gold_facts_fixtures import isolatedConfig, seedDimensions, seedOrders, seedSales


def test_daily_sales_snapshot_replace_where(spark, cfg):
    iso = isolatedConfig(spark, cfg, "snap")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    fact_sale.run(spark, iso)
    fqn = iso.fqn("gold", fact_daily_snapshots.SALES_TABLE)

    fact_daily_snapshots.runSalesSnapshot(spark, iso, ["2024-03-15", "2024-07-10"])
    snap = spark.table(fqn)
    assert snap.count() == 2
    na = snap.filter(F.col("snapshot_date_key") == date(2024, 3, 15)).collect()[0]
    assert na["net_sales_amount"] == Decimal("100.0000") and na["invoice_count"] == 1 and na["region_code"] == "NA"
    assert na["commission_rate"] == Decimal("0.0200") and na["month_to_date_net_sales"] == Decimal("100.0000")
    firstLoadedAt = na["loaded_at_utc"]

    # Replacing only 2024-03-15 rewrites that date and leaves 2024-07-10 untouched.
    fact_daily_snapshots.runSalesSnapshot(spark, iso, ["2024-03-15"])
    snap = spark.table(fqn)
    assert snap.count() == 2
    assert snap.filter(F.col("snapshot_date_key") == date(2024, 3, 15)).count() == 1
    assert snap.filter(F.col("snapshot_date_key") == date(2024, 3, 15)).collect()[0]["loaded_at_utc"] >= firstLoadedAt
    assert snap.filter(F.col("snapshot_date_key") == date(2024, 7, 10)).count() == 1

    # A date with no sales replaces to zero rows for that date without touching the others.
    fact_daily_snapshots.runSalesSnapshot(spark, iso, ["2024-07-10", "2024-12-25"])
    assert spark.table(fqn).count() == 2


def test_daily_backlog_open_lines_as_of_snapshot(spark, cfg):
    iso = isolatedConfig(spark, cfg, "backlog")
    seedDimensions(spark, iso)
    seedOrders(spark, iso)
    fact_order.run(spark, iso)
    fqn = iso.fqn("gold", fact_daily_snapshots.BACKLOG_TABLE)

    fact_daily_snapshots.runBacklog(spark, iso, ["2024-03-20", "2024-03-05"])
    backlog = spark.table(fqn)
    rows = backlog.collect()
    assert len(rows) == 1  # ORD1-2 is the only open line and only after its order date
    r = rows[0]
    assert r["order_line_business_key"] == "ORD1-2" and r["snapshot_date_key"] == date(2024, 3, 20)
    assert r["quantity_open"] == Decimal("3.0000") and r["backlog_age_days"] == 10 and r["past_promise_flag"] is False
    assert r["stock_constrained_flag"] is True

    fact_daily_snapshots.runBacklog(spark, iso, ["2024-03-20", "2024-03-30"])
    backlog = spark.table(fqn)
    assert backlog.count() == 2
    late = backlog.filter(F.col("snapshot_date_key") == date(2024, 3, 30)).collect()[0]
    assert late["past_promise_flag"] is True and late["days_past_promise"] == 5


def test_batch_snapshot_dates_default_from_batch(spark, cfg):
    iso = isolatedConfig(spark, cfg, "snapdates")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    seedOrders(spark, iso)
    fact_sale.run(spark, iso)
    fact_order.run(spark, iso)
    dates = fact_daily_snapshots.batchSnapshotDates(spark, iso)
    assert dates == ["2024-03-10", "2024-03-15", "2024-05-01", "2024-07-01", "2024-07-10"]
