"""runRefresh against local Delta: window overwrite, rejects, reconciliation, idempotency, lifecycle logging."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

import agg_common as ac
import agg_sql
from agg_package import RefreshSpec, runRefresh

SALE_COLUMNS = ("invoice_date_key date, stock_item_key int, sales_territory_key int, sales_channel_key int, "
                "region_code string, fiscal_year int, fiscal_period int, invoice_number string, customer_key int, "
                "quantity_base_uom double, gross_amount double, line_discount_amount double, promotion_key int, "
                "net_amount double, tax_amount double, freight_amount double, cost_of_sale_amount double, "
                "gross_margin_amount double, net_amount_reporting double, correction_type_code string")


def _sale(day, item, customer, net, invoice, corr=None):
    return (day, item, 1, 1, "EU", 2024, 5, invoice, customer, 1.0, net, 0.0, 0, net, 0.0, 0.0, net * 0.6,
            net * 0.4, net, corr)


@pytest.fixture(scope="module")
def sales_tables(spark, tables):
    d1, d2 = dt.date(2024, 5, 1), dt.date(2024, 5, 2)
    spark.sql(f"DROP TABLE IF EXISTS {tables['agg_daily_sales_summary']}")
    rows = [_sale(d1, 10, c, 100.0, f"I{c}") for c in (1, 2, 3)]        # 3 customers -> accepted
    rows += [_sale(d2, 10, 1, 50.0, "I9"), _sale(d2, 10, 2, 50.0, "I10")]  # 2 customers -> rejected (small cell)
    rows += [_sale(d2, 11, 1, 999.0, "I11", "REV")]                       # reversal -> filtered
    rows += [_sale(dt.date(2024, 4, 1), 10, 5, 10.0, "OLD")]              # outside window
    spark.createDataFrame(rows, SALE_COLUMNS).write.format("delta").mode("overwrite").saveAsTable(tables["fact_sale"])
    spark.createDataFrame([(10, 7), (11, 7)], "stock_item_key int, product_category_key int") \
        .write.format("delta").mode("overwrite").saveAsTable(tables["dim_stock_item"])
    spark.createDataFrame([(d1, 10, 1, "EU", 30.0)],
                          "return_date_key date, stock_item_key int, sales_territory_key int, region_code string, "
                          "net_credit_amount_reporting double") \
        .write.format("delta").mode("overwrite").saveAsTable(tables["fact_return"])
    return tables


def _spec(t, window):
    return RefreshSpec(
        packageName="AGG_Refresh_DailySalesSummary", targetKey="agg_daily_sales_summary",
        sql=agg_sql.dailySalesSummarySql(t, window), replacePredicate=window.sqlLiteral("sales_date"),
        rejectCondition=F.col("distinct_customer_count") < 3, rejectReasonCode="AGG_SUPPRESSED_SMALL_CELL",
        keyMeasures=("net_sales_amount", "gross_margin_amount"))


def test_daily_sales_refresh_end_to_end(spark, control, sales_tables):
    t = sales_tables
    window = ac.RefreshWindow(dt.date(2024, 5, 1), dt.date(2024, 5, 2))
    result = runRefresh(spark, control, "spark_catalog", 42, _spec(t, window), t["agg_daily_sales_summary"])

    assert (result.sourceRowCount, result.targetRowCount, result.rejectRowCount) == (2, 1, 1)
    assert result.measureTotals["net_sales_amount"] == pytest.approx(300.0)
    target = spark.table(t["agg_daily_sales_summary"]).collect()
    assert len(target) == 1
    row = target[0]
    assert row["sales_date"] == dt.date(2024, 5, 1)
    assert row["invoice_count"] == 3 and row["distinct_customer_count"] == 3
    assert row["returns_amount"] == pytest.approx(30.0)
    assert row["margin_percent"] == pytest.approx(40.0)
    assert row["product_category_key"] == 7
    assert row["refresh_batch_id"] == 42

    names = [n for n, _ in control.calls]
    assert names[0] == "logPackageStart" and names[-1] == "logPackageEnd"
    assert control.callsNamed("logPackageEnd")[0]["status"] == "Succeeded"
    assert control.callsNamed("logPackageEnd")[0]["rowsRejected"] == 1
    rejects = control.callsNamed("logRejectedRecordSet")[0]
    assert rejects["rejectedRowCount"] == 1 and rejects["rejectReasonCode"] == "AGG_SUPPRESSED_SMALL_CELL"
    assert rejects["businessKeys"] == ["2024-05-02|10|1|1|EU"]
    rc = control.callsNamed("logRowCount")[0]
    assert rc["objectName"] == "Aggregate.Daily Sales Summary"
    assert (rc["sourceRowCount"], rc["targetRowCount"], rc["rejectRowCount"]) == (2, 1, 1)
    assert control.callsNamed("assertRowCountTolerance")[0]["absoluteTolerance"] == 1
    assert control.callsNamed("setWatermark")[0]["objectName"] == "Aggregate.Daily Sales Summary"


def test_rerun_same_window_is_idempotent_and_keeps_other_windows(spark, control, sales_tables):
    t = sales_tables
    target = t["agg_daily_sales_summary"]
    spark.sql(f"INSERT INTO {target} SELECT * FROM {target}").collect()  # duplicate rows inside the window
    other = spark.table(target).withColumn("sales_date", F.lit(dt.date(2024, 3, 1)).cast("date"))
    other.write.format("delta").mode("append").saveAsTable(target)     # rows outside the window survive
    before = spark.table(target).count()
    assert before == 4

    window = ac.RefreshWindow(dt.date(2024, 5, 1), dt.date(2024, 5, 2))
    runRefresh(spark, control, "spark_catalog", 43, _spec(t, window), target)
    rows = spark.table(target).collect()
    assert len(rows) == 3
    assert sum(1 for r in rows if r["sales_date"] == dt.date(2024, 5, 1)) == 1
    assert sum(1 for r in rows if r["sales_date"] == dt.date(2024, 3, 1)) == 2


def test_failure_logs_error_and_failed_package_end(spark, control, sales_tables):
    t = sales_tables
    bad = RefreshSpec(packageName="AGG_Refresh_DailySalesSummary", targetKey="agg_daily_sales_summary",
                      sql="SELECT * FROM spark_catalog.gold.does_not_exist", replacePredicate=None)
    with pytest.raises(Exception):
        runRefresh(spark, control, "spark_catalog", 44, bad, t["agg_daily_sales_summary"])
    assert control.callsNamed("logPackageEnd")[-1]["status"] == "Failed"
    assert control.callsNamed("logError")


def test_full_overwrite_replaces_everything(spark, control, tables):
    target = tables["agg_customer_360"]
    spark.createDataFrame([(1, "EU"), (2, "NA")], "customer_key int, region_code string") \
        .write.format("delta").mode("overwrite").saveAsTable(target)
    spec = RefreshSpec(packageName="AGG_Refresh_Customer360", targetKey="agg_customer_360",
                       sql="SELECT 3 AS customer_key, 'APAC' AS region_code", replacePredicate=None)
    result = runRefresh(spark, control, "spark_catalog", 45, spec, target)
    assert result.targetRowCount == 1
    assert [r["customer_key"] for r in spark.table(target).collect()] == [3]
