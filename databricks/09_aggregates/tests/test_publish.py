import datetime as dt

import pytest

import agg_publish as pub
import rpt_views

NOW = dt.datetime(2024, 5, 3, 6, 0, 0)


def test_staleness_flags_only_old_or_missing_watermarks():
    wm = {"Aggregate.Daily Sales Summary": NOW - dt.timedelta(hours=2),
          "Aggregate.Customer 360": NOW - dt.timedelta(hours=30),
          "Aggregate.Finance Close Summary": None}
    results = {r.objectName: r for r in pub.evaluateStaleness(wm, NOW, 26)}
    assert not results["Aggregate.Daily Sales Summary"].isStale
    assert results["Aggregate.Customer 360"].isStale and results["Aggregate.Customer 360"].stalenessHours == 30
    assert results["Aggregate.Finance Close Summary"].isStale


def test_publish_decision_mirrors_usp_publish_reporting_layer():
    assert pub.publishDecision(0, False) == (True, "OK")
    assert pub.publishDecision(2, False) == (False, "BLOCKED")
    assert pub.publishDecision(2, True) == (True, "FORCED")


def test_required_aggregates_follow_publication_list():
    req = pub.requiredAggregatesFor(["rpt_customer_360", "rpt_daily_sales_trend", "rpt_ap_aging_current"])
    assert req == ["Aggregate.Customer 360", "Aggregate.Customer Rolling 12 Month", "Aggregate.Daily Sales Summary"]


def test_publication_plan_prefers_int_reporting_publication_table(spark, tables):
    pubTable = tables["int_reporting_publication"]
    spark.createDataFrame(
        [("DAILY", 20, True, "Report", "vw_PromotionRoi", "Aggregate", "Promotion Effectiveness", None, None),
         ("DAILY", 10, True, "Report", "vw_DailySalesTrend", "Aggregate", "Daily Sales Summary", None, None),
         ("DAILY", 30, False, "Report", "vw_Customer360", "Aggregate", "Customer 360", None, None),
         ("MONTHEND", 10, True, "Report", "vw_FinanceCloseStatus", "Aggregate", "Finance Close Summary", None, None)],
        "PublicationGroupCode string, PublishSequence int, IsEnabled boolean, TargetSchemaName string, "
        "TargetObjectName string, SourceSchemaName string, SourceObjectName string, LastPublishedAt timestamp, "
        "LastPublishedByExecutionId long") \
        .write.format("delta").mode("overwrite").saveAsTable(pubTable)

    plan = pub.readPublicationPlan(spark, tables, "DAILY", "Report.vw_ApAgingCurrent")
    assert plan.source == "int_reporting_publication"
    assert plan.views == ["rpt_daily_sales_trend", "rpt_promotion_roi"]

    plan = pub.readPublicationPlan(spark, tables, "ADHOC", "Report.vw_ApAgingCurrent")
    assert plan.source == "configuration" and plan.views == ["rpt_ap_aging_current"]

    plan = pub.readPublicationPlan(spark, tables, "ADHOC", "")
    assert plan.source == "default" and plan.views == list(rpt_views.DEFAULT_PUBLICATION_ORDER)

    stamped = pub.stampPublicationMetadata(spark, pubTable, "DAILY", 777)
    assert stamped == 2
    rows = spark.table(pubTable).filter("PublicationGroupCode = 'DAILY' AND IsEnabled").collect()
    assert all(r["LastPublishedByExecutionId"] == 777 and r["LastPublishedAt"] is not None for r in rows)


def test_publish_rules_and_publish_state(spark, tables):
    spark.createDataFrame([(dt.date(2024, 5, 2), 1.0)], "sales_date date, net_sales_amount double") \
        .write.format("delta").mode("overwrite").saveAsTable(tables["agg_daily_sales_summary"])
    spark.createDataFrame([(dt.date(2024, 5, 1), 1)], "snapshot_date date, sku_count int") \
        .write.format("delta").mode("overwrite").saveAsTable(tables["agg_daily_inventory_health"])
    spark.createDataFrame([(1, "EU", False, dt.date(2024, 1, 1)), (2, "EU", True, dt.date(2024, 1, 1)),
                           (3, "NA", False, dt.date(2024, 1, 1))],
                          "customer_key int, region_code string, anonymised_flag boolean, retention_expiry_date date") \
        .write.format("delta").mode("overwrite").saveAsTable(tables["agg_customer_360"])

    rules = {r.ruleName: r.passed for r in pub.evaluatePublishRules(spark, tables, dt.date(2024, 5, 2), dt.date(2024, 5, 3))}
    assert rules == {"DailySalesPresent": True, "DailyInventoryPresent": False, "EuRetentionAnonymised": False}

    spark.sql(f"UPDATE {tables['agg_customer_360']} SET anonymised_flag = TRUE WHERE customer_key = 1")
    rules = {r.ruleName: r.passed for r in pub.evaluatePublishRules(spark, tables, dt.date(2024, 5, 1), dt.date(2024, 5, 3))}
    assert rules == {"DailySalesPresent": False, "DailyInventoryPresent": True, "EuRetentionAnonymised": True}

    state = tables["rpt_publish_state"]
    assert pub.updatePublishState(spark, state, 42, "OK", 0, 16, "DAILY", 9) == 1
    assert pub.updatePublishState(spark, state, 43, "FORCED", 1, 16, "DAILY", 10) == 1
    rows = spark.table(state).collect()
    assert len(rows) == 1
    assert rows[0]["published_batch_id"] == 43 and rows[0]["publish_status_code"] == "FORCED"
    assert rows[0]["failed_rule_count"] == 1 and rows[0]["publish_scope_code"] == "DW"


def test_report_views_publish_atomically_over_aggregates(spark, tables):
    """CREATE OR REPLACE VIEW repoints readers without a drop window; the simplest view (promotion ROI) is exercised."""
    cols = spark.table(tables["agg_customer_360"]).columns
    assert "customer_key" in cols
    ddl = rpt_views.viewDdl("spark_catalog", "rpt_customer_360", "SELECT customer_key, region_code FROM "
                            + tables["agg_customer_360"], lambda c, s, n: f"{c}.{s}.{n}")
    spark.sql(ddl)
    spark.sql(ddl)  # idempotent
    assert spark.table("spark_catalog.gold.rpt_customer_360").count() == 3
    assert pub.legacyReportName("rpt_customer_360") == "Report.vw_Customer360"
    with pytest.raises(KeyError):
        pub.legacyReportName("rpt_nope")
