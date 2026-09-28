import datetime as dt
import json

import agg_reconcile as rec


def test_baseline_json_and_compare_tolerances():
    baseline = rec.baselineFromJson(json.dumps({"objects": [
        {"objectName": "Aggregate.Daily Sales Summary", "period": "2024-05-01", "rowCount": 10, "rowHash": 123,
         "measures": {"net_sales_amount": 1000.0}},
        {"objectName": "Aggregate.Daily Sales Summary", "period": "*", "rowCount": 10,
         "measures": {"net_sales_amount": 1000.0}},
    ]}))
    actuals = [rec.Summary("Aggregate.Daily Sales Summary", "2024-05-01", 10, 123, {"net_sales_amount": 1000.5}),
               rec.Summary("Aggregate.Daily Sales Summary", "*", 11, 999, {"net_sales_amount": 1000.5}),
               rec.Summary("Aggregate.Customer 360", "*", 5, 1, {})]
    strict = {(c.period, c.metric): c.withinTolerance for c in rec.compare(actuals, baseline)}
    assert strict == {("2024-05-01", "row_count"): True, ("2024-05-01", "row_hash"): True,
                      ("2024-05-01", "net_sales_amount"): False, ("*", "row_count"): False,
                      ("*", "net_sales_amount"): False}
    loose = {(c.period, c.metric): c.withinTolerance
             for c in rec.compare(actuals, baseline, absoluteTolerance=1, percentTolerance=0)}
    assert all(loose.values())
    pct = {(c.period, c.metric): c.withinTolerance
           for c in rec.compare(actuals, baseline, absoluteTolerance=0, percentTolerance=0.1)}
    assert pct[("2024-05-01", "net_sales_amount")] and not pct[("*", "row_count")]
    assert rec.missingBaselines(actuals[2:], baseline) == ["Aggregate.Daily Sales Summary"]


def test_summarise_is_order_independent_and_ignores_refresh_stamps(spark, tables):
    target = tables["agg_daily_inventory_health"]
    schema = ("snapshot_date date, warehouse_site_key int, sku_count int, total_quantity_on_hand double, "
              "refresh_batch_id long, refreshed_datetime timestamp")
    rowsA = [(dt.date(2024, 5, 1), 1, 5, 10.0, 1, dt.datetime(2024, 5, 2)),
             (dt.date(2024, 5, 1), 2, 7, 20.0, 1, dt.datetime(2024, 5, 2)),
             (dt.date(2024, 5, 2), 1, 5, 30.0, 1, dt.datetime(2024, 5, 3))]
    spark.createDataFrame(rowsA, schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    spec = next(s for s in rec.RECONCILIATION_SPECS if s.targetKey == "agg_daily_inventory_health")
    first = {s.period: s for s in rec.summarise(spark, target, spec)}
    assert first["2024-05-01"].rowCount == 2 and first["*"].rowCount == 3
    assert first["2024-05-01"].measures["total_quantity_on_hand"] == 30.0
    assert first["*"].measures["sku_count"] == 17.0

    rowsB = [(d, w, s, q, 99, dt.datetime(2030, 1, 1)) for (d, w, s, q, _, _) in reversed(rowsA)]
    spark.createDataFrame(rowsB, schema).write.format("delta").mode("overwrite").saveAsTable(target)
    second = {s.period: s for s in rec.summarise(spark, target, spec)}
    assert second["*"].rowHash == first["*"].rowHash
    assert second["2024-05-01"].rowHash == first["2024-05-01"].rowHash

    rowsC = rowsB[:-1] + [(dt.date(2024, 5, 1), 1, 5, 11.0, 99, dt.datetime(2030, 1, 1))]
    spark.createDataFrame(rowsC, schema).write.format("delta").mode("overwrite").saveAsTable(target)
    third = {s.period: s for s in rec.summarise(spark, target, spec)}
    assert third["*"].rowHash != first["*"].rowHash


def test_baseline_from_delta_table(spark):
    table = "spark_catalog.etl.agg_reconciliation_baseline"
    spark.createDataFrame(
        [("Aggregate.Customer 360", None, 5, 77, "lifetime_net_revenue", 100.0),
         ("Aggregate.Customer 360", None, 5, 77, "lifetime_order_count", 9.0),
         ("Aggregate.Daily Sales Summary", "2024-05-01", 3, None, None, None)],
        "ObjectName string, PeriodValue string, RowCount long, RowHash long, MeasureName string, MeasureValue double") \
        .write.format("delta").mode("overwrite").saveAsTable(table)
    b = rec.baselineFromTable(spark, table)
    assert b[("Aggregate.Customer 360", "*")].measures == {"lifetime_net_revenue": 100.0, "lifetime_order_count": 9.0}
    assert b[("Aggregate.Daily Sales Summary", "2024-05-01")].rowCount == 3
    assert rec.baselineFromTable(spark, "spark_catalog.etl.nope") == {}
