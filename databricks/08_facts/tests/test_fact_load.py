"""End-to-end run of the generic incremental loader (fact_load.run) on local Delta."""
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

import fact_common as fc
import fact_load
from conftest import writeDelta
from dbx_etl_common import control

OPEN_END = datetime(9999, 12, 31, 23, 59, 59)


class Run:
    packageExecutionId = 501
    rowsRead = rowsInserted = rowsUpdated = rowsDeleted = rowsRejected = 0


def spec(**overrides):
    base = dict(
        packageName="FACT_Load_Test", objectName="Fact.Test", targetTable="fact_test", sourceTable="stg_test",
        sourceDateCol="EventDate", sourceTimestampCol="LoadedAtUtc", businessKeyCol="TestBusinessKey",
        naturalKeyCols=("TestBusinessKey",), surrogateKeyCol="test_key", dateKeyCol="event_date_key",
        lookups=[
            fact_load.LookupSpec("Customer", "CustomerBusinessKey", "customer_key", "EventDate", onMiss=fact_load.ON_MISS_INFER),
            fact_load.LookupSpec("Stock Item", "StockItemBusinessKey", "stock_item_key", "EventDate", onMiss=fact_load.ON_MISS_HOLD),
            fact_load.LookupSpec("Promotion", "PromotionCode", "promotion_key", notApplicableWhenNull=True),
        ],
        validation=lambda df: F.col("Amount").isNull(),
        rowVersionCol="LoadedAtUtc",
    )
    base.update(overrides)
    return fact_load.FactLoadSpec(**base)


def transform(spark, catalog, df):
    return df.select(
        "natural_key_hash", "customer_key", "stock_item_key", "promotion_key",
        F.col("EventDate").alias("event_date_key"), F.col("RegionCode").alias("region_code"),
        F.col("Amount").cast("decimal(18,2)").alias("amount"),
    )


def setup(spark, catalog, scd2Dimension, sourceRows):
    for t in ("fact_test", fc.FACT_LOAD_HOLD_TABLE):
        spark.sql("DROP TABLE IF EXISTS %s" % fc.tableName(catalog, "gold", t))
    spark.sql("DROP TABLE IF EXISTS %s" % fc.tableName(catalog, "silver", fc.LATE_ARRIVING_QUEUE_TABLE))
    cust = fc.DIMENSIONS["Customer"]
    scd2Dimension(fc.tableName(catalog, "gold", cust.table), cust, [(10, "C1", "Cust 1", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1)])
    stock = fc.DIMENSIONS["Stock Item"]
    scd2Dimension(fc.tableName(catalog, "gold", stock.table), stock, [(20, "S1", "Item 1", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1)])
    spark.sql("DROP TABLE IF EXISTS %s" % fc.tableName(catalog, "gold", "dim_promotion"))
    writeDelta(spark, fc.tableName(catalog, "silver", "stg_test"), spark.createDataFrame(
        sourceRows, "TestBusinessKey string, CustomerBusinessKey string, StockItemBusinessKey string, PromotionCode string, EventDate date, RegionCode string, Amount decimal(18,2), LoadedAtUtc timestamp"))


def test_generic_loader_infers_holds_rejects_and_merges(spark, catalog, scd2Dimension, jobParams):
    ts = datetime(2024, 3, 14, 12, 0, 0)
    setup(spark, catalog, scd2Dimension, [
        ("T1", "C1", "S1", None, date(2024, 3, 14), "NA", Decimal("10.00"), ts),
        ("T1", "C1", "S1", None, date(2024, 3, 14), "NA", Decimal("11.00"), datetime(2024, 3, 14, 13, 0, 0)),  # newer row version wins
        ("T2", "C9", "S1", "PROMO", date(2024, 3, 14), "NA", Decimal("20.00"), ts),  # unknown customer -> inferred
        ("T3", "C1", "S9", None, date(2024, 3, 14), "NA", Decimal("30.00"), ts),  # unknown stock item -> held
        ("T4", "C1", "S1", None, date(2024, 3, 14), "NA", None, ts),  # invalid -> rejected
    ])
    result = fact_load.run(spark, catalog, jobParams, spec(), Run(), 7, transform)
    fact = fc.tableName(catalog, "gold", "fact_test")
    rows = {r["natural_key_hash"]: r for r in spark.table(fact).collect()}
    byKey = {r["region_code"] + str(r["amount"]): r for r in rows.values()}

    assert result["rowsRead"] == 5 and result["rejected"] == 1 and result["held"] == 1 and result["inferred"] == 1
    assert result["inserted"] == 2 and spark.table(fact).count() == 2
    assert byKey["NA11.00"]["customer_key"] == 10 and byKey["NA11.00"]["stock_item_key"] == 20
    assert byKey["NA11.00"]["promotion_key"] == fc.NOT_APPLICABLE_KEY
    inferredKey = spark.table(fc.tableName(catalog, "gold", "dim_customer")).where("wwi_customer_id = 'C9'").first()
    assert inferredKey["is_inferred_member"] is True and byKey["NA20.00"]["customer_key"] == inferredKey["customer_key"]
    assert byKey["NA20.00"]["promotion_key"] == fc.UNKNOWN_KEY  # promotion dim absent -> ordinary unknown member
    assert set(r["batch_id"] for r in rows.values()) == {7} and set(r["lineage_key"] for r in rows.values()) == {501}

    queue = spark.table(fc.tableName(catalog, "silver", fc.LATE_ARRIVING_QUEUE_TABLE)).collect()
    assert [(q["DimensionName"], q["MissingBusinessKey"], q["OccurrenceCount"]) for q in queue] == [("Customer", "C9", 1)]
    hold = spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).first()
    assert (hold["missing_business_key"], hold["hold_status_code"], hold["max_retry_count"]) == ("S9", fc.HOLD_STATUS_HELD, fc.HOLD_RETRY_LIMITS["NA"])
    assert [r for r in control.rejects if r["rejectReasonCode"] == fc.REJECT_FACT_VALIDATION][0]["count"] == 1
    assert ("setWatermark", "SQLSTG", "Fact.Test", control.watermarks[("SQLSTG", "Fact.Test")][0]) in control.calls
    assert any(rc["objectName"] == "Fact.Test" for rc in control.rowCounts)

    # Re-run: idempotent (no new inserts, stable surrogate keys); the held row is retried, not re-held.
    keysBefore = {k: r["test_key"] for k, r in rows.items()}
    control.watermarks.clear()
    again = fact_load.run(spark, catalog, jobParams, spec(), Run(), 8, transform)
    assert again["inserted"] == 0 and again["held"] == 0 and again["retried"] == 1
    assert {r["natural_key_hash"]: r["test_key"] for r in spark.table(fact).collect()} == keysBefore
    assert spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).first()["retry_count"] == 1

    # The stock item arrives: the held row is released into the fact and the hold closed.
    stock = fc.DIMENSIONS["Stock Item"]
    scd2Dimension(fc.tableName(catalog, "gold", stock.table), stock, [
        (20, "S1", "Item 1", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1),
        (21, "S9", "Item 9", datetime(2020, 1, 1), OPEN_END, True, False, "NA", 1),
    ])
    control.watermarks.clear()
    third = fact_load.run(spark, catalog, jobParams, spec(), Run(), 9, transform)
    assert third["inserted"] == 1 and third["released"] == 1
    released = spark.table(fact).where("amount = 30.00").first()
    assert released["stock_item_key"] == 21 and released["batch_id"] == 9
    assert spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).first()["hold_status_code"] == fc.HOLD_STATUS_RELEASED


def test_held_rows_past_retry_budget_load_against_unknown_member(spark, catalog, scd2Dimension, jobParams):
    ts = datetime(2024, 3, 14, 12, 0, 0)
    setup(spark, catalog, scd2Dimension, [("T3", "C1", "S9", None, date(2024, 3, 14), "NA", Decimal("30.00"), ts)])
    fact = fc.tableName(catalog, "gold", "fact_test")
    # HoldRetryLimit = "runs a line may be held before it is loaded against the unknown member"
    for batch in range(1, fc.HOLD_RETRY_LIMITS["NA"] + 1):
        control.watermarks.clear()
        result = fact_load.run(spark, catalog, jobParams, spec(), Run(), batch, transform)
        assert result["inserted"] == 0 and spark.table(fact).count() == 0
    control.watermarks.clear()
    result = fact_load.run(spark, catalog, jobParams, spec(), Run(), fc.HOLD_RETRY_LIMITS["NA"] + 1, transform)
    assert result["inserted"] == 1
    row = spark.table(fact).first()
    assert row["stock_item_key"] == fc.UNKNOWN_KEY
    hold = spark.table(fc.tableName(catalog, "gold", fc.FACT_LOAD_HOLD_TABLE)).first()
    assert hold["hold_status_code"] == fc.HOLD_STATUS_RELEASED
