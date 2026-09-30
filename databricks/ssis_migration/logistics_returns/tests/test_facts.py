from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from logistics_returns.dimensions import legacyFactSaleSchema
from logistics_returns.facts import (
    SCAN_EVENTS_SCHEMA,
    buildCreditNoteFactRows,
    buildOrderFulfilmentRows,
    buildReturnFactRows,
    buildShipmentFactRows,
    mergeOrderFulfilmentSnapshot,
    mergeShipmentSnapshot,
    ordersFromLegacyFactOrder,
    splitCreditNotes,
    summarizeScanEvents,
)
from logistics_returns.staging import standardizeReturn, standardizeShipment


def _byKey(df, key: str) -> dict:
    return {r[key]: r.asDict() for r in df.collect()}


def _stamp(df):
    return (
        df.withColumn("batch_id", F.lit(1).cast("long"))
        .withColumn("load_datetime", F.lit("2024-01-01 00:00:00").cast("timestamp"))
        .withColumn("lineage_key", F.lit(8))
        .withColumn("natural_key_hash", F.lit(0).cast("long"))
    )


def _shipmentFact(spark: SparkSession, bronzeShipments, scans=None):
    staged = standardizeShipment(bronzeShipments).withColumn("freight_charge_amount_usd", F.col("freight_charge_amount"))
    lineCounts = spark.createDataFrame([("WWI_OLTP|800001", 2)], "shipment_business_key string, package_count int")
    scanFrame = scans if scans is not None else spark.createDataFrame([], SCAN_EVENTS_SCHEMA)
    rows = buildShipmentFactRows(staged, lineCounts, summarizeScanEvents(scanFrame))
    rows = (
        rows.withColumn("customer_key", F.lit(0)).withColumn("carrier_key", F.lit(0)).withColumn("warehouse_site_key", F.lit(0))
    )
    rows = (
        rows.withColumn("sales_territory_key", F.lit(-1))
        .withColumn("city_key", F.lit(0))
        .withColumn("inferred_member_flag", F.lit(True))
    )
    return _stamp(
        rows.drop("customer_natural_key", "carrier_natural_key", "warehouse_natural_key", "sales_territory_natural_key")
    )


def test_shipment_fact_measures(spark, bronzeShipments):
    rows = _byKey(_shipmentFact(spark, bronzeShipments), "despatch_note_number")
    delivered = rows["WWI_OLTP|800001"]
    assert delivered["package_count"] == 2 and delivered["despatch_to_delivery_lag_days"] == 2
    assert delivered["on_time_delivery_flag"] is True and delivered["shipment_status_code"] == "DELIVERED"
    assert delivered["chargeable_weight_kg"] == Decimal("16.700")  # 0.1 m3 * 167 > 12.5 kg
    assert delivered["milestone_complete_flag"] is True and delivered["delivery_attempt_count"] == 1
    inTransit = rows["WWI_OLTP|800002"]
    assert inTransit["delivery_confirmed_date_key"] is None and inTransit["on_time_delivery_flag"] is None
    assert inTransit["shipment_status_code"] in ("IN_TRANSIT", "CUSTOMS_HOLD") and inTransit["milestone_complete_flag"] is False
    assert rows["WWI_OLTP|800003"]["on_time_delivery_flag"] is False


def test_shipment_snapshot_merge_never_regresses_milestones(spark, bronzeShipments):
    first = _shipmentFact(spark, bronzeShipments)
    scans = spark.createDataFrame(
        [("SHP-800002", "T1", "ATT", None, "2024-01-13 10:00:00"), ("SHP-800002", "T1", "DLV", None, "2024-01-14 10:00:00"),
         ("SHP-800001", "T2", "EXC", "DAMAGED", "2024-01-13 10:00:00")],
        "shipment_reference string, tracking_number string, scan_event_code string, exception_reason_code string, scan_timestamp_utc string",
    ).withColumn("scan_timestamp_utc", F.col("scan_timestamp_utc").cast("timestamp"))  # fmt: skip
    second = _shipmentFact(spark, bronzeShipments, scans)
    # Simulate a later run where the source lost the delivered timestamp for 800001 (must not regress).
    second = second.withColumn(
        "delivery_confirmed_date_key",
        F.when(F.col("despatch_note_number") == "WWI_OLTP|800001", F.lit(None).cast("date")).otherwise(
            F.col("delivery_confirmed_date_key")
        ),
    ).withColumn("batch_id", F.lit(2).cast("long"))
    merged = _byKey(mergeShipmentSnapshot(first, second), "despatch_note_number")
    assert len(merged) == 3
    kept = merged["WWI_OLTP|800001"]
    assert kept["delivery_confirmed_date_key"] == date(2024, 1, 12) and kept["shipment_status_code"] == "DELIVERED"
    assert kept["damaged_flag"] is True and kept["batch_id"] == 1
    progressed = merged["WWI_OLTP|800002"]
    assert progressed["delivery_confirmed_date_key"] == date(2024, 1, 14) and progressed["shipment_status_code"] == "DELIVERED"
    assert progressed["delivery_attempt_count"] == 2 and progressed["on_time_delivery_flag"] is True
    assert progressed["milestone_complete_flag"] is True
    # merging the same snapshot again is a no-op
    again = mergeShipmentSnapshot(mergeShipmentSnapshot(first, second), second)
    assert again.count() == 3


def _orders(spark: SparkSession):
    return spark.createDataFrame(
        [
            ("O-1", 1, "2024-01-01", "2024-01-01", "2024-01-02", 0, 0, 0, 0, -1, "NA", 2, "USD", "10", "10", "100.00", None, None),
            ("O-2", 2, "2024-01-01", "2024-01-01", None, 0, 0, 0, 0, -1, "EU", 1, "EUR", "5", "0", "50.00", None, None),
            ("O-3", 3, "2024-01-01", "2024-01-01", "2024-01-03", 0, 0, 0, 0, -1, "APAC", 1, "AUD", "5", "5", "50.00", None, None),
        ],
        "order_number string, wwi_order_id long, order_date_key string, allocation_date_key string, pick_date_key string, customer_key int, "
        "stock_item_key int, salesperson_key int, sales_channel_key int, sales_territory_key int, region_code string, order_line_count int, "
        "transaction_currency_code string, quantity_ordered string, quantity_despatched string, order_value_reporting string, "
        "promised_delivery_date_key string, cancellation_date_key string",
    ).select(
        "order_number", "wwi_order_id",
        *[F.col(c).cast("date").alias(c) for c in ("order_date_key", "allocation_date_key", "pick_date_key")],
        "customer_key", "stock_item_key", "salesperson_key", "sales_channel_key", "sales_territory_key", "region_code", "order_line_count",
        "transaction_currency_code",
        F.col("quantity_ordered").cast("decimal(18,3)").alias("quantity_ordered"),
        F.col("quantity_despatched").cast("decimal(18,3)").alias("quantity_despatched"),
        F.col("order_value_reporting").cast("decimal(19,4)").alias("order_value_reporting"),
        F.col("promised_delivery_date_key").cast("date").alias("promised_delivery_date_key"),
        F.col("cancellation_date_key").cast("date").alias("cancellation_date_key"),
    )  # fmt: skip


def test_order_fulfilment_milestones_lags_and_stalled_thresholds(spark):
    orders = _orders(spark)
    shipments = spark.createDataFrame(
        [("O-1", "DN-1", "2024-01-03", "2024-01-05", 3, 4)],
        "order_number string, despatch_note_number string, despatch_date_key string, delivery_confirmed_date_key string, warehouse_site_key int, carrier_key int",
    ).select("order_number", "despatch_note_number", F.col("despatch_date_key").cast("date").alias("despatch_date_key"),
             F.col("delivery_confirmed_date_key").cast("date").alias("delivery_confirmed_date_key"), "warehouse_site_key", "carrier_key")  # fmt: skip
    invoices = spark.createDataFrame(
        [(1, "2024-01-06", "2024-01-05", "INV-1", "10", "100.00")],
        "wwi_order_id long, invoice_date_key string, sale_delivery_date_key string, invoice_number string, quantity_invoiced string, invoiced_value_reporting string",
    ).select("wwi_order_id", F.col("invoice_date_key").cast("date").alias("invoice_date_key"), F.col("sale_delivery_date_key").cast("date").alias("sale_delivery_date_key"),
             "invoice_number", F.col("quantity_invoiced").cast("decimal(18,3)").alias("quantity_invoiced"), F.col("invoiced_value_reporting").cast("decimal(19,4)").alias("invoiced_value_reporting"))  # fmt: skip
    cash = spark.createDataFrame(
        [(1, "2024-01-20", "R-1", "100.00")], "wwi_order_id long, cash_applied_date_key string, receipt_number string, cash_applied_reporting string"
    ).select("wwi_order_id", F.col("cash_applied_date_key").cast("date").alias("cash_applied_date_key"), "receipt_number", F.col("cash_applied_reporting").cast("decimal(19,4)").alias("cash_applied_reporting"))  # fmt: skip

    asOf = F.lit("2024-02-20").cast("date")  # 50 days after the last milestone of O-2 / 48 days for O-3
    rows = _byKey(buildOrderFulfilmentRows(orders, shipments, invoices, cash, asOfDate=asOf), "order_number")
    o1 = rows["O-1"]
    assert o1["pipeline_status_code"] == "CASH" and o1["cycle_complete_flag"] is True and o1["stalled_flag"] is False
    assert o1["order_to_pick_lag_days"] == 1 and o1["pick_to_despatch_lag_days"] == 1 and o1["despatch_to_delivery_lag_days"] == 2
    assert o1["invoice_to_cash_lag_days"] == 14 and o1["order_to_cash_cycle_days"] == 19 and o1["open_milestone_count"] == 0
    assert o1["service_target_days"] == 5 and o1["delivery_sla_breach_flag"] is False and o1["perfect_order_flag"] is True
    o2 = rows["O-2"]  # EU threshold 45 days -> stalled at 50
    assert (
        o2["pipeline_status_code"] == "ALLOCATED"
        and o2["stalled_flag"] is True
        and o2["stalled_reason_code"] == "NO_MILESTONE_45D"
    )
    o3 = rows["O-3"]  # APAC threshold 60 days -> not stalled at 48
    assert o3["pipeline_status_code"] == "DESPATCHED" and o3["stalled_flag"] is False and o3["open_milestone_count"] == 3
    naRows = _byKey(
        buildOrderFulfilmentRows(orders.withColumn("region_code", F.lit("NA")), shipments, invoices, cash, asOfDate=asOf),
        "order_number",
    )
    assert naRows["O-3"]["stalled_flag"] is True and naRows["O-3"]["stalled_reason_code"] == "NO_MILESTONE_30D"


def test_order_fulfilment_snapshot_freezes_closed_rows(spark):
    orders = _orders(spark)
    empty = spark.createDataFrame(
        [],
        "wwi_order_id long, invoice_date_key date, sale_delivery_date_key date, invoice_number string, quantity_invoiced decimal(18,3), invoiced_value_reporting decimal(19,4)",
    )
    noCash = spark.createDataFrame(
        [], "wwi_order_id long, cash_applied_date_key date, receipt_number string, cash_applied_reporting decimal(19,4)"
    )
    noShip = spark.createDataFrame(
        [],
        "order_number string, despatch_note_number string, despatch_date_key date, delivery_confirmed_date_key date, warehouse_site_key int, carrier_key int",
    )
    base = _stamp(buildOrderFulfilmentRows(orders, noShip, empty, noCash, asOfDate=F.lit("2024-01-10").cast("date")))
    closed = base.withColumn(
        "cycle_complete_flag", F.when(F.col("order_number") == "O-1", F.lit(True)).otherwise(F.col("cycle_complete_flag"))
    )
    incoming = _stamp(
        buildOrderFulfilmentRows(orders, noShip, empty, noCash, asOfDate=F.lit("2024-03-10").cast("date"))
    ).withColumn("batch_id", F.lit(2).cast("long"))
    merged = _byKey(mergeOrderFulfilmentSnapshot(closed, incoming), "order_number")
    assert merged["O-1"]["cycle_complete_flag"] is True and merged["O-1"]["batch_id"] == 1  # frozen
    assert merged["O-2"]["batch_id"] == 2 and merged["O-2"]["stalled_flag"] is True  # refreshed
    reopened = _byKey(mergeOrderFulfilmentSnapshot(closed, incoming, reopenClosed=True), "order_number")
    assert reopened["O-1"]["batch_id"] == 2


def test_orders_from_legacy_fact_order_collapses_to_order_grain(spark):
    factOrder = spark.createDataFrame(
        [
            (1, "2013-01-01", "2013-01-02", 0, 40, 10, "100.00"),
            (1, "2013-01-01", None, 0, 40, 5, "50.00"),
            (2, "2013-01-03", None, 0, 41, 1, "1.00"),
        ],
        "`WWI Order ID` int, `Order Date Key` string, `Picked Date Key` string, `Customer Key` int, `Salesperson Key` int, Quantity int, `Total Excluding Tax` string",
    )
    for c in ("Order Number", "Order Status Code", "Transaction Currency Code", "Region Code"):
        factOrder = factOrder.withColumn(c, F.lit(None).cast("string"))
    for c in ("Sales Channel Key", "Sales Territory Key"):
        factOrder = factOrder.withColumn(c, F.lit(None).cast("int"))
    for c in ("Quantity Ordered", "Quantity Despatched", "Net Order Amount Reporting", "Net Order Amount"):
        factOrder = factOrder.withColumn(c, F.lit(None).cast("decimal(19,4)"))
    factOrder = factOrder.withColumn("Promised Delivery Date Key", F.lit(None).cast("date"))
    rows = _byKey(ordersFromLegacyFactOrder(factOrder), "order_number")
    assert rows["1"]["order_line_count"] == 2 and rows["1"]["quantity_ordered"] == Decimal("15.000")
    assert rows["1"]["quantity_despatched"] == Decimal("10.000") and rows["1"]["pick_date_key"] == date(2013, 1, 2)
    assert rows["1"]["order_value_reporting"] == Decimal("150.0000") and rows["2"]["pick_date_key"] is None


def _originalSales(spark: SparkSession):
    rows = [
        (1, date(2024, 1, 1), date(2024, 1, 3), "INV-1", 5, 5, 40, -1, Decimal("10.000"), Decimal("500.0000"), Decimal("50.0000"), Decimal("300.0000"), Decimal("10.000")),
        (2, date(2024, 2, 1), date(2024, 2, 10), "INV-2", 6, 6, 41, -1, Decimal("2.000"), Decimal("100.0000"), Decimal("20.0000"), Decimal("60.0000"), Decimal("20.000")),
    ]  # fmt: skip
    return spark.createDataFrame(rows, legacyFactSaleSchema())


def test_return_fact_negative_measures_windows_and_orphans(spark, bronzeReturns):
    staged = standardizeReturn(bronzeReturns).withColumn(
        "return_reason_code", F.coalesce(F.col("source_return_reason_code"), F.lit("UNSTATED"))
    )
    staged = staged.withColumn("refund_amount_usd", F.col("refund_amount")).withColumn("loaded_at_utc", F.current_timestamp())
    windows = spark.createDataFrame(
        [("COM", "APAC", 21, False)],
        "reason_code string, reason_region_code string, reason_window_days int, reason_is_quality_defect boolean",
    )
    rows = _byKey(buildReturnFactRows(staged, _originalSales(spark), windows), "return_line_business_key")
    na = rows["WWI_OLTP|1"]  # NA, 40 days after invoice -> outside 30-day window, 15 % restocking fee
    assert na["quantity_returned"] == Decimal("-2.000") and na["gross_return_amount"] == Decimal("-100.0000")
    assert na["days_since_invoice"] == 40 and na["statutory_window_days"] == 30 and na["within_statutory_window_flag"] is False
    assert na["restocking_fee_amount"] == Decimal("15.0000") and na["net_credit_amount"] == Decimal("-85.0000")
    assert (
        na["cost_of_returned_goods"] == Decimal("-60.0000")
        and na["disposition_code"] == "SCRAP"
        and na["faulty_goods_flag"] is True
    )
    assert na["original_sale_missing_flag"] is False and na["original_invoice_number"] == "INV-1"
    eu = rows["WWI_OLTP|2"]  # EU counts from delivery (Feb 10) -> 10 days, within 14 -> no fee
    assert (
        eu["days_since_invoice"] == 10
        and eu["within_statutory_window_flag"] is True
        and eu["restocking_fee_amount"] == Decimal("0.0000")
    )
    assert eu["statutory_window_days"] == 14 and eu["disposition_code"] == "RESTOCK"
    apac = rows["WWI_OLTP|3"]  # reason window 21 days from the dimension; original sale missing
    assert (
        apac["statutory_window_days"] == 21 and apac["original_sale_missing_flag"] is True and apac["days_since_invoice"] is None
    )
    assert apac["within_statutory_window_flag"] is None and apac["restocking_fee_amount"] == Decimal("5.0000")
    assert rows["WWI_OLTP|4"]["statutory_window_days"] == 7  # APAC default when reason unknown


def test_credit_note_fact_regional_tax_and_split(spark, bronzeCreditNotes):
    from logistics_returns.staging import standardizeCreditNote

    staged = (
        standardizeCreditNote(bronzeCreditNotes)
        .withColumn("net_amount_usd", F.col("net_amount"))
        .withColumn("loaded_at_utc", F.current_timestamp())
    )
    rows = buildCreditNoteFactRows(staged, _originalSales(spark)).withColumn("customer_key", F.lit(0))
    byNumber = _byKey(rows, "credit_note_number")
    na = byNumber["CN-1"]
    assert na["credit_excluding_tax"] == Decimal("-100.0000") and na["credit_including_tax"] == Decimal("-120.0000")
    assert na["tax_regime_code"] == "SALESTAX" and na["sales_tax_rate"] == Decimal("20.000") and na["vat_rate"] is None
    assert na["goodwill_flag"] is True and na["tax_adjustment_reason_code"] == "SALES_TAX_REFUND" and na["fiscal_period"] == 3
    eu = byNumber["CN-2"]
    assert (
        eu["tax_regime_code"] == "VAT"
        and eu["vat_rate"] == Decimal("20.000")
        and eu["tax_adjustment_reason_code"] == "VAT_CREDIT_NOTE"
    )
    assert eu["requires_second_approval_flag"] is True  # 1200 credit against a 100 net original sale
    apac = byNumber["CN-3"]
    assert (
        apac["tax_regime_code"] == "GST" and apac["gst_rate"] == Decimal("10.000") and apac["original_sale_missing_flag"] is True
    )

    existing = spark.createDataFrame([("CN-1",)], "credit_note_number string")
    returnLinked = spark.createDataFrame([("CN-2",)], "credit_note_number string")
    toLoad, duplicates, linked, held = splitCreditNotes(rows, existing, returnLinked, approvalThreshold=1000.0)
    assert {r["credit_note_number"] for r in duplicates.collect()} == {"CN-1"}
    assert {r["credit_note_number"] for r in linked.collect()} == {"CN-2"}
    assert {r["credit_note_number"] for r in toLoad.collect()} == {
        "CN-3",
        "CN-4",
    }  # approved by name (CN-4 is screened out upstream in staging)
    assert held.count() == 0
    _, _, _, heldNow = splitCreditNotes(
        rows.withColumn("approved_by_name", F.lit(None).cast("string")), existing, returnLinked, approvalThreshold=1000.0
    )
    assert {r["credit_note_number"] for r in heldNow.collect()} == {"CN-3"}
