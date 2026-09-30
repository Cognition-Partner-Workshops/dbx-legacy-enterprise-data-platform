from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from logistics_returns.reference import FALLBACK_RETURN_REASON_CROSSWALK, convertToUsd
from logistics_returns.staging import (
    applyCarrierLookup,
    attachLatestScan,
    crosswalkReturnReason,
    latestScanInputSchema,
    screenCreditNotes,
    screenReturns,
    standardizeCreditNote,
    standardizeReturn,
    standardizeShipment,
    standardizeShipmentLine,
    withinStatutoryWindow,
)


def _crosswalk(spark: SparkSession):
    return spark.createDataFrame(
        [
            Row(source_code_value=s, conformed_code_value=c, return_reason_group_code=g)
            for s, c, g in FALLBACK_RETURN_REASON_CROSSWALK
        ]
        + [Row(source_code_value="UNSTATED", conformed_code_value="UNSTATED", return_reason_group_code="OTHER")]
    )


def _byKey(df, key: str) -> dict:
    return {r[key]: r.asDict() for r in df.collect()}


def test_standardize_shipment_defaults_and_postal_rules(bronzeShipments):
    rows = _byKey(standardizeShipment(bronzeShipments), "shipment_id")
    us, gb, au = rows[800001], rows[800002], rows[800003]
    assert us["shipment_business_key"] == "WWI_OLTP|800001"
    assert us["carrier_code"] == "DHL" and us["ship_to_postal_code_standardized"] == "75001"
    assert us["on_time_delivery_flag"] == "Y" and us["region_code"] == "NA"
    assert gb["ship_to_postal_code_standardized"] == "SW1A 1AA" and gb["customs_required_flag"] == "Y"
    assert gb["on_time_delivery_flag"] is None and gb["region_code"] == "EU"
    assert au["carrier_code"] == "UNKN" and au["service_level_code"] == "STD"
    assert au["on_time_delivery_flag"] == "N" and au["shipment_status_code"] == "DELIVERED" and au["region_code"] == "APAC"


def test_carrier_lookup_strict_vs_lenient(spark, bronzeShipments):
    carriers = spark.createDataFrame(
        [("DHL", "DHL", "NA", None, "96.0")],
        "carrier_code string, carrier_name string, carrier_region_code string, service_level_list string, on_time_target_percent string",
    ).withColumn("on_time_target_percent", F.col("on_time_target_percent").cast("decimal(9,2)"))
    shipments = standardizeShipment(bronzeShipments)
    lenient, unmatched = applyCarrierLookup(shipments, carriers)
    assert lenient.count() == 3 and unmatched.count() == 2
    assert {r["dq_status_code"] for r in lenient.collect()} == {"OK", "CARRIER_UNMATCHED"}
    strict, _ = applyCarrierLookup(shipments, carriers, strict=True)
    assert strict.count() == 1


def test_latest_scan_updates_status(spark, bronzeShipments):
    scans = spark.createDataFrame(
        [
            ("SHP-800002", "T1", "ATT", "2024-01-13 10:00:00", 1),
            ("SHP-800002", "T1", "DLV", "2024-01-14 10:00:00", 2),
            ("SHP-800001", "T2", "LST", "2024-01-20 10:00:00", 3),
        ],
        "shipment_reference string, tracking_number string, scan_event_code string, scan_timestamp_utc string, "
        "source_row_number long",
    ).withColumn("scan_timestamp_utc", F.col("scan_timestamp_utc").cast("timestamp"))
    assert set(latestScanInputSchema().fieldNames()) <= set(scans.columns)
    rows = _byKey(attachLatestScan(standardizeShipment(bronzeShipments), scans), "shipment_id")
    assert rows[800002]["shipment_status_code"] == "DELIVERED" and rows[800002]["last_scan_event_code"] == "DLV"
    assert rows[800001]["shipment_status_code"] == "LOST"
    assert rows[800003]["last_scan_event_code"] is None


def test_shipment_line_constraints_and_cold_chain(bronzeShipments, bronzeShipmentLines):
    shipments = standardizeShipment(bronzeShipments)
    rows = _byKey(standardizeShipmentLine(bronzeShipmentLines, shipments), "shipment_line_id")
    assert rows[1]["dq_status_code"] == "OK" and rows[1]["shipment_business_key"] == "WWI_OLTP|800001"
    assert rows[3]["dq_status_code"] != "OK"  # zero quantity
    assert rows[4]["cold_chain_breach_flag"] == "Y" and rows[3]["cold_chain_breach_flag"] == "N"


def test_convert_to_usd_latest_rate_and_passthrough(spark):
    fx = spark.createDataFrame(
        [("EUR", "2024-01-01", "1.10"), ("EUR", "2024-01-10", "1.20"), ("EUR", "2024-02-01", "1.30")],
        "from_currency_code string, rate_date string, conversion_rate string",
    ).select(
        "from_currency_code",
        F.col("rate_date").cast("date").alias("rate_date"),
        F.col("conversion_rate").cast("decimal(18,8)").alias("conversion_rate"),
    )
    df = spark.createDataFrame(
        [
            (1, "EUR", "2024-01-15", "100.00"),
            (2, "USD", "2024-01-15", "100.00"),
            (3, "EUR", "2024-01-15", "100.00"),
            (4, "GBP", "2024-01-15", "1.00"),
        ],
        "id int, ccy string, d string, amt string",
    ).select("id", "ccy", F.col("d").cast("date").alias("d"), F.col("amt").cast("decimal(19,4)").alias("amt"))
    out = _byKey(convertToUsd(df, fx, "amt", "ccy", "d", "amt_usd"), "id")
    assert len(out) == 4  # identical rows 1 and 3 both survive
    assert out[1]["amt_usd"] == Decimal("120.0000") and out[2]["amt_usd"] == Decimal("100.0000") and out[4]["amt_usd"] is None


def test_return_rules_region_default_reason_default_and_windows(spark, bronzeReturns):
    std = standardizeReturn(bronzeReturns)
    rows = _byKey(std, "return_line_id")
    assert rows[1]["region_code"] == "NA" and rows[1]["return_window_days"] == 30
    assert rows[2]["source_return_reason_code"] == "UNSTATED" and rows[2]["return_window_days"] == 14
    assert rows[3]["return_window_days"] == 7
    matched, unmatched = crosswalkReturnReason(std, _crosswalk(spark))
    assert {r["return_line_id"] for r in unmatched.collect()} == {4}
    assert _byKey(matched, "return_line_id")[1]["return_reason_code"] == "DAMTR"
    good, bad = screenReturns(matched)
    assert {r["return_line_id"] for r in bad.collect()} == {3}
    flagged = withinStatutoryWindow(good.withColumn("days_since_sale", F.when(F.col("return_line_id") == 1, F.lit(31))))
    flags = _byKey(flagged, "return_line_id")
    assert flags[1]["within_statutory_window_flag"] == "N" and flags[2]["within_statutory_window_flag"] is None


def test_credit_note_bands_and_approval(bronzeCreditNotes):
    rows = _byKey(standardizeCreditNote(bronzeCreditNotes), "credit_note_id")
    assert (
        rows[1]["approval_band"] == "AUTO" and rows[1]["approved_flag"] == "Y" and rows[1]["gross_amount"] == Decimal("120.0000")
    )
    assert (
        rows[2]["approval_band"] == "MGR" and rows[2]["approved_flag"] == "N" and rows[2]["vat_credit_note_required_flag"] == "Y"
    )
    assert rows[3]["approval_band"] == "FIN" and rows[3]["approved_flag"] == "Y" and rows[3]["region_code"] == "APAC"
    good, unapproved, nonPositive = screenCreditNotes(standardizeCreditNote(bronzeCreditNotes))
    assert {r["credit_note_id"] for r in good.collect()} == {1, 3}
    assert {r["credit_note_id"] for r in unapproved.collect()} == {2}
    assert {r["credit_note_id"] for r in nonPositive.collect()} == {4}
    legacy = _byKey(standardizeCreditNote(bronzeCreditNotes, autoApproveBand=False), "credit_note_id")
    assert legacy[1]["approved_flag"] == "N"
