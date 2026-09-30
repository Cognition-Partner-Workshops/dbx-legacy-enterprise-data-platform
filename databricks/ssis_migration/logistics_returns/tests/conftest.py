from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession

from logistics_returns.extracts import CREDIT_NOTE_TYPES, RETURN_TYPES, SHIPMENT_LINE_TYPES, SHIPMENT_TYPES, ensureColumns


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("logistics_returns-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def typedFrame(spark: SparkSession, rows: list[dict], columnTypes: dict[str, str]) -> DataFrame:
    """Build a bronze-shaped frame from partial dicts; missing columns become typed NULLs."""
    columns = list(columnTypes)
    data = [[_coerce(row.get(c), columnTypes[c]) for c in columns] for row in rows]
    df = (
        spark.createDataFrame(data, schema=", ".join(f"`{c}` string" for c in columns))
        if rows
        else spark.createDataFrame([], ", ".join(f"`{c}` string" for c in columns))
    )
    return ensureColumns(df, columnTypes)


def _coerce(value, dataType: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date, Decimal)):
        return str(value)
    return str(value)


@pytest.fixture
def bronzeShipments(spark: SparkSession) -> DataFrame:
    rows = [
        {"shipment_id": 800001, "shipment_reference": "SHP-800001", "invoice_id": 1, "customer_id": 1000, "carrier_code": "dhl",
         "service_level_code": "EXP", "shipped_when": "2024-01-10 08:00:00", "promised_delivery_when": "2024-01-12 17:00:00",
         "delivered_when": "2024-01-12 09:30:00", "ship_from_warehouse_code": "US-DAL", "ship_to_country_code": "US",
         "ship_to_postal_code": "75001-1234", "total_weight_kg": "12.500", "total_volume_m3": "0.100", "freight_charge_amount": "45.00",
         "freight_currency_code": "USD", "shipment_status_code": "DELIVERED", "last_edited_when": "2024-01-12 10:00:00", "cross_border_flag": "N"},
        {"shipment_id": 800002, "shipment_reference": "SHP-800002", "invoice_id": 2, "customer_id": 1001, "carrier_code": "DPD",
         "service_level_code": "CHILL", "shipped_when": "2024-01-11 08:00:00", "promised_delivery_when": "2024-01-15 17:00:00",
         "delivered_when": None, "ship_from_warehouse_code": "DE-HAM", "ship_to_country_code": "GB",
         "ship_to_postal_code": "sw1a1aa", "total_weight_kg": "2.000", "total_volume_m3": "0.050", "freight_charge_amount": "20.00",
         "freight_currency_code": "EUR", "customs_declaration_ref": "CD-1", "shipment_status_code": "INTRANSIT", "cross_border_flag": "Y"},
        {"shipment_id": 800003, "shipment_reference": "SHP-800003", "invoice_id": 3, "customer_id": 1002, "carrier_code": None,
         "service_level_code": None, "shipped_when": "2024-01-12 08:00:00", "promised_delivery_when": "2024-01-14 17:00:00",
         "delivered_when": "2024-01-16 09:30:00", "ship_from_warehouse_code": "AU-SYD", "ship_to_country_code": "AU",
         "ship_to_postal_code": "2000", "total_weight_kg": "1.000", "total_volume_m3": "0.010", "freight_charge_amount": "10.00",
         "freight_currency_code": "AUD", "shipment_status_code": None, "cross_border_flag": "N"},
    ]  # fmt: skip
    return typedFrame(
        spark, rows, {**SHIPMENT_TYPES, "cross_border_flag": "string", "transit_hours": "int", "source_system_code": "string"}
    )


@pytest.fixture
def bronzeShipmentLines(spark: SparkSession) -> DataFrame:
    rows = [
        {"shipment_line_id": 1, "shipment_id": 800001, "stock_item_id": 10, "shipped_quantity": "5", "weight_kg": "2.5", "package_type_code": "BOX"},
        {"shipment_line_id": 2, "shipment_id": 800001, "stock_item_id": 11, "shipped_quantity": "1", "weight_kg": "10", "package_type_code": "BOX"},
        {"shipment_line_id": 3, "shipment_id": 800002, "stock_item_id": 12, "shipped_quantity": "0", "weight_kg": "1", "package_type_code": "BOX"},
        {"shipment_line_id": 4, "shipment_id": 800002, "stock_item_id": 13, "shipped_quantity": "2", "weight_kg": "1", "package_type_code": "CHILL",
         "temperature_at_load_c": "12.0"},
    ]  # fmt: skip
    return typedFrame(spark, rows, {**SHIPMENT_LINE_TYPES, "source_system_code": "string"})


@pytest.fixture
def bronzeReturns(spark: SparkSession) -> DataFrame:
    rows = [
        {"return_line_id": 1, "rma_number": "RMA-1", "customer_id": 1000, "stock_item_id": 10, "return_reason_code": "DMG",
         "returned_quantity": "2", "inspection_result_code": "DAMAGED", "refund_amount": "100.00", "currency_code": "USD",
         "returned_when": "2024-02-10 10:00:00", "region_code": None, "original_invoice_id": 1},
        {"return_line_id": 2, "rma_number": None, "customer_id": 1001, "stock_item_id": 11, "return_reason_code": None,
         "returned_quantity": "1", "inspection_result_code": "NEW", "refund_amount": "50.00", "currency_code": "EUR",
         "returned_when": "2024-02-20 10:00:00", "region_code": "EU", "original_invoice_id": 2},
        {"return_line_id": 3, "rma_number": "RMA-3", "customer_id": 1002, "stock_item_id": 12, "return_reason_code": "COM",
         "returned_quantity": "0", "inspection_result_code": "NEW", "refund_amount": "10.00", "currency_code": "AUD",
         "returned_when": "2024-02-21 10:00:00", "region_code": "APAC", "original_invoice_id": 3},
        {"return_line_id": 4, "rma_number": "RMA-4", "customer_id": 1003, "stock_item_id": 13, "return_reason_code": "ZZZ",
         "returned_quantity": "1", "inspection_result_code": "NEW", "refund_amount": "10.00", "currency_code": "AUD",
         "returned_when": "2024-02-22 10:00:00", "region_code": "APAC", "original_invoice_id": 4},
    ]  # fmt: skip
    return typedFrame(spark, rows, {**RETURN_TYPES, "source_system_code": "string"})


@pytest.fixture
def bronzeCreditNotes(spark: SparkSession) -> DataFrame:
    rows = [
        {"credit_note_id": 1, "credit_note_number": "CN-1", "customer_id": 1000, "original_invoice_id": 1, "credit_reason_code": "GOODWILL",
         "credit_note_date": "2024-03-01", "net_amount": "100.00", "tax_amount": "20.00", "currency_code": "USD", "approved_by": None},
        {"credit_note_id": 2, "credit_note_number": "CN-2", "customer_id": 1001, "original_invoice_id": 2, "credit_reason_code": "PRICE",
         "credit_note_date": "2024-03-02", "net_amount": "1000.00", "tax_amount": "200.00", "currency_code": "EUR", "approved_by": None},
        {"credit_note_id": 3, "credit_note_number": "CN-3", "customer_id": 1002, "original_invoice_id": 3, "credit_reason_code": "PRICE",
         "credit_note_date": "2024-03-03", "net_amount": "6000.00", "tax_amount": "600.00", "currency_code": "AUD", "approved_by": "J. Smith"},
        {"credit_note_id": 4, "credit_note_number": "CN-4", "customer_id": 1003, "original_invoice_id": 4, "credit_reason_code": "PRICE",
         "credit_note_date": "2024-03-04", "net_amount": "-5.00", "tax_amount": "0.00", "currency_code": "USD", "approved_by": "J. Smith"},
    ]  # fmt: skip
    return typedFrame(spark, rows, {**CREDIT_NOTE_TYPES, "source_system_code": "string"})
