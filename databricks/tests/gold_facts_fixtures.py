"""Small silver / bronze inputs following CONVENTIONS.md, used by the test_gold_facts_* modules."""

from __future__ import annotations

import dataclasses
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.common.tables import overwriteTable

D = Decimal
T0 = datetime(2024, 3, 16, 10, 0, 0)
T_OLD = datetime(2024, 3, 15, 10, 0, 0)


def isolatedConfig(spark: SparkSession, cfg: PipelineConfig, tag: str) -> PipelineConfig:
    overrides = {layer: f"gf_{tag}_{layer}" for layer in ("bronze", "silver", "gold", "quality")}
    iso = dataclasses.replace(cfg, schemaOverrides=overrides)
    # many small Delta MERGEs in one local driver: broadcast hash joins exhaust the 1g heap
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.catalog.clearCache()
    for layer in overrides:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {iso.schema(layer)}")
    return iso


def _pyType(value: object) -> DataType:
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()
    if isinstance(value, Decimal):
        return DecimalType(19, 8)
    if isinstance(value, datetime):
        return TimestampType()
    if isinstance(value, date):
        return DateType()
    return StringType()


def _schema(rows: list[dict]) -> StructType:
    fields = []
    for name in rows[0]:
        sample = next((r[name] for r in rows if r.get(name) is not None), None)
        fields.append(StructField(name, _pyType(sample), True))
    return StructType(fields)


def _write(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str, rows: list[dict]) -> None:
    df = spark.createDataFrame([tuple(r[f.name] for f in _schema(rows)) for r in rows], _schema(rows))
    overwriteTable(df, cfg.fqn(layer, table))


def rejected(spark: SparkSession, cfg: PipelineConfig, ruleCode: str) -> DataFrame:
    return spark.table(cfg.fqn("quality", "rejected_rows")).filter(F.col("rule_code") == ruleCode)


def fxRate(ccy: str, rateDate: date, rateToUsd: str, rateType: str, source: str, region: str) -> dict:
    """One ``silver.ref_fx_rate`` row in the shape ``silver.reference.buildRefFxRate`` writes."""
    return {
        "currency_code": ccy,
        "rate_date": rateDate,
        "effective_date": rateDate,
        "rate_to_usd": D(rateToUsd),
        "rate_source_code": source,
        "region_code": region,
        "rate_type_code": rateType,
        "reporting_currency_code": "USD",
        "is_interpolated": False,
        "derivation_code": "DIRECT",
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def seedDimensions(spark: SparkSession, cfg: PipelineConfig) -> None:
    _write(
        spark,
        cfg,
        "silver",
        "dim_customer",
        [
            {
                "customer_key": 10,
                "customer_business_key": "C1",
                "valid_from": date(2024, 1, 1),
                "valid_to": date(2024, 6, 1),
                "is_current": False,
            },
            {
                "customer_key": 11,
                "customer_business_key": "C1",
                "valid_from": date(2024, 6, 1),
                "valid_to": None,
                "is_current": True,
            },
            {
                "customer_key": 20,
                "customer_business_key": "C2",
                "valid_from": date(2020, 1, 1),
                "valid_to": None,
                "is_current": True,
            },
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "dim_salesperson",
        [
            {"salesperson_key": 100, "salesperson_business_key": "SP1", "is_current": True},
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "ref_fx_rate",
        [
            fxRate("EUR", date(2024, 1, 1), "1.10000000", "ECB", "ECB", "EU"),
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "dim_fiscal_calendar",
        [
            {
                "calendar_code": "NA445",
                "fiscal_year": 2024,
                "fiscal_period": 3,
                "period_start": date(2024, 1, 1),
                "period_end": date(2024, 12, 31),
            },
            {
                "calendar_code": "EUCAL",
                "fiscal_year": 2024,
                "fiscal_period": 7,
                "period_start": date(2024, 1, 1),
                "period_end": date(2024, 12, 31),
            },
        ],
    )


def _sale(
    key: str, inv: int, order: str, cust: str, d: date, region: str, ccy: str, delivered: datetime | None
) -> dict:
    return {
        "sale_business_key": key,
        "source_invoice_id": str(inv),
        "order_business_key": order,
        "customer_business_key": cust,
        "bill_to_customer_business_key": cust,
        "salesperson_business_key": "SP1",
        "invoice_date": d,
        "confirmed_delivery_utc": delivered,
        "is_credit_note": False,
        "currency_code": ccy,
        "region_code": region,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def _saleLine(
    key: str,
    sale: str,
    ln: int,
    item: str,
    qty: str,
    price: str,
    net: str,
    rate: str,
    tax: str,
    profit: str,
    d: date,
    region: str,
    ccy: str,
    loaded: datetime,
) -> dict:
    return {
        "sale_line_business_key": key,
        "sale_business_key": sale,
        "line_number": ln,
        "stock_item_business_key": item,
        "product_business_key": item,
        "line_description": f"Item {item}",
        "quantity": D(qty),
        "uom_code": "EA",
        "quantity_base_uom": D(qty),
        "unit_price_amount_local": D(price),
        "net_line_amount_local": D(net),
        "tax_regime_code": region,
        "tax_rate_percent": D(rate),
        "tax_amount_local": D(tax),
        "gross_line_amount_local": D(net) + D(tax),
        "line_profit_amount_local": D(profit),
        "is_reverse_charge": False,
        "vat_registration_number": None,
        "currency_code": ccy,
        "promotion_business_key": None,
        "invoice_date": d,
        "region_code": region,
        "batch_id": 1,
        "loaded_at_utc": loaded,
    }


def seedSales(spark: SparkSession, cfg: PipelineConfig) -> None:
    _write(
        spark,
        cfg,
        "silver",
        "sale",
        [
            _sale("INV1", 1, "ORD1", "C1", date(2024, 3, 15), "NA", "USD", datetime(2024, 3, 17)),
            _sale("INV2", 2, "ORD2", "C1", date(2024, 7, 10), "EU", "EUR", None),
            _sale("INV3", 3, "ORD3", "C9", date(2024, 5, 1), "APAC", "AUD", None),
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "sale_line",
        [
            _saleLine("INV1-1", "INV1", 1, "S1", "10", "10", "100", "8", "8", "40", date(2024, 3, 15), "NA", "USD", T0),
            _saleLine(
                "INV1-1", "INV1", 1, "S1", "10", "10", "99", "8", "8", "40", date(2024, 3, 15), "NA", "USD", T_OLD
            ),
            _saleLine(
                "INV2-1", "INV2", 1, "S2", "2", "50", "100", "20", "20", "30", date(2024, 7, 10), "EU", "EUR", T0
            ),
            _saleLine(
                "INV3-1", "INV3", 1, "S1", "1", "110", "110", "10", "10", "50", date(2024, 5, 1), "APAC", "AUD", T0
            ),
        ],
    )


def _order(key: str, cust: str, d: date, region: str, status: str, flags: str | None) -> dict:
    return {
        "order_business_key": key,
        "source_order_id": key[-1],
        "customer_business_key": cust,
        "salesperson_business_key": "SP1",
        "backorder_order_business_key": None,
        "order_date": d,
        "expected_delivery_date": date(d.year, d.month, min(d.day + 5, 28)),
        "customer_purchase_order_number": f"PO-{key}",
        "is_undersupply_backordered": False,
        "sales_channel_code": "WEB",
        "sales_territory_code": "T1",
        "order_status_code": status,
        "currency_code": "USD",
        "region_code": region,
        "fulfilment_flags": flags,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def _orderLine(
    key: str, order: str, ln: int, item: str, qty: str, picked: str, price: str, d: date, pickedWhen: datetime | None
) -> dict:
    return {
        "order_line_business_key": key,
        "order_business_key": order,
        "line_number": ln,
        "stock_item_business_key": item,
        "product_business_key": item,
        "line_description": f"Item {item}",
        "package_type_code": "EA",
        "ordered_quantity": D(qty),
        "picked_quantity": D(picked),
        "unit_price_amount_local": D(price),
        "line_discount_amount_local": D("0"),
        "net_line_amount_local": D(qty) * D(price),
        "tax_rate_percent": D("0"),
        "currency_code": "USD",
        "promotion_business_key": None,
        "picking_completed_when_utc": pickedWhen,
        "line_status_code": "OPEN",
        "order_date": d,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def seedOrders(spark: SparkSession, cfg: PipelineConfig) -> None:
    _write(
        spark,
        cfg,
        "silver",
        "order",
        [
            _order("ORD1", "C1", date(2024, 3, 10), "NA", "OPEN", "H|B"),
            _order("ORD2", "C1", date(2024, 7, 1), "EU", "OPEN", None),
            _order("ORD3", "C9", date(2024, 4, 20), "APAC", "OPEN", None),
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "order_line",
        [
            _orderLine("ORD1-1", "ORD1", 1, "S1", "10", "10", "10", date(2024, 3, 10), datetime(2024, 3, 12)),
            _orderLine("ORD1-2", "ORD1", 2, "S2", "5", "2", "20", date(2024, 3, 10), None),
            _orderLine("ORD2-1", "ORD2", 1, "S2", "2", "2", "50", date(2024, 7, 1), datetime(2024, 7, 3)),
            _orderLine("ORD3-1", "ORD3", 1, "S1", "0", "0", "110", date(2024, 4, 20), None),
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "backorder",
        [
            {
                "backorder_business_key": "BO1",
                "order_business_key": "ORD1",
                "order_line_business_key": "ORD1-2",
                "stock_item_business_key": "S2",
                "quantity_short": D("3"),
                "shortage_reason_code": "NOSTOCK",
                "promised_date": date(2024, 3, 25),
                "backorder_status": "OPEN",
                "closed_when_utc": None,
            },
        ],
    )
    _write(
        spark,
        cfg,
        "silver",
        "order_hold",
        [
            {
                "order_hold_business_key": "H1",
                "order_business_key": "ORD2",
                "hold_type_code": "CREDIT",
                "hold_reason_code": "LIMIT",
                "placed_when_utc": T0,
                "released_when_utc": None,
                "is_blocking_despatch": True,
            },
        ],
    )


def payment(key: str, cust: str, amount: str, d: date, region: str, status: str) -> dict:
    return {
        "payment_business_key": key,
        "payment_reference": f"REF-{key}",
        "customer_business_key": cust,
        "received_when_utc": datetime(d.year, d.month, d.day, 9),
        "value_date": d,
        "payment_method_code": "EFT",
        "currency_code": "USD",
        "payment_amount_local": D(amount),
        "exchange_rate_to_usd": None,
        "bank_charge_amount_local": D("0"),
        "source_allocated_amount_local": None,
        "bank_statement_ref": None,
        "payment_status_code": status,
        "region_code": region,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def allocation(key: str, payment: str, sale: str | None, amount: str, d: date, creditNote: str | None = None) -> dict:
    return {
        "payment_allocation_business_key": key,
        "payment_business_key": payment,
        "allocated_when_utc": datetime(d.year, d.month, d.day, 12),
        "target_type_code": "INV" if sale else "CN",
        "sale_business_key": sale,
        "credit_note_business_key": creditNote,
        "allocated_amount_local": D(amount),
        "settlement_discount_amount": D("0"),
        "exchange_difference_amount": D("0"),
        "match_method_code": "AUTO",
        "reversal_of_allocation_business_key": None,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def seedPayments(
    spark: SparkSession,
    cfg: PipelineConfig,
    allocations: list[dict] | None = None,
    extraPayments: list[dict] | None = None,
) -> None:
    _write(
        spark,
        cfg,
        "silver",
        "payment",
        [
            payment("PAY1", "C1", "100", date(2024, 3, 20), "NA", "APPLIED"),
            payment("PAY2", "C1", "50", date(2024, 3, 21), "NA", "APPLIED"),
            payment("PAY3", "C2", "30", date(2024, 3, 22), "EU", "UNMATCHED"),
        ]
        + (extraPayments or []),
    )
    _write(spark, cfg, "silver", "payment_allocation", allocations or defaultAllocations())


def defaultAllocations() -> list[dict]:
    return [
        allocation("A1", "PAY1", "INV1", "60", date(2024, 3, 20)),
        allocation("A2", "PAY1", "INV1", "20", date(2024, 3, 21)),
        allocation("A3", "PAY2", "INV1", "70", date(2024, 3, 21)),
    ]


def seedReturns(spark: SparkSession, cfg: PipelineConfig, processedDate: date | None = None) -> None:
    _write(
        spark,
        cfg,
        "silver",
        "return",
        [
            {
                "return_line_business_key": "RET1-1",
                "rma_number": "RMA1",
                "sale_line_business_key": "INV1-1",
                "customer_business_key": "C1",
                "stock_item_business_key": "S1",
                "return_reason_code": "DAMAGED",
                "return_reason_group_code": "FAULTY",
                "returned_quantity": D("2"),
                "restocked_quantity": D("0"),
                "scrapped_quantity": D("2"),
                "inspection_result_code": "SCRAP",
                "restocking_fee_amount": D("0"),
                "refund_amount": D("21.6"),
                "transaction_currency_code": "USD",
                "returned_date": date(2024, 3, 25),
                "processed_date": processedDate,
                "region_code": "NA",
                "batch_id": 1,
                "loaded_at_utc": T0,
            },
        ],
    )


def creditNote(
    key: str,
    cust: str,
    sale: str,
    rma: str | None,
    net: str,
    tax: str,
    d: date,
    approved: str | None,
    region: str,
    ccy: str,
) -> dict:
    return {
        "credit_note_business_key": key,
        "credit_note_number": key,
        "customer_business_key": cust,
        "original_sale_business_key": sale,
        "rma_number": rma,
        "credit_reason_code": "PRICING",
        "credit_note_date": d,
        "net_amount": D(net),
        "tax_amount": D(tax),
        "gross_amount": D(net) + D(tax),
        "transaction_currency_code": ccy,
        "applied_to_sale_business_key": None,
        "approved_by_name": approved,
        "credit_status_code": "ISSUED",
        "vat_credit_note_required_flag": region == "EU",
        "region_code": region,
        "batch_id": 1,
        "loaded_at_utc": T0,
    }


def seedCreditNotes(spark: SparkSession, cfg: PipelineConfig, extra: list[dict] | None = None) -> None:
    rows = [
        creditNote("CN2", "C1", "INV2", None, "20", "4", date(2024, 7, 20), "Approver", "EU", "EUR"),
        creditNote("CN3", "C1", "INV2", None, "5000", "1000", date(2024, 7, 21), None, "EU", "EUR"),
    ] + (extra or [])
    _write(spark, cfg, "silver", "credit_note", rows)


def seedProductMaster(spark: SparkSession, cfg: PipelineConfig, s1Cost: str = "4") -> None:
    _write(
        spark,
        cfg,
        "bronze",
        "oracle_wwi_mdm_product_master",
        [
            {
                "PRODUCT_ID": 1,
                "ITEM_NBR": "S1",
                "WWI_STOCK_ITEM_ID": 1,
                "UNIT_COST_STD": D(s1Cost),
                "COST_CURR_CD": "USD",
                "DELETED_FLG": "N",
            },
        ],
    )


def seedAll(spark: SparkSession, cfg: PipelineConfig) -> None:
    seedDimensions(spark, cfg)
    seedSales(spark, cfg)
    seedOrders(spark, cfg)
    seedPayments(spark, cfg)
    seedReturns(spark, cfg)
    seedCreditNotes(spark, cfg)
    seedProductMaster(spark, cfg)
