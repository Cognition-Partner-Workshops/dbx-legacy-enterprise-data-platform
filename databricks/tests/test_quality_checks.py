"""quality.checks: reconciliation, referential, duplicate and amount guards on seeded tables."""

import datetime as dt
from dataclasses import replace
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sales_lakehouse.common.quality import REJECTED_ROWS_TABLE
from sales_lakehouse.quality import checks

BATCH = 4242
SCHEMAS = {"bronze": "qc_bronze", "silver": "qc_silver", "gold": "qc_gold", "quality": "qc_quality"}


def _save(spark, fqn, rows, schema):
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)


@pytest.fixture(scope="module")
def qcCfg(spark, cfg):
    qc = replace(cfg, schemaOverrides=SCHEMAS, batchId=BATCH)
    for layer in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qc.schema(layer)}")
    ts = dt.datetime(2024, 3, 1, 6, 0, 0)
    _save(
        spark,
        qc.fqn("quality", checks.LOAD_LOG_TABLE),
        [
            (BATCH, "Sales.Orders", 10, 9, 1, "OK", ts, ts, ""),
            (BATCH, "Sales.OrderLines", 20, 18, 1, "OK", ts, ts, ""),
            (BATCH, "Sales.Quotes", 0, 0, 0, "MISSING_SOURCE", ts, ts, ""),
            (BATCH - 1, "Sales.Orders", 5, 5, 0, "OK", ts, ts, ""),
        ],
        "batch_id long, source_object string, rows_read long, rows_loaded long, rows_rejected long, status string, "
        "started_at_utc timestamp, finished_at_utc timestamp, message string",
    )
    _save(spark, qc.fqn("bronze", "sqlserver_sales_orders"), [(i, BATCH) for i in range(1, 6)], "OrderID int, _batch_id long")
    _save(
        spark,
        qc.fqn("silver", "order"),
        [(f"WWI_OLTP|{i}", BATCH, True) for i in range(1, 5)] + [("WWI_OLTP|4", BATCH, True)],
        "order_business_key string, batch_id long, is_current boolean",
    )
    _save(
        spark,
        qc.fqn("quality", REJECTED_ROWS_TABLE),
        [("ORDER_MISSING_KEY", "sqlserver_sales_orders", "no key", "{}", BATCH, ts)],
        "rule_code string, source_table string, reason_text string, row_json string, batch_id long, rejected_at_utc timestamp",
    )
    _save(spark, qc.fqn("silver", "dim_customer"), [(1,), (2,), (-1,)], "customer_key long")
    _save(
        spark,
        qc.fqn("gold", "fact_sale"),
        [
            (1, 1, Decimal("1"), Decimal("10"), Decimal("10"), Decimal("1"), Decimal("11"), Decimal("1"), False, BATCH),
            (2, 2, Decimal("1"), Decimal("10"), Decimal("10"), Decimal("1"), Decimal("11"), Decimal("1"), False, BATCH),
            (2, -1, Decimal("1"), Decimal("10"), None, Decimal("1"), Decimal("11"), Decimal("1"), False, BATCH),
            (3, 99, Decimal("-1"), Decimal("10"), Decimal("-10"), Decimal("1"), Decimal("11"), Decimal("1"), False, BATCH),
            (4, 1, Decimal("-1"), Decimal("10"), Decimal("-10"), Decimal("1"), Decimal("11"), Decimal("1"), True, BATCH),
        ],
        "sale_key long, customer_key long, quantity decimal(18,3), unit_price decimal(19,4), net_amount decimal(19,4), "
        "tax_amount decimal(19,4), total_including_tax decimal(19,4), fx_rate_to_reporting decimal(19,8), is_credit_note boolean, "
        "batch_id long",
    )
    return qc


def _by(results, code, table=None, detail=None):
    return [
        r
        for r in results
        if r.checkCode == code and (table is None or r.table.endswith(table)) and (detail is None or r.detail.startswith(detail))
    ]


def test_bronze_reconciliation_uses_current_batch_and_skips_missing_sources(spark, qcCfg):
    results = checks.reconcileBronze(spark, qcCfg)
    assert {r.table for r in results} == {"Sales.Orders", "Sales.OrderLines"}
    orders = _by(results, "RECON_BRONZE", "Sales.Orders")[0]
    lines = _by(results, "RECON_BRONZE", "Sales.OrderLines")[0]
    assert orders.status == checks.STATUS_PASS and (orders.observed, orders.expected) == (10, 10)
    assert lines.status == checks.STATUS_FAIL and (lines.observed, lines.expected) == (19, 20)


def test_silver_reconciliation_counts_loaded_plus_quarantined_against_bronze(spark, qcCfg):
    results = checks.reconcileSilver(spark, qcCfg)
    order = _by(results, "RECON_SILVER", ".order")[0]
    assert (order.observed, order.expected) == (6, 5) and order.status == checks.STATUS_WARN
    assert "rejected=1" in order.detail
    assert all(r.status == checks.STATUS_SKIPPED for r in _by(results, "RECON_SILVER", ".sale"))


def test_referential_integrity_reports_orphans_and_unknown_members(spark, qcCfg):
    results = [r for r in checks.referentialIntegrity(spark, qcCfg) if r.table.endswith("fact_sale")]
    orphans = _by(results, "RI_FACT_DIM", detail="customer_key")[0]
    unknown = _by(results, "RI_UNKNOWN_MEMBER", detail="customer_key")[0]
    notNull = _by(results, "RI_KEY_NOT_NULL", detail="customer_key")[0]
    assert orphans.status == checks.STATUS_FAIL and orphans.observed == 1
    assert unknown.status == checks.STATUS_WARN and unknown.observed == 1
    assert notNull.status == checks.STATUS_PASS


def test_duplicate_keys_on_silver_business_keys_and_gold_surrogates(spark, qcCfg):
    results = checks.duplicateKeys(spark, qcCfg)
    order = _by(results, "DUP_KEY", ".order")[0]
    sale = _by(results, "DUP_KEY", "fact_sale")[0]
    assert order.status == checks.STATUS_FAIL and order.observed == 1
    assert sale.status == checks.STATUS_FAIL and sale.observed == 1


def test_amount_guards_flag_nulls_and_unexempted_negatives(spark, qcCfg):
    results = [r for r in checks.amountGuards(spark, qcCfg) if r.table.endswith("fact_sale")]
    nullNet = _by(results, "AMOUNT_NOT_NULL", detail="net_amount")[0]
    negQty = _by(results, "AMOUNT_NON_NEGATIVE", detail="quantity")[0]
    assert nullNet.status == checks.STATUS_FAIL and nullNet.observed == 1
    assert negQty.status == checks.STATUS_WARN and negQty.observed == 1  # the credit-note row is exempt


def test_run_writes_check_results_with_contract_columns(spark, qcCfg):
    summary = checks.run(spark, qcCfg)
    assert not summary.ok and "fail" in summary.describe() and "check_code" in summary.table()
    written = spark.table(qcCfg.fqn("quality", checks.CHECK_RESULTS_TABLE)).filter(F.col("batch_id") == BATCH)
    assert {"check_code", "table", "status", "observed", "expected", "detail", "batch_id", "run_ts"} <= set(written.columns)
    assert written.count() == len(summary.results)
    assert written.filter(F.col("run_ts").isNull()).count() == 0
