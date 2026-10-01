"""quality.validate_mock: expectation derivation from the mock CSVs / manifest."""

import datetime as dt
import json
import os
from dataclasses import replace
from decimal import Decimal

from sales_lakehouse.common.quality import REJECTED_ROWS_TABLE
from sales_lakehouse.quality import validate_mock as vm


def _fx(spark):
    rows = [
        ("CAD", "USD", "2024-03-01", "CORP", "0.74", "TREASURY", "NA", "N"),
        ("CAD", "USD", "2024-03-08", "CORP", "0.75", "TREASURY", "NA", "N"),
        ("CAD", "USD", "2024-03-09", "CORP", "9.99", "TREASURY", "NA", "Y"),
        ("EUR", "USD", "2024-01-15", "ECB", "1.08", "ECB", "EU", "N"),
        ("AUD", "USD", "2024-02-28", "BANK", "0.65", "RBA", "APAC", "N"),
        ("AUD", "USD", "2024-03-20", "BANK", "0.70", "RBA", "APAC", "N"),
    ]
    return spark.createDataFrame(
        rows,
        "FROM_CURR_CD string, TO_CURR_CD string, RATE_DT string, RATE_TYPE_CD string, RATE string, RATE_SOURCE_CD string, "
        "FEED_REGION_CD string, SUPERSEDED_FLG string",
    )


def test_expected_fx_rates_follow_regional_windows(spark):
    keys = spark.createDataFrame(
        [
            ("CAD", "NA", dt.date(2024, 3, 9)),  # superseded 03-09 ignored -> 03-08 within 7 days
            ("CAD", "NA", dt.date(2024, 3, 20)),  # 03-08 is 12 days back -> outside the NA window -> default
            ("EUR", "EU", dt.date(2024, 6, 30)),  # EU: no window, 01-15 still applies
            ("AUD", "APAC", dt.date(2024, 3, 25)),  # APAC anchors on 03-01 -> 02-28 quote, not 03-20
            ("USD", "NA", dt.date(2024, 3, 25)),
            ("JPY", "APAC", dt.date(2024, 3, 25)),  # no quotes at all
        ],
        "ccy string, region string, txn_date date",
    )
    out = {(r["ccy"], r["txn_date"]): r for r in vm.expectedFxRates(_fx(spark), keys, "USD").collect()}
    assert out[("CAD", dt.date(2024, 3, 9))]["expected_fx_rate"] == Decimal("0.75000000")
    assert out[("CAD", dt.date(2024, 3, 9))]["expected_fx_effective_date"] == dt.date(2024, 3, 8)
    assert out[("CAD", dt.date(2024, 3, 20))]["expected_fx_default"] is True
    assert out[("CAD", dt.date(2024, 3, 20))]["expected_fx_rate"] == Decimal("1.00000000")
    assert out[("EUR", dt.date(2024, 6, 30))]["expected_fx_rate"] == Decimal("1.08000000")
    assert out[("AUD", dt.date(2024, 3, 25))]["expected_fx_rate"] == Decimal("0.65000000")
    assert out[("AUD", dt.date(2024, 3, 25))]["expected_fx_anchor_date"] == dt.date(2024, 3, 1)
    assert out[("USD", dt.date(2024, 3, 25))]["expected_fx_rate"] == Decimal("1.00000000")
    assert out[("USD", dt.date(2024, 3, 25))]["expected_fx_default"] is False
    assert out[("JPY", dt.date(2024, 3, 25))]["expected_fx_default"] is True


def test_manifest_helpers_and_business_keys(tmp_path):
    manifest = {"edgeCases": [{"code": "MISSING_FX_RATE", "keys": [{"FROM_CURR_CD": "CAD", "RATE_DT": "2024-03-09"}]}]}
    (tmp_path / vm.MANIFEST_FILE).write_text(json.dumps(manifest))
    loaded = vm.loadManifest(str(tmp_path))
    assert vm.edgeCaseKeys(loaded, "MISSING_FX_RATE") == [{"FROM_CURR_CD": "CAD", "RATE_DT": "2024-03-09"}]
    assert vm.edgeCaseKeys(loaded, "NOPE") == []
    assert vm.businessKey(17) == "WWI_OLTP|17" and vm.lineKey(17, 3) == "WWI_OLTP|17|3"


def test_report_table_and_exit_status():
    report = vm.ValidationReport()
    report.add(vm._pass("A", 1, 1, "fine"))
    report.add(vm._skip("B", "not in manifest"))
    assert report.ok and "1 pass" in report.describe() and "1 skipped" in report.describe()
    report.add(vm._fail("C", 2, 3))
    assert not report.ok and [r.code for r in report.failed] == ["C"]
    lines = report.table().splitlines()
    assert lines[0].split()[:2] == ["check", "status"] and any(line.startswith("C") and "FAIL" in line for line in lines)


def test_duplicate_order_line_check_matches_planted_pairs_in_quarantine(spark, cfg, tmp_path):
    vcfg = replace(cfg, schemaOverrides={"quality": "vm_quality", "silver": "vm_silver", "gold": "vm_gold"})
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {vcfg.schema('quality')}")
    ts = dt.datetime(2024, 3, 1)
    spark.createDataFrame(
        [
            ("DUP_ORDER_LINE", "sqlserver_sales_order_lines", "dup", '{"order_line_business_key":"WWI_OLTP|10|2"}', 1, ts),
            ("OL_NEG_QTY", "sqlserver_sales_order_lines", "neg", '{"order_line_business_key":"WWI_OLTP|30|1"}', 1, ts),
        ],
        "rule_code string, source_table string, reason_text string, row_json string, batch_id long, rejected_at_utc timestamp",
    ).write.format("delta").mode("overwrite").saveAsTable(vcfg.fqn("quality", REJECTED_ROWS_TABLE))
    manifest = {
        "edgeCases": [
            {
                "code": "DUPLICATE_ORDER_LINE",
                "keys": [{"OrderID": 10, "OrderLineIDs": "1,2"}, {"OrderID": 30, "OrderLineIDs": "1,2"}],
            }
        ]
    }
    report = vm.ValidationReport()
    vm.checkDuplicateOrderLines(spark, vcfg, manifest, report)
    result = report.results[0]
    assert result.code == "DUPLICATE_ORDER_LINE" and result.status == vm.STATUS_FAIL
    assert (result.observed, result.expected) == ("1", "2")


def test_skips_when_tables_or_feed_missing(spark, cfg, tmp_path):
    vcfg = replace(cfg, schemaOverrides={"quality": "vm_none", "silver": "vm_none", "gold": "vm_none"}, mockDataRoot=str(tmp_path))
    report = vm.ValidationReport()
    vm.checkGstResidual(
        spark, vcfg, {"edgeCases": [{"code": "GST_INCLUSIVE_RESIDUAL", "keys": [{"OrderID": 1, "UnitPrice": "1"}]}]}, report
    )
    vm.checkEuConsent(
        spark, vcfg, {"edgeCases": [{"code": "EU_CONSENT_N", "keys": [{"CustomerID": 1}]}]}, report, os.path.join(str(tmp_path), "feed")
    )
    assert [r.status for r in report.results] == [vm.STATUS_SKIPPED, vm.STATUS_SKIPPED]
