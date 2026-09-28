"""Partner feed export: EU consent stripping, FX restatement, manifest."""
from __future__ import annotations

import csv
import datetime as dt
import json
import os

from sales_lakehouse.gold import partner_feed
from tests.gold_reporting_fixtures import AS_OF, goldRun  # noqa: F401


def _readCsv(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_export_writes_csv_and_manifest_per_region(spark, cfg, goldRun):
    files = sorted(os.listdir(goldRun["feedDir"]))
    assert files == [
        "partner_feed_APAC_20240315.csv", "partner_feed_APAC_20240315.manifest.json",
        "partner_feed_EU_20240315.csv", "partner_feed_EU_20240315.manifest.json",
        "partner_feed_NA_20240315.csv", "partner_feed_NA_20240315.manifest.json",
    ]
    with open(os.path.join(goldRun["feedDir"], "partner_feed_EU_20240315.csv"), encoding="utf-8") as handle:
        header = handle.readline().rstrip("\r\n").split(",")
    assert header == partner_feed.FEED_COLUMNS  # legacy SLS_Export_PartnerFeed column list, no consent / PII fields


def test_eu_feed_drops_unconsented_customers_and_restates_to_eur(spark, cfg, tmp_path):
    csvPath, manifestPath = partner_feed.exportPartnerFeed(
        spark, cfg, str(tmp_path), "EU", asOfDate=AS_OF, lookbackDays=60
    )
    rows = _readCsv(csvPath)
    # invoices since 2024-01-14: INV-EU-1, INV-EU-2 (customer 4, consent = N -> dropped), INV-EU-4, INV-EU-5, INV-EU-6
    assert [r["InvoiceNumber"] for r in rows] == ["INV-EU-1", "INV-EU-4", "INV-EU-5", "INV-EU-6"]
    assert all(r["RegionCode"] == "EU" and r["SettlementCurrencyCode"] == "EUR" for r in rows)
    assert all(r["CustomerReference"] == "C-3" for r in rows)
    byInvoice = {r["InvoiceNumber"]: r for r in rows}
    assert byInvoice["INV-EU-1"]["NetAmount"] == "1000.00"  # EUR -> EUR
    assert byInvoice["INV-EU-5"]["NetAmount"] == "230.00"  # 200 GBP x 1.15 month-average
    # LEGACY QUIRK: no CHF->EUR rate -> 1.0 default, amount passes through unconverted
    assert byInvoice["INV-EU-6"]["NetAmount"] == "300.00"
    assert byInvoice["INV-EU-1"]["PartnerCode"] == "DIRECT" and byInvoice["INV-EU-1"]["StockItemCode"] == "120"
    assert byInvoice["INV-EU-1"]["Quantity"] == "10" and byInvoice["INV-EU-1"]["InvoiceDate"] == "2024-02-12"
    manifest = json.loads(open(manifestPath, encoding="utf-8").read())
    assert manifest["region"] == "EU" and manifest["row_count"] == 4
    assert manifest["net_amount_total"] == "11530.00" and manifest["columns"] == partner_feed.FEED_COLUMNS
    assert manifest["file"] == os.path.basename(csvPath) and manifest["as_of_date"] == "2024-03-15"
    assert manifest["settlement_currency_code"] == "EUR"


def test_eu_redaction_step_before_suppression(spark, cfg):
    from sales_lakehouse.gold.inputs import DIM_STOCK_ITEM_SCHEMA, readOrEmpty

    feed = partner_feed.buildPartnerFeed(
        spark.table(cfg.fqn("gold", "fact_sale")),
        spark.table(cfg.fqn("silver", "dim_customer")),
        spark.table(cfg.fqn("silver", "dim_sales_channel")),
        readOrEmpty(spark, cfg, "silver", "dim_stock_item", DIM_STOCK_ITEM_SCHEMA),
        spark.table(cfg.fqn("silver", "ref_fx_rate")),
        "EU",
        AS_OF,
        suppressUnconsentedEuRows=False,
        lookbackDays=60,
    )
    rows = {r["InvoiceNumber"]: r for r in feed.collect()}
    # legacy package first REDACTs the customer reference of consent = N rows, then deletes them
    assert rows["INV-EU-2"]["CustomerReference"] == partner_feed.REDACTED
    assert rows["INV-EU-2"]["PartnerCode"] == "EU-RESELL"
    assert rows["INV-EU-1"]["CustomerReference"] == "C-3"


def test_na_feed_ignores_consent_and_excludes_reversals(spark, cfg, tmp_path):
    csvPath, _ = partner_feed.exportPartnerFeed(spark, cfg, str(tmp_path), "NA", asOfDate=AS_OF, lookbackDays=60)
    rows = _readCsv(csvPath)
    assert sorted(r["InvoiceNumber"] for r in rows) == ["INV-NA-1", "INV-NA-1", "INV-NA-2", "INV-NA-3", "INV-NA-4"]
    bigbox = [r for r in rows if r["InvoiceNumber"] == "INV-NA-3"][0]
    assert bigbox["CustomerReference"] == "C-2"  # consent = N only matters for EU
    assert bigbox["SettlementCurrencyCode"] == "USD" and bigbox["NetAmount"] == "2000.00"


def test_default_lookback_is_one_day(spark, cfg, tmp_path):
    csvPath, manifestPath = partner_feed.exportPartnerFeed(spark, cfg, str(tmp_path), "APAC", asOfDate=dt.date(2024, 3, 2))
    rows = _readCsv(csvPath)
    assert [r["InvoiceNumber"] for r in rows] == ["INV-AP-3"]  # 2024-03-01 only
    assert rows[0]["NetAmount"] == "500.00" and rows[0]["SettlementCurrencyCode"] == "AUD"  # NZD -> AUD no rate -> 1.0
    assert json.loads(open(manifestPath, encoding="utf-8").read())["row_count"] == 1
