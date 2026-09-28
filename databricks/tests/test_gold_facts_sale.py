from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from sales_lakehouse.gold import fact_sale
from tests.gold_facts_fixtures import isolatedConfig, rejected, seedDimensions, seedSales


@pytest.fixture(scope="module")
def saleCfg(spark, cfg):
    iso = isolatedConfig(spark, cfg, "sale")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    fact_sale.run(spark, iso)
    return iso


def _row(spark, saleCfg, key):
    return spark.table(saleCfg.fqn("gold", "fact_sale")).filter(F.col("sale_line_business_key") == key).collect()[0]


def test_dedup_survivor_loaded_loser_quarantined(spark, saleCfg):
    fact = spark.table(saleCfg.fqn("gold", "fact_sale"))
    assert fact.filter(F.col("sale_line_business_key") == "INV1-1").count() == 1
    assert _row(spark, saleCfg, "INV1-1")["net_amount"] == Decimal("100.0000")
    losers = rejected(spark, saleCfg, "FACT_SALE_DUP")
    assert losers.count() == 1
    assert '"net_line_amount":99' in losers.collect()[0]["row_json"]


def test_scd2_customer_key_as_of_invoice_date(spark, saleCfg):
    assert _row(spark, saleCfg, "INV1-1")["customer_key"] == 10  # 2024-03-15 -> first version
    assert _row(spark, saleCfg, "INV2-1")["customer_key"] == 11  # 2024-07-10 -> open-ended current version
    assert _row(spark, saleCfg, "INV1-1")["bill_to_customer_key"] == 10


def test_unknown_members_default_to_minus_one(spark, saleCfg):
    r = _row(spark, saleCfg, "INV3-1")
    assert r["customer_key"] == -1
    assert r["inferred_member_flag"] is True
    assert r["sales_channel_key"] == -1 and r["stock_item_key"] == -1 and r["city_key"] == -1
    assert r["salesperson_key"] == 100


def test_regional_tax_fx_and_fiscal(spark, saleCfg):
    na = _row(spark, saleCfg, "INV1-1")
    assert na["tax_amount"] == Decimal("8.0000") and na["total_including_tax"] == Decimal("108.0000")
    assert na["fx_rate_source_code"] == "SAME_CCY" and na["fiscal_period_key"] == 202403
    eu = _row(spark, saleCfg, "INV2-1")
    assert eu["fx_rate_to_reporting"] == Decimal("1.10000000") and eu["fx_rate_source_code"] == "ECB"
    assert eu["net_amount_reporting"] == Decimal("110.0000")
    apac = _row(spark, saleCfg, "INV3-1")
    assert apac["tax_amount"] == Decimal("10.0000")  # GST extracted from the inclusive 110
    assert apac["total_excluding_tax"] == Decimal("100.0000")
    assert apac["fx_rate_source_code"] == "DEFAULT_1" and apac["fx_rate_to_reporting"] == Decimal("1.00000000")
    assert apac["fiscal_period_key"] == -1


def test_merge_is_idempotent(spark, saleCfg):
    before = spark.table(saleCfg.fqn("gold", "fact_sale")).count()
    fact_sale.run(spark, saleCfg)
    fact = spark.table(saleCfg.fqn("gold", "fact_sale"))
    assert fact.count() == before == 3
    assert fact.select("sale_line_business_key").distinct().count() == 3
    assert {"batch_id", "loaded_at_utc", "region_code", "tax_treatment_code", "fiscal_period_key"} <= set(fact.columns)


def test_region_parameter_filters_rows(spark, cfg):
    iso = isolatedConfig(spark, cfg, "sale_eu")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    fact_sale.run(spark, iso, regionCodes=["EU"])
    rows = spark.table(iso.fqn("gold", "fact_sale")).select("region_code").distinct().collect()
    assert [r[0] for r in rows] == ["EU"]
