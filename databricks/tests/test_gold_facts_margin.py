from decimal import Decimal

from pyspark.sql import functions as F

from sales_lakehouse.gold import fact_sale, fact_sales_margin
from tests.gold_facts_fixtures import isolatedConfig, seedDimensions, seedProductMaster, seedSales


def _margin(spark, cfg, key):
    return spark.table(cfg.fqn("gold", "fact_sales_margin")).filter(F.col("sale_line_business_key") == key).collect()[0]


def test_margin_full_rebuild_and_cost_missing(spark, cfg):
    iso = isolatedConfig(spark, cfg, "margin")
    seedDimensions(spark, iso)
    seedSales(spark, iso)
    seedProductMaster(spark, iso, s1Cost="4")
    fact_sale.run(spark, iso)
    fact_sales_margin.run(spark, iso)

    r = _margin(spark, iso, "INV1-1")
    assert r["cost_usd"] == Decimal("40.0000") and r["gross_margin_reporting"] == Decimal("60.0000")
    assert r["margin_status_code"] == "OK" and r["cost_basis_code"] == "STD"
    missing = _margin(spark, iso, "INV2-1")
    assert missing["cost_usd"] is None and missing["margin_status_code"] == "COST_MISSING"
    assert missing["net_usd"] == Decimal("110.0000")
    assert spark.table(iso.fqn("gold", "fact_sales_margin")).count() == 3

    seedProductMaster(spark, iso, s1Cost="5")
    fact_sales_margin.run(spark, iso)
    r = _margin(spark, iso, "INV1-1")
    assert r["cost_usd"] == Decimal("50.0000") and r["gross_margin_reporting"] == Decimal("50.0000")
    assert spark.table(iso.fqn("gold", "fact_sales_margin")).count() == 3
    assert spark.table(iso.fqn("gold", "fact_sales_margin")).filter(F.col("batch_id") == iso.batchId).count() == 3
