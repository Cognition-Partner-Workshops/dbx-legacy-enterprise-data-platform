from pyspark.sql import functions as F

from sales_lakehouse.common.quality import quarantine
from sales_lakehouse.common.tables import appendBatch, overwriteTable, tableExists


def test_fqn_two_level_locally(cfg):
    assert cfg.fqn("bronze", "sqlserver_sales_orders") == "sales_bronze.sqlserver_sales_orders"


def test_append_batch_is_idempotent(spark, cfg):
    fqn = cfg.fqn("bronze", "smoke")
    df = spark.createDataFrame([(1, 7), (2, 7)], ["id", "_batch_id"])
    appendBatch(df, fqn, "_batch_id", 7)
    appendBatch(df, fqn, "_batch_id", 7)
    assert spark.table(fqn).count() == 2
    assert tableExists(spark, fqn)


def test_quarantine_keeps_bad_rows(spark, cfg):
    df = spark.createDataFrame([("A", 1.0), ("B", None)], ["k", "rate"])
    passed = quarantine(spark, cfg, df, "FX_MISSING", "unit", F.col("rate").isNull(), "rate missing")
    assert passed.count() == 1
    rejected = spark.table(cfg.fqn("quality", "rejected_rows"))
    assert rejected.filter(F.col("rule_code") == "FX_MISSING").count() == 1
    overwriteTable(passed, cfg.fqn("silver", "smoke"))
