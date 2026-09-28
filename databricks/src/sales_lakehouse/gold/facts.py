"""Gold fact layer entry point: runs every fact loader in dependency order."""

from __future__ import annotations

from pyspark.sql import SparkSession

from sales_lakehouse.common.config import PipelineConfig
from sales_lakehouse.gold import (
    fact_credit_note,
    fact_daily_snapshots,
    fact_order,
    fact_order_fulfilment,
    fact_payment,
    fact_return,
    fact_sale,
    fact_sales_margin,
)

GOLD_TABLES = (
    fact_sale.TABLE,
    fact_order.TABLE,
    fact_payment.TABLE,
    fact_sales_margin.TABLE,
    fact_daily_snapshots.SALES_TABLE,
    fact_daily_snapshots.BACKLOG_TABLE,
    fact_return.TABLE,
    fact_credit_note.TABLE,
    fact_order_fulfilment.TABLE,
)


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('gold')}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema('quality')}")
    # transactional facts first; return / credit note before margin + daily snapshot
    # because those read fact_return / fact_credit_note for return and credit amounts
    fact_sale.run(spark, cfg)
    fact_order.run(spark, cfg)
    fact_payment.run(spark, cfg)
    fact_return.run(spark, cfg)
    fact_credit_note.run(spark, cfg)
    fact_sales_margin.run(spark, cfg)
    fact_daily_snapshots.run(spark, cfg)
    fact_order_fulfilment.run(spark, cfg)
