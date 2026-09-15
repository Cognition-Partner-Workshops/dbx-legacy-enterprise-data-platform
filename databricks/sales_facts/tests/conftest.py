"""Local PySpark fixture harness. Bronze CSVs are read all-string, exactly as
Auto Loader lands them with inferColumnTypes=false, so the typing rules in
sales_facts.transforms are exercised end to end."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_facts import transforms as t

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("sales_facts_fixtures")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def read_bronze(spark: SparkSession, relative: str) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", "false").csv(str(FIXTURES / relative))


def read_dim(spark: SparkSession, name: str, key_col: str) -> DataFrame:
    df = read_bronze(spark, f"dimensions/{name}.csv").withColumn(key_col, F.col(key_col).cast("bigint"))
    if "valid_from" in df.columns:
        df = df.withColumn("valid_from", F.to_date("valid_from")).withColumn("valid_to", F.to_date("valid_to"))
    return df


@pytest.fixture(scope="session")
def dims(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "dim_customer": read_dim(spark, "dim_customer", "customer_key"),
        "dim_stock_item": read_dim(spark, "dim_stock_item", "stock_item_key"),
        "dim_salesperson": read_dim(spark, "dim_salesperson", "salesperson_key"),
        "dim_city": read_dim(spark, "dim_city", "city_key"),
        "dim_sales_channel": read_dim(spark, "dim_sales_channel", "sales_channel_key"),
        "dim_promotion": read_dim(spark, "dim_promotion", "promotion_key"),
    }


@pytest.fixture(scope="session")
def bronze(spark: SparkSession) -> dict[str, DataFrame]:
    return {
        "invoice": read_bronze(spark, "sqlserver/Sales.Invoices.csv"),
        "invoice_line": read_bronze(spark, "sqlserver/Sales.InvoiceLines.csv"),
        "fx": read_bronze(spark, "reference/ExchangeRateDaily.csv"),
    }


def run_slice(bronze: dict[str, DataFrame], dims: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Compose the transforms in the same order as src/transformations/*.py.
    Returns every published dataset of the pipeline, keyed by dataset name."""
    sale_work = t.build_sale_work(
        t.conform_invoice_header(bronze["invoice"]), t.conform_invoice_line(bronze["invoice_line"])
    )
    accepted = t.accepted_sale_work(sale_work)
    survivors, _ = t.dedup_within_batch(accepted)
    keyed = t.resolve_dimension_keys(survivors, **dims)
    silver_sale_line = t.compute_measures(t.apply_fx(t.release_held(keyed), t.conform_fx_rates(bronze["fx"])))
    return {
        "silver_sale_line": silver_sale_line,
        "silver_rejected_invoice_line": t.rejected_invoice_lines(sale_work),
        "silver_fact_load_hold": t.fact_load_hold(keyed),
        "silver_inferred_customer": t.inferred_customers(keyed),
        "fact_sale": t.build_fact_sale(silver_sale_line),
        "fact_sale_duplicate_archive": t.duplicate_archive(accepted, silver_sale_line),
    }


@pytest.fixture(scope="session")
def outputs(bronze, dims) -> dict[str, DataFrame]:
    return {name: df.cache() for name, df in run_slice(bronze, dims).items()}


def rows(df: DataFrame, **where) -> list[dict]:
    for col, value in where.items():
        df = df.where(F.col(col) == F.lit(value))
    return [r.asDict() for r in df.collect()]


def one(df: DataFrame, **where) -> dict:
    found = rows(df, **where)
    assert len(found) == 1, f"expected exactly one row for {where}, got {len(found)}"
    return found[0]
