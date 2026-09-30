import os
import shutil
import sys
import tempfile
from datetime import date
from decimal import Decimal

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sales_performance import config  # noqa: E402

TEST_SCHEMA = "ssis_sales_performance_test"


def _deltaOnClasspath():
    """Delta jars already in pyspark/jars (offline boxes) mean we must not ask Ivy to resolve them."""
    import pyspark

    jars = os.listdir(os.path.join(os.path.dirname(pyspark.__file__), "jars"))
    return any(j.startswith("delta-spark_") for j in jars)


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="sp_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("sales_performance_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = (builder if _deltaOnClasspath() else configure_spark_with_delta_pip(builder)).getOrCreate()
    config.CATALOG = ""
    config.SCHEMA = TEST_SCHEMA
    session.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_SCHEMA}")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def d(x):
    return Decimal(str(x))


SALE_LINE_SCHEMA = (
    "sale_key bigint, invoice_number bigint, invoice_line_number int, invoice_date date, salesperson_key bigint, salesperson_id bigint, "
    "customer_key bigint, country_code_iso3 string, region_code string, territory_code string, currency_code string, line_type_code string, "
    "is_reversal boolean, is_house_account boolean, extended_price decimal(19,4), tax_amount decimal(19,4), total_including_tax decimal(19,4), "
    "net_amount decimal(19,4), net_amount_reported decimal(19,4), vat_amount decimal(19,4), vat_rate_percent decimal(9,4), gst_amount decimal(19,4), "
    "fiscal_calendar_code string, fiscal_period_label string"
)


def saleLine(
    saleKey,
    region,
    invoiceDate,
    extended,
    tax,
    *,
    salespersonId=10,
    customerKey=1,
    currency=None,
    lineType="STANDARD",
    house=False,
    iso3=None,
    vatRate=None,
    netReported=None,
    territory=None,
    reversal=False,
):
    currency = currency or {"NA": "USD", "EU": "EUR", "APAC": "AUD"}[region]
    iso3 = iso3 or {"NA": "USA", "EU": "DEU", "APAC": "AUS"}[region]
    calendar = {"NA": "NA445", "EU": "EUCAL", "APAC": "APACJUN"}[region]
    return (
        saleKey,
        1000 + saleKey,
        1,
        invoiceDate,
        100 + salespersonId,
        salespersonId,
        customerKey,
        iso3,
        region,
        territory or f"{region}-T1",
        currency,
        lineType,
        reversal,
        house,
        d(extended),
        d(tax),
        d(extended) + d(tax),
        d(extended),
        netReported,
        d(tax) if region == "EU" else None,
        vatRate,
        d(tax) if region == "APAC" else None,
        calendar,
        "FY2016-P11",
    )


PLAN_SCHEMA = (
    "commission_plan_id int, plan_code string, region_code string, commission_basis string, is_default_plan boolean, band1_upper_percent decimal(9,4), "
    "band1_rate_percent decimal(9,4), band2_rate_percent decimal(9,4), accelerator_percent decimal(9,4), effective_from_date date, "
    "effective_to_date date, statutory_cap_amount decimal(19,4)"
)


def plans(spark, capEu=None):
    rows = [
        (1, "NA-FIELD", "NA", "GROSS", True, d(100), d(5), d(7), d(2), date(2013, 1, 1), None, None),
        (2, "EU-FIELD", "EU", "NET", True, d(100), d(4), d(4), d(0), date(2013, 1, 1), None, capEu),
        (3, "AP-FIELD", "APAC", "GST_EXCL", True, d(100), d(6), d(6), d(0), date(2013, 1, 1), None, None),
    ]
    return spark.createDataFrame(rows, PLAN_SCHEMA)


def fxRates(spark, rows=None):
    rows = (
        rows
        if rows is not None
        else [
            ("EUR", "EUR", "AVERAGE", d(1)),
            ("GBP", "EUR", "AVERAGE", d(1.25)),
            ("AUD", "AUD", "AVERAGE", d(1)),
            ("SGD", "AUD", "AVERAGE", d(1.1)),
        ]
    )
    return spark.createDataFrame(
        rows, "currency_code string, quote_currency_code string, rate_type_code string, conversion_rate decimal(19,8)"
    )
