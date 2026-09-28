"""Local PySpark + delta-spark fixtures.

The control tables are created in a throw-away catalog/schema and every API call receives it
through the public ``catalog`` argument, exactly as a Databricks job would pass ``${var.catalog}``.
Local OSS Spark has a single ``spark_catalog``, so the "catalog" used here is
``spark_catalog`` with a per-session ``etl`` schema name substitution applied only while a test
holds the ``catalog`` fixture (``naming.controlTable(catalog, "batch")`` -> ``spark_catalog.etl_<id>.batch``).
"""
from __future__ import annotations

import glob
import os
import shutil
import sys
import tempfile
import uuid

import pytest

SRC = os.path.join(os.path.dirname(__file__), "..", "dbx_etl_common", "src")
if SRC not in sys.path:
    sys.path.insert(0, os.path.abspath(SRC))


def _buildSpark():
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="dbx_etl_common_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("dbx_etl_common-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}/derby")
    )
    jars = os.environ.get("DELTA_SPARK_JARS") or ",".join(glob.glob(os.path.expanduser("~/.venvs/dbx/jars/*.jar")))
    if jars:
        builder = builder.config("spark.jars", jars)
        spark = builder.getOrCreate()
    else:  # let delta-spark resolve its Maven coordinates
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark, warehouse


@pytest.fixture(scope="session")
def spark():
    spark, warehouse = _buildSpark()
    yield spark
    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def _pointEtlAt(schema):
    from dbx_etl_common import naming, bootstrap, views
    naming.ETL = schema
    bootstrap.ETL = schema
    views.ETL = schema


@pytest.fixture(scope="session")
def _controlSchema(spark):
    """Bootstrap the control tables once, in a unique local schema standing in for ``etl``."""
    from dbx_etl_common import naming, bootstrap

    schema = "etl_" + uuid.uuid4().hex[:8]
    originalEtl = naming.ETL
    cat = "spark_catalog"
    _pointEtlAt(schema)
    try:
        bootstrap.bootstrap(spark, cat)
    finally:
        _pointEtlAt(originalEtl)
    yield cat, schema
    _pointEtlAt(schema)
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {cat}.{schema} CASCADE")
    finally:
        _pointEtlAt(originalEtl)


@pytest.fixture
def catalog(_controlSchema):
    """The ``catalog`` argument for the public API; while a test holds it, ``etl`` maps to the temp schema.

    Local Spark has exactly one catalog, so the 3-part contract is kept
    (``naming.controlTable(catalog, "batch")`` -> ``spark_catalog.<tmp etl>.batch``) by pointing the
    ``etl`` schema name at the bootstrapped temp schema for the duration of the test only - tests
    that do not use ``catalog`` see the real ``etl`` name.
    """
    from dbx_etl_common import naming

    cat, schema = _controlSchema
    originalEtl = naming.ETL
    _pointEtlAt(schema)
    yield cat
    _pointEtlAt(originalEtl)


@pytest.fixture
def cleanControl(spark, catalog):
    """Truncate the mutable control tables between tests (seeds are kept)."""
    from dbx_etl_common import naming
    for t in ("data_quality_result", "rejected_record", "rejected_record_staging", "row_count_audit", "error_log",
              "package_execution", "batch_step", "batch", "watermark", "control_purge_audit", "data_quality_rule_exception"):
        spark.sql(f"DELETE FROM {naming.controlTable(catalog, t)}")
    yield


class FakeWidgets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        if name not in self.values:
            raise ValueError(f"No input widget named {name}")
        return self.values[name]


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


@pytest.fixture
def fakeDbutils():
    return FakeDbutils
