import os
import shutil
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
BUNDLE = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "fakes"))   # test-only dbx_etl_common
sys.path.insert(0, os.path.join(BUNDLE, "src"))

from pyspark.sql import SparkSession  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402

CATALOG = "spark_catalog"


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="wwi_ref_wh_")
    builder = (SparkSession.builder.master("local[2]").appName("wwi_06_reference_data_tests")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
               .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=%s/metastore_db;create=true" % warehouse))
    # offline runners: point WWI_DELTA_JARS at a directory holding delta-spark_2.12, delta-storage and
    # antlr4-runtime jars; otherwise delta-spark resolves them from Maven via ivy.
    jarDir = os.environ.get("WWI_DELTA_JARS")
    if jarDir and os.path.isdir(jarDir):
        jars = ",".join(os.path.join(jarDir, f) for f in sorted(os.listdir(jarDir)) if f.endswith(".jar"))
        session = builder.config("spark.jars", jars).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


class FakeWidgets:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, name):
        if name not in self.values:
            raise Exception("No input widget named %s is defined" % name)
        return self.values[name]


class NotebookExit(Exception):
    pass


class FakeNotebook:
    def exit(self, value):
        raise NotebookExit(value)


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)
        self.notebook = FakeNotebook()


def makeDbutils(batchId="7", businessDate="2024-03-04", restartFromStep="", **extra):
    values = {"BatchId": batchId, "BusinessDate": businessDate, "ReloadFullHistory": "False",
              "EnvironmentCode": "DEV", "RestartFromStep": restartFromStep, "catalog": CATALOG}
    values.update(extra)
    return FakeDbutils(values)


@pytest.fixture(scope="session")
def catalog(spark):
    from wwi_ref import schemas
    for schema in ("bronze", "silver", "gold", "etl"):
        spark.sql("CREATE SCHEMA IF NOT EXISTS %s.%s" % (CATALOG, schema))
    schemas.ensureAll(spark, CATALOG)
    spark.sql("""CREATE TABLE IF NOT EXISTS %s.etl.configuration (
        ConfigurationId BIGINT NOT NULL, ConfigurationKey STRING NOT NULL, EnvironmentCode STRING NOT NULL,
        ConfigurationValue STRING NOT NULL, ValueDataType STRING NOT NULL, Description STRING,
        IsSensitive BOOLEAN NOT NULL, ModifiedAtUtc TIMESTAMP NOT NULL) USING DELTA""" % CATALOG)
    return CATALOG


@pytest.fixture(autouse=True)
def resetControl():
    from dbx_etl_common import control
    control.reset()
    yield
