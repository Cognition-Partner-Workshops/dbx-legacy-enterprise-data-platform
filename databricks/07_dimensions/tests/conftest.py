import glob
import os
import shutil
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "src"))
sys.path.insert(0, os.path.join(HERE, "fakes"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

CATALOG = "spark_catalog"  # local Spark has a single catalog; schemas below live in it


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="wwi07_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_07_dimensions_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
    )
    jarsDir = os.environ.get("DELTA_JARS_DIR", os.path.expanduser("~/jars"))
    localJars = sorted(glob.glob(os.path.join(jarsDir, "*.jar"))) if os.path.isdir(jarsDir) else []
    if localJars:
        # offline boxes: Delta + antlr jars pre-downloaded instead of Ivy resolution from Maven Central
        session = builder.config("spark.jars", ",".join(localJars)).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in ("bronze", "silver", "gold", "etl"):
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def catalog():
    return CATALOG


@pytest.fixture
def cleanGold(spark):
    """Drop every gold / silver table before the test so each test starts from an empty warehouse."""
    for schema in ("gold", "silver"):
        for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{schema}.{row['tableName']}")
    yield
