import datetime
import os
import shutil
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

# Delta needs its jars on the driver classpath. `configure_spark_with_delta_pip`
# resolves them from Maven; DELTA_JARS_DIR lets an offline machine point at a
# directory holding delta-spark_2.12, delta-storage and antlr4-runtime instead.
DELTA_JARS_DIR = os.environ.get("DELTA_JARS_DIR", "")

from dbx_etl_common import control  # noqa: E402

CATALOG = "spark_catalog"


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="stg04-wh-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("stg04-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    if DELTA_JARS_DIR and os.path.isdir(DELTA_JARS_DIR):
        jars = [os.path.join(DELTA_JARS_DIR, f) for f in sorted(os.listdir(DELTA_JARS_DIR)) if f.endswith(".jar")]
        session = builder.config("spark.jars", ",".join(jars)).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in ("bronze", "silver", "etl"):
        session.sql("CREATE SCHEMA IF NOT EXISTS %s" % schema)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def jobParams():
    return {
        "batchId": 7,
        "businessDate": datetime.date(2024, 3, 4),
        "reloadFullHistory": False,
        "environmentCode": "DEV",
        "restartFromStep": "",
        "catalog": CATALOG,
    }


@pytest.fixture(autouse=True)
def resetControl():
    control.reset()
    yield
