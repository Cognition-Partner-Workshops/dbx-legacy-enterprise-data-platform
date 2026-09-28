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

from dbx_etl_common import control  # noqa: E402

CATALOG = "spark_catalog"


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="wwi08_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_08_facts_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=%s" % warehouse)
    )
    deltaJars = os.environ.get("DELTA_JARS", "")
    if deltaJars:
        session = builder.config("spark.jars", deltaJars).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in ("bronze", "silver", "gold", "etl"):
        session.sql("CREATE DATABASE IF NOT EXISTS %s" % schema)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(autouse=True)
def resetControl():
    control.reset()
    yield
    control.reset()


@pytest.fixture
def catalog():
    return CATALOG


@pytest.fixture
def jobParams():
    from datetime import date

    return {
        "batchId": 7,
        "businessDate": date(2024, 3, 15),
        "reloadFullHistory": False,
        "environmentCode": "DEV",
        "restartFromStep": "",
        "catalog": CATALOG,
    }


def writeDelta(spark, fullName, df, mode="overwrite"):
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(fullName)


@pytest.fixture
def scd2Dimension(spark):
    """Factory for a minimal SCD2 dimension table matching fact_common.DimensionSpec defaults."""
    from datetime import datetime

    from pyspark.sql import types as T

    def make(fullName, spec, rows):
        schema = T.StructType([
            T.StructField(spec.keyCol, T.IntegerType()),
            T.StructField(spec.businessKeyCol, T.StringType()),
            T.StructField("name", T.StringType()),
            T.StructField("valid_from", T.TimestampType()),
            T.StructField("valid_to", T.TimestampType()),
            T.StructField("is_current_row", T.BooleanType()),
            T.StructField("is_inferred_member", T.BooleanType()),
            T.StructField("region_code", T.StringType()),
            T.StructField("lineage_key", T.LongType()),
        ])
        data = [(-1, None, "Unknown", datetime(1900, 1, 1), datetime(9999, 12, 31, 23, 59, 59), True, False, None, 0)] + list(rows)
        writeDelta(spark, fullName, spark.createDataFrame(data, schema))
        return fullName

    return make
