import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)                              # test-only fake dbx_etl_common
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]").appName("oracle_extract_tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # DELTA_JARS: local Delta jars (offline boxes); otherwise delta-spark pulls them from Maven.
    localJars = os.environ.get("DELTA_JARS")
    if localJars:
        builder = builder.config("spark.jars", localJars)
        session = builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def resetControl():
    from dbx_etl_common import control

    control.reset()
    yield
    control.reset()
