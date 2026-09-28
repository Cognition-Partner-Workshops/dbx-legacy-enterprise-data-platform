import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(HERE, "..", "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:  # pragma: no cover
        configure_spark_with_delta_pip = None
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    builder = (SparkSession.builder.master("local[2]").appName("wwi_09_aggregates_tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.ui.enabled", "false")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    localJars = os.environ.get("DELTA_JARS")
    if localJars:
        session = builder.config("spark.jars", localJars).getOrCreate()
    elif configure_spark_with_delta_pip is not None:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    for schema in ("gold", "silver", "etl"):
        session.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema}")
    yield session
    session.stop()


@pytest.fixture
def control():
    from dbx_etl_common import control as c
    c.reset()
    return c


@pytest.fixture(scope="session")
def tables():
    import agg_common
    from dbx_etl_common import naming
    return agg_common.resolveTables("spark_catalog", naming.table)
