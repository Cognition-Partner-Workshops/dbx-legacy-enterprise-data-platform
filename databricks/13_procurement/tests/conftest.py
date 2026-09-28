import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName("prc-13-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if os.environ.get("PRC_TESTS_WITH_DELTA", "1") == "1":
        try:
            from delta import configure_spark_with_delta_pip

            builder = (builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                       .config("spark.sql.warehouse.dir", os.path.join(HERE, ".spark-warehouse")))
            session = configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception:  # delta jars unavailable offline
            session = builder.getOrCreate()
    else:
        session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def deltaAvailable(spark):
    try:
        spark.sql("CREATE TABLE IF NOT EXISTS _prc_delta_probe (x INT) USING DELTA")
        spark.sql("DROP TABLE _prc_delta_probe")
        return True
    except Exception:
        return False
