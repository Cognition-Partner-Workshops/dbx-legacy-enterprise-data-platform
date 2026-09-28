import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(HERE, "..", "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError:  # pragma: no cover
        configure_spark_with_delta_pip = None
    builder = (SparkSession.builder.master("local[2]").appName("wwi_99_maintenance_tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    builder = (builder
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    # DELTA_JARS=/path/a.jar,/path/b.jar skips the Maven resolve (delta-spark, delta-storage, antlr4-runtime)
    localJars = os.environ.get("DELTA_JARS")
    if localJars:
        session = builder.config("spark.jars", localJars).getOrCreate()
    elif configure_spark_with_delta_pip is not None:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    yield session
    session.stop()
