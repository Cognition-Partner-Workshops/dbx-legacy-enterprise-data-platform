"""Local PySpark fixtures. A minimal test-only fake of ``dbx_etl_common`` (session 00 package)
lives under ``tests/fakes`` so ``c360_lib`` imports resolve without the real wheel."""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "fakes"))
sys.path.insert(0, os.path.join(HERE, "..", "src"))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("c360-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
