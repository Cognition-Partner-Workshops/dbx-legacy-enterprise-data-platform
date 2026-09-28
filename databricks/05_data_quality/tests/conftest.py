import os
import sys

import pytest

HERE = os.path.dirname(__file__)
SRC = os.path.abspath(os.path.join(HERE, "..", "src"))
FAKES = os.path.abspath(os.path.join(HERE, "fakes"))  # test-only dbx_etl_common stand-in
for path in (SRC, FAKES):
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("dq_quality_tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
