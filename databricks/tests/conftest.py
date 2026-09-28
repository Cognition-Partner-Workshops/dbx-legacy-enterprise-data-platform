import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
# one JVM serves the whole suite (thousands of Delta stages); the 1g default heap
# runs out of broadcast memory part-way through
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 4g pyspark-shell")

from sales_lakehouse.common.config import PipelineConfig  # noqa: E402
from sales_lakehouse.common.spark import ensureSchemas, getSpark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="sales_lakehouse_wh_")
    session = getSpark("sales_lakehouse_tests", warehouseDir=warehouse)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def cfg(spark):
    config = PipelineConfig(catalog=None, mockDataRoot=tempfile.mkdtemp(prefix="sales_lakehouse_mock_"), batchId=1)
    ensureSchemas(spark, config)
    return config
