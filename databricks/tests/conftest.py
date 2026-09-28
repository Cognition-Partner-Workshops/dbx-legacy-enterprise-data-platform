import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

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
