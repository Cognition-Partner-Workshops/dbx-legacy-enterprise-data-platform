import os
import shutil
import tempfile

import pytest

from platform_control.config import PlatformConfig
from platform_control.spark import buildLocalSpark
from platform_control.tables import ensureControlTables

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="pc_warehouse_")
    os.environ.setdefault("PYSPARK_PYTHON", "python3")
    # UDF workers import platform_control too
    os.environ["PYTHONPATH"] = SRC_DIR + os.pathsep + os.environ.get("PYTHONPATH", "")
    session = buildLocalSpark(warehouseDir=warehouse)
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def cfg(spark):
    volumeRoot = tempfile.mkdtemp(prefix="pc_volume_")
    config = PlatformConfig(schema="ssis_platform_control_test", evidenceSchema="evidence_test", isLocal=True, extra={"volumeRoot": volumeRoot})
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.evidenceSchema}")
    ensureControlTables(spark, config)
    yield config
    shutil.rmtree(volumeRoot, ignore_errors=True)
