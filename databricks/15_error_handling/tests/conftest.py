"""Local pytest fixtures for the WWI_ErrorHandling notebooks.

``tests/dbx_etl_common`` is a minimal test-only fake of the shared control layer
(owned by session 00 under databricks/common); it is put on sys.path here and
is never deployed.
"""

import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # tests/dbx_etl_common fake
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_15_error_handling_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
