import os
import sys
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ssis-procurement-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = builder.getOrCreate()
    yield session
    session.stop()


def ts(value):
    return datetime.fromisoformat(value)


def d(value):
    return date.fromisoformat(value)


def dec(value):
    return Decimal(str(value))
