"""Shared pytest fixtures: one local Spark session and empty frames shaped like the federated sources."""
from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from customer_party.spark_session import getSpark

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return getSpark("customer_party_tests")


@pytest.fixture(scope="session")
def sourceSchemas() -> dict[str, str]:
    """Column names/types of the legacy objects as exposed through Lakehouse Federation (captured via DESCRIBE)."""
    with (FIXTURES / "source_schemas.json").open() as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def emptySource(spark: SparkSession, sourceSchemas: dict[str, str]) -> Callable[[str], DataFrame]:
    def _make(fqn: str) -> DataFrame:
        return spark.createDataFrame([], sourceSchemas[fqn])

    return _make
