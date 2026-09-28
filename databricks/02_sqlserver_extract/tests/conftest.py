import os
import sys
from datetime import datetime

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(TESTS_DIR, "fakes"))
sys.path.insert(0, os.path.join(os.path.dirname(TESTS_DIR), "src"))

from dbx_etl_common import control as fakeControl  # noqa: E402

NOW_UTC = datetime(2024, 3, 15, 10, 30, 0)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_02_sqlserver_extract_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )
    # Offline machines: point WWI_DELTA_JARS at a directory holding delta-spark_2.12, delta-storage and
    # antlr4-runtime jars instead of resolving them from Maven Central.
    localJars = os.environ.get("WWI_DELTA_JARS")
    if localJars:
        jars = ",".join(os.path.join(localJars, f) for f in sorted(os.listdir(localJars)) if f.endswith(".jar"))
        session = builder.config("spark.jars", jars).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def control():
    fakeControl.reset()
    fakeControl.nowUtc = NOW_UTC
    return fakeControl


class FakeWidgets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        if name not in self.values:
            raise KeyError(name)
        return self.values[name]


class FakeSecrets:
    def get(self, scope, key):
        return "secret:%s/%s" % (scope, key)


class FakeDbutils:
    def __init__(self, **values):
        self.widgets = FakeWidgets(values)
        self.secrets = FakeSecrets()


@pytest.fixture
def dbutils():
    return FakeDbutils(BatchId="42", BusinessDate="2024-03-15", ReloadFullHistory="False", EnvironmentCode="DEV",
                       RestartFromStep="", catalog="wwi_test")
