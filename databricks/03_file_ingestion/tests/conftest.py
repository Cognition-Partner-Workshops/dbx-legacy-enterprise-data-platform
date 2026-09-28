"""Local PySpark + Delta fixtures. The fake dbx_etl_common lives under tests/fakes (never under databricks/common)."""

import os
import shutil
import sys
import tempfile

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
BUNDLE_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, os.path.join(BUNDLE_DIR, "src"))
sys.path.insert(0, os.path.join(TESTS_DIR, "fakes"))
# Python UDF workers are separate processes: they need the src path too
os.environ["PYTHONPATH"] = os.pathsep.join(
    [os.path.join(BUNDLE_DIR, "src"), os.path.join(TESTS_DIR, "fakes")] + [p for p in [os.environ.get("PYTHONPATH")] if p]
)

from wwi_file_ingestion import feeds  # noqa: E402

CATALOG = "spark_catalog"


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="wwi03-warehouse-")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("wwi_03_file_ingestion_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.ansi.enabled", "true")  # serverless default: the try_cast paths must hold
    )
    # DELTA_SPARK_JARS=<comma-separated local jars> skips the Maven download (offline / rate-limited boxes)
    localJars = os.environ.get("DELTA_SPARK_JARS")
    if localJars:
        session = builder.config("spark.jars", localJars).getOrCreate()
    else:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in ("bronze", "silver", "etl"):
        session.sql("CREATE SCHEMA IF NOT EXISTS %s.%s" % (CATALOG, schema))
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def volumeRoot(monkeypatch):
    root = tempfile.mkdtemp(prefix="wwi03-volumes-")
    monkeypatch.setattr(feeds, "VOLUME_ROOT_TEMPLATE", root + "/{catalog}/bronze")
    for volume in (feeds.VOLUME_INBOUND, feeds.VOLUME_ARCHIVE, feeds.VOLUME_QUARANTINE, feeds.VOLUME_CHECKPOINTS):
        os.makedirs(feeds.volumePath(CATALOG, volume), exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


class FakeFs:
    """Just enough of dbutils.fs for the file-system tasks, over the local file system."""

    def __init__(self):
        self.moves = []

    @staticmethod
    def _local(path):
        return path[len("file:"):] if path.startswith("file:") else path

    def mkdirs(self, path):
        os.makedirs(self._local(path), exist_ok=True)

    def mv(self, src, dst, recurse=False):
        src, dst = self._local(src), self._local(dst)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.move(src, dst)
        self.moves.append((src, dst))
        return True

    def put(self, path, contents, overwrite=False):
        path = self._local(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path) and not overwrite:
            raise IOError("exists: %s" % path)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(contents)
        return True


class FakeDbutils:
    def __init__(self):
        self.fs = FakeFs()


@pytest.fixture
def dbutils():
    return FakeDbutils()


@pytest.fixture
def control():
    from dbx_etl_common import control as fakeControl

    fakeControl.reset()
    return fakeControl


@pytest.fixture
def cleanTables(spark):
    """Empty the shared tables between tests so counts are deterministic."""
    from wwi_file_ingestion import runner, schemas

    runner.ensureTables(spark, CATALOG)
    names = ["%s.bronze.%s" % (CATALOG, t) for t in schemas.BRONZE_SCHEMAS] + [
        "%s.bronze.%s" % (CATALOG, runner.MANIFEST_TABLE),
        "%s.silver.%s" % (CATALOG, feeds.ERR_REJECTED_FILE_ROW),
        "%s.etl.%s" % (CATALOG, feeds.FILE_INGESTION_LOG),
        "%s.etl.%s" % (CATALOG, feeds.FILE_CONTROL_TOTAL),
    ]
    for name in names:
        spark.sql("DELETE FROM %s" % name)
    yield
