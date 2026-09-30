import os
import re
import shutil
import tempfile

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="fin_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ssis_finance_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.sources.default", "delta")
        # Maven Central rate-limits some CI egress; the GCS mirror serves the same artifacts.
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS fin_test")
    yield spark
    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
    if os.path.isdir("metastore_db"):
        shutil.rmtree("metastore_db", ignore_errors=True)


@pytest.fixture(scope="session")
def typed(spark):
    """createDataFrame from plain python ints/floats for schemas that use decimal columns."""

    def build(rows, schema: str):
        loose = re.sub(r"decimal\(\d+,\s*\d+\)", "double", schema, flags=re.I)
        fields = T._parse_datatype_string(loose).fields
        fixed = []
        for row in rows:
            row = list(row.values()) if isinstance(row, dict) else list(row)
            fixed.append(
                tuple(
                    float(v)
                    if isinstance(f.dataType, T.DoubleType) and isinstance(v, int) and not isinstance(v, bool)
                    else v
                    for f, v in zip(fields, row)
                )
            )
        df = spark.createDataFrame(fixed, loose)
        target = T._parse_datatype_string(schema)
        return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in target.fields])

    return build
