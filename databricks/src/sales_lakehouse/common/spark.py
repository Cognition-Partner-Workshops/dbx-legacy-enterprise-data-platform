"""Spark session factory that works locally (Delta via pip) and on Databricks."""
from __future__ import annotations

import os

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from sales_lakehouse.common.config import LAYER_SCHEMAS, PipelineConfig

MAVEN_MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"


def isDatabricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


WAREHOUSE_DIR_ENV = "SALES_LAKEHOUSE_WAREHOUSE_DIR"


def getSpark(appName: str = "sales_lakehouse", warehouseDir: str | None = None) -> SparkSession:
    if isDatabricks():
        return SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(appName)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Maven Central rate-limits shared egress IPs (HTTP 429); resolve the
        # Delta jars through the Google-hosted mirror first.
        .config("spark.jars.repositories", os.environ.get("SPARK_JARS_REPOSITORIES", MAVEN_MIRROR))
    )
    persistentDir = os.environ.get(WAREHOUSE_DIR_ENV) if warehouseDir is None else None
    if persistentDir:
        # Persistent local warehouse + Derby metastore so several processes (CLI runs,
        # validation) see the same tables; without it every process starts empty.
        root = os.path.abspath(persistentDir)
        metastore = os.path.join(root, "metastore_db")
        builder = (
            builder.config("spark.sql.warehouse.dir", os.path.join(root, "warehouse"))
            .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore};create=true")
            .enableHiveSupport()
        )
    elif warehouseDir:
        builder = builder.config("spark.sql.warehouse.dir", warehouseDir)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def cacheIfSupported(df: DataFrame) -> DataFrame:
    """``df.cache()`` where the engine allows it; serverless compute rejects PERSIST, so fall back to a plain re-evaluated plan."""
    try:
        return df.cache()
    except AnalysisException as exc:
        if "NOT_SUPPORTED_WITH_SERVERLESS" in str(exc):
            return df
        raise


def unpersistQuietly(df: DataFrame) -> None:
    try:
        df.unpersist()
    except AnalysisException as exc:
        if "NOT_SUPPORTED_WITH_SERVERLESS" not in str(exc):
            raise


def ensureSchemas(spark: SparkSession, cfg: PipelineConfig) -> None:
    for layer in LAYER_SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
