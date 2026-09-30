"""SparkSession helpers that behave identically on Databricks and on a local Delta session."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


def getSpark() -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return buildLocalSpark()


def buildLocalSpark(appName: str = "platform_control_tests", warehouseDir: str | None = None) -> SparkSession:
    """Local Spark with Delta Lake enabled, used by pytest."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(appName)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if warehouseDir:
        builder = builder.config("spark.sql.warehouse.dir", warehouseDir)
    jarDir = Path(os.environ.get("PLATFORM_CONTROL_DELTA_JARS", Path.home() / "spark_jars"))
    localJars = sorted(str(p) for p in jarDir.glob("*.jar")) if jarDir.is_dir() else []
    if localJars:
        # offline fallback when Maven Central is unreachable: pre-downloaded delta-spark/delta-storage/antlr jars
        return builder.config("spark.jars", ",".join(localJars)).getOrCreate()
    return configure_spark_with_delta_pip(builder).getOrCreate()


def isDatabricks(spark: SparkSession) -> bool:
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None) is not None or bool(
        spark.conf.get("spark.databricks.workspaceUrl", None)
    )
