"""Spark session helpers shared by notebooks and tests."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def getSpark(appName: str = "customer_party") -> SparkSession:
    """Return the active session on Databricks, or a local session for tests.

    Local Delta support (needs the delta-spark jars from Maven) is opt-in via
    CP_LOCAL_DELTA=1; the pure-transformation tests do not need it.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    builder = (
        SparkSession.builder.appName(appName)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
    )
    if os.environ.get("CP_LOCAL_DELTA") == "1":
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    return builder.getOrCreate()
