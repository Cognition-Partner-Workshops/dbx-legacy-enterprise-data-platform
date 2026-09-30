"""Spark session helpers."""
from __future__ import annotations

from pyspark.sql import SparkSession


def getSpark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
