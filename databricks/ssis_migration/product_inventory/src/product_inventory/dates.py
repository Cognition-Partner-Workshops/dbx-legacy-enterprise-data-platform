"""Business-date resolution shared by the staging, fact and inventory packages."""
from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from product_inventory.config import PipelineConfig
from product_inventory.tables import tableExists


def resolveBusinessDate(spark: SparkSession, cfg: PipelineConfig, movementTable: str) -> date:
    """`$Package::BusinessDate` defaults to the latest movement date in the source so that
    the snapshot packages produce a populated day even on the frozen legacy baseline."""
    if cfg.businessDate is not None:
        return cfg.businessDate
    if tableExists(spark, movementTable):
        latest = spark.table(movementTable).agg(F.max(F.to_date("movement_timestamp"))).collect()[0][0]
        if latest is not None:
            return latest
    return date.today()
