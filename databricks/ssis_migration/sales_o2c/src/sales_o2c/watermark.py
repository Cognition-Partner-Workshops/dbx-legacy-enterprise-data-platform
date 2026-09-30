"""etl.usp_GetWatermark / etl.usp_SetWatermark on a Delta control table (etl_watermark)."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sales_o2c.config import SOURCE_SYSTEM_CODE, RunContext
from sales_o2c.tables import mergeUpsert, tableExists

EPOCH = "1900-01-01T00:00:00"
NUMERIC_KEY = "NumericKey"
TIMESTAMP = "Timestamp"


def defaultWatermark(watermarkType: str) -> str:
    return "0" if watermarkType == NUMERIC_KEY else EPOCH


def resolveWatermarkFrom(lastValue, watermarkType: str, reloadFullHistory: bool) -> str:
    """Pure rule: full reload or no history -> epoch/zero, else the stored LastValue."""
    if reloadFullHistory or lastValue is None or str(lastValue).strip() == "":
        return defaultWatermark(watermarkType)
    return str(lastValue)


def getWatermark(spark: SparkSession, ctx: RunContext, objectName: str, watermarkType: str) -> str:
    table = ctx.table("etl_watermark")
    lastValue = None
    if tableExists(spark, table):
        row = (
            spark.table(table)
            .filter((F.col("source_system_code") == SOURCE_SYSTEM_CODE) & (F.col("object_name") == objectName))
            .select("last_value")
            .first()
        )
        lastValue = row["last_value"] if row else None
    return resolveWatermarkFrom(lastValue, watermarkType, ctx.reloadFullHistory)


def setWatermark(spark: SparkSession, ctx: RunContext, objectName: str, watermarkType: str, newValue) -> None:
    table = ctx.table("etl_watermark")
    previous = getWatermark(spark, ctx, objectName, watermarkType) if tableExists(spark, table) else None
    df = spark.createDataFrame(
        [(SOURCE_SYSTEM_CODE, objectName, watermarkType, str(newValue), previous, int(ctx.packageExecutionId))],
        "source_system_code string, object_name string, watermark_type string, last_value string, previous_value string, last_package_execution_id bigint",
    ).withColumn("last_loaded_at_utc", F.current_timestamp())
    mergeUpsert(spark, table, df, ["source_system_code", "object_name"])
