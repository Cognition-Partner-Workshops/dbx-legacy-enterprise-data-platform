"""Delta table names, write helpers and the group's own watermark control table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

BRONZE_LOYALTY_LEDGER = "bronze_oltp_loyalty_ledger"
BRONZE_WEB_SESSION = "bronze_oltp_web_session"
SILVER_LOYALTY_LEDGER = "silver_loyalty_ledger"
SILVER_LOYALTY_CUSTOMER_BALANCE = "silver_loyalty_customer_balance"
SILVER_LOYALTY_LEDGER_REJECTS = "silver_loyalty_ledger_rejects"
SILVER_WEB_SESSION = "silver_web_session"
SILVER_WEB_SESSION_REJECTS = "silver_web_session_rejects"
GOLD_FACT_LOYALTY_POINTS = "gold_fact_loyalty_points"
GOLD_FACT_LOYALTY_POINTS_REJECTS = "gold_fact_loyalty_points_rejects"
GOLD_FACT_WEB_SESSION = "gold_fact_web_session"
GOLD_FACT_WEB_SESSION_REJECTS = "gold_fact_web_session_rejects"
WORK_CUSTOMER_ADDRESS_STANDARDISED = "work_customer_address_standardised"
WORK_CUSTOMER_IDENTITY_GRAPH = "work_customer_identity_graph"
GOLD_C360_CUSTOMER_PROFILE = "gold_c360_customer_profile"
WORK_LOYALTY_POINT_LEDGER = "work_loyalty_point_ledger"
GOLD_C360_LOYALTY_OVERLAY = "gold_c360_loyalty_overlay"
GOLD_C360_CUSTOMER_ROLLING_METRIC = "gold_c360_customer_rolling_metric"
GOLD_C360_CUSTOMER_CHURN_FLAG = "gold_c360_customer_churn_flag"
WORK_CUSTOMER_OUTREACH_QUEUE = "work_customer_outreach_queue"
WORK_CUSTOMER_SEGMENT_PREVIOUS = "work_customer_segment_previous"
GOLD_C360_CUSTOMER_SEGMENT = "gold_c360_customer_segment"
GOLD_AGG_CUSTOMER_360 = "gold_agg_customer_360"
GOLD_AGG_CUSTOMER_360_REJECTS = "gold_agg_customer_360_rejects"
GOLD_AGG_CUSTOMER_ROLLING_12_MONTH = "gold_agg_customer_rolling_12_month"
ETL_WATERMARK = "etl_watermark"
ETL_PACKAGE_RUN = "etl_package_run"


def utcNow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def tableExists(spark: SparkSession, fqn: str) -> bool:
    return spark.catalog.tableExists(fqn)


def readTableOrNone(spark: SparkSession, fqn: str) -> Optional[DataFrame]:
    return spark.table(fqn) if tableExists(spark, fqn) else None


def overwriteTable(df: DataFrame, fqn: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)


def appendTable(df: DataFrame, fqn: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn)


def appendInsertOnly(spark: SparkSession, df: DataFrame, fqn: str, keyCols: Sequence[str]) -> int:
    """Insert-only natural-key semantics: rows whose key already exists in the target are skipped."""
    existing = readTableOrNone(spark, fqn)
    if existing is None:
        overwriteTable(df, fqn)
        return spark.table(fqn).count()
    newRows = df.join(existing.select(*keyCols).distinct(), on=list(keyCols), how="left_anti")
    newRows = newRows.select(*[F.col(c) for c in df.columns])
    inserted = newRows.count()
    if inserted:
        appendTable(newRows, fqn)
    return inserted


def deleteWhere(spark: SparkSession, fqn: str, predicate: str) -> None:
    if tableExists(spark, fqn):
        spark.sql(f"DELETE FROM {fqn} WHERE {predicate}")


def ensureWatermarkTable(spark: SparkSession, fqn: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fqn} ("
        "object_name STRING, watermark_type STRING, last_value STRING, previous_value STRING, "
        "last_loaded_at_utc TIMESTAMP, run_id STRING) USING DELTA"
    )


def getWatermark(spark: SparkSession, fqn: str, objectName: str, default: str) -> str:
    ensureWatermarkTable(spark, fqn)
    row = spark.table(fqn).where(F.col("object_name") == objectName).select("last_value").first()
    return row["last_value"] if row and row["last_value"] is not None else default


def setWatermark(spark: SparkSession, fqn: str, objectName: str, watermarkType: str, newValue: str, runId: str) -> None:
    ensureWatermarkTable(spark, fqn)
    previous = getWatermark(spark, fqn, objectName, "")
    spark.sql(f"DELETE FROM {fqn} WHERE object_name = '{objectName}'")
    spark.createDataFrame(
        [(objectName, watermarkType, newValue, previous or None, utcNow(), runId)],
        "object_name STRING, watermark_type STRING, last_value STRING, previous_value STRING, last_loaded_at_utc TIMESTAMP, run_id STRING",
    ).write.format("delta").mode("append").saveAsTable(fqn)


def logPackageRun(spark: SparkSession, fqn: str, packageName: str, runId: str, rowsInserted: int, rowsRejected: int, detail: str) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fqn} (package_name STRING, run_id STRING, finished_at_utc TIMESTAMP, rows_inserted BIGINT, rows_rejected BIGINT, detail STRING) USING DELTA"
    )
    spark.createDataFrame(
        [(packageName, runId, utcNow(), int(rowsInserted), int(rowsRejected), detail)],
        "package_name STRING, run_id STRING, finished_at_utc TIMESTAMP, rows_inserted BIGINT, rows_rejected BIGINT, detail STRING",
    ).write.format("delta").mode("append").saveAsTable(fqn)
