"""Quarantine helper - the lakehouse equivalent of the legacy err.* tables.

Bad rows are written to ``sales_quality.rejected_rows`` with the rule that
rejected them; the caller gets back only the passing rows. Nothing is silently
dropped. The integration session extends this with the DQ rule catalogue.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from sales_lakehouse.common.config import PipelineConfig

REJECTED_ROWS_TABLE = "rejected_rows"


def quarantine(
    spark: SparkSession,
    cfg: PipelineConfig,
    df: DataFrame,
    ruleCode: str,
    sourceTable: str,
    failCondition: Column,
    reasonText: str,
) -> DataFrame:
    """Split ``df`` on ``failCondition``; persist failures, return survivors."""
    failed = df.filter(failCondition)
    passed = df.filter(~failCondition | failCondition.isNull())
    rejected = failed.select(
        F.lit(ruleCode).alias("rule_code"),
        F.lit(sourceTable).alias("source_table"),
        F.lit(reasonText).alias("reason_text"),
        F.to_json(F.struct(*[F.col(c) for c in failed.columns])).alias("row_json"),
        F.lit(cfg.batchId).cast("bigint").alias("batch_id"),
        F.current_timestamp().alias("rejected_at_utc"),
    )
    fqn = cfg.fqn("quality", REJECTED_ROWS_TABLE)
    rejected.write.format("delta").mode("append").saveAsTable(fqn)
    return passed
