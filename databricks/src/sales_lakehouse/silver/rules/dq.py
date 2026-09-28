"""Row-level data-quality tagging shared by the rules modules.

Documented legacy quirks are not rejects (CONVENTIONS.md): the row is kept,
``dq_status_code`` is escalated (PASS < WARN < FAIL) and the reason code is
appended to ``dq_reason_codes`` so the caller can decide whether to
``quarantine`` FAIL rows.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DQ_STATUS_COL = "dq_status_code"
DQ_REASON_COL = "dq_reason_codes"

_STATUS_RANK: dict[str, int] = {"PASS": 0, "WARN": 1, "FAIL": 2}


def _rank(status: Column) -> Column:
    return (
        F.when(status == "FAIL", F.lit(2))
        .when(status == "WARN", F.lit(1))
        .otherwise(F.lit(0))
    )


def tagDq(df: DataFrame, condition: Column, status: str, reasonCode: str) -> DataFrame:
    """Escalate ``dq_status_code`` to ``status`` where ``condition`` holds.

    Never lowers an existing status and never drops a row. Creates the two DQ
    columns when the frame does not carry them yet.
    """
    if status not in _STATUS_RANK:
        raise ValueError(f"unknown dq status {status!r}")
    current = F.col(DQ_STATUS_COL) if DQ_STATUS_COL in df.columns else F.lit("PASS")
    current = F.coalesce(current, F.lit("PASS"))
    reasons = F.col(DQ_REASON_COL) if DQ_REASON_COL in df.columns else F.lit(None).cast("string")
    hit = F.coalesce(condition, F.lit(False))
    newStatus = F.when(hit & (_rank(current) < F.lit(_STATUS_RANK[status])), F.lit(status)).otherwise(current)
    newReasons = F.when(hit, F.concat_ws(",", reasons, F.lit(reasonCode))).otherwise(reasons)
    return df.withColumn(DQ_STATUS_COL, newStatus).withColumn(DQ_REASON_COL, newReasons)
