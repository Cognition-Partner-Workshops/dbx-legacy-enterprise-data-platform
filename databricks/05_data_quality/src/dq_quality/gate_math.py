"""DQ_Threshold_Gate arithmetic (``DFT Reconcile Control Totals``) and the batch reject rate.

Kept free of Spark actions so the percentages, status routing and quality score can be
unit tested; the notebook feeds it the ``etl.row_count_log`` rows of the batch.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Iterable, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RECON_BREACH_REASON = "DQ_RECON_BREACH"
QUALITY_SCORE_PASS = Decimal("90")
DEFAULT_WARN_REJECT_PERCENT = Decimal("2")
DEFAULT_FAIL_REJECT_PERCENT = Decimal("5")


def percent(numerator, denominator) -> Optional[Decimal]:
    """``denominator = 0 ? 0 : 100 * numerator / denominator`` with 4-decimal rounding."""
    if denominator is None or numerator is None:
        return None
    denominator = Decimal(str(denominator))
    if denominator == 0:
        return Decimal("0")
    return (Decimal("100") * Decimal(str(numerator)) / denominator).quantize(Decimal("0.0001"))


def computeControlTotals(rowCountDf: DataFrame) -> DataFrame:
    """Aggregate ``etl.row_count_log`` rows per object, then derive percentages and status.

    Expects the PascalCase columns of etl.RowCountAudit / etl.row_count_log:
    ObjectName, SourceRowCount, TargetRowCount, RejectRowCount (VarianceRowCount is
    recomputed as Source - Target - Reject when absent).
    """
    cols = rowCountDf.columns
    variance = (F.col("VarianceRowCount") if "VarianceRowCount" in cols
                else F.coalesce(F.col("SourceRowCount"), F.lit(0)) - F.coalesce(F.col("TargetRowCount"), F.lit(0))
                - F.coalesce(F.col("RejectRowCount"), F.lit(0)))
    totals = (rowCountDf.withColumn("_Variance", variance)
              .groupBy("ObjectName")
              .agg(F.sum(F.coalesce(F.col("SourceRowCount"), F.lit(0))).alias("SourceRowCount"),
                   F.sum(F.coalesce(F.col("TargetRowCount"), F.lit(0))).alias("TargetRowCount"),
                   F.sum(F.coalesce(F.col("RejectRowCount"), F.lit(0))).alias("RejectRowCount"),
                   F.sum(F.coalesce(F.col("_Variance"), F.lit(0))).alias("VarianceRowCount")))
    rejectPct = (F.when(F.col("SourceRowCount") == 0, F.lit(0))
                 .otherwise(F.col("RejectRowCount") * 100.0 / F.col("SourceRowCount")))
    variancePct = (F.when(F.col("SourceRowCount") == 0, F.lit(0))
                   .otherwise(F.abs(F.col("VarianceRowCount")) * 100.0 / F.col("SourceRowCount")))
    derived = (totals.withColumn("RejectPercent", rejectPct.cast("decimal(9,4)"))
               .withColumn("VariancePercent", variancePct.cast("decimal(9,4)")))
    status = (F.when(F.col("VarianceRowCount") == 0, F.lit("BALANCED"))
              .when(F.col("SourceRowCount") == 0, F.lit("EMPTY"))
              .otherwise(F.lit("BREACH")))
    return (derived.withColumn("ReconciliationStatusCode", status)
            .withColumn("RejectReasonCode", F.when(F.col("ReconciliationStatusCode") == "BREACH",
                                                   F.lit(RECON_BREACH_REASON))))


def batchRejectRate(totals: Iterable[dict]) -> tuple[int, int, Decimal]:
    """``Measure Batch Reject Rate``: ``(rejectedRows, sourceRows, rejectPercent)``."""
    rejected = sum(int(t.get("RejectRowCount") or 0) for t in totals)
    source = sum(int(t.get("SourceRowCount") or 0) for t in totals)
    return rejected, source, percent(rejected, source) or Decimal("0")


def qualityScore(outcomes: Iterable[tuple[Optional[float], Optional[float]]]) -> Decimal:
    """``Compute Quality Score``: 100 - average(rule breached ? 100 : 0) over evaluated results.

    ``outcomes`` yields ``(MeasuredValue, ThresholdValue)``; when no rule has been evaluated
    the legacy query returns 100.
    """
    evaluated = [(m, t) for m, t in outcomes if m is not None]
    if not evaluated:
        return Decimal("100")
    breaches = sum(1 for m, t in evaluated if Decimal(str(m)) > Decimal(str(t if t is not None else 0)))
    return (Decimal("100") - Decimal("100") * Decimal(breaches) / Decimal(len(evaluated))).quantize(Decimal("0.01"))


def gateDecision(rejectPercent: Decimal, failedObjectCount: int, score: Decimal,
                 warnRejectPercent: Decimal = DEFAULT_WARN_REJECT_PERCENT,
                 failRejectPercent: Decimal = DEFAULT_FAIL_REJECT_PERCENT,
                 minQualityScore: Decimal = QUALITY_SCORE_PASS) -> dict:
    """Legacy gates: warn at ``MeasuredValue > 2``, fail at ``> 5`` / ``FailedObjectCount > 0`` / ``QualityScore < 90``."""
    return {
        "warnRejectRate": rejectPercent > warnRejectPercent,
        "failRejectRate": rejectPercent > failRejectPercent,
        "failReconciliation": failedObjectCount > 0,
        "failQualityScore": score < minQualityScore,
    }
