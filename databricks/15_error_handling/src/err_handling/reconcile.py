"""ERR_Reconcile_RowCounts: tolerance evaluation exactly as the legacy derived columns."""

from decimal import Decimal

MATCHED = "MATCHED"
EXPLAINED = "EXPLAINED"
TOLERATED = "TOLERATED"
FAILED = "FAILED"


def differencePercent(stagingRowCount, targetRowCount, rejectedRowCount):
    staging = int(stagingRowCount or 0)
    if staging == 0:
        return Decimal("0.00")
    diff = abs(int(targetRowCount or 0) - (staging - int(rejectedRowCount or 0)))
    return (Decimal(diff) * 100 / Decimal(staging)).quantize(Decimal("0.01"))


def reconciliationStatus(stagingRowCount, targetRowCount, rejectedRowCount, tolerancePercent, explanationCode):
    """Legacy Evaluate Tolerance expression: MATCHED > EXPLAINED > TOLERATED > FAILED."""
    staging = int(stagingRowCount or 0)
    difference = int(targetRowCount or 0) - (staging - int(rejectedRowCount or 0))
    if difference == 0:
        return MATCHED
    if explanationCode is not None and str(explanationCode) != "":
        return EXPLAINED
    if differencePercent(staging, targetRowCount, rejectedRowCount) <= Decimal(str(tolerancePercent or 0)):
        return TOLERATED
    return FAILED


def evaluateDataFrame(reconSet):
    """Legacy Derive Differences + Evaluate Tolerance derived columns over the reconciliation set."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    expected = F.col("StagingRowCount") - F.col("RejectedRowCount")
    difference = F.col("TargetRowCount") - expected
    percent = F.when(F.col("StagingRowCount") == 0, F.lit(0)).otherwise(
        F.abs(difference) * 100 / F.col("StagingRowCount")).cast(T.DecimalType(18, 2))
    status = (
        F.when(F.abs(difference) == 0, F.lit(MATCHED))
        .when(F.col("ExplanationCode").isNotNull() & (F.col("ExplanationCode") != ""), F.lit(EXPLAINED))
        .when(percent <= F.col("TolerancePercent"), F.lit(TOLERATED))
        .otherwise(F.lit(FAILED))
    )
    return (
        reconSet
        .withColumn("ExpectedTargetRowCount", expected)
        .withColumn("DifferenceRowCount", difference)
        .withColumn("DifferencePercent", percent)
        .withColumn("ReconciliationStatus", status)
        .withColumn("EvaluatedAtUtc", F.current_timestamp())
    )
