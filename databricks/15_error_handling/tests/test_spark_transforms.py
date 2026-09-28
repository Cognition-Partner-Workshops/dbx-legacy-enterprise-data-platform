from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import types as T

from err_handling import reconcile, rejects


def test_reconciliation_status_python_reference():
    assert reconcile.reconciliationStatus(100, 100, 0, 0, None) == "MATCHED"
    assert reconcile.reconciliationStatus(100, 90, 10, 0, None) == "MATCHED"
    assert reconcile.reconciliationStatus(100, 80, 10, 0, "DEDUP") == "EXPLAINED"
    assert reconcile.reconciliationStatus(100, 98, 0, 5, None) == "TOLERATED"
    assert reconcile.reconciliationStatus(100, 50, 0, 5, None) == "FAILED"
    assert reconcile.reconciliationStatus(0, 5, 0, 0, None) == "TOLERATED"  # staging 0 -> percent 0 <= tolerance
    assert reconcile.differencePercent(100, 98, 0) == Decimal("2.00")


def test_evaluate_dataframe_matches_python_reference(spark):
    schema = T.StructType([
        T.StructField("BatchId", T.LongType()), T.StructField("ObjectName", T.StringType()),
        T.StructField("SourceRowCount", T.LongType()), T.StructField("StagingRowCount", T.LongType()),
        T.StructField("TargetRowCount", T.LongType()), T.StructField("RejectedRowCount", T.LongType()),
        T.StructField("TolerancePercent", T.DecimalType(9, 4)), T.StructField("ExplanationCode", T.StringType()),
    ])
    rows = [
        (1, "stg.Customer", 100, 100, 100, 0, Decimal("0"), None),
        (1, "stg.Supplier", 100, 100, 90, 10, Decimal("0"), None),
        (1, "stg.Product", 100, 100, 80, 10, Decimal("0"), "DEDUP"),
        (1, "stg.Order", 100, 100, 98, 0, Decimal("5"), None),
        (1, "stg.Invoice", 100, 100, 50, 0, Decimal("5"), None),
        (1, "stg.Empty", 0, 0, 5, 0, Decimal("0"), None),
    ]
    out = reconcile.evaluateDataFrame(spark.createDataFrame(rows, schema))
    result = {r["ObjectName"]: r for r in out.collect()}
    assert result["stg.Customer"]["ReconciliationStatus"] == "MATCHED"
    assert result["stg.Supplier"]["ReconciliationStatus"] == "MATCHED"
    assert result["stg.Product"]["ReconciliationStatus"] == "EXPLAINED"
    assert result["stg.Order"]["ReconciliationStatus"] == "TOLERATED"
    assert result["stg.Order"]["DifferencePercent"] == Decimal("2.00")
    assert result["stg.Order"]["DifferenceRowCount"] == -2
    assert result["stg.Order"]["ExpectedTargetRowCount"] == 100
    assert result["stg.Invoice"]["ReconciliationStatus"] == "FAILED"
    assert result["stg.Empty"]["ReconciliationStatus"] == "TOLERATED"
    for r in result.values():
        expected = reconcile.reconciliationStatus(r["StagingRowCount"], r["TargetRowCount"], r["RejectedRowCount"], r["TolerancePercent"], r["ExplanationCode"])
        assert r["ReconciliationStatus"] == expected


def test_classify_rejects_splits_escalations(spark):
    rows = [
        Row(RejectedRecordId=1, BatchId=1, ObjectName="stg.Customer", RejectStage="Stage", RejectReasonCode="MISSING_NAME",
            RejectReasonDescription="x", SourceKey="C1", RejectedAtUtc=datetime(2026, 1, 1), AgeDays=2, RoutedInBatchId=9),
        Row(RejectedRecordId=2, BatchId=1, ObjectName="stg.Customer", RejectStage="Stage", RejectReasonCode="BAD_COUNTRY",
            RejectReasonDescription="y", SourceKey=None, RejectedAtUtc=datetime(2026, 1, 1), AgeDays=6, RoutedInBatchId=9),
        Row(RejectedRecordId=3, BatchId=1, ObjectName="stg.Supplier", RejectStage="Stage", RejectReasonCode="DUP_TAXNUM",
            RejectReasonDescription="z", SourceKey="S1", RejectedAtUtc=datetime(2026, 1, 1), AgeDays=5, RoutedInBatchId=9),
    ]
    out = rejects.classifyRejects(spark.createDataFrame(rows), 5, "rejects_9_20260101.csv")
    result = {r["RejectedRecordId"]: r for r in out.collect()}
    assert result[1]["IsEscalated"] is False and result[1]["RoutingDestination"] == "REPROCESS"
    assert result[2]["IsEscalated"] is True and result[2]["RoutingDestination"] == "QUARANTINE"
    assert result[3]["IsEscalated"] is False  # AgeDays == threshold is not escalated (strict >)
    assert result[1]["RejectFileLine"] == "stg.Customer|MISSING_NAME|C1"
    assert result[2]["RejectFileLine"] == "stg.Customer|BAD_COUNTRY|"
    assert result[1]["RejectFileName"] == "rejects_9_20260101.csv"
