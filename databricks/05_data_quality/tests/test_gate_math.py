from decimal import Decimal

from pyspark.sql import Row

from dq_quality import gate_math, gates


def test_batch_reject_rate():
    totals = [{"SourceRowCount": 900, "RejectRowCount": 9}, {"SourceRowCount": 100, "RejectRowCount": 1}]
    rejected, source, pct = gate_math.batchRejectRate(totals)
    assert (rejected, source, pct) == (10, 1000, Decimal("1.0000"))
    assert gate_math.batchRejectRate([])[2] == Decimal("0")


def test_quality_score_matches_legacy_scorecard():
    assert gate_math.qualityScore([]) == Decimal("100")
    assert gate_math.qualityScore([(1, 0), (0, 0), (5, 5), (6, 5)]) == Decimal("50.0000")


def test_gate_decision_defaults():
    d = gate_math.gateDecision(Decimal("2.5"), 0, Decimal("95"))
    assert d == {"warnRejectRate": True, "failRejectRate": False, "failReconciliation": False, "failQualityScore": False}
    d = gate_math.gateDecision(Decimal("5.1"), 1, Decimal("89"))
    assert d["failRejectRate"] and d["failReconciliation"] and d["failQualityScore"]


def test_gate_decision_configured_thresholds():
    d = gate_math.gateDecision(Decimal("1.5"), 0, Decimal("95"), failRejectPercent=Decimal("1"))
    assert d["failRejectRate"] is True


def test_compute_control_totals_recomputes_variance(spark):
    df = spark.createDataFrame([
        Row(ObjectName="stg.Customer", SourceRowCount=100, TargetRowCount=95, RejectRowCount=5),
        Row(ObjectName="stg.Customer", SourceRowCount=10, TargetRowCount=10, RejectRowCount=0),
        Row(ObjectName="stg.Order", SourceRowCount=50, TargetRowCount=40, RejectRowCount=5),
    ])
    totals = {r["ObjectName"]: r.asDict() for r in gate_math.computeControlTotals(df).collect()}
    assert totals["stg.Customer"]["VarianceRowCount"] == 0
    assert totals["stg.Customer"]["ReconciliationStatusCode"] == "BALANCED"
    assert totals["stg.Order"]["VarianceRowCount"] == 5
    assert totals["stg.Order"]["ReconciliationStatusCode"] == "BREACH"
    assert totals["stg.Order"]["RejectReasonCode"] == gate_math.RECON_BREACH_REASON


def test_apply_gates_logs_then_raises():
    logged = []

    def logError(spark, catalog, **kwargs):
        logged.append(kwargs)

    gateList = [
        gates.Gate("Warn", "warned", "Warning", lambda m: m["MeasuredValue"] > 2, "@[User::MeasuredValue] > 2"),
        gates.Gate("Fail", "failed", "Failure", lambda m: m["FailedRuleCount"] > 0, "@[User::FailedRuleCount] > 0"),
    ]
    warnings = gates.applyGates(None, "c", gateList, {"MeasuredValue": 3, "FailedRuleCount": 0}, 1, 2, "PKG", logError)
    assert [g.name for g in warnings] == ["Warn"]
    assert logged[0]["errorSeverity"] == "Warning"
    try:
        gates.applyGates(None, "c", gateList, {"MeasuredValue": 3, "FailedRuleCount": 1}, 1, 2, "PKG", logError)
    except gates.DataQualityGateError as exc:
        assert "failed" in str(exc)
    else:
        raise AssertionError("failure gate did not raise")
    assert [l["errorSeverity"] for l in logged] == ["Warning", "Warning", "Error"]
