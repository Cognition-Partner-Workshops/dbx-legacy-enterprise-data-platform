from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from platform_control import rules


def testClassifyFailureRetryableCodes():
    for code in (1205, 1222, 10054, 12154, 64):
        category, retryable = rules.classifyFailure(code, "x")
        assert retryable and category == "TRANSIENT"
    category, retryable = rules.classifyFailure(547, "FK violation")
    assert not retryable


def testRetryDecisionMatchesLegacyExpressions():
    assert rules.retryDecision(attemptNumber=1, maxRetryAttempts=3, retryableStepCount=2) == "sweep"
    assert rules.retryDecision(attemptNumber=4, maxRetryAttempts=3, retryableStepCount=2) == "exhausted"
    assert rules.retryDecision(attemptNumber=1, maxRetryAttempts=3, retryableStepCount=0) == "exhausted"


def testReconcileRowCountsStatuses():
    r = rules.reconcileRowCounts(1000, 990, 10, 0, None)
    assert (r.expectedTargetRowCount, r.differenceRowCount, r.reconciliationStatus) == (990, 0, "MATCHED")
    assert rules.reconcileRowCounts(1000, 985, 10, 1, None).reconciliationStatus == "TOLERATED"
    assert rules.reconcileRowCounts(1000, 900, 10, 0, "LATE_ARRIVING").reconciliationStatus == "EXPLAINED"
    failed = rules.reconcileRowCounts(1000, 900, 10, 0, None)
    assert failed.reconciliationStatus == "FAILED" and failed.differencePercent == Decimal("9.00")


def testDqResultStatus():
    assert rules.dqResultStatus(Decimal("0"), Decimal("1"), "Error") == "Passed"
    assert rules.dqResultStatus(Decimal("5"), Decimal("1"), "FAIL") == "Failed"
    assert rules.dqResultStatus(Decimal("5"), Decimal("1"), "WARN") == "Warned"
    assert rules.dqResultStatus(Decimal("5"), Decimal("1"), "FAIL", hasException=True) == "Warned"
    assert rules.dqResultStatus(None, Decimal("1"), "FAIL") == "NotEvaluated"


def testSsisExpressionEvaluation():
    variables = {"ExtractAttempt": 2, "NightlyBatchRunning": 0}
    parameters = {"MaxExtractAttempts": "3", "SkipWhenNightlyRunning": "True"}
    assert rules.evaluateSsisExpression("@[User::ExtractAttempt] <= @[$Package::MaxExtractAttempts]", variables, parameters)
    assert rules.evaluateSsisExpression(
        "@[User::NightlyBatchRunning] == 0 || !@[$Package::SkipWhenNightlyRunning]", variables, parameters
    )
    assert not rules.evaluateSsisExpression(
        "@[User::NightlyBatchRunning] > 0 && @[$Package::SkipWhenNightlyRunning]", variables, parameters
    )
    assert rules.evaluateSsisExpression('@[$Package::RestartFromStep] == "" || @[$Package::RestartFromStep] == "File Screen"', {}, {"RestartFromStep": ""})
    name, value = rules.evaluateSsisAssignment("@[User::ExtractAttempt] = @[User::ExtractAttempt] + 1", variables, parameters)
    assert (name, value) == ("ExtractAttempt", 3)
    with pytest.raises(ValueError):
        rules.evaluateSsisExpression("__import__('os').system('x')", {}, {})


def testTranslateRuleExpressionIsSafe():
    translated = rules.translateRuleExpression(
        "NOT EXISTS (SELECT 1 FROM ref.Currency c WHERE c.CurrencyCode = t.CurrencyCode)",
        "stg.Sale",
        lambda schemaName, tableName: f"legacy.{schemaName}.{tableName}",
    )
    assert "legacy.ref.Currency" in translated
    with pytest.raises(ValueError):
        rules.translateRuleExpression("1=1; DROP TABLE x", "stg.Sale", lambda a, b: f"{a}.{b}")


def testScreenFileRow():
    good = rules.screenFileRow("a|b|c|d|e|f|g|h|i", "2026-09-28", "12.50")
    assert good.wellFormed
    assert not rules.screenFileRow("a|b|c", "2026-09-28", "1").wellFormed
    assert rules.screenFileRow("a|b|c|d|e|f|g|h|i", "2026-09-2X", "1").rejectReasonCode == "DQ_FILE_DATE"
    assert rules.screenFileRow("a|b|c|d|e|f|g|h|i", "2026-09-28", "abc").rejectReasonCode == "DQ_FILE_AMOUNT"
    assert rules.screenFileRow("a|b|c|d|e|f|g|h|i", "28/09/2026", "1,299.00").wellFormed


def testWatermarkWindowRespectsLookbackAndFloor():
    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    floor = now - timedelta(days=7)
    start, end = rules.computeWatermarkWindow(now - timedelta(hours=1), 15, now, floor)
    assert start == now - timedelta(hours=1, minutes=15) and end == now
    start, _ = rules.computeWatermarkWindow(None, 15, now, floor)
    assert start == floor
    start, _ = rules.computeWatermarkWindow(now - timedelta(days=30), 15, now, floor)
    assert start == floor


def testLateArrivingRejectReprocessDecision():
    now = datetime(2026, 9, 30, tzinfo=timezone.utc)
    assert rules.rejectReprocessDecision(0, now - timedelta(days=1), now, resolves=True) == "REPLAY"
    assert rules.rejectReprocessDecision(0, now - timedelta(days=1), now, resolves=False) == "UNRESOLVED"
    assert rules.rejectReprocessDecision(5, now - timedelta(days=1), now, resolves=False) == "EXHAUSTED"
    assert rules.rejectReprocessDecision(0, now - timedelta(days=60), now, resolves=False) == "AGED_OUT"


def testDeriveBatchStatus():
    assert rules.deriveBatchStatus(0, 0, 0) == "Succeeded"
    assert rules.deriveBatchStatus(0, 0, 3) == "SucceededWithWarnings"
    assert rules.deriveBatchStatus(1, 0, 0) == "Failed"
    assert rules.deriveBatchStatus(0, 0, 0, "Failed") == "Failed"
